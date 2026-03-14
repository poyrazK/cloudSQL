/**
 * @file query_executor.cpp
 * @brief High-level query executor implementation
 */

#include "executor/query_executor.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.hpp"
#include "common/cluster_manager.hpp"
#include "common/value.hpp"
#include "distributed/raft_group.hpp"
#include "distributed/raft_manager.hpp"
#include "distributed/shard_manager.hpp"
#include "executor/operator.hpp"

#include "executor/types.hpp"
#include "network/rpc_message.hpp"
#include "parser/expression.hpp"
#include "parser/statement.hpp"
#include "parser/token.hpp"
#include "recovery/log_manager.hpp"
#include "recovery/log_record.hpp"
#include "storage/btree_index.hpp"
#include "storage/buffer_pool_manager.hpp"
#include "storage/heap_table.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace cloudsql::executor {

namespace {
enum class IndexOp { Insert, Remove };

/**
 * @brief Helper to perform index writes and check for success
 */
bool apply_index_write(storage::BTreeIndex& index, const common::Value& key,
                       const storage::HeapTable::TupleId& rid, IndexOp op, std::string& error_msg) {
    bool success = false;
    if (op == IndexOp::Insert) {
        success = index.insert(key, rid);
    } else {
        success = index.remove(key, rid);
    }

    if (!success) {
        error_msg = "Index operation failed for key: " + key.to_string();
        return false;
    }
    return true;
}
}  // namespace

void ShardStateMachine::apply(const raft::LogEntry& entry) {
    if (entry.data.empty()) return;

    // Binary format for Shard DML:
    // [Type:1] (1:Insert, 2:Delete, 3:Update)
    // [TableLen:4][TableName]
    // [Payload...]
    uint8_t type = entry.data[0];
    size_t offset = 1;

    uint32_t table_len = 0;
    if (offset + 4 > entry.data.size()) return;
    std::memcpy(&table_len, entry.data.data() + offset, 4);
    offset += 4;

    if (offset + table_len > entry.data.size()) return;
    std::string table_name(reinterpret_cast<const char*>(entry.data.data() + offset), table_len);
    offset += table_len;

    auto table_meta_opt = catalog_.get_table_by_name(table_name);
    if (!table_meta_opt.has_value()) return;
    const auto* table_meta = table_meta_opt.value();

    Schema schema;
    for (const auto& col : table_meta->columns) {
        schema.add_column(col.name, col.type);
    }
    storage::HeapTable table(table_name, bpm_, schema);

    if (type == 1) {  // INSERT
        Tuple tuple =
            network::Serializer::deserialize_tuple(entry.data.data(), offset, entry.data.size());
        table.insert(tuple, 0);
    } else if (type == 2) {  // DELETE
        storage::HeapTable::TupleId rid;
        if (offset + 8 > entry.data.size()) return;
        std::memcpy(&rid.page_num, entry.data.data() + offset, 4);
        std::memcpy(&rid.slot_num, entry.data.data() + offset + 4, 4);
        table.remove(rid, 0);
    }
}

QueryExecutor::QueryExecutor(Catalog& catalog, storage::BufferPoolManager& bpm,
                             transaction::LockManager& lock_manager,
                             transaction::TransactionManager& transaction_manager,
                             recovery::LogManager* log_manager,
                             cluster::ClusterManager* cluster_manager)
    : catalog_(catalog),
      bpm_(bpm),
      lock_manager_(lock_manager),
      transaction_manager_(transaction_manager),
      log_manager_(log_manager),
      cluster_manager_(cluster_manager) {}

QueryExecutor::~QueryExecutor() {
    if (current_txn_ != nullptr) {
        transaction_manager_.abort(current_txn_);
    }
}

QueryResult QueryExecutor::execute(const parser::Statement& stmt) {
    const auto start = std::chrono::high_resolution_clock::now();
    QueryResult result;

    /* Handle Explicit Transaction Control */
    if (stmt.type() == parser::StmtType::TransactionBegin) {
        return execute_begin();
    }
    if (stmt.type() == parser::StmtType::TransactionCommit) {
        return execute_commit();
    }
    if (stmt.type() == parser::StmtType::TransactionRollback) {
        return execute_rollback();
    }

    /* Auto-commit mode if no current transaction */
    const bool is_auto_commit = (current_txn_ == nullptr);
    transaction::Transaction* txn = current_txn_;

    if (is_auto_commit &&
        (stmt.type() == parser::StmtType::Select || stmt.type() == parser::StmtType::Insert ||
         stmt.type() == parser::StmtType::Update || stmt.type() == parser::StmtType::Delete)) {
        txn = transaction_manager_.begin();
    }

    try {
        if (stmt.type() == parser::StmtType::Select) {
            result = execute_select(dynamic_cast<const parser::SelectStatement&>(stmt), txn);
        } else if (stmt.type() == parser::StmtType::CreateTable) {
            result = execute_create_table(dynamic_cast<const parser::CreateTableStatement&>(stmt));
        } else if (stmt.type() == parser::StmtType::CreateIndex) {
            result = execute_create_index(dynamic_cast<const parser::CreateIndexStatement&>(stmt));
        } else if (stmt.type() == parser::StmtType::DropTable) {
            result = execute_drop_table(dynamic_cast<const parser::DropTableStatement&>(stmt));
        } else if (stmt.type() == parser::StmtType::DropIndex) {
            result = execute_drop_index(dynamic_cast<const parser::DropIndexStatement&>(stmt));
        } else if (stmt.type() == parser::StmtType::Insert) {
            result = execute_insert(dynamic_cast<const parser::InsertStatement&>(stmt), txn);
        } else if (stmt.type() == parser::StmtType::Delete) {
            result = execute_delete(dynamic_cast<const parser::DeleteStatement&>(stmt), txn);
        } else if (stmt.type() == parser::StmtType::Update) {
            result = execute_update(dynamic_cast<const parser::UpdateStatement&>(stmt), txn);
        } else {
            result.set_error("Unsupported statement type");
        }

        /* Auto-commit success */
        if (is_auto_commit && txn != nullptr) {
            transaction_manager_.commit(txn);
        }
    } catch (const std::exception& e) {
        if (is_auto_commit && txn != nullptr) {
            transaction_manager_.abort(txn);
        }
        result.set_error(std::string("Execution error: ") + e.what());
    } catch (...) {
        if (is_auto_commit && txn != nullptr) {
            transaction_manager_.abort(txn);
        }
        result.set_error("Unknown execution error");
    }

    const auto end = std::chrono::high_resolution_clock::now();
    const auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    result.set_execution_time(static_cast<uint64_t>(duration.count()));

    return result;
}

QueryResult QueryExecutor::execute_begin() {
    QueryResult res;
    if (current_txn_ != nullptr) {
        res.set_error("Transaction already in progress");
        return res;
    }
    current_txn_ = transaction_manager_.begin();
    return res;
}

QueryResult QueryExecutor::execute_commit() {
    QueryResult res;
    if (current_txn_ == nullptr) {
        res.set_error("No transaction in progress");
        return res;
    }
    transaction_manager_.commit(current_txn_);
    current_txn_ = nullptr;
    return res;
}

QueryResult QueryExecutor::execute_rollback() {
    QueryResult res;
    if (current_txn_ == nullptr) {
        res.set_error("No transaction in progress");
        return res;
    }
    transaction_manager_.abort(current_txn_);
    current_txn_ = nullptr;
    return res;
}

QueryResult QueryExecutor::execute_select(const parser::SelectStatement& stmt,
                                          transaction::Transaction* txn) {
    QueryResult result;

    /* Build execution plan */
    auto root = build_plan(stmt, txn);
    if (!root) {
        result.set_error("Failed to build execution plan (check table existence and FROM clause)");
        return result;
    }

    /* Initialize and open operators */
    if (!root->init() || !root->open()) {
        result.set_error(root->error().empty() ? "Failed to open execution plan" : root->error());
        return result;
    }

    /* Set result schema */
    result.set_schema(root->output_schema());

    /* Pull tuples (Volcano model) */
    Tuple tuple;
    while (root->next(tuple)) {
        result.add_row(std::move(tuple));
    }

    root->close();
    return result;
}

QueryResult QueryExecutor::execute_create_table(const parser::CreateTableStatement& stmt) {
    QueryResult result;

    /* Convert parser columns to catalog columns */
    std::vector<ColumnInfo> catalog_cols;
    uint16_t pos = 0;
    for (const auto& col : stmt.columns()) {
        common::ValueType type = common::ValueType::TYPE_TEXT;
        if (col.type_ == "INT" || col.type_ == "INTEGER") {
            type = common::ValueType::TYPE_INT32;
        } else if (col.type_ == "BIGINT") {
            type = common::ValueType::TYPE_INT64;
        } else if (col.type_ == "FLOAT" || col.type_ == "DOUBLE") {
            type = common::ValueType::TYPE_FLOAT64;
        } else if (col.type_ == "BOOLEAN" || col.type_ == "BOOL") {
            type = common::ValueType::TYPE_BOOL;
        }

        catalog_cols.emplace_back(col.name_, type, pos++);
    }

    /* Update catalog */
    oid_t table_id = 0;
    if (is_local_only_) {
        table_id = catalog_.create_table_local(stmt.table_name(), std::move(catalog_cols));
    } else {
        table_id = catalog_.create_table(stmt.table_name(), std::move(catalog_cols));
    }

    if (table_id == 0) {
        result.set_error("Failed to create table in catalog");
        return result;
    }

    /* Create physical file */
    auto table_info_opt = catalog_.get_table(table_id);
    if (!table_info_opt.has_value()) {
        result.set_error("Failed to retrieve table info from catalog");
        return result;
    }
    const auto* table_info = table_info_opt.value();
    storage::HeapTable table(table_info->name, bpm_, executor::Schema());
    if (!table.create()) {
        static_cast<void>(catalog_.drop_table(table_id));
        result.set_error("Failed to create table file");
        return result;
    }

    result.set_rows_affected(1);
    return result;
}

QueryResult QueryExecutor::execute_create_index(const parser::CreateIndexStatement& stmt) {
    QueryResult result;

    /* Reject composite indexes */
    if (stmt.columns().size() != 1) {
        result.set_error("Composite indexes not supported");
        return result;
    }

    auto table_meta_opt = catalog_.get_table_by_name(stmt.table_name());
    if (!table_meta_opt.has_value()) {
        result.set_error("Table not found: " + stmt.table_name());
        return result;
    }
    const auto* table_meta = table_meta_opt.value();

    std::vector<uint16_t> col_positions;
    common::ValueType key_type = common::ValueType::TYPE_NULL;

    const auto& col_name = stmt.columns()[0];
    bool found = false;
    for (const auto& col : table_meta->columns) {
        if (col.name == col_name) {
            col_positions.push_back(col.position);
            key_type = col.type;
            found = true;
            break;
        }
    }
    if (!found) {
        result.set_error("Column not found: " + col_name);
        return result;
    }

    /* Update Catalog */
    const oid_t index_id = catalog_.create_index(stmt.index_name(), table_meta->table_id,
                                                 col_positions, IndexType::BTree, stmt.unique());
    if (index_id == 0) {
        result.set_error("Failed to create index in catalog");
        return result;
    }

    /* Create Physical Index File */
    storage::BTreeIndex index(stmt.index_name(), bpm_, key_type);
    if (!index.create()) {
        static_cast<void>(catalog_.drop_index(index_id));
        result.set_error("Failed to create index file");
        return result;
    }

    /* Populate Index with existing data (Backfill) */
    Schema schema;
    for (const auto& col : table_meta->columns) {
        schema.add_column(col.name, col.type);
    }
    storage::HeapTable table(stmt.table_name(), bpm_, schema);
    auto iter = table.scan();
    storage::HeapTable::TupleMeta meta;
    std::string err;
    while (iter.next_meta(meta)) {
        if (meta.xmax == 0) {
            /* Extract key from tuple */
            const common::Value& key = meta.tuple.get(col_positions[0]);
            if (!apply_index_write(index, key, iter.current_id(), IndexOp::Insert, err)) {
                static_cast<void>(index.drop());
                static_cast<void>(catalog_.drop_index(index_id));
                result.set_error(err);
                return result;
            }
        }
    }

    result.set_rows_affected(1);
    return result;
}

QueryResult QueryExecutor::execute_insert(const parser::InsertStatement& stmt,
                                          transaction::Transaction* txn) {
    QueryResult result;

    if (!stmt.table()) {
        result.set_error("Target table not specified");
        return result;
    }

    const std::string table_name = stmt.table()->to_string();
    auto table_meta_opt = catalog_.get_table_by_name(table_name);
    if (!table_meta_opt.has_value()) {
        result.set_error("Table not found: " + table_name);
        return result;
    }
    const auto* table_meta = table_meta_opt.value();

    /* Construct Schema */
    Schema schema;
    for (const auto& col : table_meta->columns) {
        schema.add_column(col.name, col.type);
    }

    storage::HeapTable table(table_name, bpm_, schema);

    uint64_t rows_inserted = 0;
    const uint64_t xmin = (txn != nullptr) ? txn->get_id() : 0;

    for (const auto& row_exprs : stmt.values()) {
        std::vector<common::Value> values;
        values.reserve(row_exprs.size());
        for (const auto& expr : row_exprs) {
            values.push_back(expr->evaluate());
        }

        const Tuple tuple(std::move(values));

        // Distributed Routing: Skip if is_local_only_
        if (!is_local_only_ && cluster_manager_ != nullptr && !table_meta->shards.empty()) {
            uint32_t shard_id = 0;
            if (!tuple.empty()) {
                shard_id = cluster::ShardManager::compute_shard(tuple.get(0), static_cast<uint32_t>(table_meta->shards.size()));
            }
            auto shard_info_opt = cluster::ShardManager::get_target_node(*table_meta, shard_id);
            
            if (shard_info_opt.has_value()) {
                const auto& shard_info = shard_info_opt.value();
                std::cerr << "--- [QueryExecutor] Routing tuple to data node " << shard_info.node_address << " ---" << std::endl;
                network::RpcClient client(shard_info.node_address, shard_info.port);
                if (client.connect()) {
                    network::ExecuteFragmentArgs args;
                    args.context_id = context_id_;
                    // Optimization: Only forward the current row
                    args.sql = "INSERT INTO " + table_name + " VALUES " + tuple.to_string() + ";";
                    
                    std::vector<uint8_t> resp;
                    if (!client.call(network::RpcType::ExecuteFragment, args.serialize(), resp)) {
                        result.set_error("Failed to forward INSERT to data node " + shard_info.node_address);
                        return result;
                    }
                    auto reply = network::QueryResultsReply::deserialize(resp);
                    if (!reply.success) {
                        result.set_error("Remote INSERT failed: " + reply.error_msg);
                        return result;
                    }
                    rows_inserted++;
                    continue;
                }
            }
        }

        const auto tid = table.insert(tuple, xmin);

        /* Update Indexes */
        std::string err;
        for (const auto& idx_info : table_meta->indexes) {
            if (!idx_info.column_positions.empty()) {
                uint16_t pos = idx_info.column_positions[0];
                common::ValueType ktype = table_meta->columns[pos].type;
                storage::BTreeIndex index(idx_info.name, bpm_, ktype);
                if (!apply_index_write(index, tuple.get(pos), tid, IndexOp::Insert, err)) {
                    throw std::runtime_error(err);
                }
            }
        }

        /* Log INSERT */
        if (log_manager_ != nullptr && txn != nullptr) {
            recovery::LogRecord log(txn->get_id(), txn->get_prev_lsn(),
                                    recovery::LogRecordType::INSERT, table_name, tid, tuple);
            const auto lsn = log_manager_->append_log_record(log);
            txn->set_prev_lsn(lsn);
        }

        /* Record undo log and Acquire Exclusive Lock if in transaction */
        if (txn != nullptr) {
            txn->add_undo_log(transaction::UndoLog::Type::INSERT, table_name, tid);
            if (!lock_manager_.acquire_exclusive(
                    txn, std::to_string(tid.page_num) + ":" + std::to_string(tid.slot_num))) {
                throw std::runtime_error("Failed to acquire exclusive lock");
            }
        }

        rows_inserted++;
    }

    result.set_rows_affected(rows_inserted);
    return result;
}

QueryResult QueryExecutor::execute_delete(const parser::DeleteStatement& stmt,
                                          transaction::Transaction* txn) {
    QueryResult result;
    const std::string table_name = stmt.table()->to_string();
    auto table_meta_opt = catalog_.get_table_by_name(table_name);
    if (!table_meta_opt.has_value()) {
        result.set_error("Table not found: " + table_name);
        return result;
    }
    const auto* table_meta = table_meta_opt.value();

    Schema schema;
    for (const auto& col : table_meta->columns) {
        schema.add_column(col.name, col.type);
    }

    storage::HeapTable table(table_name, bpm_, schema);
    const uint64_t xmax = (txn != nullptr) ? txn->get_id() : 0;
    uint64_t rows_deleted = 0;

    /* Phase 1: Collect RIDs to avoid Halloween Problem */
    std::vector<storage::HeapTable::TupleId> target_rids;
    auto iter = table.scan();
    storage::HeapTable::TupleMeta meta;
    while (iter.next_meta(meta)) {
        bool match = true;
        if (stmt.where()) {
            match = stmt.where()->evaluate(&meta.tuple, &schema).as_bool();
        }

        if (match && meta.xmax == 0) {
            target_rids.push_back(iter.current_id());
        }
    }

    /* Phase 2: Apply Deletions */
    for (const auto& rid : target_rids) {
        // POC: Replication Logic
        if (cluster_manager_ != nullptr && cluster_manager_->get_raft_manager() != nullptr) {
            auto shard_group = cluster_manager_->get_raft_manager()->get_group(1);
            if (shard_group && shard_group->is_leader()) {
                std::vector<uint8_t> cmd;
                cmd.push_back(2);  // Type 2: DELETE
                uint32_t tlen = static_cast<uint32_t>(table_name.size());
                size_t off = cmd.size();
                cmd.resize(off + 4 + tlen + 8);
                std::memcpy(cmd.data() + off, &tlen, 4);
                std::memcpy(cmd.data() + off + 4, table_name.data(), tlen);
                std::memcpy(cmd.data() + off + 4 + tlen, &rid.page_num, 4);
                std::memcpy(cmd.data() + off + 4 + tlen + 4, &rid.slot_num, 4);

                if (!shard_group->replicate(cmd)) {
                    result.set_error("Replication failed for shard 1");
                    return result;
                }
            }
        }

        /* Retrieve old tuple for logging and index maintenance (unconditional) */
        Tuple old_tuple;
        if (!table.get(rid, old_tuple)) {
            result.set_error("Failed to retrieve tuple for deletion maintenance: " +
                             rid.to_string());
            return result;
        }

        if (table.remove(rid, xmax)) {
            /* Update Indexes */
            std::string err;
            if (!old_tuple.empty()) {
                for (const auto& idx_info : table_meta->indexes) {
                    if (!idx_info.column_positions.empty()) {
                        uint16_t pos = idx_info.column_positions[0];
                        common::ValueType ktype = table_meta->columns[pos].type;
                        storage::BTreeIndex index(idx_info.name, bpm_, ktype);
                        if (!apply_index_write(index, old_tuple.get(pos), rid, IndexOp::Remove,
                                               err)) {
                            throw std::runtime_error(err);
                        }
                    }
                }
            }

            /* Log DELETE */
            if (log_manager_ != nullptr && txn != nullptr) {
                recovery::LogRecord log(txn->get_id(), txn->get_prev_lsn(),
                                        recovery::LogRecordType::MARK_DELETE, table_name, rid,
                                        old_tuple);
                const auto lsn = log_manager_->append_log_record(log);
                txn->set_prev_lsn(lsn);
            }

            if (txn != nullptr) {
                txn->add_undo_log(transaction::UndoLog::Type::DELETE, table_name, rid);
            }
            rows_deleted++;
        }
    }

    result.set_rows_affected(rows_deleted);
    return result;
}

QueryResult QueryExecutor::execute_update(const parser::UpdateStatement& stmt,
                                          transaction::Transaction* txn) {
    QueryResult result;
    const std::string table_name = stmt.table()->to_string();
    auto table_meta_opt = catalog_.get_table_by_name(table_name);
    if (!table_meta_opt.has_value()) {
        result.set_error("Table not found: " + table_name);
        return result;
    }
    const auto* table_meta = table_meta_opt.value();

    Schema schema;
    for (const auto& col : table_meta->columns) {
        schema.add_column(col.name, col.type);
    }

    storage::HeapTable table(table_name, bpm_, schema);
    const uint64_t txn_id = (txn != nullptr) ? txn->get_id() : 0;
    uint64_t rows_updated = 0;

    /* Phase 1: Collect RIDs and compute new values to avoid Halloween Problem */
    struct UpdateOp {
        storage::HeapTable::TupleId rid;
        Tuple old_tuple;
        Tuple new_tuple;
    };
    std::vector<UpdateOp> updates;

    auto iter = table.scan();
    storage::HeapTable::TupleMeta meta;
    while (iter.next_meta(meta)) {
        bool match = true;
        if (stmt.where()) {
            match = stmt.where()->evaluate(&meta.tuple, &schema).as_bool();
        }

        if (match && meta.xmax == 0) {
            /* Compute new tuple values */
            Tuple new_tuple = meta.tuple;
            for (const auto& [col_expr, val_expr] : stmt.set_clauses()) {
                const std::string col_name = col_expr->to_string();
                const size_t idx = schema.find_column(col_name);
                if (idx != static_cast<size_t>(-1)) {
                    new_tuple.set(idx, val_expr->evaluate(&meta.tuple, &schema));
                }
            }
            updates.push_back({iter.current_id(), meta.tuple, std::move(new_tuple)});
        }
    }

    /* Phase 2: Apply Updates */
    for (const auto& op : updates) {
        if (table.remove(op.rid, txn_id)) {
            /* Update Indexes - Remove old, Insert new */
            std::string err;
            for (const auto& idx_info : table_meta->indexes) {
                if (!idx_info.column_positions.empty()) {
                    uint16_t pos = idx_info.column_positions[0];
                    common::ValueType ktype = table_meta->columns[pos].type;
                    storage::BTreeIndex index(idx_info.name, bpm_, ktype);
                    if (!apply_index_write(index, op.old_tuple.get(pos), op.rid, IndexOp::Remove,
                                           err)) {
                        throw std::runtime_error(err);
                    }
                }
            }

            /* Log DELETE part of update */
            if (log_manager_ != nullptr && txn != nullptr) {
                recovery::LogRecord log(txn->get_id(), txn->get_prev_lsn(),
                                        recovery::LogRecordType::MARK_DELETE, table_name, op.rid,
                                        op.old_tuple);
                const auto lsn = log_manager_->append_log_record(log);
                txn->set_prev_lsn(lsn);
            }

            const auto new_tid = table.insert(op.new_tuple, txn_id);

            /* Update Indexes - Insert new */
            for (const auto& idx_info : table_meta->indexes) {
                if (!idx_info.column_positions.empty()) {
                    uint16_t pos = idx_info.column_positions[0];
                    common::ValueType ktype = table_meta->columns[pos].type;
                    storage::BTreeIndex index(idx_info.name, bpm_, ktype);
                    if (!apply_index_write(index, op.new_tuple.get(pos), new_tid, IndexOp::Insert,
                                           err)) {
                        throw std::runtime_error(err);
                    }
                }
            }

            /* Log INSERT part of update */
            if (log_manager_ != nullptr && txn != nullptr) {
                recovery::LogRecord log(txn->get_id(), txn->get_prev_lsn(),
                                        recovery::LogRecordType::INSERT, table_name, new_tid,
                                        op.new_tuple);
                const auto lsn = log_manager_->append_log_record(log);
                txn->set_prev_lsn(lsn);
            }

            if (txn != nullptr) {
                txn->add_undo_log(transaction::UndoLog::Type::UPDATE, table_name, new_tid, op.rid);
            }
            rows_updated++;
        }
    }

    result.set_rows_affected(rows_updated);
    return result;
}

std::unique_ptr<Operator> QueryExecutor::build_plan(const parser::SelectStatement& stmt,
                                                    transaction::Transaction* txn) {
    /* 1. Base: Initial table access (Sequential Scan or Index Scan) */
    if (!stmt.from()) {
        return nullptr;
    }

    const std::string base_table_name = stmt.from()->to_string();
    std::unique_ptr<Operator> current_root = nullptr;

    /* Check if table is in cluster shuffle buffers (e.g. Broadcast or Shuffle Join) */
    if (cluster_manager_ != nullptr &&
        cluster_manager_->has_shuffle_data(context_id_, base_table_name)) {
        auto data = cluster_manager_->fetch_shuffle_data(context_id_, base_table_name);
        /* We need a schema for the buffered data. */
        auto meta_opt = catalog_.get_table_by_name(base_table_name);
        Schema buffer_schema;
        if (meta_opt.has_value()) {
            for (const auto& col : meta_opt.value()->columns) {
                buffer_schema.add_column(base_table_name + "." + col.name, col.type);
            }
        }
        std::cerr << "--- [BuildPlan] Table " << base_table_name << " found in SHUFFLE buffer. Schema size=" << buffer_schema.column_count() << " ---" << std::endl;
        current_root = std::make_unique<BufferScanOperator>(context_id_, base_table_name, std::move(data),
                                                            std::move(buffer_schema));
    } else {
        auto base_table_meta_opt = catalog_.get_table_by_name(base_table_name);
        if (!base_table_meta_opt.has_value()) {
            return nullptr;
        }
        const auto* base_table_meta = base_table_meta_opt.value();

        Schema base_schema;
        for (const auto& col : base_table_meta->columns) {
            base_schema.add_column(base_table_name + "." + col.name, col.type);
        }

        /* Index Selection Optimization:
         * If there's a simple equality filter on an indexed column, use IndexScanOperator.
         */
        bool index_used = false;

        if (stmt.where() && stmt.where()->type() == parser::ExprType::Binary && stmt.joins().empty()) {
            const auto* bin_expr = dynamic_cast<const parser::BinaryExpr*>(stmt.where());
            if (bin_expr->op() == parser::TokenType::Eq) {
                std::string col_name;
                common::Value const_val;
                bool eligible = false;

                if (bin_expr->left().type() == parser::ExprType::Column &&
                    bin_expr->right().type() == parser::ExprType::Constant) {
                    col_name = bin_expr->left().to_string();
                    const_val = bin_expr->right().evaluate();
                    eligible = true;
                } else if (bin_expr->right().type() == parser::ExprType::Column &&
                           bin_expr->left().type() == parser::ExprType::Constant) {
                    col_name = bin_expr->right().to_string();
                    const_val = bin_expr->left().evaluate();
                    eligible = true;
                }

                if (eligible) {
                    /* Check if col_name is indexed */
                    for (const auto& idx_info : base_table_meta->indexes) {
                        if (!idx_info.column_positions.empty()) {
                            uint16_t pos = idx_info.column_positions[0];
                            /* Handle both qualified and unqualified names */
                            if (base_table_meta->columns[pos].name == col_name ||
                                (base_table_name + "." + base_table_meta->columns[pos].name) ==
                                    col_name) {
                                common::ValueType ktype = base_table_meta->columns[pos].type;
                                current_root = std::make_unique<IndexScanOperator>(
                                    std::make_unique<storage::HeapTable>(base_table_name, bpm_,
                                                                         base_schema),
                                    std::make_unique<storage::BTreeIndex>(idx_info.name, bpm_, ktype),
                                    std::move(const_val), txn, &lock_manager_);
                                index_used = true;
                                break;
                            }
                        }
                    }
                }
            }
        }

        if (!index_used) {
            current_root = std::make_unique<SeqScanOperator>(
                std::make_unique<storage::HeapTable>(base_table_name, bpm_, base_schema), txn,
                &lock_manager_);
        }
    }

    if (!current_root) return nullptr;

    std::cerr << "--- [BuildPlan] Base root schema size=" << current_root->output_schema().column_count() << " ---" << std::endl;

    /* 2. Add JOINs */
    for (const auto& join : stmt.joins()) {
        const std::string join_table_name = join.table->to_string();

        std::unique_ptr<Operator> join_scan = nullptr;

        /* Check if JOIN table is in shuffle buffers */
        if (cluster_manager_ != nullptr &&
            cluster_manager_->has_shuffle_data(context_id_, join_table_name)) {
            auto data = cluster_manager_->fetch_shuffle_data(context_id_, join_table_name);
            auto meta_opt = catalog_.get_table_by_name(join_table_name);
            Schema buffer_schema;
            if (meta_opt.has_value()) {
                for (const auto& col : meta_opt.value()->columns) {
                    buffer_schema.add_column(join_table_name + "." + col.name, col.type);
                }
            }
            std::cerr << "--- [BuildPlan] JOIN Table " << join_table_name << " found in SHUFFLE buffer. Schema size=" << buffer_schema.column_count() << " ---" << std::endl;
            join_scan = std::make_unique<BufferScanOperator>(
                context_id_, join_table_name, std::move(data), std::move(buffer_schema));
        } else {
            auto join_table_meta_opt = catalog_.get_table_by_name(join_table_name);
            if (!join_table_meta_opt.has_value()) {
                return nullptr;
            }
            const auto* join_table_meta = join_table_meta_opt.value();

            Schema join_schema;
            for (const auto& col : join_table_meta->columns) {
                join_schema.add_column(join_table_name + "." + col.name, col.type);
            }

            join_scan = std::make_unique<SeqScanOperator>(
                std::make_unique<storage::HeapTable>(join_table_name, bpm_, join_schema), txn,
                &lock_manager_);
            std::cerr << "--- [BuildPlan] JOIN Table " << join_table_name << " from LOCAL. Schema size=" << join_scan->output_schema().column_count() << " ---" << std::endl;
        }

        bool use_hash_join = false;
        std::unique_ptr<parser::Expression> left_key = nullptr;
        std::unique_ptr<parser::Expression> right_key = nullptr;

        if (join.condition && join.condition->type() == parser::ExprType::Binary) {
            const auto* bin_expr = dynamic_cast<const parser::BinaryExpr*>(join.condition.get());
            if (bin_expr != nullptr && bin_expr->op() == parser::TokenType::Eq) {
                /* Check which side of Eq belongs to which table */
                const auto left_side_schema = current_root->output_schema();
                const auto right_side_schema = join_scan->output_schema();

                const std::string left_col_name = bin_expr->left().to_string();
                const std::string right_col_name = bin_expr->right().to_string();

                const bool left_in_left =
                    (left_side_schema.find_column(left_col_name) != static_cast<size_t>(-1));
                const bool right_in_right =
                    (right_side_schema.find_column(right_col_name) != static_cast<size_t>(-1));

                if (left_in_left && right_in_right) {
                    use_hash_join = true;
                    left_key = bin_expr->left().clone();
                    right_key = bin_expr->right().clone();
                } else {
                    const bool left_in_right =
                        (right_side_schema.find_column(left_col_name) != static_cast<size_t>(-1));
                    const bool right_in_left =
                        (left_side_schema.find_column(right_col_name) != static_cast<size_t>(-1));

                    if (left_in_right && right_in_left) {
                        use_hash_join = true;
                        left_key = bin_expr->right().clone();
                        right_key = bin_expr->left().clone();
                    }
                }
            }
        }

        if (use_hash_join) {
            executor::JoinType exec_join_type = executor::JoinType::Inner;
            if (join.type == parser::SelectStatement::JoinType::Left) {
                exec_join_type = executor::JoinType::Left;
            } else if (join.type == parser::SelectStatement::JoinType::Right) {
                exec_join_type = executor::JoinType::Right;
            } else if (join.type == parser::SelectStatement::JoinType::Full) {
                exec_join_type = executor::JoinType::Full;
            }

            current_root = std::make_unique<HashJoinOperator>(
                std::move(current_root), std::move(join_scan), std::move(left_key),
                std::move(right_key), exec_join_type);
            std::cerr << "--- [BuildPlan] Added HashJoin. Combined schema size=" << current_root->output_schema().column_count() << " ---" << std::endl;
        } else {
            /* TODO: Implement NestedLoopJoin for non-equality or missing conditions */
            return nullptr;
        }
    }

    /* 3. Filter (WHERE) - Only if not already handled by IndexScan */
    if (stmt.where()) {
        current_root =
            std::make_unique<FilterOperator>(std::move(current_root), stmt.where()->clone());
    }

    /* 3. Aggregate (GROUP BY or implicit aggregates) */
    bool has_aggregates = false;
    std::vector<AggregateInfo> aggs;
    for (const auto& col : stmt.columns()) {
        if (col->type() == parser::ExprType::Function) {
            const auto* func = dynamic_cast<const parser::FunctionExpr*>(col.get());
            if (func == nullptr) {
                continue;
            }
            std::string name = func->name();
            std::transform(name.begin(), name.end(), name.begin(),
                           [](unsigned char c) { return static_cast<char>(std::toupper(c)); });

            if (name == "COUNT" || name == "SUM" || name == "MIN" || name == "MAX" ||
                name == "AVG") {
                has_aggregates = true;
                AggregateType type = AggregateType::Count;
                if (name == "SUM") {
                    type = AggregateType::Sum;
                } else if (name == "MIN") {
                    type = AggregateType::Min;
                } else if (name == "MAX") {
                    type = AggregateType::Max;
                } else if (name == "AVG") {
                    type = AggregateType::Avg;
                }

                AggregateInfo info;
                info.type = type;
                info.expr = (!func->args().empty()) ? func->args()[0]->clone() : nullptr;
                info.is_distinct = func->distinct();

                /* Normalize aggregate name for schema lookup */
                std::string agg_name = name + "(";
                if (info.is_distinct) {
                    agg_name += "DISTINCT ";
                }
                agg_name += (info.expr ? info.expr->to_string() : "*") + ")";
                info.name = agg_name;

                aggs.push_back(std::move(info));
            }
        }
    }

    if (!stmt.group_by().empty() || has_aggregates) {
        std::vector<std::unique_ptr<parser::Expression>> group_by;
        for (const auto& gb : stmt.group_by()) {
            group_by.push_back(gb->clone());
        }
        current_root = std::make_unique<AggregateOperator>(std::move(current_root),
                                                           std::move(group_by), std::move(aggs));

        /* 3.5. Having */
        if (stmt.having()) {
            current_root =
                std::make_unique<FilterOperator>(std::move(current_root), stmt.having()->clone());
        }
    }

    /* 4. Sort (ORDER BY) */
    if (!stmt.order_by().empty()) {
        std::vector<std::unique_ptr<parser::Expression>> sort_keys;
        std::vector<bool> ascending;
        for (const auto& ob : stmt.order_by()) {
            sort_keys.push_back(ob->clone());
            ascending.push_back(true); /* Default to ASC */
        }
        current_root = std::make_unique<SortOperator>(std::move(current_root), std::move(sort_keys),
                                                      std::move(ascending));
    }

    /* 5. Project (SELECT columns) */
    if (!stmt.columns().empty()) {
        std::vector<std::unique_ptr<parser::Expression>> projection;
        for (const auto& col : stmt.columns()) {
            projection.push_back(col->clone());
        }
        current_root =
            std::make_unique<ProjectOperator>(std::move(current_root), std::move(projection));
        std::cerr << "--- [BuildPlan] Added Projection. Result schema size=" << current_root->output_schema().column_count() << " ---" << std::endl;
    }

    /* 6. Limit */
    if (stmt.has_limit() || stmt.has_offset()) {
        current_root =
            std::make_unique<LimitOperator>(std::move(current_root), stmt.limit(), stmt.offset());
    }

    return current_root;
}

QueryResult QueryExecutor::execute_drop_table(const parser::DropTableStatement& stmt) {
    QueryResult result;
    auto table_meta_opt = catalog_.get_table_by_name(stmt.table_name());

    if (!table_meta_opt.has_value()) {
        if (stmt.if_exists()) {
            result.set_rows_affected(0);
            return result;
        }
        result.set_error("Table not found: " + stmt.table_name());
        return result;
    }
    const auto* table_meta = table_meta_opt.value();

    const oid_t table_id = table_meta->table_id;

    /* 1. Drop associated indexes from physical storage */
    const auto indexes = catalog_.get_table_indexes(table_id);
    for (const auto& idx_info : indexes) {
        storage::BTreeIndex idx(idx_info->name, bpm_, common::ValueType::TYPE_NULL);
        static_cast<void>(idx.drop());
    }

    /* 2. Drop table physical file */
    storage::HeapTable table(stmt.table_name(), bpm_, executor::Schema());
    static_cast<void>(table.drop());

    /* 3. Update catalog */
    if (is_local_only_) {
        if (!catalog_.drop_table_local(table_id)) {
            result.set_error("Failed to drop table from local catalog");
            return result;
        }
    } else {
        if (!catalog_.drop_table(table_id)) {
            result.set_error("Failed to drop table from catalog");
            return result;
        }
    }

    result.set_rows_affected(1);
    return result;
}

QueryResult QueryExecutor::execute_drop_index(const parser::DropIndexStatement& stmt) {
    QueryResult result;

    /* Find index by name since catalog doesn't have direct get_index_by_name */
    oid_t index_id = 0;
    for (auto* table : catalog_.get_all_tables()) {
        for (auto& idx : table->indexes) {
            if (idx.name == stmt.index_name()) {
                index_id = idx.index_id;
                break;
            }
        }
        if (index_id != 0) {
            break;
        }
    }

    if (index_id == 0) {
        if (stmt.if_exists()) {
            result.set_rows_affected(0);
            return result;
        }
        result.set_error("Index not found: " + stmt.index_name());
        return result;
    }

    /* 1. Drop physical file */
    storage::BTreeIndex idx(stmt.index_name(), bpm_, common::ValueType::TYPE_NULL);
    static_cast<void>(idx.drop());

    /* 2. Update catalog */
    if (!catalog_.drop_index(index_id)) {
        result.set_error("Failed to drop index from catalog");
        return result;
    }

    result.set_rows_affected(1);
    return result;
}

}  // namespace cloudsql::executor
