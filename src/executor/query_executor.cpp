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
#include "executor/vectorized_operator.hpp"
#include "network/rpc_message.hpp"
#include "parser/expression.hpp"
#include "parser/lexer.hpp"
#include "parser/parser.hpp"
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

// Define static members for statement cache
std::unordered_map<std::string, std::shared_ptr<parser::Statement>> QueryExecutor::statement_cache_;
std::mutex QueryExecutor::cache_mutex_;

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

std::shared_ptr<PreparedStatement> QueryExecutor::prepare(const std::string& sql) {
    auto lexer = std::make_unique<parser::Lexer>(sql);
    parser::Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    if (!stmt) return nullptr;

    auto prepared = std::make_shared<PreparedStatement>();
    prepared->stmt = std::shared_ptr<parser::Statement>(stmt.release());
    prepared->sql = sql;

    // Cache metadata for INSERT fast-path
    if (prepared->stmt->type() == parser::StmtType::Insert) {
        const auto& insert_stmt = dynamic_cast<const parser::InsertStatement&>(*prepared->stmt);
        if (insert_stmt.table()) {
            const std::string table_name = insert_stmt.table()->to_string();
            auto table_meta_opt = catalog_.get_table_by_name(table_name);
            if (table_meta_opt.has_value()) {
                prepared->table_meta = table_meta_opt.value();
                prepared->schema = std::make_unique<Schema>();
                for (const auto& col : prepared->table_meta->columns) {
                    prepared->schema->add_column(col.name, col.type);
                }
                prepared->table =
                    std::make_unique<storage::HeapTable>(table_name, bpm_, *prepared->schema);

                // Cache B-tree index objects
                for (const auto& idx_info : prepared->table_meta->indexes) {
                    if (!idx_info.column_positions.empty()) {
                        uint16_t pos = idx_info.column_positions[0];
                        common::ValueType ktype = prepared->table_meta->columns[pos].type;
                        prepared->indexes.push_back(
                            std::make_unique<storage::BTreeIndex>(idx_info.name, bpm_, ktype));
                    }
                }
            }
        }
    }

    return prepared;
}

QueryResult QueryExecutor::execute(const PreparedStatement& prepared,
                                   const std::vector<common::Value>& params) {
    // Fast-path for INSERT
    if (prepared.stmt->type() == parser::StmtType::Insert && prepared.table) {
        const auto start = std::chrono::high_resolution_clock::now();
        QueryResult result;
        current_params_ = &params;

        const bool is_auto_commit = (current_txn_ == nullptr);
        transaction::Transaction* txn = current_txn_;
        if (is_auto_commit) txn = transaction_manager_.begin();

        try {
            const auto& insert_stmt = dynamic_cast<const parser::InsertStatement&>(*prepared.stmt);
            uint64_t rows_inserted = 0;
            const uint64_t xmin = (txn != nullptr) ? txn->get_id() : 0;

            for (const auto& row_exprs : insert_stmt.values()) {
                std::pmr::vector<common::Value> values(&arena_);
                values.reserve(row_exprs.size());
                for (const auto& expr : row_exprs) {
                    values.push_back(expr->evaluate(nullptr, nullptr, current_params_));
                }

                const Tuple tuple(std::move(values));
                const auto tid = prepared.table->insert(tuple, xmin);

                // Index updates using cached index objects
                std::string err;
                size_t cached_idx_ptr = 0;
                for (const auto& idx_info : prepared.table_meta->indexes) {
                    if (!idx_info.column_positions.empty()) {
                        uint16_t pos = idx_info.column_positions[0];
                        if (!apply_index_write(*prepared.indexes[cached_idx_ptr++], tuple.get(pos),
                                               tid, IndexOp::Insert, err)) {
                            throw std::runtime_error(err);
                        }
                    }
                }

                if (txn != nullptr) {
                    txn->add_undo_log(transaction::UndoLog::Type::INSERT, prepared.table_meta->name,
                                      tid);
                    if (!batch_insert_mode_) {
                        if (!lock_manager_.acquire_exclusive(txn, tid)) {
                            throw std::runtime_error("Failed to acquire exclusive lock");
                        }
                    }
                }
                rows_inserted++;
            }

            if (is_auto_commit && txn != nullptr) transaction_manager_.commit(txn);
            result.set_rows_affected(rows_inserted);
        } catch (const std::exception& e) {
            if (is_auto_commit && txn != nullptr) transaction_manager_.abort(txn);
            result.set_error(std::string("Execution error: ") + e.what());
        }

        current_params_ = nullptr;
        const auto end = std::chrono::high_resolution_clock::now();
        result.set_execution_time(
            std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());
        arena_.reset();
        return result;
    }

    // Fallback for other statement types
    current_params_ = &params;
    QueryResult res = execute(*(prepared.stmt));
    current_params_ = nullptr;
    return res;
}

QueryResult QueryExecutor::execute(const std::string& sql) {
    std::shared_ptr<parser::Statement> stmt = nullptr;

    {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        auto it = statement_cache_.find(sql);
        if (it != statement_cache_.end()) {
            stmt = it->second;
        }
    }

    if (!stmt) {
        auto lexer = std::make_unique<parser::Lexer>(sql);
        parser::Parser parser(std::move(lexer));
        auto parsed_stmt = parser.parse_statement();
        if (parsed_stmt) {
            stmt = std::shared_ptr<parser::Statement>(parsed_stmt.release());
            std::lock_guard<std::mutex> lock(cache_mutex_);
            statement_cache_[sql] = stmt;
        }
    }

    if (!stmt) {
        QueryResult res;
        res.set_error("Failed to parse SQL statement");
        return res;
    }

    return execute(*stmt);
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

    // Reset arena for the next query to reclaim zero-allocation memory
    arena_.reset();

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
    std::unique_ptr<Operator> root;
    std::unique_ptr<VectorizedOperator> vec_root;
    bool has_sort_or_limit = !stmt.order_by().empty() || stmt.has_limit() || stmt.has_offset();
    bool use_vectorized = parallel_ && storage_manager_ && !has_sort_or_limit;

    if (use_vectorized) {
        vec_root = build_vectorized_plan(stmt, txn, has_sort_or_limit);
        root = std::move(vec_root);
    } else {
        root = build_plan(stmt, txn);
    }

    if (!root) {
        result.set_error("Failed to build execution plan (check table existence and FROM clause)");
        return result;
    }

    /* Initialize and open operators */
    root->set_memory_resource(&arena_);
    root->set_params(current_params_);
    if (!root->init() || !root->open()) {
        result.set_error(root->error().empty() ? "Failed to open execution plan" : root->error());
        return result;
    }

    /* Set result schema */
    result.set_schema(root->output_schema());

    if (use_vectorized) {
        /* Vectorized batch iteration */
        auto batch = VectorBatch::create(root->output_schema());
        auto* vec_op = dynamic_cast<VectorizedOperator*>(root.get());
        assert(vec_op && "root must be a VectorizedOperator when use_vectorized is true");

        while (true) {
            bool has_more = false;
            try {
                has_more = vec_op->next_batch(*batch);
            } catch (const std::out_of_range& e) {
                result.set_error(std::string("vector access error in next_batch: ") + e.what() +
                                 " batch_cols=" + std::to_string(batch->column_count()) +
                                 " batch_rows=" + std::to_string(batch->row_count()));
                break;
            } catch (const std::exception& e) {
                result.set_error(std::string("next_batch error: ") + e.what());
                break;
            } catch (...) {
                result.set_error("next_batch error: unknown exception type");
                break;
            }
            if (!has_more) break;
            for (size_t r = 0; r < batch->row_count(); ++r) {
                Tuple tuple;
                for (size_t c = 0; c < batch->column_count(); ++c) {
                    tuple.set(c, batch->get_column(c).get(r));
                }
                result.add_row(Tuple(tuple.values(), nullptr));
            }
        }
    } else {
        /* Pull tuples (Volcano model) */
        Tuple tuple;
        while (root->next(tuple)) {
            // MUST deep-copy tuple to default allocator (heap) so it outlives the arena reset
            result.add_row(Tuple(tuple.values(), nullptr));
        }
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
        // Zero-allocation vector construction via Arena
        std::pmr::vector<common::Value> values(&arena_);
        values.reserve(row_exprs.size());
        for (const auto& expr : row_exprs) {
            // Include bound parameters in expression evaluation
            values.push_back(expr->evaluate(nullptr, nullptr, current_params_));
        }

        const Tuple tuple(std::move(values));

        // Distributed Routing: Skip if is_local_only_
        if (!is_local_only_ && cluster_manager_ != nullptr && !table_meta->shards.empty()) {
            uint32_t shard_id = 0;
            if (!tuple.empty()) {
                shard_id = cluster::ShardManager::compute_shard(
                    tuple.get(0), static_cast<uint32_t>(table_meta->shards.size()));
            }
            auto shard_info_opt = cluster::ShardManager::get_target_node(*table_meta, shard_id);

            if (shard_info_opt.has_value()) {
                const auto& shard_info = shard_info_opt.value();
                network::RpcClient client(shard_info.node_address, shard_info.port);
                if (client.connect()) {
                    network::ExecuteFragmentArgs args;
                    args.context_id = context_id_;
                    // Optimization: Only forward the current row
                    args.sql = "INSERT INTO " + table_name + " VALUES " + tuple.to_string() + ";";

                    std::vector<uint8_t> resp;
                    if (!client.call(network::RpcType::ExecuteFragment, args.serialize(), resp)) {
                        result.set_error("Failed to forward INSERT to data node " +
                                         shard_info.node_address);
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
            if (!lock_manager_.acquire_exclusive(txn, tid)) {
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
            // Support parameters in DELETE WHERE
            match = stmt.where()->evaluate(&meta.tuple, &schema, current_params_).as_bool();
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
            match = stmt.where()->evaluate(&meta.tuple, &schema, current_params_).as_bool();
        }

        if (match && meta.xmax == 0) {
            /* Compute new tuple values */
            Tuple new_tuple = meta.tuple;
            for (const auto& [col_expr, val_expr] : stmt.set_clauses()) {
                const std::string col_name = col_expr->to_string();
                const size_t idx = schema.find_column(col_name);
                if (idx != static_cast<size_t>(-1)) {
                    new_tuple.set(idx, val_expr->evaluate(&meta.tuple, &schema, current_params_));
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
        /* We need a schema for the buffered data. Use unqualified names as BufferScan will qualify
         * them. */
        auto meta_opt = catalog_.get_table_by_name(base_table_name);
        Schema buffer_schema;
        if (meta_opt.has_value()) {
            for (const auto& col : meta_opt.value()->columns) {
                buffer_schema.add_column(col.name, col.type);
            }
        }

        current_root = std::make_unique<BufferScanOperator>(
            context_id_, base_table_name, std::move(data), std::move(buffer_schema));
    } else {
        auto base_table_meta_opt = catalog_.get_table_by_name(base_table_name);
        if (!base_table_meta_opt.has_value()) {
            return nullptr;
        }
        const auto* base_table_meta = base_table_meta_opt.value();

        Schema base_schema;
        for (const auto& col : base_table_meta->columns) {
            base_schema.add_column(col.name, col.type);
        }

        /* Index Selection Optimization:
         * If there's a simple equality filter on an indexed column, use IndexScanOperator.
         */
        bool index_used = false;

        if (stmt.where() && stmt.where()->type() == parser::ExprType::Binary &&
            stmt.joins().empty()) {
            const auto* bin_expr = dynamic_cast<const parser::BinaryExpr*>(stmt.where());
            if (bin_expr->op() == parser::TokenType::Eq) {
                std::string col_name;
                common::Value const_val;
                bool eligible = false;

                if (bin_expr->left().type() == parser::ExprType::Column &&
                    bin_expr->right().type() == parser::ExprType::Constant) {
                    col_name = bin_expr->left().to_string();
                    const_val = bin_expr->right().evaluate(nullptr, nullptr, current_params_);
                    eligible = true;
                } else if (bin_expr->right().type() == parser::ExprType::Column &&
                           bin_expr->left().type() == parser::ExprType::Constant) {
                    col_name = bin_expr->right().to_string();
                    const_val = bin_expr->left().evaluate(nullptr, nullptr, current_params_);
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
                                    std::make_shared<storage::HeapTable>(base_table_name, bpm_,
                                                                         base_schema),
                                    std::make_unique<storage::BTreeIndex>(idx_info.name, bpm_,
                                                                          ktype),
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
                std::make_shared<storage::HeapTable>(base_table_name, bpm_, base_schema), txn,
                &lock_manager_);
        }
    }

    if (!current_root) return nullptr;

    // Propagate memory resource and bound parameters to the operator tree
    current_root->set_memory_resource(&arena_);
    current_root->set_params(current_params_);

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
                    buffer_schema.add_column(col.name, col.type);
                }
            }

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
                join_schema.add_column(col.name, col.type);
            }

            join_scan = std::make_unique<SeqScanOperator>(
                std::make_shared<storage::HeapTable>(join_table_name, bpm_, join_schema), txn,
                &lock_manager_);
        }

        join_scan->set_memory_resource(&arena_);
        join_scan->set_params(current_params_);

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

            auto join_op = std::make_unique<HashJoinOperator>(
                std::move(current_root), std::move(join_scan), std::move(left_key),
                std::move(right_key), exec_join_type);
            join_op->set_memory_resource(&arena_);
            join_op->set_params(current_params_);
            current_root = std::move(join_op);
        } else {
            /* TODO: Implement NestedLoopJoin for non-equality or missing conditions */
            return nullptr;
        }
    }

    /* 3. Filter (WHERE) - Only if not already handled by IndexScan */
    if (stmt.where()) {
        auto filter_op =
            std::make_unique<FilterOperator>(std::move(current_root), stmt.where()->clone());
        filter_op->set_memory_resource(&arena_);
        filter_op->set_params(current_params_);
        current_root = std::move(filter_op);
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
        auto agg_op = std::make_unique<AggregateOperator>(std::move(current_root),
                                                          std::move(group_by), std::move(aggs));
        agg_op->set_memory_resource(&arena_);
        agg_op->set_params(current_params_);
        current_root = std::move(agg_op);

        /* 3.5. Having */
        if (stmt.having()) {
            auto having_filter =
                std::make_unique<FilterOperator>(std::move(current_root), stmt.having()->clone());
            having_filter->set_memory_resource(&arena_);
            having_filter->set_params(current_params_);
            current_root = std::move(having_filter);
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
        auto sort_op = std::make_unique<SortOperator>(std::move(current_root), std::move(sort_keys),
                                                      std::move(ascending));
        sort_op->set_memory_resource(&arena_);
        sort_op->set_params(current_params_);
        current_root = std::move(sort_op);
    }

    /* 5. Project (SELECT columns) */
    if (!stmt.columns().empty()) {
        std::vector<std::unique_ptr<parser::Expression>> projection;
        for (const auto& col : stmt.columns()) {
            projection.push_back(col->clone());
        }
        auto project_op =
            std::make_unique<ProjectOperator>(std::move(current_root), std::move(projection));
        project_op->set_memory_resource(&arena_);
        project_op->set_params(current_params_);
        current_root = std::move(project_op);
    }

    /* 6. Limit */
    if (stmt.has_limit() || stmt.has_offset()) {
        auto limit_op =
            std::make_unique<LimitOperator>(std::move(current_root), stmt.limit(), stmt.offset());
        limit_op->set_memory_resource(&arena_);
        limit_op->set_params(current_params_);
        current_root = std::move(limit_op);
    }

    return current_root;
}

std::unique_ptr<VectorizedOperator> QueryExecutor::build_vectorized_plan(
    const parser::SelectStatement& stmt, [[maybe_unused]] transaction::Transaction* txn,
    [[maybe_unused]] bool has_sort_or_limit) {
    // Currently unused — reserved for vectorized sort/limit support

    if (!stmt.from()) {
        return nullptr;
    }

    const std::string base_table_name = stmt.from()->to_string();

    /* Build base scan using ColumnarTable */
    auto base_table_meta_opt = catalog_.get_table_by_name(base_table_name);
    if (!base_table_meta_opt.has_value()) {
        return nullptr;
    }
    const auto* base_table_meta = base_table_meta_opt.value();

    executor::Schema base_schema;
    for (const auto& col : base_table_meta->columns) {
        base_schema.add_column(col.name, col.type, col.nullable);
    }

    auto col_table =
        std::make_shared<storage::ColumnarTable>(base_table_name, *storage_manager_, base_schema);

    /* Migrate HeapTable data to ColumnarTable if needed.
       INSERT writes to HeapTable, but vectorized path reads from ColumnarTable.
       We only migrate when ColumnarTable is empty (first time or after clear).
       On failure, partial files are cleaned up so next attempt starts fresh. */
    storage::HeapTable heap_table(base_table_name, bpm_, base_schema);
    uint64_t count = heap_table.tuple_count();
    bool needs_migration = (count > 0);
    if (needs_migration) {
        // Check if already migrated by trying to open existing columnar table
        auto existing_col_table = std::make_shared<storage::ColumnarTable>(
            base_table_name, *storage_manager_, base_schema);
        if (existing_col_table->open() && existing_col_table->row_count() > 0) {
            needs_migration = false;  // Already migrated, skip
        }
    }
    if (needs_migration) {
        // Clean up any existing partial columnar files before starting fresh
        storage_manager_->delete_file(base_table_name + ".meta.bin");
        for (size_t i = 0; i < base_schema.column_count(); ++i) {
            storage_manager_->delete_file(base_table_name + ".col" + std::to_string(i) +
                                          ".data.bin");
            storage_manager_->delete_file(base_table_name + ".col" + std::to_string(i) +
                                          ".nulls.bin");
        }

        if (!col_table->create()) {
            return nullptr;  // Failed to create columnar table
        }

        auto batch = executor::VectorBatch::create(base_schema);
        auto iter = heap_table.scan();
        executor::Tuple tuple;
        bool migration_failed = false;
        while (iter.next(tuple)) {
            batch->append_tuple(tuple);
            if (batch->row_count() >= 1024) {
                if (!col_table->append_batch(*batch)) {
                    migration_failed = true;
                    break;
                }
                batch->clear();
            }
        }
        if (!migration_failed && batch->row_count() > 0) {
            if (!col_table->append_batch(*batch)) {
                migration_failed = true;
            }
        }

        if (migration_failed) {
            // Clean up partial files so next attempt starts fresh
            storage_manager_->delete_file(base_table_name + ".meta.bin");
            for (size_t i = 0; i < base_schema.column_count(); ++i) {
                storage_manager_->delete_file(base_table_name + ".col" + std::to_string(i) +
                                              ".data.bin");
                storage_manager_->delete_file(base_table_name + ".col" + std::to_string(i) +
                                              ".nulls.bin");
            }
        }
    }

    if (!col_table->open()) {
        return nullptr;  // Table not found or not columnar
    }

    std::unique_ptr<VectorizedOperator> current_root =
        std::make_unique<VectorizedSeqScanOperator>(base_table_name, col_table);

    /* Add JOINs (VectorizedHashJoinOperator) */
    for (const auto& join : stmt.joins()) {
        const std::string join_table_name = join.table->to_string();

        auto join_table_meta_opt = catalog_.get_table_by_name(join_table_name);
        if (!join_table_meta_opt.has_value()) {
            return nullptr;
        }
        const auto* join_table_meta = join_table_meta_opt.value();

        executor::Schema join_schema;
        for (const auto& col : join_table_meta->columns) {
            join_schema.add_column(col.name, col.type, col.nullable);
        }

        auto join_col_table = std::make_shared<storage::ColumnarTable>(
            join_table_name, *storage_manager_, join_schema);

        /* Migrate HeapTable data to ColumnarTable for join table */
        storage::HeapTable join_heap_table(join_table_name, bpm_, join_schema);
        uint64_t join_count = join_heap_table.tuple_count();
        if (join_count > 0) {
            join_col_table->create();
            auto batch = executor::VectorBatch::create(join_schema);
            auto iter = join_heap_table.scan();
            executor::Tuple tuple;
            while (iter.next(tuple)) {
                batch->append_tuple(tuple);
                if (batch->row_count() >= 1024) {
                    join_col_table->append_batch(*batch);
                    batch->clear();
                }
            }
            if (batch->row_count() > 0) {
                join_col_table->append_batch(*batch);
            }
        }

        if (!join_col_table->open()) {
            return nullptr;
        }

        std::unique_ptr<VectorizedOperator> right_scan =
            std::make_unique<VectorizedSeqScanOperator>(join_table_name, join_col_table);

        bool use_hash_join = false;
        std::unique_ptr<parser::Expression> left_key = nullptr;
        std::unique_ptr<parser::Expression> right_key = nullptr;

        if (join.condition && join.condition->type() == parser::ExprType::Binary) {
            const auto* bin_expr = dynamic_cast<const parser::BinaryExpr*>(join.condition.get());
            if (bin_expr != nullptr && bin_expr->op() == parser::TokenType::Eq) {
                const auto& left_schema = current_root->output_schema();
                const auto& right_schema = right_scan->output_schema();

                const std::string left_col_name = bin_expr->left().to_string();
                const std::string right_col_name = bin_expr->right().to_string();

                bool left_in_left =
                    (left_schema.find_column(left_col_name) != static_cast<size_t>(-1));
                bool right_in_right =
                    (right_schema.find_column(right_col_name) != static_cast<size_t>(-1));

                if (left_in_left && right_in_right) {
                    use_hash_join = true;
                    left_key = bin_expr->left().clone();
                    right_key = bin_expr->right().clone();
                } else {
                    bool left_in_right =
                        (right_schema.find_column(left_col_name) != static_cast<size_t>(-1));
                    bool right_in_left =
                        (left_schema.find_column(right_col_name) != static_cast<size_t>(-1));
                    if (left_in_right && right_in_left) {
                        use_hash_join = true;
                        left_key = bin_expr->right().clone();
                        right_key = bin_expr->left().clone();
                    }
                }
            }
        }

        if (!use_hash_join) {
            return nullptr;  // Vectorized path only supports equi-joins
        }

        executor::JoinType exec_join_type = executor::JoinType::Inner;
        if (join.type == parser::SelectStatement::JoinType::Left) {
            exec_join_type = executor::JoinType::Left;
        } else if (join.type == parser::SelectStatement::JoinType::Right) {
            exec_join_type = executor::JoinType::Right;
        } else if (join.type == parser::SelectStatement::JoinType::Full) {
            exec_join_type = executor::JoinType::Full;
        }

        executor::Schema output_schema;
        for (const auto& col : current_root->output_schema().columns()) {
            output_schema.add_column(col.name(), col.type(), col.nullable());
        }
        for (const auto& col : right_scan->output_schema().columns()) {
            output_schema.add_column(col.name(), col.type(), col.nullable());
        }

        auto join_op = std::make_unique<VectorizedHashJoinOperator>(
            std::move(current_root), std::move(right_scan), std::move(left_key),
            std::move(right_key), exec_join_type, output_schema);

        current_root = std::move(join_op);
    }

    /* Filter (WHERE) */
    if (stmt.where()) {
        auto filter_op = std::make_unique<VectorizedFilterOperator>(std::move(current_root),
                                                                    stmt.where()->clone());
        current_root = std::move(filter_op);
    }

    /* Aggregate (GROUP BY) */
    bool has_aggregates = false;
    std::vector<VectorizedAggregateInfo> agg_infos;
    for (const auto& col : stmt.columns()) {
        if (col->type() == parser::ExprType::Function) {
            const auto* func = dynamic_cast<const parser::FunctionExpr*>(col.get());
            if (func == nullptr) continue;
            std::string name = func->name();
            std::transform(name.begin(), name.end(), name.begin(),
                           [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
            if (name == "COUNT" || name == "SUM" || name == "MIN" || name == "MAX" ||
                name == "AVG") {
                has_aggregates = true;
                VectorizedAggregateInfo info;
                if (name == "COUNT")
                    info.type = AggregateType::Count;
                else if (name == "SUM")
                    info.type = AggregateType::Sum;
                else if (name == "MIN")
                    info.type = AggregateType::Min;
                else if (name == "MAX")
                    info.type = AggregateType::Max;
                else
                    info.type = AggregateType::Avg;
                info.input_col_idx = -1;  // default
                agg_infos.push_back(info);
            }
        }
    }

    if (!stmt.group_by().empty() || has_aggregates) {
        std::vector<std::unique_ptr<parser::Expression>> group_by;
        for (const auto& gb : stmt.group_by()) {
            group_by.push_back(gb->clone());
        }

        executor::Schema output_schema;
        for (const auto& gb : stmt.group_by()) {
            const auto& gb_name = gb->to_string();
            size_t idx = current_root->output_schema().find_column(gb_name);
            if (idx != static_cast<size_t>(-1)) {
                output_schema.add_column(current_root->output_schema().get_column(idx).name(),
                                         current_root->output_schema().get_column(idx).type(),
                                         current_root->output_schema().get_column(idx).nullable());
            }
        }
        for (size_t i = 0; i < agg_infos.size(); ++i) {
            output_schema.add_column("agg_" + std::to_string(i), common::ValueType::TYPE_FLOAT64,
                                     false);
        }

        auto agg_op = std::make_unique<VectorizedGroupByOperator>(
            std::move(current_root), std::move(group_by), std::move(agg_infos), output_schema);
        current_root = std::move(agg_op);

        if (stmt.having()) {
            auto having_filter = std::make_unique<VectorizedFilterOperator>(std::move(current_root),
                                                                            stmt.having()->clone());
            current_root = std::move(having_filter);
        }
    }

    /* Project (SELECT columns) - before Sort/Limit since they are Volcano operators */
    if (!stmt.columns().empty()) {
        std::vector<std::unique_ptr<parser::Expression>> proj_exprs;
        for (const auto& col : stmt.columns()) {
            proj_exprs.push_back(col->clone());
        }

        executor::Schema proj_schema;
        for (const auto& col : stmt.columns()) {
            if (col->type() == parser::ExprType::Column) {
                const auto& col_name = col->to_string();
                size_t idx = current_root->output_schema().find_column(col_name);
                if (idx != static_cast<size_t>(-1)) {
                    proj_schema.add_column(
                        current_root->output_schema().get_column(idx).name(),
                        current_root->output_schema().get_column(idx).type(),
                        current_root->output_schema().get_column(idx).nullable());
                }
            } else {
                // Infer expression result type from constant value, fallback to TYPE_TEXT
                common::ValueType expr_type = common::ValueType::TYPE_TEXT;
                if (col->type() == parser::ExprType::Constant) {
                    const auto* const_expr = static_cast<const parser::ConstantExpr*>(col.get());
                    expr_type = const_expr->value().type();
                }
                proj_schema.add_column("expr", expr_type, true);
            }
        }

        auto project_op = std::make_unique<VectorizedProjectOperator>(
            std::move(current_root), proj_schema, std::move(proj_exprs));
        current_root = std::move(project_op);
    }

    /* Sort and Limit are NOT created here in the vectorized path.
       When has_sort_or_limit is true, use_vectorized is false so this function
       is only called for pure vectorized queries (no ORDER BY, no LIMIT).
       The Volcano path handles Sort/Limit via build_plan(). */
    // has_sort_or_limit reserved for vectorized sort/limit in future

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
