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
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.hpp"
#include "common/value.hpp"
#include "executor/operator.hpp"
#include "executor/types.hpp"
#include "parser/expression.hpp"
#include "parser/statement.hpp"
#include "parser/token.hpp"
#include "recovery/log_manager.hpp"
#include "recovery/log_record.hpp"
#include "storage/btree_index.hpp"
#include "storage/heap_table.hpp"
#include "storage/storage_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

namespace cloudsql::executor {

QueryExecutor::QueryExecutor(Catalog& catalog, storage::StorageManager& storage_manager,
                             transaction::LockManager& lock_manager,
                             transaction::TransactionManager& transaction_manager,
                             recovery::LogManager* log_manager)
    : catalog_(catalog),
      storage_manager_(storage_manager),
      lock_manager_(lock_manager),
      transaction_manager_(transaction_manager),
      log_manager_(log_manager) {}

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
    const oid_t table_id = catalog_.create_table(stmt.table_name(), std::move(catalog_cols));
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
    storage::HeapTable table(table_info->name, storage_manager_, executor::Schema());
    if (!table.create()) {
        static_cast<void>(catalog_.drop_table(table_id));
        result.set_error("Failed to create table file");
        return result;
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

    storage::HeapTable table(table_name, storage_manager_, schema);

    uint64_t rows_inserted = 0;
    const uint64_t xmin = (txn != nullptr) ? txn->get_id() : 0;

    for (const auto& row_exprs : stmt.values()) {
        std::vector<common::Value> values;
        values.reserve(row_exprs.size());
        for (const auto& expr : row_exprs) {
            values.push_back(expr->evaluate());
        }

        const Tuple tuple(std::move(values));
        const auto tid = table.insert(tuple, xmin);

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

    storage::HeapTable table(table_name, storage_manager_, schema);
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
        /* Retrieve old tuple for logging */
        Tuple old_tuple;
        if (log_manager_ != nullptr && txn != nullptr) {
            static_cast<void>(table.get(rid, old_tuple));
        }

        if (table.remove(rid, xmax)) {
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

    storage::HeapTable table(table_name, storage_manager_, schema);
    const uint64_t txn_id = (txn != nullptr) ? txn->get_id() : 0;
    uint64_t rows_updated = 0;

    /* Phase 1: Collect RIDs and compute new values to avoid Halloween Problem */
    struct UpdateOp {
        storage::HeapTable::TupleId rid;
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
            updates.push_back({iter.current_id(), std::move(new_tuple)});
        }
    }

    /* Phase 2: Apply Updates */
    for (const auto& op : updates) {
        /* Retrieve old tuple for logging */
        Tuple old_tuple;
        if (log_manager_ != nullptr && txn != nullptr) {
            static_cast<void>(table.get(op.rid, old_tuple));
        }

        if (table.remove(op.rid, txn_id)) {
            /* Log DELETE part of update */
            if (log_manager_ != nullptr && txn != nullptr) {
                recovery::LogRecord log(txn->get_id(), txn->get_prev_lsn(),
                                        recovery::LogRecordType::MARK_DELETE, table_name, op.rid,
                                        old_tuple);
                const auto lsn = log_manager_->append_log_record(log);
                txn->set_prev_lsn(lsn);
            }

            const auto new_tid = table.insert(op.new_tuple, txn_id);

            /* Log INSERT part of update */
            if (log_manager_ != nullptr && txn != nullptr) {
                recovery::LogRecord log(txn->get_id(), txn->get_prev_lsn(),
                                        recovery::LogRecordType::INSERT, table_name, new_tid,
                                        op.new_tuple);
                const auto lsn = log_manager_->append_log_record(log);
                txn->set_prev_lsn(lsn);
            }

            if (txn != nullptr) {
                txn->add_undo_log(transaction::UndoLog::Type::UPDATE, table_name, op.rid);
                txn->add_undo_log(transaction::UndoLog::Type::INSERT, table_name, new_tid);
            }
            rows_updated++;
        }
    }

    result.set_rows_affected(rows_updated);
    return result;
}

std::unique_ptr<Operator> QueryExecutor::build_plan(const parser::SelectStatement& stmt,
                                                    transaction::Transaction* txn) {
    /* 1. Base: SeqScan of the initial table */
    if (!stmt.from()) {
        return nullptr;
    }

    const std::string base_table_name = stmt.from()->to_string();
    auto base_table_meta_opt = catalog_.get_table_by_name(base_table_name);
    if (!base_table_meta_opt.has_value()) {
        return nullptr;
    }
    const auto* base_table_meta = base_table_meta_opt.value();

    Schema base_schema;
    for (const auto& col : base_table_meta->columns) {
        base_schema.add_column(col.name, col.type);
    }

    std::unique_ptr<Operator> current_root = std::make_unique<SeqScanOperator>(
        std::make_unique<storage::HeapTable>(base_table_name, storage_manager_, base_schema), txn,
        &lock_manager_);

    /* 2. Add JOINs */
    for (const auto& join : stmt.joins()) {
        const std::string join_table_name = join.table->to_string();
        auto join_table_meta_opt = catalog_.get_table_by_name(join_table_name);
        if (!join_table_meta_opt.has_value()) {
            return nullptr;
        }
        const auto* join_table_meta = join_table_meta_opt.value();

        Schema join_schema;
        for (const auto& col : join_table_meta->columns) {
            join_schema.add_column(col.name, col.type);
        }

        auto join_scan = std::make_unique<SeqScanOperator>(
            std::make_unique<storage::HeapTable>(join_table_name, storage_manager_, join_schema),
            txn, &lock_manager_);

        /* For now, we use HashJoin if a condition exists, otherwise NestedLoop would be needed.
         * Note: HashJoin requires equality condition. We'll assume equality for now or default to
         * NLJ. Currently cloudSQL only has HashJoin implemented in operator.cpp.
         */

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
            current_root =
                std::make_unique<HashJoinOperator>(std::move(current_root), std::move(join_scan),
                                                   std::move(left_key), std::move(right_key));
        } else {
            /* TODO: Implement NestedLoopJoin for non-equality or missing conditions */
            return nullptr;
        }
    }

    /* 3. Filter (WHERE) */
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
        storage::BTreeIndex idx(idx_info->name, storage_manager_, common::ValueType::TYPE_NULL);
        static_cast<void>(idx.drop());
    }

    /* 2. Drop table physical file */
    storage::HeapTable table(stmt.table_name(), storage_manager_, executor::Schema());
    static_cast<void>(table.drop());

    /* 3. Update catalog */
    if (!catalog_.drop_table(table_id)) {
        result.set_error("Failed to drop table from catalog");
        return result;
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
    storage::BTreeIndex idx(stmt.index_name(), storage_manager_, common::ValueType::TYPE_NULL);
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
