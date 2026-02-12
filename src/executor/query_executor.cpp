/**
 * @file query_executor.cpp
 * @brief High-level query executor implementation
 */

#include "executor/query_executor.hpp"
#include <chrono>

namespace cloudsql {
namespace executor {

QueryExecutor::QueryExecutor(Catalog& catalog, storage::StorageManager& storage_manager)
    : catalog_(catalog), storage_manager_(storage_manager) {}

QueryResult QueryExecutor::execute(const parser::Statement& stmt) {
    auto start = std::chrono::high_resolution_clock::now();
    QueryResult result;

    try {
        switch (stmt.type()) {
            case parser::StmtType::Select:
                result = execute_select(static_cast<const parser::SelectStatement&>(stmt));
                break;
            case parser::StmtType::CreateTable:
                result = execute_create_table(static_cast<const parser::CreateTableStatement&>(stmt));
                break;
            case parser::StmtType::Insert:
                result = execute_insert(static_cast<const parser::InsertStatement&>(stmt));
                break;
            default:
                result.set_error("Unsupported statement type");
                break;
        }
    } catch (const std::exception& e) {
        result.set_error(std::string("Execution error: ") + e.what());
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    result.set_execution_time(duration.count());
    
    return result;
}

QueryResult QueryExecutor::execute_select(const parser::SelectStatement& stmt) {
    QueryResult result;
    
    /* Build execution plan */
    auto root = build_plan(stmt);
    if (!root) {
        result.set_error("Failed to build execution plan");
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
        common::ValueType type = common::TYPE_TEXT;
        if (col.type_ == "INT" || col.type_ == "INTEGER") type = common::TYPE_INT32;
        else if (col.type_ == "BIGINT") type = common::TYPE_INT64;
        else if (col.type_ == "FLOAT" || col.type_ == "DOUBLE") type = common::TYPE_FLOAT64;
        else if (col.type_ == "BOOLEAN" || col.type_ == "BOOL") type = common::TYPE_BOOL;
        
        catalog_cols.emplace_back(col.name_, type, pos++);
    }

    /* Update catalog */
    oid_t table_id = catalog_.create_table(stmt.table_name(), std::move(catalog_cols));
    if (table_id == 0) {
        result.set_error("Failed to create table in catalog");
        return result;
    }

    /* Create physical file */
    auto table_info = catalog_.get_table(table_id);
    storage::HeapTable table((*table_info)->name, storage_manager_, executor::Schema());
    if (!table.create()) {
        catalog_.drop_table(table_id);
        result.set_error("Failed to create table file");
        return result;
    }

    result.set_rows_affected(1);
    return result;
}

QueryResult QueryExecutor::execute_insert(const parser::InsertStatement& stmt) {
    QueryResult result;
    
    if (!stmt.table()) {
        result.set_error("Target table not specified");
        return result;
    }

    std::string table_name = stmt.table()->to_string();
    auto table_meta = catalog_.get_table_by_name(table_name);
    if (!table_meta) {
        result.set_error("Table not found: " + table_name);
        return result;
    }

    /* Construct Schema */
    Schema schema;
    for (const auto& col : (*table_meta)->columns) {
        schema.add_column(col.name, col.type);
    }

    storage::HeapTable table(table_name, storage_manager_, schema);
    
    uint64_t rows_inserted = 0;
    for (const auto& row_exprs : stmt.values()) {
        std::vector<common::Value> values;
        for (const auto& expr : row_exprs) {
            values.push_back(expr->evaluate());
        }
        
        table.insert(Tuple(std::move(values)));
        rows_inserted++;
    }

    result.set_rows_affected(rows_inserted);
    return result;
}

std::unique_ptr<Operator> QueryExecutor::build_plan(const parser::SelectStatement& stmt) {
    /* 1. Base: SeqScan */
    if (!stmt.from()) return nullptr;
    
    std::string table_name = stmt.from()->to_string();
    auto table_meta = catalog_.get_table_by_name(table_name);
    if (!table_meta) return nullptr;

    /* Construct Schema for HeapTable */
    Schema schema;
    for (const auto& col : (*table_meta)->columns) {
        schema.add_column(col.name, col.type);
    }

    auto scan = std::make_unique<SeqScanOperator>(
        std::make_unique<storage::HeapTable>(table_name, storage_manager_, schema)
    );

    std::unique_ptr<Operator> current_root = std::move(scan);

    /* 2. Filter (WHERE) */
    if (stmt.where()) {
        current_root = std::make_unique<FilterOperator>(
            std::move(current_root),
            stmt.where()->clone()
        );
    }

    /* 3. Project (SELECT columns) */
    if (!stmt.columns().empty()) {
        std::vector<std::unique_ptr<parser::Expression>> projection;
        for (const auto& col : stmt.columns()) {
            projection.push_back(col->clone());
        }
        current_root = std::make_unique<ProjectOperator>(
            std::move(current_root),
            std::move(projection)
        );
    }

    /* 4. Limit */
    if (stmt.has_limit() || stmt.has_offset()) {
        current_root = std::make_unique<LimitOperator>(
            std::move(current_root),
            stmt.limit(),
            stmt.offset()
        );
    }

    return current_root;
}

} // namespace executor
} // namespace cloudsql
