/**
 * @file analytics_tests.cpp
 * @brief Integration tests for columnar storage and vectorized execution
 */

#include <gtest/gtest.h>
#include <memory>
#include <vector>

#include "storage/columnar_table.hpp"
#include "storage/storage_manager.hpp"
#include "executor/vectorized_operator.hpp"
#include "parser/expression.hpp"

using namespace cloudsql;
using namespace cloudsql::storage;
using namespace cloudsql::executor;
using namespace cloudsql::parser;

namespace {

TEST(AnalyticsTests, ColumnarTableLifecycle) {
    StorageManager storage("./test_analytics");
    Schema schema;
    schema.add_column("id", common::ValueType::TYPE_INT64);
    
    ColumnarTable table("analytics_test", storage, schema);
    ASSERT_TRUE(table.create());
    ASSERT_TRUE(table.open());

    // 1. Create a batch
    VectorBatch batch;
    auto col = std::make_unique<NumericVector<int64_t>>(common::ValueType::TYPE_INT64);
    batch.add_column(std::move(col));

    for (int64_t i = 0; i < 100; ++i) {
        batch.append_tuple(Tuple({common::Value::make_int64(i)}));
    }

    // 2. Append to table
    ASSERT_TRUE(table.append_batch(batch));
    EXPECT_EQ(table.row_count(), 100);

    // 3. Scan via vectorized operator
    auto table_ptr = std::make_shared<ColumnarTable>(table);
    VectorizedSeqScanOperator scan("analytics_test", table_ptr);
    
    VectorBatch result_batch;
    auto res_col = std::make_unique<NumericVector<int64_t>>(common::ValueType::TYPE_INT64);
    result_batch.add_column(std::move(res_col));

    ASSERT_TRUE(scan.next_batch(result_batch));
    EXPECT_EQ(result_batch.row_count(), 100);
    EXPECT_EQ(result_batch.get_column(0).get(50).as_int64(), 50);
}

TEST(AnalyticsTests, VectorizedExecutionPipeline) {
    StorageManager storage("./test_analytics");
    Schema schema;
    schema.add_column("id", common::ValueType::TYPE_INT64);
    schema.add_column("val", common::ValueType::TYPE_INT64);
    
    auto table = std::make_shared<ColumnarTable>("pipeline_test", storage, schema);
    ASSERT_TRUE(table->create());
    ASSERT_TRUE(table->open());

    // 1. Populate table with 1000 rows
    auto input_batch = VectorBatch::create(schema);

    for (int64_t i = 0; i < 1000; ++i) {
        std::vector<common::Value> row;
        row.push_back(common::Value::make_int64(i));
        row.push_back(common::Value::make_int64(i * 2));
        input_batch->append_tuple(Tuple(std::move(row)));
    }
    ASSERT_TRUE(table->append_batch(*input_batch));

    // 2. Build Pipeline: Scan -> Filter(id > 500) -> Project(val)
    auto scan = std::make_unique<VectorizedSeqScanOperator>("pipeline_test", table);
    
    // Filter condition: id > 500
    auto col_expr = std::make_unique<ColumnExpr>("id");
    auto const_expr = std::make_unique<ConstantExpr>(common::Value::make_int64(500));
    auto filter_cond = std::make_unique<BinaryExpr>(std::move(col_expr), TokenType::Gt, std::move(const_expr));
    
    auto filter = std::make_unique<VectorizedFilterOperator>(std::move(scan), std::move(filter_cond));

    // Project expressions: just the second column (val)
    std::vector<std::unique_ptr<Expression>> project_exprs;
    project_exprs.push_back(std::make_unique<ColumnExpr>("val"));
    
    Schema out_schema;
    out_schema.add_column("val", common::ValueType::TYPE_INT64);
    
    VectorizedProjectOperator project(std::move(filter), std::move(out_schema), std::move(project_exprs));

    // 3. Execute and Verify
    auto result_batch = VectorBatch::create(project.output_schema());
    int total_rows = 0;
    while (project.next_batch(*result_batch)) {
        total_rows += result_batch->row_count();
        // Verify values: id 501 -> val 1002, id 999 -> val 1998
        for (size_t i = 0; i < result_batch->row_count(); ++i) {
            int64_t val = result_batch->get_column(0).get(i).as_int64();
            EXPECT_GT(val, 1000);
            EXPECT_EQ(val % 2, 0);
        }
        result_batch->clear();
    }

    EXPECT_EQ(total_rows, 499); // 501 to 999 inclusive
}

TEST(AnalyticsTests, VectorizedAggregation) {
    StorageManager storage("./test_analytics");
    Schema schema;
    schema.add_column("val", common::ValueType::TYPE_INT64);
    
    auto table = std::make_shared<ColumnarTable>("agg_test", storage, schema);
    ASSERT_TRUE(table->create());
    ASSERT_TRUE(table->open());

    // 1. Populate table with 10 rows: [1, 2, 3, ..., 10]
    auto input_batch = VectorBatch::create(schema);
    for (int64_t i = 1; i <= 10; ++i) {
        input_batch->append_tuple(Tuple({common::Value::make_int64(i)}));
    }
    ASSERT_TRUE(table->append_batch(*input_batch));

    // 2. Build Agg Pipeline: Scan -> Aggregate(COUNT(*), SUM(val))
    auto scan = std::make_unique<VectorizedSeqScanOperator>("agg_test", table);
    
    Schema out_schema;
    out_schema.add_column("count", common::ValueType::TYPE_INT64);
    out_schema.add_column("sum", common::ValueType::TYPE_INT64);
    
    std::vector<VectorizedAggregateInfo> aggs = {
        {AggregateType::Count, -1},
        {AggregateType::Sum, 0}
    };
    
    VectorizedAggregateOperator agg(std::move(scan), std::move(out_schema), aggs);

    // 3. Execute and Verify
    auto result_batch = VectorBatch::create(agg.output_schema());
    ASSERT_TRUE(agg.next_batch(*result_batch));
    EXPECT_EQ(result_batch->row_count(), 1);
    EXPECT_EQ(result_batch->get_column(0).get(0).as_int64(), 10);    // COUNT
    EXPECT_EQ(result_batch->get_column(1).get(0).as_int64(), 55);    // SUM (1..10)
}

} // namespace
