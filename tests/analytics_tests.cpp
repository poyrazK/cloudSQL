/**
 * @file analytics_tests.cpp
 * @brief Integration tests for columnar storage and vectorized execution
 */

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "executor/vectorized_operator.hpp"
#include "parser/expression.hpp"
#include "storage/columnar_table.hpp"
#include "storage/storage_manager.hpp"

using namespace cloudsql;
using namespace cloudsql::storage;
using namespace cloudsql::executor;
using namespace cloudsql::parser;

namespace {

TEST(AnalyticsTests, ColumnarTableLifecycle) {
    StorageManager storage("./test_analytics");
    Schema schema;
    schema.add_column("id", common::ValueType::TYPE_INT64);
    schema.add_column("maybe_val", common::ValueType::TYPE_INT64, true);
    schema.add_column("float_val", common::ValueType::TYPE_FLOAT64);

    ColumnarTable table("lifecycle_test", storage, schema);
    ASSERT_TRUE(table.create());
    ASSERT_TRUE(table.open());

    // 1. Create and populate a batch with mixed types and nulls
    auto batch = VectorBatch::create(schema);
    for (int64_t i = 0; i < 100; ++i) {
        std::vector<common::Value> row;
        row.push_back(common::Value::make_int64(i));

        // Populate maybe_val: null for even rows, value for odd rows
        if (i % 2 == 0) {
            row.push_back(common::Value::make_null());
        } else {
            row.push_back(common::Value::make_int64(i * 10));
        }

        row.push_back(common::Value::make_float64(static_cast<double>(i) + 0.5));
        batch->append_tuple(Tuple(std::move(row)));
    }

    // 2. Persist to storage
    ASSERT_TRUE(table.append_batch(*batch));
    EXPECT_EQ(table.row_count(), 100);

    // 3. Scan and verify round-trip integrity
    auto table_ptr = std::make_shared<ColumnarTable>(table);
    VectorizedSeqScanOperator scan("lifecycle_test", table_ptr);

    auto result_batch = VectorBatch::create(schema);
    ASSERT_TRUE(scan.next_batch(*result_batch));
    EXPECT_EQ(result_batch->row_count(), 100);

    for (size_t i = 0; i < 100; ++i) {
        // Verify INT64 id
        EXPECT_EQ(result_batch->get_column(0).get(i).as_int64(), static_cast<int64_t>(i));

        // Verify Nullable INT64 maybe_val
        if (i % 2 == 0) {
            EXPECT_TRUE(result_batch->get_column(1).is_null(i));
        } else {
            EXPECT_EQ(result_batch->get_column(1).get(i).as_int64(), static_cast<int64_t>(i * 10));
        }

        // Verify FLOAT64 float_val (exact match for binary representation)
        EXPECT_DOUBLE_EQ(result_batch->get_column(2).get(i).to_float64(),
                         static_cast<double>(i) + 0.5);
    }
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
    auto filter_cond =
        std::make_unique<BinaryExpr>(std::move(col_expr), TokenType::Gt, std::move(const_expr));

    auto filter =
        std::make_unique<VectorizedFilterOperator>(std::move(scan), std::move(filter_cond));

    // Project expressions: just the second column (val)
    std::vector<std::unique_ptr<Expression>> project_exprs;
    project_exprs.push_back(std::make_unique<ColumnExpr>("val"));

    Schema out_schema;
    out_schema.add_column("val", common::ValueType::TYPE_INT64);

    VectorizedProjectOperator project(std::move(filter), std::move(out_schema),
                                      std::move(project_exprs));

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

    EXPECT_EQ(total_rows, 499);  // 501 to 999 inclusive
}

TEST(AnalyticsTests, VectorizedAggregation) {
    StorageManager storage("./test_analytics");
    Schema schema;
    schema.add_column("val", common::ValueType::TYPE_INT64);
    schema.add_column("fval", common::ValueType::TYPE_FLOAT64);

    auto table = std::make_shared<ColumnarTable>("agg_test", storage, schema);
    ASSERT_TRUE(table->create());
    ASSERT_TRUE(table->open());

    // 1. Populate table with 10 rows: val=[1..10], fval=[1.5..10.5]
    auto input_batch = VectorBatch::create(schema);
    for (int64_t i = 1; i <= 10; ++i) {
        std::vector<common::Value> row;
        row.push_back(common::Value::make_int64(i));
        row.push_back(common::Value::make_float64(static_cast<double>(i) + 0.5));
        input_batch->append_tuple(Tuple(std::move(row)));
    }
    ASSERT_TRUE(table->append_batch(*input_batch));

    // 2. Build Agg Pipeline: Scan -> Aggregate(COUNT(*), SUM(val), SUM(fval))
    auto scan = std::make_unique<VectorizedSeqScanOperator>("agg_test", table);

    Schema out_schema;
    out_schema.add_column("count", common::ValueType::TYPE_INT64);
    out_schema.add_column("sum_i", common::ValueType::TYPE_INT64);
    out_schema.add_column("sum_f", common::ValueType::TYPE_FLOAT64);

    std::vector<VectorizedAggregateInfo> aggs = {
        {AggregateType::Count, -1}, {AggregateType::Sum, 0}, {AggregateType::Sum, 1}};

    VectorizedAggregateOperator agg(std::move(scan), std::move(out_schema), aggs);

    // 3. Execute and Verify
    auto result_batch = VectorBatch::create(agg.output_schema());
    ASSERT_TRUE(agg.next_batch(*result_batch));
    EXPECT_EQ(result_batch->row_count(), 1);

    // COUNT(*) -> 10
    EXPECT_EQ(result_batch->get_column(0).get(0).as_int64(), 10);
    // SUM(val) -> 55
    EXPECT_EQ(result_batch->get_column(1).get(0).as_int64(), 55);
    // SUM(fval) -> (1..10) + 10*0.5 = 55 + 5 = 60.0
    EXPECT_DOUBLE_EQ(result_batch->get_column(2).get(0).to_float64(), 60.0);
}

TEST(AnalyticsTests, AggregateNullHandling) {
    StorageManager storage("./test_analytics");
    Schema schema;
    schema.add_column("val", common::ValueType::TYPE_INT64, true);

    auto table = std::make_shared<ColumnarTable>("null_agg_test", storage, schema);
    ASSERT_TRUE(table->create());
    ASSERT_TRUE(table->open());

    // 1. Populate table with 5 NULLs
    auto input_batch = VectorBatch::create(schema);
    for (int i = 0; i < 5; ++i) {
        input_batch->append_tuple(Tuple({common::Value::make_null()}));
    }
    ASSERT_TRUE(table->append_batch(*input_batch));

    // 2. Build Agg Pipeline: Scan -> Aggregate(COUNT(*), SUM(val))
    auto scan = std::make_unique<VectorizedSeqScanOperator>("null_agg_test", table);

    Schema out_schema;
    out_schema.add_column("count", common::ValueType::TYPE_INT64);
    out_schema.add_column("sum", common::ValueType::TYPE_INT64);

    std::vector<VectorizedAggregateInfo> aggs = {{AggregateType::Count, -1},
                                                 {AggregateType::Sum, 0}};

    VectorizedAggregateOperator agg(std::move(scan), std::move(out_schema), aggs);

    // 3. Verify: COUNT(*) should be 5, SUM(val) should be NULL
    auto result_batch = VectorBatch::create(agg.output_schema());
    ASSERT_TRUE(agg.next_batch(*result_batch));

    EXPECT_EQ(result_batch->get_column(0).get(0).as_int64(), 5);
    EXPECT_TRUE(result_batch->get_column(1).is_null(0));
}

TEST(AnalyticsTests, VectorizedExpressionAdvanced) {
    StorageManager storage("./test_analytics");
    Schema schema;
    schema.add_column("a", common::ValueType::TYPE_INT64, true);
    schema.add_column("b", common::ValueType::TYPE_INT64, true);

    auto batch = VectorBatch::create(schema);
    // Row 0: (10, 20)
    batch->append_tuple(Tuple({common::Value::make_int64(10), common::Value::make_int64(20)}));
    // Row 1: (NULL, 30)
    batch->append_tuple(Tuple({common::Value::make_null(), common::Value::make_int64(30)}));
    // Row 2: (40, NULL)
    batch->append_tuple(Tuple({common::Value::make_int64(40), common::Value::make_null()}));

    // Test: (a IS NULL) OR (a > 20)
    auto col_a = std::make_unique<ColumnExpr>("a");
    auto is_null = std::make_unique<IsNullExpr>(std::move(col_a), false);
    auto col_a_2 = std::make_unique<ColumnExpr>("a");
    auto gt_20 =
        std::make_unique<BinaryExpr>(std::move(col_a_2), TokenType::Gt,
                                     std::make_unique<ConstantExpr>(common::Value::make_int64(20)));

    BinaryExpr or_expr(std::move(is_null), TokenType::Or, std::move(gt_20));

    NumericVector<bool> res(common::ValueType::TYPE_BOOL);
    or_expr.evaluate_vectorized(*batch, schema, res);

    ASSERT_EQ(res.size(), 3U);
    // Row 0: (10 IS NULL) OR (10 > 20) -> FALSE OR FALSE -> FALSE
    EXPECT_FALSE(res.get(0).as_bool());
    // Row 1: (NULL IS NULL) OR (NULL > 20) -> TRUE OR NULL -> TRUE
    EXPECT_TRUE(res.get(1).as_bool());
    // Row 2: (40 IS NULL) OR (40 > 20) -> FALSE OR TRUE -> TRUE
    EXPECT_TRUE(res.get(2).as_bool());
}

}  // namespace
