/**
 * @file vectorized_operator_tests.cpp
 * @brief Unit tests for individual vectorized operators
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

class VectorizedSeqScanTests : public ::testing::Test {
   protected:
    void SetUp() override { storage_ = std::make_unique<StorageManager>("./test_scan_ops"); }
    void TearDown() override { storage_.reset(); }

    std::unique_ptr<StorageManager> storage_;
};

TEST_F(VectorizedSeqScanTests, EmptyTable) {
    Schema schema;
    schema.add_column("id", common::ValueType::TYPE_INT64);

    ColumnarTable table("empty_scan", *storage_, schema);
    ASSERT_TRUE(table.create());
    ASSERT_TRUE(table.open());

    auto table_ptr = std::make_shared<ColumnarTable>(table);
    VectorizedSeqScanOperator scan("empty_scan", table_ptr);

    auto batch = VectorBatch::create(schema);
    EXPECT_FALSE(scan.next_batch(*batch));
}

TEST_F(VectorizedSeqScanTests, SingleBatch) {
    Schema schema;
    schema.add_column("id", common::ValueType::TYPE_INT64);

    ColumnarTable table("single_batch_scan", *storage_, schema);
    ASSERT_TRUE(table.create());
    ASSERT_TRUE(table.open());

    auto batch = VectorBatch::create(schema);
    for (int64_t i = 0; i < 100; ++i) {
        batch->append_tuple(Tuple({common::Value::make_int64(i)}));
    }
    ASSERT_TRUE(table.append_batch(*batch));
    EXPECT_EQ(table.row_count(), 100);

    auto table_ptr = std::make_shared<ColumnarTable>(table);
    VectorizedSeqScanOperator scan("single_batch_scan", table_ptr);

    auto result = VectorBatch::create(schema);
    ASSERT_TRUE(scan.next_batch(*result));
    EXPECT_EQ(result->row_count(), 100);
    EXPECT_FALSE(scan.next_batch(*result));  // EOF

    for (size_t i = 0; i < 100; ++i) {
        EXPECT_EQ(result->get_column(0).get(i).as_int64(), static_cast<int64_t>(i));
    }
}

TEST_F(VectorizedSeqScanTests, NonAlignedBoundaries) {
    Schema schema;
    schema.add_column("val", common::ValueType::TYPE_INT64);

    ColumnarTable table("nonaligned_scan", *storage_, schema);
    ASSERT_TRUE(table.create());
    ASSERT_TRUE(table.open());

    auto batch = VectorBatch::create(schema);
    for (int64_t i = 0; i < 2500; ++i) {
        batch->append_tuple(Tuple({common::Value::make_int64(i)}));
    }
    ASSERT_TRUE(table.append_batch(*batch));
    EXPECT_EQ(table.row_count(), 2500);

    auto table_ptr = std::make_shared<ColumnarTable>(table);
    VectorizedSeqScanOperator scan("nonaligned_scan", table_ptr);

    auto result = VectorBatch::create(schema);
    int total = 0;
    while (scan.next_batch(*result)) {
        total += result->row_count();
        result->clear();
    }
    EXPECT_EQ(total, 2500);
}

TEST_F(VectorizedSeqScanTests, SequentialCallsUntilEOF) {
    Schema schema;
    schema.add_column("id", common::ValueType::TYPE_INT64);

    ColumnarTable table("sequential_scan", *storage_, schema);
    ASSERT_TRUE(table.create());
    ASSERT_TRUE(table.open());

    auto batch = VectorBatch::create(schema);
    for (int64_t i = 0; i < 3500; ++i) {
        batch->append_tuple(Tuple({common::Value::make_int64(i)}));
    }
    ASSERT_TRUE(table.append_batch(*batch));

    auto table_ptr = std::make_shared<ColumnarTable>(table);
    VectorizedSeqScanOperator scan("sequential_scan", table_ptr);

    auto result = VectorBatch::create(schema);
    int64_t expected = 0;
    int batch_count = 0;
    while (scan.next_batch(*result)) {
        ++batch_count;
        // Batches 1-3 are full (1024), batch 4 has remainder (428)
        size_t expected_batch_size = (batch_count < 4) ? 1024u : 428u;
        EXPECT_EQ(result->row_count(), expected_batch_size);
        for (size_t i = 0; i < result->row_count(); ++i) {
            EXPECT_EQ(result->get_column(0).get(i).as_int64(), expected++);
        }
        result->clear();
    }
    EXPECT_EQ(batch_count, 4);
    EXPECT_EQ(expected, 3500);
}

class VectorizedProjectTests : public ::testing::Test {
   protected:
    void SetUp() override { storage_ = std::make_unique<StorageManager>("./test_project_ops"); }
    void TearDown() override { storage_.reset(); }

    std::unique_ptr<StorageManager> storage_;
};

TEST_F(VectorizedProjectTests, EmptyInput) {
    Schema in_schema;
    in_schema.add_column("a", common::ValueType::TYPE_INT64);
    in_schema.add_column("b", common::ValueType::TYPE_INT64);

    ColumnarTable table("empty_project", *storage_, in_schema);
    ASSERT_TRUE(table.create());
    ASSERT_TRUE(table.open());

    auto table_ptr = std::make_shared<ColumnarTable>(table);
    auto scan = std::make_unique<VectorizedSeqScanOperator>("empty_project", table_ptr);

    Schema out_schema;
    out_schema.add_column("result", common::ValueType::TYPE_INT64);
    std::vector<std::unique_ptr<Expression>> exprs;
    exprs.push_back(std::make_unique<ColumnExpr>("a"));

    VectorizedProjectOperator project(std::move(scan), std::move(out_schema), std::move(exprs));

    auto result = VectorBatch::create(project.output_schema());
    EXPECT_FALSE(project.next_batch(*result));  // No input rows
}

TEST_F(VectorizedProjectTests, MultipleExpressions) {
    Schema schema;
    schema.add_column("a", common::ValueType::TYPE_INT64);
    schema.add_column("b", common::ValueType::TYPE_INT64);

    ColumnarTable table("multi_expr_project", *storage_, schema);
    ASSERT_TRUE(table.create());
    ASSERT_TRUE(table.open());

    auto batch = VectorBatch::create(schema);
    for (int64_t i = 0; i < 5; ++i) {
        batch->append_tuple(
            Tuple({common::Value::make_int64(i), common::Value::make_int64(i * 2)}));
    }
    ASSERT_TRUE(table.append_batch(*batch));

    auto table_ptr = std::make_shared<ColumnarTable>(table);
    auto scan = std::make_unique<VectorizedSeqScanOperator>("multi_expr_project", table_ptr);

    Schema out_schema;
    out_schema.add_column("sum", common::ValueType::TYPE_INT64);
    out_schema.add_column("product", common::ValueType::TYPE_INT64);

    std::vector<std::unique_ptr<Expression>> exprs;
    // a + b
    exprs.push_back(std::make_unique<BinaryExpr>(std::make_unique<ColumnExpr>("a"), TokenType::Plus,
                                                 std::make_unique<ColumnExpr>("b")));
    // a * b
    exprs.push_back(std::make_unique<BinaryExpr>(std::make_unique<ColumnExpr>("a"), TokenType::Star,
                                                 std::make_unique<ColumnExpr>("b")));

    VectorizedProjectOperator project(std::move(scan), std::move(out_schema), std::move(exprs));

    auto result = VectorBatch::create(project.output_schema());
    ASSERT_TRUE(project.next_batch(*result));
    EXPECT_EQ(result->row_count(), 5);

    for (size_t i = 0; i < 5; ++i) {
        int64_t a = static_cast<int64_t>(i);
        int64_t b = a * 2;
        EXPECT_EQ(result->get_column(0).get(i).as_int64(), a + b);  // sum
        EXPECT_EQ(result->get_column(1).get(i).as_int64(), a * b);  // product
    }
}

TEST_F(VectorizedProjectTests, ComputedExpression) {
    Schema schema;
    schema.add_column("x", common::ValueType::TYPE_INT64);

    ColumnarTable table("computed_project", *storage_, schema);
    ASSERT_TRUE(table.create());
    ASSERT_TRUE(table.open());

    auto batch = VectorBatch::create(schema);
    for (int64_t i = 0; i < 3; ++i) {
        batch->append_tuple(Tuple({common::Value::make_int64(i)}));
    }
    ASSERT_TRUE(table.append_batch(*batch));

    auto table_ptr = std::make_shared<ColumnarTable>(table);
    auto scan = std::make_unique<VectorizedSeqScanOperator>("computed_project", table_ptr);

    Schema out_schema;
    out_schema.add_column("doubled_plus_one", common::ValueType::TYPE_INT64);

    std::vector<std::unique_ptr<Expression>> exprs;
    // (x * 2) + 1
    exprs.push_back(std::make_unique<BinaryExpr>(
        std::make_unique<BinaryExpr>(std::make_unique<ColumnExpr>("x"), TokenType::Star,
                                     std::make_unique<ConstantExpr>(common::Value::make_int64(2))),
        TokenType::Plus, std::make_unique<ConstantExpr>(common::Value::make_int64(1))));

    VectorizedProjectOperator project(std::move(scan), std::move(out_schema), std::move(exprs));

    auto result = VectorBatch::create(project.output_schema());
    ASSERT_TRUE(project.next_batch(*result));
    EXPECT_EQ(result->row_count(), 3);
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(result->get_column(0).get(i).as_int64(), static_cast<int64_t>(i) * 2 + 1);
    }
}

class VectorizedAggregateTests : public ::testing::Test {
   protected:
    void SetUp() override { storage_ = std::make_unique<StorageManager>("./test_agg_ops"); }
    void TearDown() override { storage_.reset(); }

    std::unique_ptr<StorageManager> storage_;
};

TEST_F(VectorizedAggregateTests, CountOnlyEmpty) {
    // COUNT(*) on empty table should return 0 per SQL spec
    Schema schema;
    schema.add_column("val", common::ValueType::TYPE_INT64);

    ColumnarTable table("count_empty_agg", *storage_, schema);
    ASSERT_TRUE(table.create());
    ASSERT_TRUE(table.open());

    auto table_ptr = std::make_shared<ColumnarTable>(table);
    auto scan = std::make_unique<VectorizedSeqScanOperator>("count_empty_agg", table_ptr);

    Schema out_schema;
    out_schema.add_column("cnt", common::ValueType::TYPE_INT64);

    std::vector<VectorizedAggregateInfo> aggs = {{AggregateType::Count, -1}};

    VectorizedAggregateOperator agg(std::move(scan), std::move(out_schema), aggs);

    auto result = VectorBatch::create(agg.output_schema());
    ASSERT_TRUE(agg.next_batch(*result));
    EXPECT_EQ(result->row_count(), 1);
    EXPECT_EQ(result->get_column(0).get(0).as_int64(), 0);  // COUNT(*) = 0 for empty
}

TEST_F(VectorizedAggregateTests, SumWithFloat64) {
    Schema schema;
    schema.add_column("fval", common::ValueType::TYPE_FLOAT64);

    ColumnarTable table("float_agg", *storage_, schema);
    ASSERT_TRUE(table.create());
    ASSERT_TRUE(table.open());

    auto batch = VectorBatch::create(schema);
    for (int64_t i = 1; i <= 4; ++i) {
        batch->append_tuple(Tuple({common::Value::make_float64(static_cast<double>(i))}));
    }
    ASSERT_TRUE(table.append_batch(*batch));

    auto table_ptr = std::make_shared<ColumnarTable>(table);
    auto scan = std::make_unique<VectorizedSeqScanOperator>("float_agg", table_ptr);

    Schema out_schema;
    out_schema.add_column("cnt", common::ValueType::TYPE_INT64);
    out_schema.add_column("sum_f", common::ValueType::TYPE_FLOAT64);

    std::vector<VectorizedAggregateInfo> aggs = {{AggregateType::Count, -1},
                                                 {AggregateType::Sum, 0}};

    VectorizedAggregateOperator agg(std::move(scan), std::move(out_schema), aggs);

    auto result = VectorBatch::create(agg.output_schema());
    ASSERT_TRUE(agg.next_batch(*result));
    EXPECT_EQ(result->row_count(), 1);
    EXPECT_EQ(result->get_column(0).get(0).as_int64(), 4);
    EXPECT_DOUBLE_EQ(result->get_column(1).get(0).to_float64(), 10.0);  // 1+2+3+4
}

TEST_F(VectorizedAggregateTests, CountOnly) {
    Schema schema;
    schema.add_column("val", common::ValueType::TYPE_INT64);

    ColumnarTable table("count_agg", *storage_, schema);
    ASSERT_TRUE(table.create());
    ASSERT_TRUE(table.open());

    auto batch = VectorBatch::create(schema);
    for (int64_t i = 0; i < 7; ++i) {
        batch->append_tuple(Tuple({common::Value::make_int64(i)}));
    }
    ASSERT_TRUE(table.append_batch(*batch));

    auto table_ptr = std::make_shared<ColumnarTable>(table);
    auto scan = std::make_unique<VectorizedSeqScanOperator>("count_agg", table_ptr);

    Schema out_schema;
    out_schema.add_column("cnt", common::ValueType::TYPE_INT64);

    std::vector<VectorizedAggregateInfo> aggs = {{AggregateType::Count, -1}};

    VectorizedAggregateOperator agg(std::move(scan), std::move(out_schema), aggs);

    auto result = VectorBatch::create(agg.output_schema());
    ASSERT_TRUE(agg.next_batch(*result));
    EXPECT_EQ(result->get_column(0).get(0).as_int64(), 7);
}

class VectorizedFilterTests : public ::testing::Test {
   protected:
    void SetUp() override { storage_ = std::make_unique<StorageManager>("./test_filter_ops"); }
    void TearDown() override { storage_.reset(); }

    std::unique_ptr<StorageManager> storage_;
};

TEST_F(VectorizedFilterTests, FilterWithNoMatches) {
    Schema schema;
    schema.add_column("id", common::ValueType::TYPE_INT64);

    ColumnarTable table("no_match_filter", *storage_, schema);
    ASSERT_TRUE(table.create());
    ASSERT_TRUE(table.open());

    auto batch = VectorBatch::create(schema);
    for (int64_t i = 0; i < 10; ++i) {
        batch->append_tuple(Tuple({common::Value::make_int64(i)}));
    }
    ASSERT_TRUE(table.append_batch(*batch));

    auto table_ptr = std::make_shared<ColumnarTable>(table);
    auto scan = std::make_unique<VectorizedSeqScanOperator>("no_match_filter", table_ptr);

    // Filter: id > 100 (no matches)
    auto cond = std::make_unique<BinaryExpr>(
        std::make_unique<ColumnExpr>("id"), TokenType::Gt,
        std::make_unique<ConstantExpr>(common::Value::make_int64(100)));

    VectorizedFilterOperator filter(std::move(scan), std::move(cond));

    auto result = VectorBatch::create(filter.output_schema());
    EXPECT_FALSE(filter.next_batch(*result));  // No matches
}

TEST_F(VectorizedFilterTests, FilterAllMatch) {
    Schema schema;
    schema.add_column("val", common::ValueType::TYPE_INT64);

    ColumnarTable table("all_match_filter", *storage_, schema);
    ASSERT_TRUE(table.create());
    ASSERT_TRUE(table.open());

    auto batch = VectorBatch::create(schema);
    for (int64_t i = 0; i < 20; ++i) {
        batch->append_tuple(Tuple({common::Value::make_int64(i)}));
    }
    ASSERT_TRUE(table.append_batch(*batch));

    auto table_ptr = std::make_shared<ColumnarTable>(table);
    auto scan = std::make_unique<VectorizedSeqScanOperator>("all_match_filter", table_ptr);

    // Filter: val >= 0 (all match)
    auto cond =
        std::make_unique<BinaryExpr>(std::make_unique<ColumnExpr>("val"), TokenType::Ge,
                                     std::make_unique<ConstantExpr>(common::Value::make_int64(0)));

    VectorizedFilterOperator filter(std::move(scan), std::move(cond));

    auto result = VectorBatch::create(filter.output_schema());
    ASSERT_TRUE(filter.next_batch(*result));
    EXPECT_EQ(result->row_count(), 20);
}

TEST_F(VectorizedFilterTests, PipelinedBatches) {
    // Tests that filter processes child batches and accumulates results
    // until matches are found, then returns them in pipelined fashion
    Schema schema;
    schema.add_column("id", common::ValueType::TYPE_INT64);

    ColumnarTable table("pipelined_filter", *storage_, schema);
    ASSERT_TRUE(table.create());
    ASSERT_TRUE(table.open());

    auto batch = VectorBatch::create(schema);
    for (int64_t i = 0; i < 2500; ++i) {
        batch->append_tuple(Tuple({common::Value::make_int64(i)}));
    }
    ASSERT_TRUE(table.append_batch(*batch));

    auto table_ptr = std::make_shared<ColumnarTable>(table);
    auto scan = std::make_unique<VectorizedSeqScanOperator>("pipelined_filter", table_ptr);

    // Filter: id >= 1500
    auto cond = std::make_unique<BinaryExpr>(
        std::make_unique<ColumnExpr>("id"), TokenType::Ge,
        std::make_unique<ConstantExpr>(common::Value::make_int64(1500)));

    VectorizedFilterOperator filter(std::move(scan), std::move(cond));

    auto result = VectorBatch::create(filter.output_schema());
    int total = 0;
    while (filter.next_batch(*result)) {
        int batch_rows = result->row_count();
        total += batch_rows;
        // Verify values
        for (size_t i = 0; i < result->row_count(); ++i) {
            EXPECT_GE(result->get_column(0).get(i).as_int64(), 1500);
        }
        result->clear();
    }
    // 1500 to 2499 inclusive = 1000 rows
    EXPECT_EQ(total, 1000);
}

}  // namespace