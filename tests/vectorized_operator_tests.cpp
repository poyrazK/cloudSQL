/**
 * @file vectorized_operator_tests.cpp
 * @brief Unit tests for individual vectorized operators
 */

#include <gtest/gtest.h>

#include <algorithm>
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
    VectorizedSeqScanOperator scan("sequential_scan", table_ptr, nullptr);

    auto result = VectorBatch::create(schema);
    int64_t expected = 0;
    int batch_count = 0;
    while (scan.next_batch(*result)) {
        ++batch_count;
        // batch_size_ is 4096, so 3500 rows fit in 1 batch
        EXPECT_EQ(result->row_count(), 3500u);
        for (size_t i = 0; i < result->row_count(); ++i) {
            EXPECT_EQ(result->get_column(0).get(i).as_int64(), expected++);
        }
        result->clear();
    }
    EXPECT_EQ(batch_count, 1);
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

class VectorizedGroupByTests : public ::testing::Test {
   protected:
    void SetUp() override { storage_ = std::make_unique<StorageManager>("./test_groupby"); }
    void TearDown() override { storage_.reset(); }

    std::unique_ptr<StorageManager> storage_;
};

TEST_F(VectorizedGroupByTests, SingleGroup) {
    // GROUP BY with single group (same as global aggregation)
    Schema schema;
    schema.add_column("cat", common::ValueType::TYPE_TEXT);
    schema.add_column("val", common::ValueType::TYPE_INT64);

    ColumnarTable table("single_group", *storage_, schema);
    ASSERT_TRUE(table.create());
    ASSERT_TRUE(table.open());

    // Insert 10 rows all with same category
    auto batch = VectorBatch::create(schema);
    for (int64_t i = 0; i < 10; ++i) {
        batch->append_tuple(
            Tuple({common::Value::make_text("A"), common::Value::make_int64(i + 1)}));
    }
    ASSERT_TRUE(table.append_batch(*batch));

    // SELECT cat, COUNT(*), SUM(val) FROM t GROUP BY cat
    // Expected: 1 group (cat=A, count=10, sum=55)
    auto scan = std::make_unique<VectorizedSeqScanOperator>("single_group",
                                                            std::make_shared<ColumnarTable>(table));

    Schema out_schema;
    out_schema.add_column("cat", common::ValueType::TYPE_TEXT);
    out_schema.add_column("cnt", common::ValueType::TYPE_INT64);
    out_schema.add_column("sum", common::ValueType::TYPE_FLOAT64);

    std::vector<std::unique_ptr<parser::Expression>> group_by;
    group_by.push_back(std::make_unique<ColumnExpr>("cat"));

    std::vector<VectorizedAggregateInfo> aggs = {
        {AggregateType::Count, -1},  // COUNT(*)
        {AggregateType::Sum, 1}      // SUM(val)
    };

    VectorizedGroupByOperator groupby(std::move(scan), std::move(group_by), std::move(aggs),
                                      std::move(out_schema));

    auto result = VectorBatch::create(groupby.output_schema());
    ASSERT_TRUE(groupby.next_batch(*result));
    EXPECT_EQ(result->row_count(), 1);
    EXPECT_EQ(result->get_column(0).get(0).as_text(), "A");
    EXPECT_EQ(result->get_column(1).get(0).as_int64(), 10);      // COUNT(*)
    EXPECT_EQ(result->get_column(2).get(0).to_float64(), 55.0);  // SUM(1..10)

    EXPECT_FALSE(groupby.next_batch(*result));  // EOF
}

TEST_F(VectorizedGroupByTests, MultipleGroups) {
    // GROUP BY with 3 distinct groups
    Schema schema;
    schema.add_column("cat", common::ValueType::TYPE_TEXT);
    schema.add_column("val", common::ValueType::TYPE_INT64);

    ColumnarTable table("multi_group", *storage_, schema);
    ASSERT_TRUE(table.create());
    ASSERT_TRUE(table.open());

    // Insert 9 rows: 3 each of A, B, C
    auto batch = VectorBatch::create(schema);
    for (int64_t i = 0; i < 3; ++i) {
        batch->append_tuple(Tuple({common::Value::make_text("A"), common::Value::make_int64(10)}));
        batch->append_tuple(Tuple({common::Value::make_text("B"), common::Value::make_int64(20)}));
        batch->append_tuple(Tuple({common::Value::make_text("C"), common::Value::make_int64(30)}));
    }
    ASSERT_TRUE(table.append_batch(*batch));

    auto scan = std::make_unique<VectorizedSeqScanOperator>("multi_group",
                                                            std::make_shared<ColumnarTable>(table));

    Schema out_schema;
    out_schema.add_column("cat", common::ValueType::TYPE_TEXT);
    out_schema.add_column("cnt", common::ValueType::TYPE_INT64);

    std::vector<std::unique_ptr<parser::Expression>> group_by;
    group_by.push_back(std::make_unique<ColumnExpr>("cat"));

    std::vector<VectorizedAggregateInfo> aggs = {{AggregateType::Count, -1}};

    VectorizedGroupByOperator groupby(std::move(scan), std::move(group_by), std::move(aggs),
                                      std::move(out_schema));

    auto result = VectorBatch::create(groupby.output_schema());
    ASSERT_TRUE(groupby.next_batch(*result));
    EXPECT_EQ(result->row_count(), 3);  // 3 groups

    // Verify all 3 groups have count=3
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(result->get_column(1).get(i).as_int64(), 3);
    }
}

TEST_F(VectorizedGroupByTests, EmptyInput) {
    // GROUP BY on empty table should return 0 groups
    Schema schema;
    schema.add_column("cat", common::ValueType::TYPE_TEXT);
    schema.add_column("val", common::ValueType::TYPE_INT64);

    ColumnarTable table("empty_groupby", *storage_, schema);
    ASSERT_TRUE(table.create());
    ASSERT_TRUE(table.open());

    auto scan = std::make_unique<VectorizedSeqScanOperator>("empty_groupby",
                                                            std::make_shared<ColumnarTable>(table));

    Schema out_schema;
    out_schema.add_column("cat", common::ValueType::TYPE_TEXT);
    out_schema.add_column("cnt", common::ValueType::TYPE_INT64);

    std::vector<std::unique_ptr<parser::Expression>> group_by;
    group_by.push_back(std::make_unique<ColumnExpr>("cat"));

    std::vector<VectorizedAggregateInfo> aggs = {{AggregateType::Count, -1}};

    VectorizedGroupByOperator groupby(std::move(scan), std::move(group_by), std::move(aggs),
                                      std::move(out_schema));

    auto result = VectorBatch::create(groupby.output_schema());
    EXPECT_FALSE(groupby.next_batch(*result));  // No groups from empty input
}

TEST_F(VectorizedGroupByTests, MultiBatchGroups) {
    // 2500 rows with 10 groups using TEXT keys, verify groups span multiple input batches
    Schema schema;
    schema.add_column("cat", common::ValueType::TYPE_TEXT);
    schema.add_column("val", common::ValueType::TYPE_INT64);

    ColumnarTable table("multibatch_group", *storage_, schema);
    ASSERT_TRUE(table.create());
    ASSERT_TRUE(table.open());

    // 2500 rows: cat = "cat_" + (i % 10) (10 groups, ~250 rows each)
    auto batch = VectorBatch::create(schema);
    for (int64_t i = 0; i < 2500; ++i) {
        std::string cat = "cat_" + std::to_string(i % 10);
        batch->append_tuple(Tuple({common::Value::make_text(cat), common::Value::make_int64(i)}));
    }
    ASSERT_TRUE(table.append_batch(*batch));

    auto scan = std::make_unique<VectorizedSeqScanOperator>("multibatch_group",
                                                            std::make_shared<ColumnarTable>(table));

    Schema out_schema;
    out_schema.add_column("cat", common::ValueType::TYPE_TEXT);
    out_schema.add_column("cnt", common::ValueType::TYPE_INT64);
    out_schema.add_column("sum", common::ValueType::TYPE_FLOAT64);

    std::vector<std::unique_ptr<parser::Expression>> group_by;
    group_by.push_back(std::make_unique<ColumnExpr>("cat"));

    std::vector<VectorizedAggregateInfo> aggs = {{AggregateType::Count, -1},
                                                 {AggregateType::Sum, 1}};

    VectorizedGroupByOperator groupby(std::move(scan), std::move(group_by), std::move(aggs),
                                      std::move(out_schema));

    auto result = VectorBatch::create(groupby.output_schema());
    int group_count = 0;
    while (groupby.next_batch(*result)) {
        group_count += result->row_count();
    }
    EXPECT_EQ(group_count, 10);  // 10 groups
}

TEST_F(VectorizedGroupByTests, MultipleColumnGroupBy) {
    // GROUP BY on two columns
    Schema schema;
    schema.add_column("cat1", common::ValueType::TYPE_TEXT);
    schema.add_column("cat2", common::ValueType::TYPE_TEXT);
    schema.add_column("val", common::ValueType::TYPE_INT64);

    ColumnarTable table("multi_col_group", *storage_, schema);
    ASSERT_TRUE(table.create());
    ASSERT_TRUE(table.open());

    // 20 rows: (cat1 = i%4, cat2 = i%5) -> 20 unique pairs
    auto batch = VectorBatch::create(schema);
    for (int64_t i = 0; i < 20; ++i) {
        std::string c1 = "A" + std::to_string(i % 4);
        std::string c2 = "B" + std::to_string(i % 5);
        batch->append_tuple(Tuple({common::Value::make_text(c1), common::Value::make_text(c2),
                                   common::Value::make_int64(i)}));
    }
    ASSERT_TRUE(table.append_batch(*batch));

    auto scan = std::make_unique<VectorizedSeqScanOperator>("multi_col_group",
                                                            std::make_shared<ColumnarTable>(table));

    Schema out_schema;
    out_schema.add_column("cat1", common::ValueType::TYPE_TEXT);
    out_schema.add_column("cat2", common::ValueType::TYPE_TEXT);
    out_schema.add_column("cnt", common::ValueType::TYPE_INT64);

    std::vector<std::unique_ptr<parser::Expression>> group_by;
    group_by.push_back(std::make_unique<ColumnExpr>("cat1"));
    group_by.push_back(std::make_unique<ColumnExpr>("cat2"));

    std::vector<VectorizedAggregateInfo> aggs = {{AggregateType::Count, -1}};

    VectorizedGroupByOperator groupby(std::move(scan), std::move(group_by), std::move(aggs),
                                      std::move(out_schema));

    auto result = VectorBatch::create(groupby.output_schema());
    int group_count = 0;
    while (groupby.next_batch(*result)) {
        group_count += result->row_count();
    }
    // 4 * 5 = 20 unique pairs
    EXPECT_EQ(group_count, 20);
}

TEST_F(VectorizedGroupByTests, MinMaxAggregates) {
    // Test MIN and MAX aggregates
    Schema schema;
    schema.add_column("cat", common::ValueType::TYPE_TEXT);
    schema.add_column("val", common::ValueType::TYPE_INT64);

    ColumnarTable table("minmax_group", *storage_, schema);
    ASSERT_TRUE(table.create());
    ASSERT_TRUE(table.open());

    // Insert rows: A->{5,10}, B->{3,7}
    auto batch = VectorBatch::create(schema);
    batch->append_tuple(Tuple({common::Value::make_text("A"), common::Value::make_int64(5)}));
    batch->append_tuple(Tuple({common::Value::make_text("A"), common::Value::make_int64(10)}));
    batch->append_tuple(Tuple({common::Value::make_text("B"), common::Value::make_int64(3)}));
    batch->append_tuple(Tuple({common::Value::make_text("B"), common::Value::make_int64(7)}));
    ASSERT_TRUE(table.append_batch(*batch));

    auto scan = std::make_unique<VectorizedSeqScanOperator>("minmax_group",
                                                            std::make_shared<ColumnarTable>(table));

    Schema out_schema;
    out_schema.add_column("cat", common::ValueType::TYPE_TEXT);
    out_schema.add_column("cnt", common::ValueType::TYPE_INT64);
    out_schema.add_column("sum", common::ValueType::TYPE_FLOAT64);
    out_schema.add_column("min", common::ValueType::TYPE_INT64);
    out_schema.add_column("max", common::ValueType::TYPE_INT64);

    std::vector<std::unique_ptr<parser::Expression>> group_by;
    group_by.push_back(std::make_unique<ColumnExpr>("cat"));

    std::vector<VectorizedAggregateInfo> aggs = {{AggregateType::Count, -1},
                                                 {AggregateType::Sum, 1},
                                                 {AggregateType::Min, 1},
                                                 {AggregateType::Max, 1}};

    VectorizedGroupByOperator groupby(std::move(scan), std::move(group_by), std::move(aggs),
                                      std::move(out_schema));

    auto result = VectorBatch::create(groupby.output_schema());
    ASSERT_TRUE(groupby.next_batch(*result));
    EXPECT_EQ(result->row_count(), 2);

    // Verify values for both groups
    for (size_t i = 0; i < result->row_count(); ++i) {
        std::string cat = result->get_column(0).get(i).as_text();
        int64_t cnt = result->get_column(1).get(i).as_int64();
        int64_t sum = static_cast<int64_t>(result->get_column(2).get(i).to_float64());
        int64_t min_val = result->get_column(3).get(i).as_int64();
        int64_t max_val = result->get_column(4).get(i).as_int64();

        EXPECT_EQ(cnt, 2);
        if (cat == "A") {
            EXPECT_EQ(sum, 15);  // 5 + 10
            EXPECT_EQ(min_val, 5);
            EXPECT_EQ(max_val, 10);
        } else if (cat == "B") {
            EXPECT_EQ(sum, 10);  // 3 + 7
            EXPECT_EQ(min_val, 3);
            EXPECT_EQ(max_val, 7);
        }
    }
}

TEST_F(VectorizedGroupByTests, NullGroupKeys) {
    // GROUP BY with NULL keys
    Schema schema;
    schema.add_column("cat", common::ValueType::TYPE_TEXT);
    schema.add_column("val", common::ValueType::TYPE_INT64);

    ColumnarTable table("null_group", *storage_, schema);
    ASSERT_TRUE(table.create());
    ASSERT_TRUE(table.open());

    // Insert: A, NULL, B, NULL
    auto batch = VectorBatch::create(schema);
    batch->append_tuple(Tuple({common::Value::make_text("A"), common::Value::make_int64(1)}));
    batch->append_tuple(Tuple({common::Value::make_null(), common::Value::make_int64(2)}));
    batch->append_tuple(Tuple({common::Value::make_text("B"), common::Value::make_int64(3)}));
    batch->append_tuple(Tuple({common::Value::make_null(), common::Value::make_int64(4)}));
    ASSERT_TRUE(table.append_batch(*batch));

    auto scan = std::make_unique<VectorizedSeqScanOperator>("null_group",
                                                            std::make_shared<ColumnarTable>(table));

    Schema out_schema;
    out_schema.add_column("cat", common::ValueType::TYPE_TEXT);
    out_schema.add_column("cnt", common::ValueType::TYPE_INT64);

    std::vector<std::unique_ptr<parser::Expression>> group_by;
    group_by.push_back(std::make_unique<ColumnExpr>("cat"));

    std::vector<VectorizedAggregateInfo> aggs = {{AggregateType::Count, -1}};

    VectorizedGroupByOperator groupby(std::move(scan), std::move(group_by), std::move(aggs),
                                      std::move(out_schema));

    auto result = VectorBatch::create(groupby.output_schema());
    int group_count = 0;
    while (groupby.next_batch(*result)) {
        group_count += result->row_count();
    }
    // 3 groups: "A", "B", and NULL
    EXPECT_EQ(group_count, 3);
}

TEST_F(VectorizedGroupByTests, VerifyGroupKeyValues) {
    // Verify actual group key values in output
    Schema schema;
    schema.add_column("cat", common::ValueType::TYPE_TEXT);
    schema.add_column("val", common::ValueType::TYPE_INT64);

    ColumnarTable table("verify_keys", *storage_, schema);
    ASSERT_TRUE(table.create());
    ASSERT_TRUE(table.open());

    auto batch = VectorBatch::create(schema);
    batch->append_tuple(Tuple({common::Value::make_text("X"), common::Value::make_int64(10)}));
    batch->append_tuple(Tuple({common::Value::make_text("X"), common::Value::make_int64(20)}));
    batch->append_tuple(Tuple({common::Value::make_text("Y"), common::Value::make_int64(5)}));
    ASSERT_TRUE(table.append_batch(*batch));

    auto scan = std::make_unique<VectorizedSeqScanOperator>("verify_keys",
                                                            std::make_shared<ColumnarTable>(table));

    Schema out_schema;
    out_schema.add_column("cat", common::ValueType::TYPE_TEXT);
    out_schema.add_column("cnt", common::ValueType::TYPE_INT64);

    std::vector<std::unique_ptr<parser::Expression>> group_by;
    group_by.push_back(std::make_unique<ColumnExpr>("cat"));

    std::vector<VectorizedAggregateInfo> aggs = {{AggregateType::Count, -1}};

    VectorizedGroupByOperator groupby(std::move(scan), std::move(group_by), std::move(aggs),
                                      std::move(out_schema));

    auto result = VectorBatch::create(groupby.output_schema());
    ASSERT_TRUE(groupby.next_batch(*result));
    EXPECT_EQ(result->row_count(), 2);

    // Find X and Y groups and verify
    std::string found_x, found_y;
    int64_t cnt_x = 0, cnt_y = 0;
    for (size_t i = 0; i < result->row_count(); ++i) {
        std::string cat = result->get_column(0).get(i).as_text();
        int64_t cnt = result->get_column(1).get(i).as_int64();
        if (cat == "X") {
            found_x = cat;
            cnt_x = cnt;
        } else if (cat == "Y") {
            found_y = cat;
            cnt_y = cnt;
        }
    }
    EXPECT_EQ(found_x, "X");
    EXPECT_EQ(cnt_x, 2);
    EXPECT_EQ(found_y, "Y");
    EXPECT_EQ(cnt_y, 1);
}

// Helper to create a VectorizedHashJoinOperator
std::unique_ptr<VectorizedHashJoinOperator> make_vectorized_hash_join(
    std::unique_ptr<VectorizedOperator> left, std::unique_ptr<VectorizedOperator> right,
    const std::string& left_key, const std::string& right_key, JoinType join_type) {
    // Build output schema: left columns + right columns
    Schema out_schema;
    const auto& left_schema = left->output_schema();
    const auto& right_schema = right->output_schema();

    for (size_t i = 0; i < left_schema.columns().size(); ++i) {
        out_schema.add_column(left_schema.columns()[i].name(), left_schema.columns()[i].type());
    }
    for (size_t i = 0; i < right_schema.columns().size(); ++i) {
        out_schema.add_column(right_schema.columns()[i].name(), right_schema.columns()[i].type());
    }

    return std::make_unique<VectorizedHashJoinOperator>(
        std::move(left), std::move(right), std::make_unique<ColumnExpr>(left_key),
        std::make_unique<ColumnExpr>(right_key), join_type, std::move(out_schema));
}

TEST_F(VectorizedGroupByTests, VectorizedHashJoinLeft) {
    // Left table: id=1,2,3 | Right table: id=2,3,4
    // LEFT join on id: expect (1,"A",NULL), (2,"B",2,20), (3,"C",3,30)
    Schema left_schema;
    left_schema.add_column("id", common::ValueType::TYPE_INT64);
    left_schema.add_column("name", common::ValueType::TYPE_TEXT);

    Schema right_schema;
    right_schema.add_column("id", common::ValueType::TYPE_INT64);
    right_schema.add_column("val", common::ValueType::TYPE_INT64);

    // Left table
    ColumnarTable left_table("hashjoin_left", *storage_, left_schema);
    ASSERT_TRUE(left_table.create());
    ASSERT_TRUE(left_table.open());
    auto left_batch = VectorBatch::create(left_schema);
    left_batch->append_tuple(Tuple({common::Value::make_int64(1), common::Value::make_text("A")}));
    left_batch->append_tuple(Tuple({common::Value::make_int64(2), common::Value::make_text("B")}));
    left_batch->append_tuple(Tuple({common::Value::make_int64(3), common::Value::make_text("C")}));
    ASSERT_TRUE(left_table.append_batch(*left_batch));

    // Right table
    ColumnarTable right_table("hashjoin_right", *storage_, right_schema);
    ASSERT_TRUE(right_table.create());
    ASSERT_TRUE(right_table.open());
    auto right_batch = VectorBatch::create(right_schema);
    right_batch->append_tuple(Tuple({common::Value::make_int64(2), common::Value::make_int64(20)}));
    right_batch->append_tuple(Tuple({common::Value::make_int64(3), common::Value::make_int64(30)}));
    right_batch->append_tuple(Tuple({common::Value::make_int64(4), common::Value::make_int64(40)}));
    ASSERT_TRUE(right_table.append_batch(*right_batch));

    auto left_scan = std::make_unique<VectorizedSeqScanOperator>(
        "hashjoin_left", std::make_shared<ColumnarTable>(left_table));
    auto right_scan = std::make_unique<VectorizedSeqScanOperator>(
        "hashjoin_right", std::make_shared<ColumnarTable>(right_table));

    auto join = make_vectorized_hash_join(std::move(left_scan), std::move(right_scan), "id", "id",
                                          JoinType::Left);

    auto result = VectorBatch::create(join->output_schema());
    std::vector<std::tuple<int64_t, std::string, int64_t, int64_t>> matches;
    int null_right_count = 0;

    while (join->next_batch(*result)) {
        for (size_t i = 0; i < result->row_count(); ++i) {
            int64_t left_id = result->get_column(0).get(i).as_int64();
            std::string name = result->get_column(1).get(i).as_text();
            if (result->get_column(2).get(i).is_null()) {
                null_right_count++;
            } else {
                int64_t right_id = result->get_column(2).get(i).as_int64();
                int64_t right_val = result->get_column(3).get(i).as_int64();
                matches.push_back(std::make_tuple(left_id, name, right_id, right_val));
            }
        }
        result->clear();
    }

    // LEFT join: id=1 has no match, should be emitted with NULLs
    EXPECT_EQ(null_right_count, 1);
    EXPECT_EQ(std::get<0>(matches[0]), 2);
    EXPECT_EQ(std::get<2>(matches[0]), 2);
    EXPECT_EQ(std::get<0>(matches[1]), 3);
    EXPECT_EQ(std::get<2>(matches[1]), 3);
}

TEST_F(VectorizedGroupByTests, VectorizedHashJoinInner) {
    // Left table: id=1,2,3 | Right table: id=2,3,4
    // Inner join on id: expect (2,2), (3,3)
    Schema left_schema;
    left_schema.add_column("id", common::ValueType::TYPE_INT64);
    left_schema.add_column("name", common::ValueType::TYPE_TEXT);

    Schema right_schema;
    right_schema.add_column("id", common::ValueType::TYPE_INT64);
    right_schema.add_column("val", common::ValueType::TYPE_INT64);

    // Left table
    ColumnarTable left_table("hashjoin_left", *storage_, left_schema);
    ASSERT_TRUE(left_table.create());
    ASSERT_TRUE(left_table.open());
    auto left_batch = VectorBatch::create(left_schema);
    left_batch->append_tuple(Tuple({common::Value::make_int64(1), common::Value::make_text("A")}));
    left_batch->append_tuple(Tuple({common::Value::make_int64(2), common::Value::make_text("B")}));
    left_batch->append_tuple(Tuple({common::Value::make_int64(3), common::Value::make_text("C")}));
    ASSERT_TRUE(left_table.append_batch(*left_batch));

    // Right table
    ColumnarTable right_table("hashjoin_right", *storage_, right_schema);
    ASSERT_TRUE(right_table.create());
    ASSERT_TRUE(right_table.open());
    auto right_batch = VectorBatch::create(right_schema);
    right_batch->append_tuple(Tuple({common::Value::make_int64(2), common::Value::make_int64(20)}));
    right_batch->append_tuple(Tuple({common::Value::make_int64(3), common::Value::make_int64(30)}));
    right_batch->append_tuple(Tuple({common::Value::make_int64(4), common::Value::make_int64(40)}));
    ASSERT_TRUE(right_table.append_batch(*right_batch));

    auto left_scan = std::make_unique<VectorizedSeqScanOperator>(
        "hashjoin_left", std::make_shared<ColumnarTable>(left_table));
    auto right_scan = std::make_unique<VectorizedSeqScanOperator>(
        "hashjoin_right", std::make_shared<ColumnarTable>(right_table));

    auto join = make_vectorized_hash_join(std::move(left_scan), std::move(right_scan), "id", "id",
                                          JoinType::Inner);

    auto result = VectorBatch::create(join->output_schema());
    std::vector<std::tuple<int64_t, std::string, int64_t, int64_t>> matches;

    while (join->next_batch(*result)) {
        for (size_t i = 0; i < result->row_count(); ++i) {
            matches.push_back(std::make_tuple(result->get_column(0).get(i).as_int64(),  // left.id
                                              result->get_column(1).get(i).as_text(),   // left.name
                                              result->get_column(2).get(i).as_int64(),  // right.id
                                              result->get_column(3).get(i).as_int64()   // right.val
                                              ));
        }
        result->clear();
    }

    // Inner join: (2,"B",2,20), (3,"C",3,30)
    EXPECT_EQ(matches.size(), 2U);
    EXPECT_EQ(std::get<0>(matches[0]), 2);
    EXPECT_EQ(std::get<2>(matches[0]), 2);
    EXPECT_EQ(std::get<0>(matches[1]), 3);
    EXPECT_EQ(std::get<2>(matches[1]), 3);
}

TEST_F(VectorizedGroupByTests, VectorizedHashJoinNullKeys) {
    // Test that NULL keys in either side don't produce matches
    Schema left_schema;
    left_schema.add_column("id", common::ValueType::TYPE_INT64);

    Schema right_schema;
    right_schema.add_column("id", common::ValueType::TYPE_INT64);

    ColumnarTable left_table("hashjoin_null_left", *storage_, left_schema);
    ASSERT_TRUE(left_table.create());
    ASSERT_TRUE(left_table.open());
    auto left_batch = VectorBatch::create(left_schema);
    left_batch->append_tuple(Tuple({common::Value::make_int64(1)}));
    left_batch->append_tuple(Tuple({common::Value::make_null()}));  // NULL key
    left_batch->append_tuple(Tuple({common::Value::make_int64(2)}));
    ASSERT_TRUE(left_table.append_batch(*left_batch));

    ColumnarTable right_table("hashjoin_null_right", *storage_, right_schema);
    ASSERT_TRUE(right_table.create());
    ASSERT_TRUE(right_table.open());
    auto right_batch = VectorBatch::create(right_schema);
    right_batch->append_tuple(Tuple({common::Value::make_int64(1)}));
    right_batch->append_tuple(Tuple({common::Value::make_null()}));  // NULL key
    right_batch->append_tuple(Tuple({common::Value::make_int64(2)}));
    ASSERT_TRUE(right_table.append_batch(*right_batch));

    auto left_scan = std::make_unique<VectorizedSeqScanOperator>(
        "hashjoin_null_left", std::make_shared<ColumnarTable>(left_table));
    auto right_scan = std::make_unique<VectorizedSeqScanOperator>(
        "hashjoin_null_right", std::make_shared<ColumnarTable>(right_table));

    auto join = make_vectorized_hash_join(std::move(left_scan), std::move(right_scan), "id", "id",
                                          JoinType::Inner);

    auto result = VectorBatch::create(join->output_schema());
    int match_count = 0;

    while (join->next_batch(*result)) {
        match_count += result->row_count();
        result->clear();
    }

    // INNER join with NULLs: only (1,1) and (2,2) should match
    // NULL keys never match
    EXPECT_EQ(match_count, 2);
}

TEST_F(VectorizedGroupByTests, VectorizedHashJoinEmptyRight) {
    // Test LEFT join with empty right table - all left rows should emit with NULLs
    Schema left_schema;
    left_schema.add_column("id", common::ValueType::TYPE_INT64);
    left_schema.add_column("name", common::ValueType::TYPE_TEXT);

    Schema right_schema;
    right_schema.add_column("id", common::ValueType::TYPE_INT64);
    right_schema.add_column("val", common::ValueType::TYPE_INT64);

    // Left table with 3 rows
    ColumnarTable left_table("hashjoin_left", *storage_, left_schema);
    ASSERT_TRUE(left_table.create());
    ASSERT_TRUE(left_table.open());
    auto left_batch = VectorBatch::create(left_schema);
    left_batch->append_tuple(Tuple({common::Value::make_int64(1), common::Value::make_text("A")}));
    left_batch->append_tuple(Tuple({common::Value::make_int64(2), common::Value::make_text("B")}));
    left_batch->append_tuple(Tuple({common::Value::make_int64(3), common::Value::make_text("C")}));
    ASSERT_TRUE(left_table.append_batch(*left_batch));

    // Right table is EMPTY
    ColumnarTable right_table("hashjoin_right", *storage_, right_schema);
    ASSERT_TRUE(right_table.create());
    ASSERT_TRUE(right_table.open());
    // No rows added - right table is empty

    auto left_scan = std::make_unique<VectorizedSeqScanOperator>(
        "hashjoin_left", std::make_shared<ColumnarTable>(left_table));
    auto right_scan = std::make_unique<VectorizedSeqScanOperator>(
        "hashjoin_right", std::make_shared<ColumnarTable>(right_table));

    auto join = make_vectorized_hash_join(std::move(left_scan), std::move(right_scan), "id", "id",
                                          JoinType::Left);

    auto result = VectorBatch::create(join->output_schema());
    int total_rows = 0;
    int null_count = 0;

    while (join->next_batch(*result)) {
        total_rows += result->row_count();
        for (size_t i = 0; i < result->row_count(); ++i) {
            if (result->get_column(2).get(i).is_null()) {
                null_count++;
            }
        }
        result->clear();
    }

    // LEFT join with empty right: all 3 left rows with NULLs for right columns
    EXPECT_EQ(total_rows, 3);
    EXPECT_EQ(null_count, 3);  // All rows have NULL for right.id
}

TEST_F(VectorizedGroupByTests, VectorizedHashJoinEmptyLeft) {
    // Test with empty left table
    Schema left_schema;
    left_schema.add_column("id", common::ValueType::TYPE_INT64);

    Schema right_schema;
    right_schema.add_column("id", common::ValueType::TYPE_INT64);

    // Left table is EMPTY
    ColumnarTable left_table("hashjoin_left", *storage_, left_schema);
    ASSERT_TRUE(left_table.create());
    ASSERT_TRUE(left_table.open());

    // Right table with rows
    ColumnarTable right_table("hashjoin_right", *storage_, right_schema);
    ASSERT_TRUE(right_table.create());
    ASSERT_TRUE(right_table.open());
    auto right_batch = VectorBatch::create(right_schema);
    right_batch->append_tuple(Tuple({common::Value::make_int64(1)}));
    right_batch->append_tuple(Tuple({common::Value::make_int64(2)}));
    ASSERT_TRUE(right_table.append_batch(*right_batch));

    auto left_scan = std::make_unique<VectorizedSeqScanOperator>(
        "hashjoin_left", std::make_shared<ColumnarTable>(left_table));
    auto right_scan = std::make_unique<VectorizedSeqScanOperator>(
        "hashjoin_right", std::make_shared<ColumnarTable>(right_table));

    auto join = make_vectorized_hash_join(std::move(left_scan), std::move(right_scan), "id", "id",
                                          JoinType::Inner);

    auto result = VectorBatch::create(join->output_schema());
    int total_rows = 0;

    while (join->next_batch(*result)) {
        total_rows += result->row_count();
        result->clear();
    }

    // Empty left table: 0 rows regardless of join type
    EXPECT_EQ(total_rows, 0);
}

TEST_F(VectorizedGroupByTests, VectorizedHashJoinMultipleMatches) {
    // Test when right has duplicate keys: id=1 appears twice
    // Each left row should match ALL right rows with the same key
    Schema left_schema;
    left_schema.add_column("id", common::ValueType::TYPE_INT64);

    Schema right_schema;
    right_schema.add_column("id", common::ValueType::TYPE_INT64);

    ColumnarTable left_table("hashjoin_left", *storage_, left_schema);
    ASSERT_TRUE(left_table.create());
    ASSERT_TRUE(left_table.open());
    auto left_batch = VectorBatch::create(left_schema);
    left_batch->append_tuple(Tuple({common::Value::make_int64(1)}));
    left_batch->append_tuple(Tuple({common::Value::make_int64(2)}));
    ASSERT_TRUE(left_table.append_batch(*left_batch));

    ColumnarTable right_table("hashjoin_right", *storage_, right_schema);
    ASSERT_TRUE(right_table.create());
    ASSERT_TRUE(right_table.open());
    auto right_batch = VectorBatch::create(right_schema);
    right_batch->append_tuple(Tuple({common::Value::make_int64(1)}));  // duplicate
    right_batch->append_tuple(Tuple({common::Value::make_int64(1)}));  // duplicate
    right_batch->append_tuple(Tuple({common::Value::make_int64(2)}));
    ASSERT_TRUE(right_table.append_batch(*right_batch));

    auto left_scan = std::make_unique<VectorizedSeqScanOperator>(
        "hashjoin_left", std::make_shared<ColumnarTable>(left_table));
    auto right_scan = std::make_unique<VectorizedSeqScanOperator>(
        "hashjoin_right", std::make_shared<ColumnarTable>(right_table));

    auto join = make_vectorized_hash_join(std::move(left_scan), std::move(right_scan), "id", "id",
                                          JoinType::Inner);

    auto result = VectorBatch::create(join->output_schema());
    std::vector<int64_t> left_ids;
    std::vector<int64_t> right_ids;

    while (join->next_batch(*result)) {
        for (size_t i = 0; i < result->row_count(); ++i) {
            left_ids.push_back(result->get_column(0).get(i).as_int64());
            right_ids.push_back(result->get_column(1).get(i).as_int64());
        }
        result->clear();
    }

    // INNER: left_id=1 matches 2 right rows, left_id=2 matches 1 right row = 3 total
    EXPECT_EQ(left_ids.size(), 3);
    EXPECT_EQ(right_ids.size(), 3);
    EXPECT_EQ(left_ids[0], 1);
    EXPECT_EQ(left_ids[1], 1);  // Second match for left id=1
    EXPECT_EQ(left_ids[2], 2);
    // Right-side: two rows with id=1 (matches for left_id=1), then one row with id=2 (match for
    // left_id=2)
    EXPECT_EQ(right_ids[0], 1);
    EXPECT_EQ(right_ids[1], 1);  // Second right row with id=1
    EXPECT_EQ(right_ids[2], 2);
}

TEST_F(VectorizedGroupByTests, VectorizedHashJoinLeftNullKeys) {
    // Test LEFT join with NULL keys in left table
    // NULL key should NOT match anything, row should emit with NULLs for right
    Schema left_schema;
    left_schema.add_column("id", common::ValueType::TYPE_INT64);

    Schema right_schema;
    right_schema.add_column("id", common::ValueType::TYPE_INT64);

    ColumnarTable left_table("hashjoin_left", *storage_, left_schema);
    ASSERT_TRUE(left_table.create());
    ASSERT_TRUE(left_table.open());
    auto left_batch = VectorBatch::create(left_schema);
    left_batch->append_tuple(Tuple({common::Value::make_null()}));  // NULL key
    left_batch->append_tuple(Tuple({common::Value::make_int64(1)}));
    ASSERT_TRUE(left_table.append_batch(*left_batch));

    ColumnarTable right_table("hashjoin_right", *storage_, right_schema);
    ASSERT_TRUE(right_table.create());
    ASSERT_TRUE(right_table.open());
    auto right_batch = VectorBatch::create(right_schema);
    right_batch->append_tuple(Tuple({common::Value::make_int64(1)}));
    ASSERT_TRUE(right_table.append_batch(*right_batch));

    auto left_scan = std::make_unique<VectorizedSeqScanOperator>(
        "hashjoin_left", std::make_shared<ColumnarTable>(left_table));
    auto right_scan = std::make_unique<VectorizedSeqScanOperator>(
        "hashjoin_right", std::make_shared<ColumnarTable>(right_table));

    auto join = make_vectorized_hash_join(std::move(left_scan), std::move(right_scan), "id", "id",
                                          JoinType::Left);

    auto result = VectorBatch::create(join->output_schema());
    int total_rows = 0;
    int null_count = 0;

    while (join->next_batch(*result)) {
        total_rows += result->row_count();
        for (size_t i = 0; i < result->row_count(); ++i) {
            if (result->get_column(1).get(i).is_null()) {
                null_count++;
            }
        }
        result->clear();
    }

    // LEFT: NULL key emits with NULLs, id=1 matches right.id=1
    EXPECT_EQ(total_rows, 2);
    EXPECT_EQ(null_count, 1);  // One row with NULL for right.id
}

TEST_F(VectorizedGroupByTests, VectorizedHashJoinOutputValues) {
    // Verify that actual column VALUES are correct, not just IDs
    Schema left_schema;
    left_schema.add_column("id", common::ValueType::TYPE_INT64);
    left_schema.add_column("name", common::ValueType::TYPE_TEXT);

    Schema right_schema;
    right_schema.add_column("id", common::ValueType::TYPE_INT64);
    right_schema.add_column("val", common::ValueType::TYPE_INT64);

    ColumnarTable left_table("hashjoin_left", *storage_, left_schema);
    ASSERT_TRUE(left_table.create());
    ASSERT_TRUE(left_table.open());
    auto left_batch = VectorBatch::create(left_schema);
    left_batch->append_tuple(Tuple({common::Value::make_int64(1), common::Value::make_text("A")}));
    left_batch->append_tuple(Tuple({common::Value::make_int64(2), common::Value::make_text("B")}));
    left_batch->append_tuple(Tuple({common::Value::make_int64(3), common::Value::make_text("C")}));
    ASSERT_TRUE(left_table.append_batch(*left_batch));

    ColumnarTable right_table("hashjoin_right", *storage_, right_schema);
    ASSERT_TRUE(right_table.create());
    ASSERT_TRUE(right_table.open());
    auto right_batch = VectorBatch::create(right_schema);
    right_batch->append_tuple(Tuple({common::Value::make_int64(2), common::Value::make_int64(20)}));
    right_batch->append_tuple(Tuple({common::Value::make_int64(3), common::Value::make_int64(30)}));
    right_batch->append_tuple(Tuple({common::Value::make_int64(4), common::Value::make_int64(40)}));
    ASSERT_TRUE(right_table.append_batch(*right_batch));

    auto left_scan = std::make_unique<VectorizedSeqScanOperator>(
        "hashjoin_left", std::make_shared<ColumnarTable>(left_table));
    auto right_scan = std::make_unique<VectorizedSeqScanOperator>(
        "hashjoin_right", std::make_shared<ColumnarTable>(right_table));

    auto join = make_vectorized_hash_join(std::move(left_scan), std::move(right_scan), "id", "id",
                                          JoinType::Inner);

    auto result = VectorBatch::create(join->output_schema());
    std::vector<std::tuple<int64_t, std::string, int64_t, int64_t>> matches;

    while (join->next_batch(*result)) {
        for (size_t i = 0; i < result->row_count(); ++i) {
            matches.push_back(std::make_tuple(result->get_column(0).get(i).as_int64(),  // left.id
                                              result->get_column(1).get(i).as_text(),   // left.name
                                              result->get_column(2).get(i).as_int64(),  // right.id
                                              result->get_column(3).get(i).as_int64()   // right.val
                                              ));
        }
        result->clear();
    }

    // Inner join: (2,"B",2,20), (3,"C",3,30)
    ASSERT_EQ(matches.size(), 2);

    // Verify first match: id=2 should pair with name="B" and val=20
    EXPECT_EQ(std::get<0>(matches[0]), 2);
    EXPECT_EQ(std::get<1>(matches[0]), "B");
    EXPECT_EQ(std::get<2>(matches[0]), 2);
    EXPECT_EQ(std::get<3>(matches[0]), 20);

    // Verify second match: id=3 should pair with name="C" and val=30
    EXPECT_EQ(std::get<0>(matches[1]), 3);
    EXPECT_EQ(std::get<1>(matches[1]), "C");
    EXPECT_EQ(std::get<2>(matches[1]), 3);
    EXPECT_EQ(std::get<3>(matches[1]), 30);
}

TEST_F(VectorizedGroupByTests, VectorizedHashJoinMultiBatch) {
    // Test with >1024 rows to verify batch boundary handling
    Schema left_schema;
    left_schema.add_column("id", common::ValueType::TYPE_INT64);

    Schema right_schema;
    right_schema.add_column("id", common::ValueType::TYPE_INT64);

    ColumnarTable left_table("hashjoin_left", *storage_, left_schema);
    ASSERT_TRUE(left_table.create());
    ASSERT_TRUE(left_table.open());
    auto left_batch = VectorBatch::create(left_schema);
    for (int i = 0; i < 2000; ++i) {
        left_batch->append_tuple(Tuple({common::Value::make_int64(i)}));
    }
    ASSERT_TRUE(left_table.append_batch(*left_batch));

    ColumnarTable right_table("hashjoin_right", *storage_, right_schema);
    ASSERT_TRUE(right_table.create());
    ASSERT_TRUE(right_table.open());
    auto right_batch = VectorBatch::create(right_schema);
    for (int i = 0; i < 2000; ++i) {
        right_batch->append_tuple(Tuple({common::Value::make_int64(i)}));
    }
    ASSERT_TRUE(right_table.append_batch(*right_batch));

    auto left_scan = std::make_unique<VectorizedSeqScanOperator>(
        "hashjoin_left", std::make_shared<ColumnarTable>(left_table));
    auto right_scan = std::make_unique<VectorizedSeqScanOperator>(
        "hashjoin_right", std::make_shared<ColumnarTable>(right_table));

    auto join = make_vectorized_hash_join(std::move(left_scan), std::move(right_scan), "id", "id",
                                          JoinType::Inner);

    auto result = VectorBatch::create(join->output_schema());
    int64_t total_rows = 0;

    while (join->next_batch(*result)) {
        total_rows += result->row_count();
        result->clear();
    }

    // 2000 rows on each side, all should match
    EXPECT_EQ(total_rows, 2000);
}

TEST_F(VectorizedGroupByTests, VectorizedHashJoinLeftMultiBatch) {
    // Test LEFT join with >BATCH_SIZE (1024) right rows requiring multiple batches
    // Left: id=1,2,3 (3 rows) | Right: id=1 (1500 rows)
    // LEFT id=1 matches all 1500 right rows
    // LEFT id=2,3 have NO match - should emit with NULLs
    Schema left_schema;
    left_schema.add_column("id", common::ValueType::TYPE_INT64);

    Schema right_schema;
    right_schema.add_column("id", common::ValueType::TYPE_INT64);

    ColumnarTable left_table("hashjoin_left", *storage_, left_schema);
    ASSERT_TRUE(left_table.create());
    ASSERT_TRUE(left_table.open());
    auto left_batch = VectorBatch::create(left_schema);
    left_batch->append_tuple(Tuple({common::Value::make_int64(1)}));
    left_batch->append_tuple(Tuple({common::Value::make_int64(2)}));
    left_batch->append_tuple(Tuple({common::Value::make_int64(3)}));
    ASSERT_TRUE(left_table.append_batch(*left_batch));

    ColumnarTable right_table("hashjoin_right", *storage_, right_schema);
    ASSERT_TRUE(right_table.create());
    ASSERT_TRUE(right_table.open());
    auto right_batch = VectorBatch::create(right_schema);
    // Right: 1500 rows ALL with id=1 (forces multi-batch processing with BATCH_SIZE=1024)
    for (int i = 0; i < 1500; ++i) {
        right_batch->append_tuple(Tuple({common::Value::make_int64(1)}));
    }
    ASSERT_TRUE(right_table.append_batch(*right_batch));

    auto left_scan = std::make_unique<VectorizedSeqScanOperator>(
        "hashjoin_left", std::make_shared<ColumnarTable>(left_table));
    auto right_scan = std::make_unique<VectorizedSeqScanOperator>(
        "hashjoin_right", std::make_shared<ColumnarTable>(right_table));

    auto join = make_vectorized_hash_join(std::move(left_scan), std::move(right_scan), "id", "id",
                                          JoinType::Left);

    auto result = VectorBatch::create(join->output_schema());
    int64_t total_rows = 0;
    int64_t rows_with_nulls = 0;         // LEFT id=2,3 should emit with NULLs
    std::vector<int64_t> null_left_ids;  // Track which left ids had null right

    while (join->next_batch(*result)) {
        for (size_t i = 0; i < result->row_count(); ++i) {
            int64_t left_id = result->get_column(0).get(i).as_int64();
            if (result->get_column(1).get(i).is_null()) {
                rows_with_nulls++;
                null_left_ids.push_back(left_id);
            } else {
                int64_t right_id = result->get_column(1).get(i).as_int64();
                // Every non-null row should be left.id=1 matched with right.id=1
                EXPECT_EQ(left_id, 1);
                EXPECT_EQ(right_id, 1);
            }
        }
        total_rows += result->row_count();
        result->clear();
    }

    // LEFT join: id=1 matches 1500 rows, id=2,3 emit with NULLs
    EXPECT_EQ(total_rows, 1502);    // 1500 matches + 2 unmatched with NULLs
    EXPECT_EQ(rows_with_nulls, 2);  // id=2 and id=3 have no match
    // Verify the two null rows are for left ids 2 and 3
    EXPECT_EQ(null_left_ids.size(), 2);
    std::sort(null_left_ids.begin(), null_left_ids.end());
    EXPECT_EQ(null_left_ids[0], 2);
    EXPECT_EQ(null_left_ids[1], 3);
}

TEST_F(VectorizedGroupByTests, VectorizedHashJoinRight) {
    // Test RIGHT outer join: all right rows appear, NULLs for left when no match
    // Left table: id=1,2,3 | Right table: id=2,3,4
    // RIGHT join on id: (2,2,20), (3,3,30) matched; (4,NULL,NULL,40) unmatched right
    // LEFT join part: left.id=1 has no right match, but RIGHT join doesn't emit unmatched left
    // So total expected: 3 rows (2 matched + 1 unmatched right)
    Schema left_schema;
    left_schema.add_column("id", common::ValueType::TYPE_INT64);
    left_schema.add_column("name", common::ValueType::TYPE_TEXT);

    Schema right_schema;
    right_schema.add_column("id", common::ValueType::TYPE_INT64);
    right_schema.add_column("val", common::ValueType::TYPE_INT64);

    ColumnarTable left_table("hj_right_left", *storage_, left_schema);
    ASSERT_TRUE(left_table.create());
    ASSERT_TRUE(left_table.open());
    auto left_batch = VectorBatch::create(left_schema);
    left_batch->append_tuple(Tuple({common::Value::make_int64(1), common::Value::make_text("A")}));
    left_batch->append_tuple(Tuple({common::Value::make_int64(2), common::Value::make_text("B")}));
    left_batch->append_tuple(Tuple({common::Value::make_int64(3), common::Value::make_text("C")}));
    ASSERT_TRUE(left_table.append_batch(*left_batch));

    ColumnarTable right_table("hj_right_right", *storage_, right_schema);
    ASSERT_TRUE(right_table.create());
    ASSERT_TRUE(right_table.open());
    auto right_batch = VectorBatch::create(right_schema);
    right_batch->append_tuple(Tuple({common::Value::make_int64(2), common::Value::make_int64(20)}));
    right_batch->append_tuple(Tuple({common::Value::make_int64(3), common::Value::make_int64(30)}));
    right_batch->append_tuple(Tuple({common::Value::make_int64(4), common::Value::make_int64(40)}));
    ASSERT_TRUE(right_table.append_batch(*right_batch));

    auto left_scan = std::make_unique<VectorizedSeqScanOperator>(
        "hj_right_left", std::make_shared<ColumnarTable>(left_table));
    auto right_scan = std::make_unique<VectorizedSeqScanOperator>(
        "hj_right_right", std::make_shared<ColumnarTable>(right_table));

    auto join = make_vectorized_hash_join(std::move(left_scan), std::move(right_scan), "id", "id",
                                          JoinType::Right);

    auto result = VectorBatch::create(join->output_schema());
    int matched_count = 0;
    int null_left_count = 0;

    while (join->next_batch(*result)) {
        for (size_t i = 0; i < result->row_count(); ++i) {
            // Check if this is an unmatched right row (NULL left columns)
            if (result->get_column(0).get(i).is_null()) {
                // Unmatched right row - should have id=4, val=40 in right columns
                ASSERT_FALSE(result->get_column(2).get(i).is_null())
                    << "right.id should not be null";
                ASSERT_FALSE(result->get_column(3).get(i).is_null())
                    << "right.val should not be null";
                EXPECT_EQ(result->get_column(2).get(i).as_int64(), 4);
                EXPECT_EQ(result->get_column(3).get(i).as_int64(), 40);
                null_left_count++;
            } else {
                // Matched row
                matched_count++;
            }
        }
        result->clear();
    }

    // RIGHT join: 2 matched rows + 1 unmatched right row = 3 total
    EXPECT_EQ(matched_count, 2);    // (2,2,20) and (3,3,30)
    EXPECT_EQ(null_left_count, 1);  // (4,NULL,NULL,40) - right.id=4 unmatched
}

TEST_F(VectorizedGroupByTests, VectorizedHashJoinFull) {
    // Test FULL outer join: all rows from both sides appear
    // Left table: id=1,2,3 | Right table: id=2,3,4
    // FULL join on id: expect (2,2,20), (3,3,30) matched
    // Plus unmatched left: (1,NULL,NULL) and unmatched right: (NULL,NULL,4,40)
    // Total: 4 rows
    Schema left_schema;
    left_schema.add_column("id", common::ValueType::TYPE_INT64);
    left_schema.add_column("name", common::ValueType::TYPE_TEXT);

    Schema right_schema;
    right_schema.add_column("id", common::ValueType::TYPE_INT64);
    right_schema.add_column("val", common::ValueType::TYPE_INT64);

    ColumnarTable left_table("hj_full_left", *storage_, left_schema);
    ASSERT_TRUE(left_table.create());
    ASSERT_TRUE(left_table.open());
    auto left_batch = VectorBatch::create(left_schema);
    left_batch->append_tuple(Tuple({common::Value::make_int64(1), common::Value::make_text("A")}));
    left_batch->append_tuple(Tuple({common::Value::make_int64(2), common::Value::make_text("B")}));
    left_batch->append_tuple(Tuple({common::Value::make_int64(3), common::Value::make_text("C")}));
    ASSERT_TRUE(left_table.append_batch(*left_batch));

    ColumnarTable right_table("hj_full_right", *storage_, right_schema);
    ASSERT_TRUE(right_table.create());
    ASSERT_TRUE(right_table.open());
    auto right_batch = VectorBatch::create(right_schema);
    right_batch->append_tuple(Tuple({common::Value::make_int64(2), common::Value::make_int64(20)}));
    right_batch->append_tuple(Tuple({common::Value::make_int64(3), common::Value::make_int64(30)}));
    right_batch->append_tuple(Tuple({common::Value::make_int64(4), common::Value::make_int64(40)}));
    ASSERT_TRUE(right_table.append_batch(*right_batch));

    auto left_scan = std::make_unique<VectorizedSeqScanOperator>(
        "hj_full_left", std::make_shared<ColumnarTable>(left_table));
    auto right_scan = std::make_unique<VectorizedSeqScanOperator>(
        "hj_full_right", std::make_shared<ColumnarTable>(right_table));

    auto join = make_vectorized_hash_join(std::move(left_scan), std::move(right_scan), "id", "id",
                                          JoinType::Full);

    auto result = VectorBatch::create(join->output_schema());
    int64_t total_rows = 0;
    int null_left_count = 0;   // rows with NULL left columns (unmatched right)
    int null_right_count = 0;  // rows with NULL right columns (unmatched left)

    while (join->next_batch(*result)) {
        for (size_t i = 0; i < result->row_count(); ++i) {
            total_rows++;
            if (result->get_column(2).get(i).is_null()) {
                null_right_count++;  // No right match
            }
            if (result->get_column(1).get(i).is_null()) {
                null_left_count++;  // No left match
            }
        }
        result->clear();
    }

    // FULL: 2 matched rows + 1 unmatched left (id=1) + 1 unmatched right (id=4) = 4 total
    EXPECT_EQ(total_rows, 4);
    EXPECT_EQ(null_right_count, 1);  // id=1 has no right match
    EXPECT_EQ(null_left_count, 1);   // id=4 has no left match
}

TEST_F(VectorizedGroupByTests, ParallelAggregationCorrectness) {
    // Test that parallel aggregation (num_threads > 1) produces correct results
    // This test creates a ThreadPool with 4 threads and verifies GROUP BY
    // produces the same results as expected (computed manually)

    // Use TEXT column to ensure hash aggregation path (not DirectIndexAgg)
    Schema schema;
    schema.add_column("cat", common::ValueType::TYPE_TEXT);
    schema.add_column("val", common::ValueType::TYPE_INT64);

    auto table_ptr = std::make_shared<ColumnarTable>("parallel_group", *storage_, schema);
    ASSERT_TRUE(table_ptr->create());
    ASSERT_TRUE(table_ptr->open());

    // Insert 100 rows with 10 distinct group keys (10 rows each)
    auto batch = VectorBatch::create(schema);
    for (int64_t i = 0; i < 100; ++i) {
        std::string cat = "cat" + std::to_string(i % 10);  // 10 distinct categories
        batch->append_tuple(
            Tuple({common::Value::make_text(cat), common::Value::make_int64(i + 1)}));
    }
    ASSERT_TRUE(table_ptr->append_batch(*batch));

    // Create a 4-thread pool for parallel execution
    auto thread_pool = std::make_shared<ThreadPool>(4);

    auto scan = std::make_unique<VectorizedSeqScanOperator>("parallel_group", table_ptr);

    Schema out_schema;
    out_schema.add_column("cat", common::ValueType::TYPE_TEXT);
    out_schema.add_column("cnt", common::ValueType::TYPE_INT64);
    out_schema.add_column("sum", common::ValueType::TYPE_INT64);

    std::vector<std::unique_ptr<parser::Expression>> group_by;
    group_by.push_back(std::make_unique<ColumnExpr>("cat"));

    std::vector<VectorizedAggregateInfo> aggs;
    aggs.push_back({AggregateType::Count, -1});
    aggs.push_back({AggregateType::Sum, 1});  // sum of "val" column

    VectorizedGroupByOperator groupby(std::move(scan), std::move(group_by), std::move(aggs),
                                      std::move(out_schema), thread_pool);

    auto result = VectorBatch::create(groupby.output_schema());
    ASSERT_TRUE(groupby.next_batch(*result));
    ASSERT_EQ(result->row_count(), 10);  // 10 distinct groups

    // Verify results: each category should have count=10 and sum = 10*(catIdx+1) + 45 = 10*catIdx +
    // 55 Actually for cat0 (i=0,10,20,...90): sum = 1+11+21+...+91 = 460 For cat1
    // (i=1,11,21,...91): sum = 2+12+22+...+92 = 470, etc.
    for (size_t i = 0; i < 10; ++i) {
        int64_t cnt = result->get_column(1).get(i).as_int64();
        int64_t sum = result->get_column(2).get(i).as_int64();

        EXPECT_EQ(cnt, 10) << "Count mismatch for category " << i;
        // Sum formula: (i+1) + (i+11) + ... + (i+91) = 10*i + (1+11+21+...+91) = 10*i + 460
        EXPECT_EQ(sum, 10 * static_cast<int64_t>(i) + 460) << "Sum mismatch for category " << i;
    }
}

}  // namespace

// ============= ThreadPool Tests =============

#include "executor/thread_pool.hpp"

using namespace cloudsql::executor;

class ThreadPoolTests : public ::testing::Test {
   protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(ThreadPoolTests, Constructor) {
    ThreadPool pool(4);
    EXPECT_EQ(pool.num_threads(), 4U);
}

TEST_F(ThreadPoolTests, SubmitAndWait) {
    ThreadPool pool(4);
    std::atomic<int> counter{0};

    for (int i = 0; i < 10; ++i) {
        pool.submit([&counter]() { counter.fetch_add(1, std::memory_order_acq_rel); });
    }
    pool.wait();

    EXPECT_EQ(counter.load(), 10);
}

TEST_F(ThreadPoolTests, MultipleWait) {
    ThreadPool pool(2);
    std::atomic<int> counter{0};

    pool.submit([&counter]() { counter.fetch_add(1, std::memory_order_acq_rel); });
    pool.wait();
    EXPECT_EQ(counter.load(), 1);

    pool.submit([&counter]() { counter.fetch_add(1, std::memory_order_acq_rel); });
    pool.submit([&counter]() { counter.fetch_add(1, std::memory_order_acq_rel); });
    pool.wait();
    EXPECT_EQ(counter.load(), 3);
}

TEST_F(ThreadPoolTests, DefaultConstructor) {
    ThreadPool pool;  // Uses hardware_concurrency
    EXPECT_GE(pool.num_threads(), 1U);
}

TEST_F(ThreadPoolTests, FutureResults) {
    ThreadPool pool(2);
    auto f1 = pool.submit([]() { return 42; });
    auto f2 = pool.submit([]() { return std::string("hello"); });
    pool.wait();

    EXPECT_EQ(f1.get(), 42);
    EXPECT_EQ(f2.get(), "hello");
}