/**
 * @file operator_tests.cpp
 * @brief Unit tests for executor/operator.cpp - Volcano-style execution operators
 */

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "common/value.hpp"
#include "executor/operator.hpp"
#include "executor/types.hpp"
#include "parser/expression.hpp"

using namespace cloudsql;
using namespace cloudsql::executor;
using namespace cloudsql::parser;

namespace {

// Helper to create a simple schema
Schema make_schema(const std::vector<std::pair<std::string, common::ValueType>>& cols) {
    Schema s;
    for (const auto& [name, type] : cols) {
        s.add_column(name, type);
    }
    return s;
}

// Helper to create a tuple
Tuple make_tuple(const std::vector<common::Value>& vals) {
    return Tuple(std::pmr::vector<common::Value>(vals.begin(), vals.end()));
}

// Helper to create ColumnExpr
std::unique_ptr<Expression> col_expr(const std::string& name) {
    return std::make_unique<ColumnExpr>(name);
}

// Helper to create ConstantExpr
std::unique_ptr<Expression> const_expr(const common::Value& val) {
    return std::make_unique<ConstantExpr>(val);
}

// Helper to create BinaryExpr (comparison)
std::unique_ptr<Expression> binary_expr(std::unique_ptr<Expression> left, TokenType op,
                                        std::unique_ptr<Expression> right) {
    return std::make_unique<BinaryExpr>(std::move(left), op, std::move(right));
}

// Helper to create AggregateInfo
AggregateInfo make_agg(AggregateType type, const std::string& name,
                       std::unique_ptr<Expression> expr = nullptr) {
    AggregateInfo info;
    info.type = type;
    info.name = name;
    info.expr = std::move(expr);
    return info;
}

// Helper: create a BufferScanOperator with test data
std::unique_ptr<BufferScanOperator> make_buffer_scan(const std::string& table_name,
                                                     const std::vector<Tuple>& data,
                                                     const Schema& schema) {
    return std::make_unique<BufferScanOperator>("context1", table_name, data, schema);
}

// Helper: create a FilterOperator with a condition
std::unique_ptr<FilterOperator> make_filter(std::unique_ptr<Operator> child,
                                            std::unique_ptr<Expression> condition) {
    return std::make_unique<FilterOperator>(std::move(child), std::move(condition));
}

// Helper: create a ProjectOperator
std::unique_ptr<ProjectOperator> make_project(std::unique_ptr<Operator> child,
                                              std::vector<std::unique_ptr<Expression>> cols) {
    return std::make_unique<ProjectOperator>(std::move(child), std::move(cols));
}

// Helper: create a SortOperator
std::unique_ptr<SortOperator> make_sort(std::unique_ptr<Operator> child,
                                        std::vector<std::unique_ptr<Expression>> keys,
                                        std::vector<bool> asc) {
    return std::make_unique<SortOperator>(std::move(child), std::move(keys), std::move(asc));
}

// Helper: create an AggregateOperator
std::unique_ptr<AggregateOperator> make_agg_op(std::unique_ptr<Operator> child,
                                               std::vector<std::unique_ptr<Expression>> group_by,
                                               std::vector<AggregateInfo> aggs) {
    return std::make_unique<AggregateOperator>(std::move(child), std::move(group_by),
                                               std::move(aggs));
}

// Helper: create a LimitOperator
std::unique_ptr<LimitOperator> make_limit(std::unique_ptr<Operator> child, int64_t limit,
                                          int64_t offset = 0) {
    return std::make_unique<LimitOperator>(std::move(child), limit, offset);
}

// Helper: create a HashJoinOperator
std::unique_ptr<HashJoinOperator> make_hash_join(std::unique_ptr<Operator> left,
                                                 std::unique_ptr<Operator> right,
                                                 std::unique_ptr<Expression> left_key,
                                                 std::unique_ptr<Expression> right_key,
                                                 JoinType join_type = JoinType::Inner) {
    return std::make_unique<HashJoinOperator>(std::move(left), std::move(right),
                                              std::move(left_key), std::move(right_key), join_type);
}

class OperatorTests : public ::testing::Test {
   protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(OperatorTests, BufferScanBasic) {
    Schema schema = make_schema({{"id", common::ValueType::TYPE_INT64}});
    std::vector<Tuple> data;
    data.push_back(make_tuple({common::Value::make_int64(1)}));
    data.push_back(make_tuple({common::Value::make_int64(2)}));
    data.push_back(make_tuple({common::Value::make_int64(3)}));

    auto scan = make_buffer_scan("test_table", data, schema);
    ASSERT_TRUE(scan->init());
    ASSERT_TRUE(scan->open());

    int count = 0;
    Tuple tuple;
    while (scan->next(tuple)) {
        count++;
    }
    EXPECT_EQ(count, 3);
    scan->close();
}

TEST_F(OperatorTests, BufferScanEmpty) {
    Schema schema = make_schema({{"id", common::ValueType::TYPE_INT64}});
    std::vector<Tuple> data;

    auto scan = make_buffer_scan("test_table", data, schema);
    ASSERT_TRUE(scan->init());
    ASSERT_TRUE(scan->open());

    Tuple tuple;
    EXPECT_FALSE(scan->next(tuple));
    scan->close();
}

TEST_F(OperatorTests, BufferScanExhausted) {
    Schema schema = make_schema({{"id", common::ValueType::TYPE_INT64}});
    std::vector<Tuple> data;
    data.push_back(make_tuple({common::Value::make_int64(1)}));

    auto scan = make_buffer_scan("test_table", data, schema);
    ASSERT_TRUE(scan->init());
    ASSERT_TRUE(scan->open());

    Tuple tuple;
    EXPECT_TRUE(scan->next(tuple));
    EXPECT_FALSE(scan->next(tuple));
    EXPECT_FALSE(scan->next(tuple));
    scan->close();
}

TEST_F(OperatorTests, LimitBasic) {
    Schema schema = make_schema({{"id", common::ValueType::TYPE_INT64}});
    std::vector<Tuple> data;
    for (int i = 0; i < 5; i++) {
        data.push_back(make_tuple({common::Value::make_int64(i)}));
    }

    auto scan = make_buffer_scan("test_table", data, schema);
    auto limit = make_limit(std::move(scan), 2);

    ASSERT_TRUE(limit->init());
    ASSERT_TRUE(limit->open());

    int count = 0;
    Tuple tuple;
    while (limit->next(tuple)) {
        count++;
    }
    EXPECT_EQ(count, 2);
    limit->close();
}

TEST_F(OperatorTests, LimitWithOffset) {
    Schema schema = make_schema({{"id", common::ValueType::TYPE_INT64}});
    std::vector<Tuple> data;
    for (int i = 0; i < 5; i++) {
        data.push_back(make_tuple({common::Value::make_int64(i)}));
    }

    auto scan = make_buffer_scan("test_table", data, schema);
    auto limit = make_limit(std::move(scan), 2, 2);  // offset 2, limit 2

    ASSERT_TRUE(limit->init());
    ASSERT_TRUE(limit->open());

    int count = 0;
    Tuple tuple;
    while (limit->next(tuple)) {
        count++;
    }
    EXPECT_EQ(count, 2);
    limit->close();
}

TEST_F(OperatorTests, LimitZero) {
    Schema schema = make_schema({{"id", common::ValueType::TYPE_INT64}});
    std::vector<Tuple> data;
    for (int i = 0; i < 5; i++) {
        data.push_back(make_tuple({common::Value::make_int64(i)}));
    }

    auto scan = make_buffer_scan("test_table", data, schema);
    auto limit = make_limit(std::move(scan), 0);

    ASSERT_TRUE(limit->init());
    ASSERT_TRUE(limit->open());

    Tuple tuple;
    EXPECT_FALSE(limit->next(tuple));
    limit->close();
}

TEST_F(OperatorTests, LimitNegative) {
    Schema schema = make_schema({{"id", common::ValueType::TYPE_INT64}});
    std::vector<Tuple> data;
    for (int i = 0; i < 5; i++) {
        data.push_back(make_tuple({common::Value::make_int64(i)}));
    }

    auto scan = make_buffer_scan("test_table", data, schema);
    auto limit = make_limit(std::move(scan), -1);  // negative = no limit

    ASSERT_TRUE(limit->init());
    ASSERT_TRUE(limit->open());

    int count = 0;
    Tuple tuple;
    while (limit->next(tuple)) {
        count++;
    }
    EXPECT_EQ(count, 5);  // all tuples returned
    limit->close();
}

TEST_F(OperatorTests, FilterBasic) {
    Schema schema = make_schema({{"id", common::ValueType::TYPE_INT64}});
    std::vector<Tuple> data;
    for (int i = 0; i < 5; i++) {
        data.push_back(make_tuple({common::Value::make_int64(i)}));
    }

    auto scan = make_buffer_scan("test_table", data, schema);
    // Filter: id >= 2
    auto filter = make_filter(
        std::move(scan),
        binary_expr(col_expr("id"), TokenType::Ge, const_expr(common::Value::make_int64(2))));

    ASSERT_TRUE(filter->init());
    ASSERT_TRUE(filter->open());

    int count = 0;
    Tuple tuple;
    while (filter->next(tuple)) {
        count++;
    }
    EXPECT_EQ(count, 3);  // 2, 3, 4
    filter->close();
}

TEST_F(OperatorTests, FilterAllFiltered) {
    Schema schema = make_schema({{"id", common::ValueType::TYPE_INT64}});
    std::vector<Tuple> data;
    for (int i = 0; i < 5; i++) {
        data.push_back(make_tuple({common::Value::make_int64(i)}));
    }

    auto scan = make_buffer_scan("test_table", data, schema);
    // Filter: id > 100 (filters all)
    auto filter = make_filter(
        std::move(scan),
        binary_expr(col_expr("id"), TokenType::Gt, const_expr(common::Value::make_int64(100))));

    ASSERT_TRUE(filter->init());
    ASSERT_TRUE(filter->open());

    Tuple tuple;
    EXPECT_FALSE(filter->next(tuple));
    filter->close();
}

TEST_F(OperatorTests, ProjectBasic) {
    Schema schema = make_schema(
        {{"id", common::ValueType::TYPE_INT64}, {"name", common::ValueType::TYPE_TEXT}});
    std::vector<Tuple> data;
    data.push_back(make_tuple({common::Value::make_int64(1), common::Value::make_text("alice")}));
    data.push_back(make_tuple({common::Value::make_int64(2), common::Value::make_text("bob")}));

    auto scan = make_buffer_scan("test_table", data, schema);
    std::vector<std::unique_ptr<Expression>> cols;
    cols.push_back(col_expr("name"));
    auto project = make_project(std::move(scan), std::move(cols));

    ASSERT_TRUE(project->init());
    ASSERT_TRUE(project->open());

    int count = 0;
    Tuple tuple;
    while (project->next(tuple)) {
        count++;
        EXPECT_EQ(tuple.size(), 1U);
    }
    EXPECT_EQ(count, 2);
    project->close();
}

TEST_F(OperatorTests, SortBasic) {
    Schema schema = make_schema({{"id", common::ValueType::TYPE_INT64}});
    std::vector<Tuple> data;
    data.push_back(make_tuple({common::Value::make_int64(3)}));
    data.push_back(make_tuple({common::Value::make_int64(1)}));
    data.push_back(make_tuple({common::Value::make_int64(2)}));

    auto scan = make_buffer_scan("test_table", data, schema);
    std::vector<std::unique_ptr<Expression>> keys;
    keys.push_back(col_expr("id"));
    auto sort = make_sort(std::move(scan), std::move(keys), {true});  // ascending

    ASSERT_TRUE(sort->init());
    ASSERT_TRUE(sort->open());

    std::vector<int64_t> values;
    Tuple tuple;
    while (sort->next(tuple)) {
        values.push_back(tuple.get(0).to_int64());
    }
    ASSERT_EQ(values.size(), 3U);
    EXPECT_EQ(values[0], 1);
    EXPECT_EQ(values[1], 2);
    EXPECT_EQ(values[2], 3);
    sort->close();
}

TEST_F(OperatorTests, SortDescending) {
    Schema schema = make_schema({{"id", common::ValueType::TYPE_INT64}});
    std::vector<Tuple> data;
    data.push_back(make_tuple({common::Value::make_int64(1)}));
    data.push_back(make_tuple({common::Value::make_int64(3)}));
    data.push_back(make_tuple({common::Value::make_int64(2)}));

    auto scan = make_buffer_scan("test_table", data, schema);
    std::vector<std::unique_ptr<Expression>> keys;
    keys.push_back(col_expr("id"));
    auto sort = make_sort(std::move(scan), std::move(keys), {false});  // descending

    ASSERT_TRUE(sort->init());
    ASSERT_TRUE(sort->open());

    std::vector<int64_t> values;
    Tuple tuple;
    while (sort->next(tuple)) {
        values.push_back(tuple.get(0).to_int64());
    }
    ASSERT_EQ(values.size(), 3U);
    EXPECT_EQ(values[0], 3);
    EXPECT_EQ(values[1], 2);
    EXPECT_EQ(values[2], 1);
    sort->close();
}

TEST_F(OperatorTests, AggregateCountAll) {
    Schema schema = make_schema({{"id", common::ValueType::TYPE_INT64}});
    std::vector<Tuple> data;
    for (int i = 0; i < 5; i++) {
        data.push_back(make_tuple({common::Value::make_int64(i * 10)}));
    }

    auto scan = make_buffer_scan("test_table", data, schema);
    std::vector<AggregateInfo> aggs;
    aggs.push_back(make_agg(AggregateType::Count, "count"));  // COUNT(*)
    auto agg = make_agg_op(std::move(scan), {}, std::move(aggs));

    ASSERT_TRUE(agg->init());
    ASSERT_TRUE(agg->open());

    Tuple tuple;
    EXPECT_TRUE(agg->next(tuple));
    EXPECT_EQ(tuple.get(0).to_int64(), 5);
    EXPECT_FALSE(agg->next(tuple));
    agg->close();
}

TEST_F(OperatorTests, AggregateSum) {
    Schema schema = make_schema({{"val", common::ValueType::TYPE_INT64}});
    std::vector<Tuple> data;
    data.push_back(make_tuple({common::Value::make_int64(10)}));
    data.push_back(make_tuple({common::Value::make_int64(20)}));
    data.push_back(make_tuple({common::Value::make_int64(30)}));

    auto scan = make_buffer_scan("test_table", data, schema);
    std::vector<AggregateInfo> aggs;
    aggs.push_back(make_agg(AggregateType::Sum, "total", col_expr("val")));
    auto agg = make_agg_op(std::move(scan), {}, std::move(aggs));

    ASSERT_TRUE(agg->init());
    ASSERT_TRUE(agg->open());

    Tuple tuple;
    EXPECT_TRUE(agg->next(tuple));
    EXPECT_EQ(tuple.get(0).to_int64(), 60);
    EXPECT_FALSE(agg->next(tuple));
    agg->close();
}

TEST_F(OperatorTests, AggregateMinMax) {
    Schema schema = make_schema({{"val", common::ValueType::TYPE_INT64}});
    std::vector<Tuple> data;
    data.push_back(make_tuple({common::Value::make_int64(30)}));
    data.push_back(make_tuple({common::Value::make_int64(10)}));
    data.push_back(make_tuple({common::Value::make_int64(20)}));

    auto scan = make_buffer_scan("test_table", data, schema);
    std::vector<AggregateInfo> aggs;
    aggs.push_back(make_agg(AggregateType::Min, "min_val", col_expr("val")));
    aggs.push_back(make_agg(AggregateType::Max, "max_val", col_expr("val")));
    auto agg = make_agg_op(std::move(scan), {}, std::move(aggs));

    ASSERT_TRUE(agg->init());
    ASSERT_TRUE(agg->open());

    Tuple tuple;
    EXPECT_TRUE(agg->next(tuple));
    EXPECT_EQ(tuple.get(0).to_int64(), 10);  // min
    EXPECT_EQ(tuple.get(1).to_int64(), 30);  // max
    EXPECT_FALSE(agg->next(tuple));
    agg->close();
}

TEST_F(OperatorTests, HashJoinInner) {
    // Left table: one column with values 1, 2
    Schema left_schema = make_schema({{"id", common::ValueType::TYPE_INT64}});
    std::vector<Tuple> left_data;
    left_data.push_back(make_tuple({common::Value::make_int64(1)}));
    left_data.push_back(make_tuple({common::Value::make_int64(2)}));
    left_data.push_back(make_tuple({common::Value::make_int64(3)}));  // no match

    // Right table: one column with values 2, 3
    Schema right_schema = make_schema({{"id", common::ValueType::TYPE_INT64}});
    std::vector<Tuple> right_data;
    right_data.push_back(make_tuple({common::Value::make_int64(2)}));
    right_data.push_back(make_tuple({common::Value::make_int64(3)}));
    right_data.push_back(make_tuple({common::Value::make_int64(4)}));  // no match

    auto left_scan = make_buffer_scan("left_table", left_data, left_schema);
    auto right_scan = make_buffer_scan("right_table", right_data, right_schema);

    auto join = make_hash_join(std::move(left_scan), std::move(right_scan), col_expr("id"),
                               col_expr("id"), JoinType::Inner);

    ASSERT_TRUE(join->init());
    ASSERT_TRUE(join->open());

    std::vector<std::pair<int64_t, int64_t>> results;
    Tuple tuple;
    while (join->next(tuple)) {
        results.push_back({tuple.get(0).to_int64(), tuple.get(1).to_int64()});
    }

    EXPECT_EQ(results.size(), 2U);
    // 2 matches: (1,?) no, (2,2) yes, (3,3) yes
    // Left has 1,2,3 - Right has 2,3,4
    // Inner join: (2,2), (3,3)
    EXPECT_EQ(results[0].first, 2);
    EXPECT_EQ(results[0].second, 2);
    EXPECT_EQ(results[1].first, 3);
    EXPECT_EQ(results[1].second, 3);
    join->close();
}

TEST_F(OperatorTests, HashJoinLeft) {
    // Left table: values 1, 2
    Schema left_schema = make_schema({{"id", common::ValueType::TYPE_INT64}});
    std::vector<Tuple> left_data;
    left_data.push_back(make_tuple({common::Value::make_int64(1)}));  // no match
    left_data.push_back(make_tuple({common::Value::make_int64(2)}));  // matches

    // Right table: values 2, 3
    Schema right_schema = make_schema({{"id", common::ValueType::TYPE_INT64}});
    std::vector<Tuple> right_data;
    right_data.push_back(make_tuple({common::Value::make_int64(2)}));
    right_data.push_back(make_tuple({common::Value::make_int64(3)}));

    auto left_scan = make_buffer_scan("left_table", left_data, left_schema);
    auto right_scan = make_buffer_scan("right_table", right_data, right_schema);

    auto join = make_hash_join(std::move(left_scan), std::move(right_scan), col_expr("id"),
                               col_expr("id"), JoinType::Left);

    ASSERT_TRUE(join->init());
    ASSERT_TRUE(join->open());

    std::vector<int64_t> results;
    Tuple tuple;
    while (join->next(tuple)) {
        results.push_back(tuple.get(0).to_int64());
    }

    EXPECT_EQ(results.size(), 2U);
    EXPECT_EQ(results[0], 1);  // left tuple with no match - NULLs
    EXPECT_EQ(results[1], 2);  // matched
    join->close();
}

TEST_F(OperatorTests, HashJoinLeftUnmatchedCollection) {
    // Test that get_unmatched_left_rows/keys correctly tracks unmatched left tuples
    // Left table: values 1, 2, 3 (only 2 has a match)
    Schema left_schema = make_schema({{"id", common::ValueType::TYPE_INT64}});
    std::vector<Tuple> left_data;
    left_data.push_back(make_tuple({common::Value::make_int64(1)}));  // no match
    left_data.push_back(make_tuple({common::Value::make_int64(2)}));  // matches
    left_data.push_back(make_tuple({common::Value::make_int64(3)}));  // no match

    // Right table: values 2, 4
    Schema right_schema = make_schema({{"id", common::ValueType::TYPE_INT64}});
    std::vector<Tuple> right_data;
    right_data.push_back(make_tuple({common::Value::make_int64(2)}));
    right_data.push_back(make_tuple({common::Value::make_int64(4)}));

    auto left_scan = make_buffer_scan("left_table", left_data, left_schema);
    auto right_scan = make_buffer_scan("right_table", right_data, right_schema);

    auto join = make_hash_join(std::move(left_scan), std::move(right_scan), col_expr("id"),
                               col_expr("id"), JoinType::Left);

    ASSERT_TRUE(join->init());
    ASSERT_TRUE(join->open());

    // Consume all join results
    Tuple tuple;
    while (join->next(tuple)) {
    }

    // After join completes, verify unmatched left tracking
    auto unmatched_rows = join->get_unmatched_left_rows();
    auto unmatched_keys = join->get_unmatched_left_keys();

    // We expect 2 unmatched left tuples: id=1 and id=3
    EXPECT_EQ(unmatched_rows.size(), 2U);
    EXPECT_EQ(unmatched_keys.size(), 2U);

    // Keys should be "1" and "3" (to_string of int64)
    EXPECT_EQ(unmatched_keys[0], "1");
    EXPECT_EQ(unmatched_keys[1], "3");

    // Check the actual tuple values
    EXPECT_EQ(unmatched_rows[0].get(0).to_int64(), 1);
    EXPECT_EQ(unmatched_rows[1].get(0).to_int64(), 3);

    join->close();
}

TEST_F(OperatorTests, HashJoinFullUnmatchedLeftCollection) {
    // Test LEFT unmatched collection for FULL join
    // Similar to LEFT join but tests the FULL join path
    Schema left_schema = make_schema({{"id", common::ValueType::TYPE_INT64}});
    std::vector<Tuple> left_data;
    left_data.push_back(make_tuple({common::Value::make_int64(1)}));  // no match
    left_data.push_back(make_tuple({common::Value::make_int64(2)}));  // matches

    Schema right_schema = make_schema({{"id", common::ValueType::TYPE_INT64}});
    std::vector<Tuple> right_data;
    right_data.push_back(make_tuple({common::Value::make_int64(2)}));
    right_data.push_back(make_tuple({common::Value::make_int64(3)}));  // no match

    auto left_scan = make_buffer_scan("left_table", left_data, left_schema);
    auto right_scan = make_buffer_scan("right_table", right_data, right_schema);

    auto join = make_hash_join(std::move(left_scan), std::move(right_scan), col_expr("id"),
                               col_expr("id"), JoinType::Full);

    ASSERT_TRUE(join->init());
    ASSERT_TRUE(join->open());

    // Consume all join results
    Tuple tuple;
    while (join->next(tuple)) {
    }

    // For FULL join, we should track unmatched LEFT tuples
    // Note: RIGHT unmatched tuples are emitted during right scan phase and marked matched,
    // so get_unmatched_right_keys() won't include them (they're already "accounted for")
    auto unmatched_left_keys = join->get_unmatched_left_keys();

    // Left unmatched: id=1
    EXPECT_EQ(unmatched_left_keys.size(), 1U);
    EXPECT_EQ(unmatched_left_keys[0], "1");

    join->close();
}

TEST_F(OperatorTests, HashJoinEmpty) {
    // Left has data
    Schema left_schema = make_schema({{"id", common::ValueType::TYPE_INT64}});
    std::vector<Tuple> left_data;
    left_data.push_back(make_tuple({common::Value::make_int64(1)}));

    // Right is empty
    Schema right_schema = make_schema({{"id", common::ValueType::TYPE_INT64}});
    std::vector<Tuple> right_data;

    auto left_scan = make_buffer_scan("left_table", left_data, left_schema);
    auto right_scan = make_buffer_scan("right_table", right_data, right_schema);

    auto join = make_hash_join(std::move(left_scan), std::move(right_scan), col_expr("id"),
                               col_expr("id"), JoinType::Inner);

    ASSERT_TRUE(join->init());
    ASSERT_TRUE(join->open());

    Tuple tuple;
    EXPECT_FALSE(join->next(tuple));
    join->close();
}

TEST_F(OperatorTests, PipelineFilterProject) {
    // Input: (1,"alice"), (2,"bob"), (3,"charlie")
    Schema schema = make_schema(
        {{"id", common::ValueType::TYPE_INT64}, {"name", common::ValueType::TYPE_TEXT}});
    std::vector<Tuple> data;
    data.push_back(make_tuple({common::Value::make_int64(1), common::Value::make_text("alice")}));
    data.push_back(make_tuple({common::Value::make_int64(2), common::Value::make_text("bob")}));
    data.push_back(make_tuple({common::Value::make_int64(3), common::Value::make_text("charlie")}));

    auto scan = make_buffer_scan("test_table", data, schema);

    // Filter: id >= 2
    auto filter = make_filter(
        std::move(scan),
        binary_expr(col_expr("id"), TokenType::Ge, const_expr(common::Value::make_int64(2))));

    // Project: name column only
    std::vector<std::unique_ptr<Expression>> cols;
    cols.push_back(col_expr("name"));
    auto project = make_project(std::move(filter), std::move(cols));

    ASSERT_TRUE(project->init());
    ASSERT_TRUE(project->open());

    int count = 0;
    Tuple tuple;
    while (project->next(tuple)) {
        count++;
        EXPECT_EQ(tuple.size(), 1U);
        EXPECT_STREQ(tuple.get(0).as_text().c_str(), count == 1 ? "bob" : "charlie");
    }
    EXPECT_EQ(count, 2);
    project->close();
}

TEST_F(OperatorTests, OperatorTypeEnum) {
    EXPECT_EQ(OperatorType::SeqScan, OperatorType::SeqScan);
    EXPECT_EQ(OperatorType::IndexScan, OperatorType::IndexScan);
    EXPECT_EQ(OperatorType::Filter, OperatorType::Filter);
    EXPECT_EQ(OperatorType::Project, OperatorType::Project);
    EXPECT_EQ(OperatorType::HashJoin, OperatorType::HashJoin);
    EXPECT_EQ(OperatorType::Sort, OperatorType::Sort);
    EXPECT_EQ(OperatorType::Aggregate, OperatorType::Aggregate);
    EXPECT_EQ(OperatorType::Limit, OperatorType::Limit);
    EXPECT_EQ(OperatorType::BufferScan, OperatorType::BufferScan);
}

TEST_F(OperatorTests, LimitOffsetExceedsTotal) {
    Schema schema = make_schema({{"id", common::ValueType::TYPE_INT64}});
    std::vector<Tuple> data;
    for (int i = 0; i < 3; i++) {
        data.push_back(make_tuple({common::Value::make_int64(i)}));
    }

    auto scan = make_buffer_scan("test_table", data, schema);
    auto limit = make_limit(std::move(scan), 10, 100);  // offset 100, limit 10

    ASSERT_TRUE(limit->init());
    ASSERT_TRUE(limit->open());

    // offset exceeds total - should return nothing
    Tuple tuple;
    EXPECT_FALSE(limit->next(tuple));
    limit->close();
}

TEST_F(OperatorTests, SortStable) {
    // Test stable sort: equal keys preserve input order
    Schema schema = make_schema(
        {{"id", common::ValueType::TYPE_INT64}, {"val", common::ValueType::TYPE_INT64}});
    std::vector<Tuple> data;
    // Three tuples with same key (1)
    data.push_back(make_tuple({common::Value::make_int64(1), common::Value::make_int64(100)}));
    data.push_back(make_tuple({common::Value::make_int64(1), common::Value::make_int64(200)}));
    data.push_back(make_tuple({common::Value::make_int64(1), common::Value::make_int64(300)}));

    auto scan = make_buffer_scan("test_table", data, schema);
    std::vector<std::unique_ptr<Expression>> keys;
    keys.push_back(col_expr("id"));
    auto sort = make_sort(std::move(scan), std::move(keys), {true});  // sort by id, ascending

    ASSERT_TRUE(sort->init());
    ASSERT_TRUE(sort->open());

    std::vector<int64_t> values;
    Tuple tuple;
    while (sort->next(tuple)) {
        values.push_back(tuple.get(1).to_int64());
    }
    // Should preserve input order for equal keys: 100, 200, 300
    ASSERT_EQ(values.size(), 3U);
    EXPECT_EQ(values[0], 100);
    EXPECT_EQ(values[1], 200);
    EXPECT_EQ(values[2], 300);
    sort->close();
}

TEST_F(OperatorTests, AggregateAvg) {
    Schema schema = make_schema({{"val", common::ValueType::TYPE_INT64}});
    std::vector<Tuple> data;
    data.push_back(make_tuple({common::Value::make_int64(10)}));
    data.push_back(make_tuple({common::Value::make_int64(20)}));
    data.push_back(make_tuple({common::Value::make_int64(30)}));

    auto scan = make_buffer_scan("test_table", data, schema);
    std::vector<AggregateInfo> aggs;
    aggs.push_back(make_agg(AggregateType::Avg, "avg_val", col_expr("val")));
    auto agg = make_agg_op(std::move(scan), {}, std::move(aggs));

    ASSERT_TRUE(agg->init());
    ASSERT_TRUE(agg->open());

    Tuple tuple;
    EXPECT_TRUE(agg->next(tuple));
    EXPECT_EQ(tuple.get(0).to_int64(), 20);  // (10+20+30)/3 = 20
    EXPECT_FALSE(agg->next(tuple));
    agg->close();
}

}  // namespace
