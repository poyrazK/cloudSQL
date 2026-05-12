/**
 * @file expression_tests.cpp
 * @brief Unit tests for Expression evaluation
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/value.hpp"
#include "executor/types.hpp"
#include "parser/expression.hpp"
#include "parser/token.hpp"

using namespace cloudsql::common;
using namespace cloudsql::executor;
using namespace cloudsql::parser;

namespace {

// Helper to create schema and tuple for expression evaluation
static Schema make_schema(const std::vector<std::string>& names,
                          const std::vector<ValueType>& types) {
    Schema schema;
    for (size_t i = 0; i < names.size(); ++i) {
        schema.add_column(names[i], types[i], false);
    }
    return schema;
}

static Tuple make_tuple(const std::vector<Value>& values) {
    return Tuple(values);
}

// ============= BinaryExpr Arithmetic Tests =============

TEST(ExpressionTests, BinaryExprPlusInt) {
    auto left = std::make_unique<ConstantExpr>(Value::make_int64(5));
    auto right = std::make_unique<ConstantExpr>(Value::make_int64(3));
    BinaryExpr expr(std::move(left), TokenType::Plus, std::move(right));

    auto result = expr.evaluate();
    EXPECT_EQ(result.as_int64(), 8);
}

TEST(ExpressionTests, BinaryExprMinusInt) {
    auto left = std::make_unique<ConstantExpr>(Value::make_int64(10));
    auto right = std::make_unique<ConstantExpr>(Value::make_int64(3));
    BinaryExpr expr(std::move(left), TokenType::Minus, std::move(right));

    auto result = expr.evaluate();
    EXPECT_EQ(result.as_int64(), 7);
}

TEST(ExpressionTests, BinaryExprStarInt) {
    auto left = std::make_unique<ConstantExpr>(Value::make_int64(6));
    auto right = std::make_unique<ConstantExpr>(Value::make_int64(7));
    BinaryExpr expr(std::move(left), TokenType::Star, std::move(right));

    auto result = expr.evaluate();
    EXPECT_EQ(result.as_int64(), 42);
}

TEST(ExpressionTests, BinaryExprSlashInt) {
    auto left = std::make_unique<ConstantExpr>(Value::make_int64(20));
    auto right = std::make_unique<ConstantExpr>(Value::make_int64(4));
    BinaryExpr expr(std::move(left), TokenType::Slash, std::move(right));

    // Division always returns float64
    auto result = expr.evaluate();
    EXPECT_DOUBLE_EQ(result.as_float64(), 5.0);
}

TEST(ExpressionTests, BinaryExprArithmeticFloat) {
    auto left = std::make_unique<ConstantExpr>(Value::make_float64(3.5));
    auto right = std::make_unique<ConstantExpr>(Value::make_float64(2.0));
    BinaryExpr expr(std::move(left), TokenType::Plus, std::move(right));

    auto result = expr.evaluate();
    EXPECT_DOUBLE_EQ(result.as_float64(), 5.5);
}

TEST(ExpressionTests, BinaryExprIntPlusFloat) {
    auto left = std::make_unique<ConstantExpr>(Value::make_int64(5));
    auto right = std::make_unique<ConstantExpr>(Value::make_float64(2.5));
    BinaryExpr expr(std::move(left), TokenType::Plus, std::move(right));

    auto result = expr.evaluate();
    EXPECT_DOUBLE_EQ(result.as_float64(), 7.5);
}

// ============= BinaryExpr Comparison Tests =============

TEST(ExpressionTests, BinaryExprEqInt) {
    auto left = std::make_unique<ConstantExpr>(Value::make_int64(5));
    auto right = std::make_unique<ConstantExpr>(Value::make_int64(5));
    BinaryExpr expr(std::move(left), TokenType::Eq, std::move(right));

    auto result = expr.evaluate();
    EXPECT_TRUE(result.as_bool());
}

TEST(ExpressionTests, BinaryExprNeInt) {
    auto left = std::make_unique<ConstantExpr>(Value::make_int64(5));
    auto right = std::make_unique<ConstantExpr>(Value::make_int64(3));
    BinaryExpr expr(std::move(left), TokenType::Ne, std::move(right));

    auto result = expr.evaluate();
    EXPECT_TRUE(result.as_bool());
}

TEST(ExpressionTests, BinaryExprLtInt) {
    auto left = std::make_unique<ConstantExpr>(Value::make_int64(3));
    auto right = std::make_unique<ConstantExpr>(Value::make_int64(5));
    BinaryExpr expr(std::move(left), TokenType::Lt, std::move(right));

    auto result = expr.evaluate();
    EXPECT_TRUE(result.as_bool());
}

TEST(ExpressionTests, BinaryExprLeInt) {
    auto left = std::make_unique<ConstantExpr>(Value::make_int64(5));
    auto right = std::make_unique<ConstantExpr>(Value::make_int64(5));
    BinaryExpr expr(std::move(left), TokenType::Le, std::move(right));

    auto result = expr.evaluate();
    EXPECT_TRUE(result.as_bool());
}

TEST(ExpressionTests, BinaryExprGtInt) {
    auto left = std::make_unique<ConstantExpr>(Value::make_int64(5));
    auto right = std::make_unique<ConstantExpr>(Value::make_int64(3));
    BinaryExpr expr(std::move(left), TokenType::Gt, std::move(right));

    auto result = expr.evaluate();
    EXPECT_TRUE(result.as_bool());
}

TEST(ExpressionTests, BinaryExprGeInt) {
    auto left = std::make_unique<ConstantExpr>(Value::make_int64(5));
    auto right = std::make_unique<ConstantExpr>(Value::make_int64(5));
    BinaryExpr expr(std::move(left), TokenType::Ge, std::move(right));

    auto result = expr.evaluate();
    EXPECT_TRUE(result.as_bool());
}

// ============= BinaryExpr Logical Tests =============

TEST(ExpressionTests, BinaryExprAnd) {
    auto left = std::make_unique<ConstantExpr>(Value::make_bool(true));
    auto right = std::make_unique<ConstantExpr>(Value::make_bool(true));
    BinaryExpr expr(std::move(left), TokenType::And, std::move(right));

    auto result = expr.evaluate();
    EXPECT_TRUE(result.as_bool());
}

TEST(ExpressionTests, BinaryExprAndFalse) {
    auto left = std::make_unique<ConstantExpr>(Value::make_bool(true));
    auto right = std::make_unique<ConstantExpr>(Value::make_bool(false));
    BinaryExpr expr(std::move(left), TokenType::And, std::move(right));

    auto result = expr.evaluate();
    EXPECT_FALSE(result.as_bool());
}

TEST(ExpressionTests, BinaryExprOr) {
    auto left = std::make_unique<ConstantExpr>(Value::make_bool(false));
    auto right = std::make_unique<ConstantExpr>(Value::make_bool(true));
    BinaryExpr expr(std::move(left), TokenType::Or, std::move(right));

    auto result = expr.evaluate();
    EXPECT_TRUE(result.as_bool());
}

TEST(ExpressionTests, BinaryExprOrBothFalse) {
    auto left = std::make_unique<ConstantExpr>(Value::make_bool(false));
    auto right = std::make_unique<ConstantExpr>(Value::make_bool(false));
    BinaryExpr expr(std::move(left), TokenType::Or, std::move(right));

    auto result = expr.evaluate();
    EXPECT_FALSE(result.as_bool());
}

// ============= UnaryExpr Tests =============

TEST(ExpressionTests, UnaryExprMinus) {
    auto inner = std::make_unique<ConstantExpr>(Value::make_int64(42));
    UnaryExpr expr(TokenType::Minus, std::move(inner));

    auto result = expr.evaluate();
    EXPECT_EQ(result.as_int64(), -42);
}

TEST(ExpressionTests, UnaryExprMinusFloat) {
    auto inner = std::make_unique<ConstantExpr>(Value::make_float64(3.14));
    UnaryExpr expr(TokenType::Minus, std::move(inner));

    auto result = expr.evaluate();
    EXPECT_DOUBLE_EQ(result.as_float64(), -3.14);
}

TEST(ExpressionTests, UnaryExprNot) {
    auto inner = std::make_unique<ConstantExpr>(Value::make_bool(false));
    UnaryExpr expr(TokenType::Not, std::move(inner));

    auto result = expr.evaluate();
    EXPECT_TRUE(result.as_bool());
}

TEST(ExpressionTests, UnaryExprNotTrue) {
    auto inner = std::make_unique<ConstantExpr>(Value::make_bool(true));
    UnaryExpr expr(TokenType::Not, std::move(inner));

    auto result = expr.evaluate();
    EXPECT_FALSE(result.as_bool());
}

// ============= ColumnExpr Tests =============

TEST(ExpressionTests, ColumnExprSimple) {
    auto schema = make_schema({"id", "name"}, {ValueType::TYPE_INT64, ValueType::TYPE_TEXT});
    auto tuple = make_tuple({Value::make_int64(42), Value::make_text("Alice")});

    ColumnExpr expr("id");
    auto result = expr.evaluate(&tuple, &schema);
    EXPECT_EQ(result.as_int64(), 42);
}

TEST(ExpressionTests, ColumnExprQualified) {
    auto schema = make_schema({"id", "name"}, {ValueType::TYPE_INT64, ValueType::TYPE_TEXT});
    auto tuple = make_tuple({Value::make_int64(42), Value::make_text("Alice")});

    ColumnExpr expr("users", "id");
    auto result = expr.evaluate(&tuple, &schema);
    EXPECT_EQ(result.as_int64(), 42);
}

TEST(ExpressionTests, ColumnExprNotFound) {
    auto schema = make_schema({"id", "name"}, {ValueType::TYPE_INT64, ValueType::TYPE_TEXT});
    auto tuple = make_tuple({Value::make_int64(42), Value::make_text("Alice")});

    ColumnExpr expr("notfound");
    auto result = expr.evaluate(&tuple, &schema);
    EXPECT_TRUE(result.is_null());
}

TEST(ExpressionTests, ColumnExprNullTuple) {
    ColumnExpr expr("id");
    auto result = expr.evaluate(nullptr, nullptr);
    EXPECT_TRUE(result.is_null());
}

// ============= ConstantExpr Tests =============

TEST(ExpressionTests, ConstantExprInt64) {
    ConstantExpr expr(Value::make_int64(42));
    auto result = expr.evaluate();
    EXPECT_EQ(result.as_int64(), 42);
}

TEST(ExpressionTests, ConstantExprFloat64) {
    ConstantExpr expr(Value::make_float64(3.14));
    auto result = expr.evaluate();
    EXPECT_DOUBLE_EQ(result.as_float64(), 3.14);
}

TEST(ExpressionTests, ConstantExprText) {
    ConstantExpr expr(Value::make_text("hello"));
    auto result = expr.evaluate();
    EXPECT_EQ(result.as_text(), "hello");
}

TEST(ExpressionTests, ConstantExprBool) {
    ConstantExpr expr1(Value::make_bool(true));
    ConstantExpr expr2(Value::make_bool(false));
    EXPECT_TRUE(expr1.evaluate().as_bool());
    EXPECT_FALSE(expr2.evaluate().as_bool());
}

TEST(ExpressionTests, ConstantExprNull) {
    ConstantExpr expr(Value::make_null());
    auto result = expr.evaluate();
    EXPECT_TRUE(result.is_null());
}

// ============= ParameterExpr Tests =============

TEST(ExpressionTests, ParameterExpr) {
    std::vector<Value> params = {Value::make_int64(42), Value::make_text("hello")};

    ParameterExpr expr1(0);
    ParameterExpr expr2(1);
    ParameterExpr expr3(2);  // Out of bounds

    EXPECT_EQ(expr1.evaluate(nullptr, nullptr, &params).as_int64(), 42);
    EXPECT_EQ(expr2.evaluate(nullptr, nullptr, &params).as_text(), "hello");
    EXPECT_TRUE(expr3.evaluate(nullptr, nullptr, &params).is_null());
}

TEST(ExpressionTests, ParameterExprNullParams) {
    ParameterExpr expr(0);
    auto result = expr.evaluate(nullptr, nullptr, nullptr);
    EXPECT_TRUE(result.is_null());
}

// ============= FunctionExpr Tests =============

TEST(ExpressionTests, FunctionExprBasic) {
    FunctionExpr expr("MAX");
    expr.add_arg(std::make_unique<ConstantExpr>(Value::make_int64(5)));

    auto result = expr.to_string();
    EXPECT_EQ(result, "MAX(5)");
}

TEST(ExpressionTests, FunctionExprDistinct) {
    FunctionExpr expr("COUNT");
    expr.set_distinct(true);
    expr.add_arg(std::make_unique<ColumnExpr>("id"));

    auto result = expr.to_string();
    EXPECT_EQ(result, "COUNT(DISTINCT id)");
}

TEST(ExpressionTests, FunctionExprMultipleArgs) {
    FunctionExpr expr("CONCAT");
    expr.add_arg(std::make_unique<ConstantExpr>(Value::make_text("a")));
    expr.add_arg(std::make_unique<ConstantExpr>(Value::make_text("b")));

    auto result = expr.to_string();
    EXPECT_EQ(result, "CONCAT('a', 'b')");
}

// ============= InExpr Tests =============

TEST(ExpressionTests, InExprFound) {
    auto column = std::make_unique<ConstantExpr>(Value::make_int64(5));
    std::vector<std::unique_ptr<Expression>> values;
    values.push_back(std::make_unique<ConstantExpr>(Value::make_int64(1)));
    values.push_back(std::make_unique<ConstantExpr>(Value::make_int64(5)));
    values.push_back(std::make_unique<ConstantExpr>(Value::make_int64(10)));

    InExpr expr(std::move(column), std::move(values), false);
    auto result = expr.evaluate();
    EXPECT_TRUE(result.as_bool());
}

TEST(ExpressionTests, InExprNotFound) {
    auto column = std::make_unique<ConstantExpr>(Value::make_int64(7));
    std::vector<std::unique_ptr<Expression>> values;
    values.push_back(std::make_unique<ConstantExpr>(Value::make_int64(1)));
    values.push_back(std::make_unique<ConstantExpr>(Value::make_int64(5)));
    values.push_back(std::make_unique<ConstantExpr>(Value::make_int64(10)));

    InExpr expr(std::move(column), std::move(values), false);
    auto result = expr.evaluate();
    EXPECT_FALSE(result.as_bool());
}

TEST(ExpressionTests, InExprNotFlag) {
    auto column = std::make_unique<ConstantExpr>(Value::make_int64(7));
    std::vector<std::unique_ptr<Expression>> values;
    values.push_back(std::make_unique<ConstantExpr>(Value::make_int64(1)));
    values.push_back(std::make_unique<ConstantExpr>(Value::make_int64(5)));
    values.push_back(std::make_unique<ConstantExpr>(Value::make_int64(10)));

    InExpr expr(std::move(column), std::move(values), true);  // NOT IN
    auto result = expr.evaluate();
    EXPECT_TRUE(result.as_bool());  // 7 NOT IN (1, 5, 10) = true
}

// ============= IsNullExpr Tests =============

TEST(ExpressionTests, IsNullExprTrue) {
    auto inner = std::make_unique<ConstantExpr>(Value::make_null());
    IsNullExpr expr(std::move(inner), false);

    auto result = expr.evaluate();
    EXPECT_TRUE(result.as_bool());
}

TEST(ExpressionTests, IsNullExprFalse) {
    auto inner = std::make_unique<ConstantExpr>(Value::make_int64(5));
    IsNullExpr expr(std::move(inner), false);

    auto result = expr.evaluate();
    EXPECT_FALSE(result.as_bool());
}

TEST(ExpressionTests, IsNotNullExprTrue) {
    auto inner = std::make_unique<ConstantExpr>(Value::make_int64(5));
    IsNullExpr expr(std::move(inner), true);  // IS NOT NULL

    auto result = expr.evaluate();
    EXPECT_TRUE(result.as_bool());
}

TEST(ExpressionTests, IsNotNullExprFalse) {
    auto inner = std::make_unique<ConstantExpr>(Value::make_null());
    IsNullExpr expr(std::move(inner), true);  // IS NOT NULL

    auto result = expr.evaluate();
    EXPECT_FALSE(result.as_bool());
}

// ============= Expression Clone Tests =============

TEST(ExpressionTests, BinaryExprClone) {
    auto left = std::make_unique<ConstantExpr>(Value::make_int64(5));
    auto right = std::make_unique<ConstantExpr>(Value::make_int64(3));
    BinaryExpr expr(std::move(left), TokenType::Plus, std::move(right));

    auto cloned = expr.clone();
    EXPECT_EQ(cloned->type(), ExprType::Binary);
    EXPECT_EQ(cloned->evaluate().as_int64(), 8);
}

TEST(ExpressionTests, ConstantExprClone) {
    ConstantExpr expr(Value::make_int64(42));
    auto cloned = expr.clone();

    EXPECT_EQ(cloned->type(), ExprType::Constant);
    EXPECT_EQ(cloned->evaluate().as_int64(), 42);
}

TEST(ExpressionTests, UnaryExprClone) {
    auto inner = std::make_unique<ConstantExpr>(Value::make_int64(5));
    UnaryExpr expr(TokenType::Minus, std::move(inner));

    auto cloned = expr.clone();
    EXPECT_EQ(cloned->type(), ExprType::Unary);
    EXPECT_EQ(cloned->evaluate().as_int64(), -5);
}

// ============= Expression to_string Tests =============

TEST(ExpressionTests, BinaryExprToString) {
    auto left = std::make_unique<ConstantExpr>(Value::make_int64(5));
    auto right = std::make_unique<ConstantExpr>(Value::make_int64(3));
    BinaryExpr expr(std::move(left), TokenType::Plus, std::move(right));

    EXPECT_EQ(expr.to_string(), "5 + 3");
}

TEST(ExpressionTests, BinaryExprToStringComparison) {
    auto left = std::make_unique<ConstantExpr>(Value::make_int64(5));
    auto right = std::make_unique<ConstantExpr>(Value::make_int64(3));
    BinaryExpr expr(std::move(left), TokenType::Ne, std::move(right));

    EXPECT_EQ(expr.to_string(), "5 <> 3");
}

TEST(ExpressionTests, ColumnExprToString) {
    ColumnExpr expr("users", "id");
    EXPECT_EQ(expr.to_string(), "users.id");
}

TEST(ExpressionTests, ColumnExprSimpleToString) {
    ColumnExpr expr("id");
    EXPECT_EQ(expr.to_string(), "id");
}

TEST(ExpressionTests, ConstantExprToStringInt) {
    ConstantExpr expr(Value::make_int64(42));
    EXPECT_EQ(expr.to_string(), "42");
}

TEST(ExpressionTests, ConstantExprToStringText) {
    ConstantExpr expr(Value::make_text("hello"));
    EXPECT_EQ(expr.to_string(), "'hello'");
}

TEST(ExpressionTests, IsNullExprToString) {
    auto inner = std::make_unique<ColumnExpr>("age");
    IsNullExpr expr(std::move(inner), false);
    EXPECT_EQ(expr.to_string(), "age IS NULL");
}

TEST(ExpressionTests, IsNotNullExprToString) {
    auto inner = std::make_unique<ColumnExpr>("age");
    IsNullExpr expr(std::move(inner), true);
    EXPECT_EQ(expr.to_string(), "age IS NOT NULL");
}

// ============= Complex Expression Tests =============

TEST(ExpressionTests, ComplexArithmeticExpression) {
    // (5 + 3) * 2 = 16
    auto left = std::make_unique<BinaryExpr>(std::make_unique<ConstantExpr>(Value::make_int64(5)),
                                             TokenType::Plus,
                                             std::make_unique<ConstantExpr>(Value::make_int64(3)));
    auto right = std::make_unique<ConstantExpr>(Value::make_int64(2));
    BinaryExpr expr(std::move(left), TokenType::Star, std::move(right));

    auto result = expr.evaluate();
    EXPECT_EQ(result.as_int64(), 16);
}

TEST(ExpressionTests, ComplexLogicalExpression) {
    // (5 > 3) AND (2 < 10)
    auto left = std::make_unique<BinaryExpr>(std::make_unique<ConstantExpr>(Value::make_int64(5)),
                                             TokenType::Gt,
                                             std::make_unique<ConstantExpr>(Value::make_int64(3)));
    auto right = std::make_unique<BinaryExpr>(
        std::make_unique<ConstantExpr>(Value::make_int64(2)), TokenType::Lt,
        std::make_unique<ConstantExpr>(Value::make_int64(10)));
    BinaryExpr expr(std::move(left), TokenType::And, std::move(right));

    auto result = expr.evaluate();
    EXPECT_TRUE(result.as_bool());
}

// ============= Expression Type Tests =============

TEST(ExpressionTests, BinaryExprType) {
    auto left = std::make_unique<ConstantExpr>(Value::make_int64(5));
    auto right = std::make_unique<ConstantExpr>(Value::make_int64(3));
    BinaryExpr expr(std::move(left), TokenType::Plus, std::move(right));

    EXPECT_EQ(expr.type(), ExprType::Binary);
}

TEST(ExpressionTests, UnaryExprType) {
    auto inner = std::make_unique<ConstantExpr>(Value::make_int64(5));
    UnaryExpr expr(TokenType::Minus, std::move(inner));

    EXPECT_EQ(expr.type(), ExprType::Unary);
}

TEST(ExpressionTests, ColumnExprType) {
    ColumnExpr expr("id");
    EXPECT_EQ(expr.type(), ExprType::Column);
}

TEST(ExpressionTests, ConstantExprType) {
    ConstantExpr expr(Value::make_int64(42));
    EXPECT_EQ(expr.type(), ExprType::Constant);
}

TEST(ExpressionTests, FunctionExprType) {
    FunctionExpr expr("COUNT");
    EXPECT_EQ(expr.type(), ExprType::Function);
}

TEST(ExpressionTests, InExprType) {
    auto column = std::make_unique<ConstantExpr>(Value::make_int64(5));
    std::vector<std::unique_ptr<Expression>> values;
    values.push_back(std::make_unique<ConstantExpr>(Value::make_int64(1)));

    InExpr expr(std::move(column), std::move(values), false);
    EXPECT_EQ(expr.type(), ExprType::In);
}

TEST(ExpressionTests, IsNullExprType) {
    auto inner = std::make_unique<ConstantExpr>(Value::make_int64(5));
    IsNullExpr expr(std::move(inner), false);

    EXPECT_EQ(expr.type(), ExprType::IsNull);
}

TEST(ExpressionTests, ParameterExprType) {
    ParameterExpr expr(0);
    EXPECT_EQ(expr.type(), ExprType::Parameter);
}

// ============= InExpr Edge Case Tests =============

TEST(ExpressionTests, InExprWithDuplicates) {
    // Column value appears multiple times in IN list
    auto column = std::make_unique<ConstantExpr>(Value::make_int64(5));
    std::vector<std::unique_ptr<Expression>> values;
    values.push_back(std::make_unique<ConstantExpr>(Value::make_int64(5)));
    values.push_back(std::make_unique<ConstantExpr>(Value::make_int64(5)));  // duplicate
    values.push_back(std::make_unique<ConstantExpr>(Value::make_int64(5)));  // duplicate

    InExpr expr(std::move(column), std::move(values), false);
    auto result = expr.evaluate();
    EXPECT_TRUE(result.as_bool());  // 5 IN (5, 5, 5) = true
}

TEST(ExpressionTests, InExprWithManyValues) {
    // IN with many values (10)
    auto column = std::make_unique<ConstantExpr>(Value::make_int64(7));
    std::vector<std::unique_ptr<Expression>> values;
    for (int i = 1; i <= 10; i++) {
        values.push_back(std::make_unique<ConstantExpr>(Value::make_int64(i)));
    }

    InExpr expr(std::move(column), std::move(values), false);
    auto result = expr.evaluate();
    EXPECT_TRUE(result.as_bool());  // 7 IN (1..10) = true
}

TEST(ExpressionTests, InExprNotWithManyValues) {
    // NOT IN with many values where value is not present
    auto column = std::make_unique<ConstantExpr>(Value::make_int64(15));
    std::vector<std::unique_ptr<Expression>> values;
    for (int i = 1; i <= 10; i++) {
        values.push_back(std::make_unique<ConstantExpr>(Value::make_int64(i)));
    }

    InExpr expr(std::move(column), std::move(values), true);  // NOT IN
    auto result = expr.evaluate();
    EXPECT_TRUE(result.as_bool());  // 15 NOT IN (1..10) = true
}

TEST(ExpressionTests, InExprWithNullInList) {
    // IN list containing NULL - SQL semantics: 5 IN (1, NULL, 10) = false
    // because 5 == NULL is NULL (not true), so NULL doesn't match
    auto column = std::make_unique<ConstantExpr>(Value::make_int64(5));
    std::vector<std::unique_ptr<Expression>> values;
    values.push_back(std::make_unique<ConstantExpr>(Value::make_int64(1)));
    values.push_back(std::make_unique<ConstantExpr>(Value::make_null()));  // NULL in list
    values.push_back(std::make_unique<ConstantExpr>(Value::make_int64(10)));

    InExpr expr(std::move(column), std::move(values), false);
    auto result = expr.evaluate();
    EXPECT_FALSE(result.as_bool());  // 5 IN (1, NULL, 10) = false (NULL doesn't match)
}

TEST(ExpressionTests, InExprEmptyList) {
    // IN with empty list
    auto column = std::make_unique<ConstantExpr>(Value::make_int64(5));
    std::vector<std::unique_ptr<Expression>> values;  // empty

    InExpr expr(std::move(column), std::move(values), false);
    auto result = expr.evaluate();
    EXPECT_FALSE(result.as_bool());  // 5 IN () = false
}

// ============= evaluate_vectorized() Tests =============

TEST(ExpressionTests, EvaluateVectorized_BinaryExpr_Int64_Gt) {
    // Test val > 5 with VectorBatch
    Schema schema;
    schema.add_column("val", ValueType::TYPE_INT64);

    VectorBatch batch;
    batch.init_from_schema(schema);
    // Append val=5 and val=10
    auto& col = batch.get_column(0);
    col.append(Value::make_int64(5));
    col.append(Value::make_int64(10));
    batch.set_row_count(2);

    NumericVector<bool> result(ValueType::TYPE_BOOL);

    auto left = std::make_unique<ColumnExpr>("val");
    auto right = std::make_unique<ConstantExpr>(Value::make_int64(5));
    BinaryExpr binary(std::move(left), TokenType::Gt, std::move(right));

    binary.evaluate_vectorized(batch, schema, result);

    EXPECT_EQ(result.size(), 2);
    EXPECT_EQ(result.get(0).as_bool(), false);  // 5 > 5 = false
    EXPECT_EQ(result.get(1).as_bool(), true);   // 10 > 5 = true
}

TEST(ExpressionTests, EvaluateVectorized_BinaryExpr_Int64_Eq) {
    Schema schema;
    schema.add_column("val", ValueType::TYPE_INT64);

    VectorBatch batch;
    batch.init_from_schema(schema);
    auto& col = batch.get_column(0);
    col.append(Value::make_int64(42));
    col.append(Value::make_int64(42));
    batch.set_row_count(2);

    NumericVector<bool> result(ValueType::TYPE_BOOL);

    auto left = std::make_unique<ColumnExpr>("val");
    auto right = std::make_unique<ConstantExpr>(Value::make_int64(42));
    BinaryExpr binary(std::move(left), TokenType::Eq, std::move(right));

    binary.evaluate_vectorized(batch, schema, result);

    EXPECT_EQ(result.size(), 2);
    EXPECT_EQ(result.get(0).as_bool(), true);  // 42 == 42 = true
    EXPECT_EQ(result.get(1).as_bool(), true);  // 42 == 42 = true
}

TEST(ExpressionTests, EvaluateVectorized_BinaryExpr_Int64_Ne) {
    Schema schema;
    schema.add_column("val", ValueType::TYPE_INT64);

    VectorBatch batch;
    batch.init_from_schema(schema);
    auto& col = batch.get_column(0);
    col.append(Value::make_int64(5));
    col.append(Value::make_int64(10));
    batch.set_row_count(2);

    NumericVector<bool> result(ValueType::TYPE_BOOL);

    auto left = std::make_unique<ColumnExpr>("val");
    auto right = std::make_unique<ConstantExpr>(Value::make_int64(5));
    BinaryExpr binary(std::move(left), TokenType::Ne, std::move(right));

    binary.evaluate_vectorized(batch, schema, result);

    EXPECT_EQ(result.size(), 2);
    EXPECT_EQ(result.get(0).as_bool(), false);  // 5 != 5 = false
    EXPECT_EQ(result.get(1).as_bool(), true);   // 10 != 5 = true
}

TEST(ExpressionTests, EvaluateVectorized_BinaryExpr_Int64_Lt) {
    Schema schema;
    schema.add_column("val", ValueType::TYPE_INT64);

    VectorBatch batch;
    batch.init_from_schema(schema);
    auto& col = batch.get_column(0);
    col.append(Value::make_int64(3));
    col.append(Value::make_int64(7));
    batch.set_row_count(2);

    NumericVector<bool> result(ValueType::TYPE_BOOL);

    auto left = std::make_unique<ColumnExpr>("val");
    auto right = std::make_unique<ConstantExpr>(Value::make_int64(5));
    BinaryExpr binary(std::move(left), TokenType::Lt, std::move(right));

    binary.evaluate_vectorized(batch, schema, result);

    EXPECT_EQ(result.size(), 2);
    EXPECT_EQ(result.get(0).as_bool(), true);   // 3 < 5 = true
    EXPECT_EQ(result.get(1).as_bool(), false);  // 7 < 5 = false
}

TEST(ExpressionTests, EvaluateVectorized_BinaryExpr_Fallback_BothColumns) {
    // When neither operand is Column op Constant, fallback path is used
    Schema schema;
    schema.add_column("a", ValueType::TYPE_INT64);
    schema.add_column("b", ValueType::TYPE_INT64);

    VectorBatch batch;
    batch.init_from_schema(schema);
    batch.get_column(0).append(Value::make_int64(3));
    batch.get_column(1).append(Value::make_int64(5));
    batch.set_row_count(1);

    NumericVector<int64_t> result(ValueType::TYPE_INT64);

    // Both columns - triggers fallback
    auto left = std::make_unique<ColumnExpr>("a");
    auto right = std::make_unique<ColumnExpr>("b");
    BinaryExpr binary(std::move(left), TokenType::Plus, std::move(right));

    binary.evaluate_vectorized(batch, schema, result);

    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ(result.get(0).to_int64(), 8);  // 3 + 5 = 8
}

TEST(ExpressionTests, EvaluateVectorized_ColumnExpr_NotFound) {
    Schema schema;
    schema.add_column("val", ValueType::TYPE_INT64);

    VectorBatch batch;
    batch.init_from_schema(schema);
    batch.get_column(0).append(Value::make_int64(5));
    batch.set_row_count(1);

    NumericVector<int64_t> result(ValueType::TYPE_INT64);

    ColumnExpr col("nonexistent");
    col.evaluate_vectorized(batch, schema, result);

    // Column not found - should append NULL
    EXPECT_EQ(result.size(), 1);
    EXPECT_TRUE(result.is_null(0));
}

TEST(ExpressionTests, EvaluateVectorized_ConstantExpr) {
    Schema schema;
    schema.add_column("val", ValueType::TYPE_INT64);

    VectorBatch batch;
    batch.init_from_schema(schema);
    batch.get_column(0).append(Value::make_int64(1));
    batch.get_column(0).append(Value::make_int64(2));
    batch.set_row_count(2);

    NumericVector<int64_t> result(ValueType::TYPE_INT64);

    ConstantExpr const_expr(Value::make_int64(42));
    const_expr.evaluate_vectorized(batch, schema, result);

    EXPECT_EQ(result.size(), 2);
    EXPECT_EQ(result.get(0).to_int64(), 42);
    EXPECT_EQ(result.get(1).to_int64(), 42);
}

TEST(ExpressionTests, EvaluateVectorized_UnaryExpr) {
    Schema schema;
    schema.add_column("val", ValueType::TYPE_INT64);

    VectorBatch batch;
    batch.init_from_schema(schema);
    batch.get_column(0).append(Value::make_int64(5));
    batch.get_column(0).append(Value::make_int64(-3));
    batch.set_row_count(2);

    NumericVector<int64_t> result(ValueType::TYPE_INT64);

    auto inner = std::make_unique<ColumnExpr>("val");
    UnaryExpr expr(TokenType::Minus, std::move(inner));
    expr.evaluate_vectorized(batch, schema, result);

    EXPECT_EQ(result.size(), 2);
    EXPECT_EQ(result.get(0).to_int64(), -5);
    EXPECT_EQ(result.get(1).to_int64(), 3);
}

// ============= BinaryExpr to_string Coverage =============

TEST(ExpressionTests, ToString_BinaryExpr_Le) {
    auto left = std::make_unique<ConstantExpr>(Value::make_int64(5));
    auto right = std::make_unique<ConstantExpr>(Value::make_int64(10));
    BinaryExpr expr(std::move(left), TokenType::Le, std::move(right));

    auto str = expr.to_string();
    EXPECT_TRUE(str.find("<=") != std::string::npos);
}

TEST(ExpressionTests, ToString_BinaryExpr_Ge) {
    auto left = std::make_unique<ConstantExpr>(Value::make_int64(5));
    auto right = std::make_unique<ConstantExpr>(Value::make_int64(10));
    BinaryExpr expr(std::move(left), TokenType::Ge, std::move(right));

    auto str = expr.to_string();
    EXPECT_TRUE(str.find(">=") != std::string::npos);
}

TEST(ExpressionTests, ToString_BinaryExpr_And) {
    auto left = std::make_unique<ConstantExpr>(Value::make_bool(true));
    auto right = std::make_unique<ConstantExpr>(Value::make_bool(false));
    BinaryExpr expr(std::move(left), TokenType::And, std::move(right));

    auto str = expr.to_string();
    EXPECT_TRUE(str.find("AND") != std::string::npos);
}

TEST(ExpressionTests, ToString_BinaryExpr_Or) {
    auto left = std::make_unique<ConstantExpr>(Value::make_bool(true));
    auto right = std::make_unique<ConstantExpr>(Value::make_bool(false));
    BinaryExpr expr(std::move(left), TokenType::Or, std::move(right));

    auto str = expr.to_string();
    EXPECT_TRUE(str.find("OR") != std::string::npos);
}

// ============= IsNullExpr Vectorized Evaluation =============

TEST(ExpressionTests, EvaluateVectorized_IsNullExpr_IsNull) {
    Schema schema;
    schema.add_column("val", ValueType::TYPE_INT64);

    VectorBatch batch;
    batch.init_from_schema(schema);
    batch.get_column(0).append(Value::make_int64(5));
    batch.get_column(0).append(Value::make_null());
    batch.set_row_count(2);

    NumericVector<bool> result(ValueType::TYPE_BOOL);

    auto inner = std::make_unique<ColumnExpr>("val");
    IsNullExpr expr(std::move(inner), false);
    expr.evaluate_vectorized(batch, schema, result);

    EXPECT_EQ(result.size(), 2);
    EXPECT_EQ(result.get(0).as_bool(), false);  // 5 IS NULL = false
    EXPECT_EQ(result.get(1).as_bool(), true);  // NULL IS NULL = true
}

TEST(ExpressionTests, EvaluateVectorized_IsNullExpr_IsNotNull) {
    Schema schema;
    schema.add_column("val", ValueType::TYPE_INT64);

    VectorBatch batch;
    batch.init_from_schema(schema);
    batch.get_column(0).append(Value::make_int64(5));
    batch.get_column(0).append(Value::make_null());
    batch.set_row_count(2);

    NumericVector<bool> result(ValueType::TYPE_BOOL);

    auto inner = std::make_unique<ColumnExpr>("val");
    IsNullExpr expr(std::move(inner), true);  // not_flag = true
    expr.evaluate_vectorized(batch, schema, result);

    EXPECT_EQ(result.size(), 2);
    EXPECT_EQ(result.get(0).as_bool(), true);   // 5 IS NOT NULL = true
    EXPECT_EQ(result.get(1).as_bool(), false);  // NULL IS NOT NULL = false
}

// ============= InExpr Basic Evaluation =============

TEST(ExpressionTests, Evaluate_InExpr_Basic) {
    auto col = std::make_unique<ColumnExpr>("id");
    std::vector<std::unique_ptr<Expression>> values;
    values.push_back(std::make_unique<ConstantExpr>(Value::make_int64(1)));
    values.push_back(std::make_unique<ConstantExpr>(Value::make_int64(3)));

    InExpr expr(std::move(col), std::move(values), false);

    // Test with tuple
    Schema schema;
    schema.add_column("id", ValueType::TYPE_INT64, false);
    Tuple tuple({Value::make_int64(1)});

    auto result = expr.evaluate(&tuple, &schema);
    EXPECT_EQ(result.as_bool(), true);  // 1 IN (1, 3) = true
}

TEST(ExpressionTests, Evaluate_InExpr_NotIn) {
    auto col = std::make_unique<ColumnExpr>("id");
    std::vector<std::unique_ptr<Expression>> values;
    values.push_back(std::make_unique<ConstantExpr>(Value::make_int64(1)));
    values.push_back(std::make_unique<ConstantExpr>(Value::make_int64(3)));

    InExpr expr(std::move(col), std::move(values), true);  // NOT IN

    Schema schema;
    schema.add_column("id", ValueType::TYPE_INT64, false);
    Tuple tuple({Value::make_int64(2)});

    auto result = expr.evaluate(&tuple, &schema);
    EXPECT_EQ(result.as_bool(), true);  // 2 NOT IN (1, 3) = true
}

// ============= InExpr to_string =============

TEST(ExpressionTests, ToString_InExpr) {
    auto col = std::make_unique<ColumnExpr>("id");
    std::vector<std::unique_ptr<Expression>> values;
    values.push_back(std::make_unique<ConstantExpr>(Value::make_int64(1)));
    values.push_back(std::make_unique<ConstantExpr>(Value::make_int64(2)));

    InExpr expr(std::move(col), std::move(values), false);

    auto str = expr.to_string();
    EXPECT_TRUE(str.find("IN") != std::string::npos);
}

TEST(ExpressionTests, ToString_InExpr_NotIn) {
    auto col = std::make_unique<ColumnExpr>("id");
    std::vector<std::unique_ptr<Expression>> values;
    values.push_back(std::make_unique<ConstantExpr>(Value::make_int64(1)));

    InExpr expr(std::move(col), std::move(values), true);  // NOT IN

    auto str = expr.to_string();
    EXPECT_TRUE(str.find("NOT") != std::string::npos);
}

}  // namespace
