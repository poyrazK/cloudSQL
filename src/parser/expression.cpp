/**
 * @file expression.cpp
 * @brief Expression evaluator implementation
 */

#include "parser/expression.hpp"

#include "executor/types.hpp"

namespace cloudsql {
namespace parser {

/**
 * @brief Evaluate binary expression
 */
common::Value BinaryExpr::evaluate(const executor::Tuple* tuple,
                                   const executor::Schema* schema) const {
    common::Value left_val = left_->evaluate(tuple, schema);
    common::Value right_val = right_->evaluate(tuple, schema);

    switch (op_) {
        case TokenType::Plus:
            if (left_val.type() == common::TYPE_FLOAT64 ||
                right_val.type() == common::TYPE_FLOAT64) {
                return common::Value::make_float64(left_val.to_float64() + right_val.to_float64());
            }
            return common::Value::make_int64(left_val.to_int64() + right_val.to_int64());
        case TokenType::Minus:
            if (left_val.type() == common::TYPE_FLOAT64 ||
                right_val.type() == common::TYPE_FLOAT64) {
                return common::Value::make_float64(left_val.to_float64() - right_val.to_float64());
            }
            return common::Value::make_int64(left_val.to_int64() - right_val.to_int64());
        case TokenType::Star:
            if (left_val.type() == common::TYPE_FLOAT64 ||
                right_val.type() == common::TYPE_FLOAT64) {
                return common::Value::make_float64(left_val.to_float64() * right_val.to_float64());
            }
            return common::Value::make_int64(left_val.to_int64() * right_val.to_int64());
        case TokenType::Slash:
            return common::Value::make_float64(left_val.to_float64() / right_val.to_float64());
        case TokenType::Eq:
            return common::Value(left_val == right_val);
        case TokenType::Ne:
            return common::Value(left_val != right_val);
        case TokenType::Lt:
            return common::Value(left_val < right_val);
        case TokenType::Le:
            return common::Value(left_val <= right_val);
        case TokenType::Gt:
            return common::Value(left_val > right_val);
        case TokenType::Ge:
            return common::Value(left_val >= right_val);
        case TokenType::And:
            return common::Value(left_val.as_bool() && right_val.as_bool());
        case TokenType::Or:
            return common::Value(left_val.as_bool() || right_val.as_bool());
        default:
            return common::Value::make_null();
    }
}

std::string BinaryExpr::to_string() const {
    std::string op_str;
    switch (op_) {
        case TokenType::Plus:
            op_str = " + ";
            break;
        case TokenType::Minus:
            op_str = " - ";
            break;
        case TokenType::Star:
            op_str = " * ";
            break;
        case TokenType::Slash:
            op_str = " / ";
            break;
        case TokenType::Eq:
            op_str = " = ";
            break;
        case TokenType::Ne:
            op_str = " <> ";
            break;
        case TokenType::Lt:
            op_str = " < ";
            break;
        case TokenType::Le:
            op_str = " <= ";
            break;
        case TokenType::Gt:
            op_str = " > ";
            break;
        case TokenType::Ge:
            op_str = " >= ";
            break;
        case TokenType::And:
            op_str = " AND ";
            break;
        case TokenType::Or:
            op_str = " OR ";
            break;
        default:
            op_str = " ";
            break;
    }
    return left_->to_string() + op_str + right_->to_string();
}

std::unique_ptr<Expression> BinaryExpr::clone() const {
    return std::make_unique<BinaryExpr>(left_->clone(), op_, right_->clone());
}

/**
 * @brief Evaluate unary expression
 */
common::Value UnaryExpr::evaluate(const executor::Tuple* tuple,
                                  const executor::Schema* schema) const {
    common::Value val = expr_->evaluate(tuple, schema);
    switch (op_) {
        case TokenType::Minus:
            if (val.is_numeric()) {
                return common::Value(-val.to_float64());
            }
            break;
        case TokenType::Not:
            return common::Value(!val.as_bool());
        default:
            break;
    }
    return common::Value::make_null();
}

std::string UnaryExpr::to_string() const {
    return (op_ == TokenType::Minus ? "-" : "NOT ") + expr_->to_string();
}

std::unique_ptr<Expression> UnaryExpr::clone() const {
    return std::make_unique<UnaryExpr>(op_, expr_->clone());
}

/**
 * @brief Evaluate column expression using tuple and schema
 */
common::Value ColumnExpr::evaluate(const executor::Tuple* tuple,
                                   const executor::Schema* schema) const {
    if (!tuple || !schema) return common::Value::make_null();

    size_t index = schema->find_column(name_);
    if (index == static_cast<size_t>(-1)) {
        return common::Value::make_null();
    }

    return tuple->get(index);
}

std::string ColumnExpr::to_string() const {
    return has_table() ? table_name_ + "." + name_ : name_;
}

std::unique_ptr<Expression> ColumnExpr::clone() const {
    return has_table() ? std::make_unique<ColumnExpr>(table_name_, name_)
                       : std::make_unique<ColumnExpr>(name_);
}

common::Value ConstantExpr::evaluate(const executor::Tuple* tuple,
                                     const executor::Schema* schema) const {
    (void)tuple;
    (void)schema;
    return value_;
}

std::string ConstantExpr::to_string() const {
    if (value_.type() == common::TYPE_TEXT) {
        return "'" + value_.to_string() + "'";
    }
    return value_.to_string();
}

std::unique_ptr<Expression> ConstantExpr::clone() const {
    return std::make_unique<ConstantExpr>(value_);
}

/**
 * @brief Evaluate function expression
 */
common::Value FunctionExpr::evaluate(const executor::Tuple* tuple,
                                     const executor::Schema* schema) const {
    if (!tuple || !schema) return common::Value::make_null();

    /* Attempt to look up the function result in the schema (e.g. for aggregates) */
    size_t index = schema->find_column(this->to_string());
    if (index != static_cast<size_t>(-1)) {
        return tuple->get(index);
    }

    return common::Value::make_null();
}

std::string FunctionExpr::to_string() const {
    std::string result = func_name_ + "(";
    if (distinct_) result += "DISTINCT ";
    bool first = true;
    for (const auto& arg : args_) {
        if (!first) result += ", ";
        result += arg->to_string();
        first = false;
    }
    if (args_.empty() && func_name_ == "COUNT") result += "*";
    result += ")";
    return result;
}

std::unique_ptr<Expression> FunctionExpr::clone() const {
    auto result = std::make_unique<FunctionExpr>(func_name_);
    result->set_distinct(distinct_);
    for (const auto& arg : args_) {
        result->add_arg(arg->clone());
    }
    return result;
}

/**
 * @brief Evaluate IN expression
 */
common::Value InExpr::evaluate(const executor::Tuple* tuple, const executor::Schema* schema) const {
    common::Value col_val = column_->evaluate(tuple, schema);
    for (const auto& val : values_) {
        if (col_val == val->evaluate(tuple, schema)) {
            return common::Value(!not_flag_);
        }
    }
    return common::Value(not_flag_);
}

std::string InExpr::to_string() const {
    std::string result = column_->to_string() + (not_flag_ ? " NOT IN (" : " IN (");
    bool first = true;
    for (const auto& val : values_) {
        if (!first) result += ", ";
        result += val->to_string();
        first = false;
    }
    result += ")";
    return result;
}

std::unique_ptr<Expression> InExpr::clone() const {
    std::vector<std::unique_ptr<Expression>> cloned_vals;
    for (const auto& val : values_) {
        cloned_vals.push_back(val->clone());
    }
    return std::make_unique<InExpr>(column_->clone(), std::move(cloned_vals), not_flag_);
}

/**
 * @brief Evaluate IS NULL expression
 */
common::Value IsNullExpr::evaluate(const executor::Tuple* tuple,
                                   const executor::Schema* schema) const {
    common::Value val = expr_->evaluate(tuple, schema);
    bool result = val.is_null();
    return common::Value(not_flag_ ? !result : result);
}

std::string IsNullExpr::to_string() const {
    return expr_->to_string() + (not_flag_ ? " IS NOT NULL" : " IS NULL");
}

std::unique_ptr<Expression> IsNullExpr::clone() const {
    return std::make_unique<IsNullExpr>(expr_->clone(), not_flag_);
}

}  // namespace parser
}  // namespace cloudsql
