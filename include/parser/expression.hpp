/**
 * @file expression.hpp
 * @brief Expression classes for SQL AST
 */

#ifndef CLOUDSQL_PARSER_EXPRESSION_HPP
#define CLOUDSQL_PARSER_EXPRESSION_HPP

#include <memory>
#include <string>
#include <vector>

#include "common/value.hpp"
#include "parser/token.hpp"

namespace cloudsql {

/* Forward declarations */
namespace executor {
class Tuple;
class Schema;
}  // namespace executor

namespace parser {

class Expression;

enum class ExprType {
    Binary,
    Unary,
    Column,
    Constant,
    Function,
    Subquery,
    In,
    Like,
    Between,
    IsNull
};

/**
 * @brief Base class for all SQL expressions
 */
class Expression {
   public:
    virtual ~Expression() = default;
    virtual ExprType type() const = 0;

    /**
     * @brief Evaluate expression against an optional tuple context
     */
    virtual common::Value evaluate(const executor::Tuple* tuple = nullptr,
                                   const executor::Schema* schema = nullptr) const = 0;

    virtual std::string to_string() const = 0;
    virtual std::unique_ptr<Expression> clone() const = 0;
};

class BinaryExpr : public Expression {
   private:
    std::unique_ptr<Expression> left_;
    TokenType op_;
    std::unique_ptr<Expression> right_;

   public:
    BinaryExpr(std::unique_ptr<Expression> left, TokenType op, std::unique_ptr<Expression> right)
        : left_(std::move(left)), op_(op), right_(std::move(right)) {}

    ExprType type() const override { return ExprType::Binary; }
    const Expression& left() const { return *left_; }
    const Expression& right() const { return *right_; }
    TokenType op() const { return op_; }

    common::Value evaluate(const executor::Tuple* tuple = nullptr,
                           const executor::Schema* schema = nullptr) const override;
    std::string to_string() const override;
    std::unique_ptr<Expression> clone() const override;
};

class UnaryExpr : public Expression {
   private:
    TokenType op_;
    std::unique_ptr<Expression> expr_;

   public:
    UnaryExpr(TokenType op, std::unique_ptr<Expression> expr) : op_(op), expr_(std::move(expr)) {}

    ExprType type() const override { return ExprType::Unary; }
    TokenType op() const { return op_; }
    const Expression& expr() const { return *expr_; }

    common::Value evaluate(const executor::Tuple* tuple = nullptr,
                           const executor::Schema* schema = nullptr) const override;
    std::string to_string() const override;
    std::unique_ptr<Expression> clone() const override;
};

class ColumnExpr : public Expression {
   private:
    std::string name_;
    std::string table_name_;

   public:
    explicit ColumnExpr(std::string name) : name_(std::move(name)), table_name_() {}

    ColumnExpr(std::string table, std::string name)
        : name_(std::move(name)), table_name_(std::move(table)) {}

    ExprType type() const override { return ExprType::Column; }
    const std::string& name() const { return name_; }
    const std::string& table() const { return table_name_; }
    bool has_table() const { return !table_name_.empty(); }

    common::Value evaluate(const executor::Tuple* tuple = nullptr,
                           const executor::Schema* schema = nullptr) const override;
    std::string to_string() const override;
    std::unique_ptr<Expression> clone() const override;
};

class ConstantExpr : public Expression {
   private:
    common::Value value_;

   public:
    explicit ConstantExpr(common::Value value) : value_(std::move(value)) {}

    ExprType type() const override { return ExprType::Constant; }
    const common::Value& value() const { return value_; }

    common::Value evaluate(const executor::Tuple* tuple = nullptr,
                           const executor::Schema* schema = nullptr) const override;
    std::string to_string() const override;
    std::unique_ptr<Expression> clone() const override;
};

class FunctionExpr : public Expression {
   private:
    std::string func_name_;
    std::vector<std::unique_ptr<Expression>> args_;
    bool distinct_ = false;

   public:
    explicit FunctionExpr(std::string name) : func_name_(std::move(name)) {}

    ExprType type() const override { return ExprType::Function; }
    const std::string& name() const { return func_name_; }
    void add_arg(std::unique_ptr<Expression> arg) { args_.push_back(std::move(arg)); }
    const auto& args() const { return args_; }

    bool distinct() const { return distinct_; }
    void set_distinct(bool distinct) { distinct_ = distinct; }

    common::Value evaluate(const executor::Tuple* tuple = nullptr,
                           const executor::Schema* schema = nullptr) const override;
    std::string to_string() const override;
    std::unique_ptr<Expression> clone() const override;
};

class InExpr : public Expression {
   private:
    std::unique_ptr<Expression> column_;
    std::vector<std::unique_ptr<Expression>> values_;
    bool not_flag_;

   public:
    InExpr(std::unique_ptr<Expression> column, std::vector<std::unique_ptr<Expression>> values,
           bool not_flag = false)
        : column_(std::move(column)), values_(std::move(values)), not_flag_(not_flag) {}

    ExprType type() const override { return ExprType::In; }
    const Expression& column() const { return *column_; }
    const auto& values() const { return values_; }
    bool is_not() const { return not_flag_; }

    common::Value evaluate(const executor::Tuple* tuple = nullptr,
                           const executor::Schema* schema = nullptr) const override;
    std::string to_string() const override;
    std::unique_ptr<Expression> clone() const override;
};

class IsNullExpr : public Expression {
   private:
    std::unique_ptr<Expression> expr_;
    bool not_flag_;

   public:
    IsNullExpr(std::unique_ptr<Expression> expr, bool not_flag = false)
        : expr_(std::move(expr)), not_flag_(not_flag) {}

    ExprType type() const override { return ExprType::IsNull; }
    const Expression& expr() const { return *expr_; }
    bool is_not() const { return not_flag_; }

    common::Value evaluate(const executor::Tuple* tuple = nullptr,
                           const executor::Schema* schema = nullptr) const override;
    std::string to_string() const override;
    std::unique_ptr<Expression> clone() const override;
};

}  // namespace parser
}  // namespace cloudsql

#endif  // CLOUDSQL_PARSER_EXPRESSION_HPP
