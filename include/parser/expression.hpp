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

/* Forward declarations */
namespace cloudsql::executor {
class Tuple;
class Schema;
}  // namespace cloudsql::executor

namespace cloudsql::parser {

class Expression;

enum class ExprType : uint8_t {
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
    Expression() = default;
    virtual ~Expression() = default;

    // Disable copy for expressions
    Expression(const Expression&) = delete;
    Expression& operator=(const Expression&) = delete;

    // Enable move
    Expression(Expression&&) noexcept = default;
    Expression& operator=(Expression&&) noexcept = default;

    [[nodiscard]] virtual ExprType type() const = 0;

    /**
     * @brief Evaluate expression against an optional tuple context
     */
    [[nodiscard]] virtual common::Value evaluate(const executor::Tuple* tuple = nullptr,
                                                 const executor::Schema* schema = nullptr) const = 0;

    [[nodiscard]] virtual std::string to_string() const = 0;
    [[nodiscard]] virtual std::unique_ptr<Expression> clone() const = 0;
};

/**
 * @brief Binary expression (e.g. a + b, x = y)
 */
class BinaryExpr : public Expression {
   private:
    std::unique_ptr<Expression> left_;
    TokenType op_;
    std::unique_ptr<Expression> right_;

   public:
    BinaryExpr(std::unique_ptr<Expression> left, TokenType op, std::unique_ptr<Expression> right)
        : left_(std::move(left)), op_(op), right_(std::move(right)) {}

    [[nodiscard]] ExprType type() const override { return ExprType::Binary; }
    [[nodiscard]] common::Value evaluate(const executor::Tuple* tuple = nullptr,
                                         const executor::Schema* schema = nullptr) const override;
    [[nodiscard]] std::string to_string() const override;
    [[nodiscard]] std::unique_ptr<Expression> clone() const override;

    [[nodiscard]] const Expression& left() const { return *left_; }
    [[nodiscard]] const Expression& right() const { return *right_; }
    [[nodiscard]] TokenType op() const { return op_; }
};

/**
 * @brief Unary expression (e.g. -a, NOT x)
 */
class UnaryExpr : public Expression {
   private:
    TokenType op_;
    std::unique_ptr<Expression> expr_;

   public:
    UnaryExpr(TokenType op, std::unique_ptr<Expression> expr) : op_(op), expr_(std::move(expr)) {}

    [[nodiscard]] ExprType type() const override { return ExprType::Unary; }
    [[nodiscard]] common::Value evaluate(const executor::Tuple* tuple = nullptr,
                                         const executor::Schema* schema = nullptr) const override;
    [[nodiscard]] std::string to_string() const override;
    [[nodiscard]] std::unique_ptr<Expression> clone() const override;
};

/**
 * @brief Column reference expression
 */
class ColumnExpr : public Expression {
   private:
    std::string table_name_;
    std::string name_;

   public:
    explicit ColumnExpr(std::string name) : name_(std::move(name)) {}
    ColumnExpr(std::string table, std::string name)
        : table_name_(std::move(table)), name_(std::move(name)) {}

    [[nodiscard]] ExprType type() const override { return ExprType::Column; }
    [[nodiscard]] common::Value evaluate(const executor::Tuple* tuple = nullptr,
                                         const executor::Schema* schema = nullptr) const override;
    [[nodiscard]] std::string to_string() const override;
    [[nodiscard]] std::unique_ptr<Expression> clone() const override;

    [[nodiscard]] const std::string& name() const { return name_; }
    [[nodiscard]] const std::string& table_name() const { return table_name_; }
    [[nodiscard]] bool has_table() const { return !table_name_.empty(); }
};

/**
 * @brief Literal constant expression
 */
class ConstantExpr : public Expression {
   private:
    common::Value value_;

   public:
    explicit ConstantExpr(common::Value val) : value_(std::move(val)) {}

    [[nodiscard]] ExprType type() const override { return ExprType::Constant; }
    [[nodiscard]] common::Value evaluate(const executor::Tuple* tuple = nullptr,
                                         const executor::Schema* schema = nullptr) const override;
    [[nodiscard]] std::string to_string() const override;
    [[nodiscard]] std::unique_ptr<Expression> clone() const override;

    [[nodiscard]] const common::Value& value() const { return value_; }
};

/**
 * @brief Scalar function or aggregate expression
 */
class FunctionExpr : public Expression {
   private:
    std::string func_name_;
    std::vector<std::unique_ptr<Expression>> args_;
    bool distinct_ = false;

   public:
    explicit FunctionExpr(std::string name) : func_name_(std::move(name)) {}

    [[nodiscard]] ExprType type() const override { return ExprType::Function; }
    [[nodiscard]] common::Value evaluate(const executor::Tuple* tuple = nullptr,
                                         const executor::Schema* schema = nullptr) const override;
    [[nodiscard]] std::string to_string() const override;
    [[nodiscard]] std::unique_ptr<Expression> clone() const override;

    void add_arg(std::unique_ptr<Expression> arg) { args_.push_back(std::move(arg)); }
    [[nodiscard]] const std::string& name() const { return func_name_; }
    [[nodiscard]] const std::vector<std::unique_ptr<Expression>>& args() const { return args_; }
    void set_distinct(bool d) { distinct_ = d; }
    [[nodiscard]] bool distinct() const { return distinct_; }
};

/**
 * @brief IN expression
 */
class InExpr : public Expression {
   private:
    std::unique_ptr<Expression> column_;
    std::vector<std::unique_ptr<Expression>> values_;
    bool not_flag_;

   public:
    InExpr(std::unique_ptr<Expression> col, std::vector<std::unique_ptr<Expression>> vals,
           bool is_not = false)
        : column_(std::move(col)), values_(std::move(vals)), not_flag_(is_not) {}

    [[nodiscard]] ExprType type() const override { return ExprType::In; }
    [[nodiscard]] common::Value evaluate(const executor::Tuple* tuple = nullptr,
                                         const executor::Schema* schema = nullptr) const override;
    [[nodiscard]] std::string to_string() const override;
    [[nodiscard]] std::unique_ptr<Expression> clone() const override;
};

/**
 * @brief IS NULL expression
 */
class IsNullExpr : public Expression {
   private:
    std::unique_ptr<Expression> expr_;
    bool not_flag_;

   public:
    explicit IsNullExpr(std::unique_ptr<Expression> expr, bool not_flag = false)
        : expr_(std::move(expr)), not_flag_(not_flag) {}

    [[nodiscard]] ExprType type() const override { return ExprType::IsNull; }
    [[nodiscard]] common::Value evaluate(const executor::Tuple* tuple = nullptr,
                                         const executor::Schema* schema = nullptr) const override;
    [[nodiscard]] std::string to_string() const override;
    [[nodiscard]] std::unique_ptr<Expression> clone() const override;
};

}  // namespace cloudsql::parser

#endif  // CLOUDSQL_PARSER_EXPRESSION_HPP
