/**
 * @file statement.hpp
 * @brief Statement classes for SQL AST
 */

#ifndef CLOUDSQL_PARSER_STATEMENT_HPP
#define CLOUDSQL_PARSER_STATEMENT_HPP

#include <memory>
#include <string>
#include <vector>

#include "parser/expression.hpp"

namespace cloudsql::parser {

/**
 * @brief Statement types
 */
enum class StmtType : uint8_t {
    Select,
    Insert,
    Update,
    Delete,
    CreateTable,
    DropTable,
    AlterTable,
    CreateIndex,
    DropIndex,
    TransactionBegin,
    TransactionCommit,
    TransactionRollback,
    Explain
};

/**
 * @brief Base statement class
 */
class Statement {
   public:
    virtual ~Statement() = default;
    
    // Disable copy/move for base statement
    Statement(const Statement&) = delete;
    Statement& operator=(const Statement&) = delete;
    Statement(Statement&&) = delete;
    Statement& operator=(Statement&&) = delete;
    
    Statement() = default;

    [[nodiscard]] virtual StmtType type() const = 0;
    [[nodiscard]] virtual std::string to_string() const = 0;
};

/**
 * @brief SELECT statement
 */
class SelectStatement : public Statement {
   public:
    enum class JoinType : uint8_t { Inner, Left, Right, Full };

    struct JoinInfo {
        JoinType type = JoinType::Inner;
        std::unique_ptr<Expression> table;
        std::unique_ptr<Expression> condition;
    };

   private:
    std::vector<std::unique_ptr<Expression>> columns_;
    std::unique_ptr<Expression> from_;
    std::vector<JoinInfo> joins_;
    std::unique_ptr<Expression> where_;
    std::vector<std::unique_ptr<Expression>> group_by_;
    std::unique_ptr<Expression> having_;
    std::vector<std::unique_ptr<Expression>> order_by_;
    int64_t limit_ = 0;
    int64_t offset_ = 0;
    bool distinct_ = false;

   public:
    SelectStatement() = default;

    [[nodiscard]] StmtType type() const override { return StmtType::Select; }

    void add_column(std::unique_ptr<Expression> col) { columns_.push_back(std::move(col)); }
    void add_from(std::unique_ptr<Expression> table) { from_ = std::move(table); }
    void add_join(JoinType type, std::unique_ptr<Expression> table,
                  std::unique_ptr<Expression> condition) {
        joins_.push_back({type, std::move(table), std::move(condition)});
    }
    void set_where(std::unique_ptr<Expression> where) { where_ = std::move(where); }
    void add_group_by(std::unique_ptr<Expression> expr) { group_by_.push_back(std::move(expr)); }
    void set_having(std::unique_ptr<Expression> having) { having_ = std::move(having); }
    void add_order_by(std::unique_ptr<Expression> expr) { order_by_.push_back(std::move(expr)); }
    void set_limit(int64_t limit) { limit_ = limit; }
    void set_offset(int64_t offset) { offset_ = offset; }
    void set_distinct(bool distinct) { distinct_ = distinct; }

    [[nodiscard]] const std::vector<std::unique_ptr<Expression>>& columns() const { return columns_; }
    [[nodiscard]] const Expression* from() const { return from_.get(); }
    [[nodiscard]] const std::vector<JoinInfo>& joins() const { return joins_; }
    [[nodiscard]] const Expression* where() const { return where_.get(); }
    [[nodiscard]] const std::vector<std::unique_ptr<Expression>>& group_by() const { return group_by_; }
    [[nodiscard]] const Expression* having() const { return having_.get(); }
    [[nodiscard]] const std::vector<std::unique_ptr<Expression>>& order_by() const { return order_by_; }
    [[nodiscard]] int64_t limit() const { return limit_; }
    [[nodiscard]] int64_t offset() const { return offset_; }
    [[nodiscard]] bool distinct() const { return distinct_; }
    [[nodiscard]] bool has_limit() const { return limit_ > 0; }
    [[nodiscard]] bool has_offset() const { return offset_ > 0; }

    [[nodiscard]] std::string to_string() const override;
};

/**
 * @brief INSERT statement
 */
class InsertStatement : public Statement {
   private:
    std::unique_ptr<Expression> table_;
    std::vector<std::unique_ptr<Expression>> columns_;
    std::vector<std::vector<std::unique_ptr<Expression>>> values_;

   public:
    InsertStatement() = default;

    [[nodiscard]] StmtType type() const override { return StmtType::Insert; }

    void set_table(std::unique_ptr<Expression> table) { table_ = std::move(table); }
    void add_column(std::unique_ptr<Expression> col) { columns_.push_back(std::move(col)); }
    void add_row(std::vector<std::unique_ptr<Expression>> row) {
        values_.push_back(std::move(row));
    }

    [[nodiscard]] const Expression* table() const { return table_.get(); }
    [[nodiscard]] const std::vector<std::unique_ptr<Expression>>& columns() const { return columns_; }
    [[nodiscard]] const std::vector<std::vector<std::unique_ptr<Expression>>>& values() const { return values_; }
    [[nodiscard]] size_t value_count() const { return values_.size(); }

    [[nodiscard]] std::string to_string() const override;
};

/**
 * @brief UPDATE statement
 */
class UpdateStatement : public Statement {
   private:
    std::unique_ptr<Expression> table_;
    std::vector<std::pair<std::unique_ptr<Expression>, std::unique_ptr<Expression>>> set_clauses_;
    std::unique_ptr<Expression> where_;

   public:
    UpdateStatement() = default;

    [[nodiscard]] StmtType type() const override { return StmtType::Update; }

    void set_table(std::unique_ptr<Expression> table) { table_ = std::move(table); }
    void add_set(std::unique_ptr<Expression> col, std::unique_ptr<Expression> val) {
        set_clauses_.emplace_back(std::move(col), std::move(val));
    }
    void set_where(std::unique_ptr<Expression> where) { where_ = std::move(where); }

    [[nodiscard]] const Expression* table() const { return table_.get(); }
    [[nodiscard]] const std::vector<std::pair<std::unique_ptr<Expression>, std::unique_ptr<Expression>>>& set_clauses() const { return set_clauses_; }
    [[nodiscard]] const Expression* where() const { return where_.get(); }

    [[nodiscard]] std::string to_string() const override;
};

/**
 * @brief DELETE statement
 */
class DeleteStatement : public Statement {
   private:
    std::unique_ptr<Expression> table_;
    std::unique_ptr<Expression> where_;

   public:
    DeleteStatement() = default;

    [[nodiscard]] StmtType type() const override { return StmtType::Delete; }

    void set_table(std::unique_ptr<Expression> table) { table_ = std::move(table); }
    void set_where(std::unique_ptr<Expression> where) { where_ = std::move(where); }

    [[nodiscard]] const Expression* table() const { return table_.get(); }
    [[nodiscard]] const Expression* where() const { return where_.get(); }
    [[nodiscard]] bool has_where() const { return where_ != nullptr; }

    [[nodiscard]] std::string to_string() const override;
};

/**
 * @brief CREATE TABLE statement
 */
class CreateTableStatement : public Statement {
   public:
    struct ColumnDef {
        std::string name_;
        std::string type_;
        bool is_primary_key_ = false;
        bool is_not_null_ = false;
        bool is_unique_ = false;
        std::unique_ptr<Expression> default_value_ = nullptr;
    };

   private:
    std::string table_name_;
    std::vector<ColumnDef> columns_;

   public:
    CreateTableStatement() = default;

    [[nodiscard]] StmtType type() const override { return StmtType::CreateTable; }

    void set_table_name(std::string name) { table_name_ = std::move(name); }
    void add_column(std::string name, std::string type) {
        columns_.push_back({std::move(name), std::move(type), false, false, false, nullptr});
    }
    [[nodiscard]] ColumnDef& get_last_column() { return columns_.back(); }

    [[nodiscard]] const std::string& table_name() const { return table_name_; }
    [[nodiscard]] const std::vector<ColumnDef>& columns() const { return columns_; }

    [[nodiscard]] std::string to_string() const override;
};

/**
 * @brief DROP TABLE statement
 */
class DropTableStatement : public Statement {
   private:
    std::string table_name_;
    bool if_exists_ = false;

   public:
    explicit DropTableStatement(std::string name, bool if_exists = false)
        : table_name_(std::move(name)), if_exists_(if_exists) {}
    [[nodiscard]] StmtType type() const override { return StmtType::DropTable; }
    [[nodiscard]] const std::string& table_name() const { return table_name_; }
    [[nodiscard]] bool if_exists() const { return if_exists_; }
    [[nodiscard]] std::string to_string() const override {
        return std::string("DROP TABLE ") + (if_exists_ ? "IF EXISTS " : "") + table_name_;
    }
};

/**
 * @brief DROP INDEX statement
 */
class DropIndexStatement : public Statement {
   private:
    std::string index_name_;
    bool if_exists_ = false;

   public:
    explicit DropIndexStatement(std::string name, bool if_exists = false)
        : index_name_(std::move(name)), if_exists_(if_exists) {}
    [[nodiscard]] StmtType type() const override { return StmtType::DropIndex; }
    [[nodiscard]] const std::string& index_name() const { return index_name_; }
    [[nodiscard]] bool if_exists() const { return if_exists_; }
    [[nodiscard]] std::string to_string() const override {
        return std::string("DROP INDEX ") + (if_exists_ ? "IF EXISTS " : "") + index_name_;
    }
};

/**
 * @brief BEGIN statement
 */
class TransactionBeginStatement : public Statement {
   public:
    [[nodiscard]] StmtType type() const override { return StmtType::TransactionBegin; }
    [[nodiscard]] std::string to_string() const override { return "BEGIN"; }
};

/**
 * @brief COMMIT statement
 */
class TransactionCommitStatement : public Statement {
   public:
    [[nodiscard]] StmtType type() const override { return StmtType::TransactionCommit; }
    [[nodiscard]] std::string to_string() const override { return "COMMIT"; }
};

/**
 * @brief ROLLBACK statement
 */
class TransactionRollbackStatement : public Statement {
   public:
    [[nodiscard]] StmtType type() const override { return StmtType::TransactionRollback; }
    [[nodiscard]] std::string to_string() const override { return "ROLLBACK"; }
};

}  // namespace cloudsql::parser

#endif  // CLOUDSQL_PARSER_STATEMENT_HPP
