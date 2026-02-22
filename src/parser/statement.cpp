/**
 * @file statement.cpp
 * @brief SQL Statement implementation
 */

#include "parser/statement.hpp"

#include <string>

namespace cloudsql::parser {

/**
 * @brief Convert SELECT statement to string
 */
std::string SelectStatement::to_string() const {
    std::string result = "SELECT ";

    if (distinct_) {
        result += "DISTINCT ";
    }

    bool first = true;
    for (const auto& col : columns_) {
        if (!first) {
            result += ", ";
        }
        result += col->to_string();
        first = false;
    }

    if (from_) {
        result += " FROM " + from_->to_string();
    }

    for (const auto& join : joins_) {
        switch (join.type) {
            case JoinType::Inner:
                result += " JOIN ";
                break;
            case JoinType::Left:
                result += " LEFT JOIN ";
                break;
            case JoinType::Right:
                result += " RIGHT JOIN ";
                break;
            case JoinType::Full:
                result += " FULL JOIN ";
                break;
        }
        result += join.table->to_string();
        if (join.condition) {
            result += " ON " + join.condition->to_string();
        }
    }

    if (where_) {
        result += " WHERE " + where_->to_string();
    }

    if (!group_by_.empty()) {
        result += " GROUP BY ";
        first = true;
        for (const auto& expr : group_by_) {
            if (!first) {
                result += ", ";
            }
            result += expr->to_string();
            first = false;
        }
    }

    if (having_) {
        result += " HAVING " + having_->to_string();
    }

    if (!order_by_.empty()) {
        result += " ORDER BY ";
        first = true;
        for (const auto& expr : order_by_) {
            if (!first) {
                result += ", ";
            }
            result += expr->to_string();
            first = false;
        }
    }

    if (has_limit()) {
        result += " LIMIT " + std::to_string(limit_);
    }

    if (has_offset()) {
        result += " OFFSET " + std::to_string(offset_);
    }

    return result;
}

/**
 * @brief Convert INSERT statement to string
 */
std::string InsertStatement::to_string() const {
    std::string result = "INSERT INTO " + table_->to_string() + " (";

    bool first = true;
    for (const auto& col : columns_) {
        if (!first) {
            result += ", ";
        }
        result += col->to_string();
        first = false;
    }
    result += ") VALUES ";

    first = true;
    for (const auto& row : values_) {
        if (!first) {
            result += ", ";
        }
        result += "(";
        bool inner_first = true;
        for (const auto& val : row) {
            if (!inner_first) {
                result += ", ";
            }
            result += val->to_string();
            inner_first = false;
        }
        result += ")";
        first = false;
    }

    return result;
}

/**
 * @brief Convert UPDATE statement to string
 */
std::string UpdateStatement::to_string() const {
    std::string result = "UPDATE " + table_->to_string() + " SET ";

    bool first = true;
    for (const auto& [col, val] : set_clauses_) {
        if (!first) {
            result += ", ";
        }
        result += col->to_string() + " = " + val->to_string();
        first = false;
    }

    if (where_) {
        result += " WHERE " + where_->to_string();
    }

    return result;
}

/**
 * @brief Convert DELETE statement to string
 */
std::string DeleteStatement::to_string() const {
    std::string result = "DELETE FROM " + table_->to_string();

    if (where_) {
        result += " WHERE " + where_->to_string();
    }

    return result;
}

/**
 * @brief Convert CREATE TABLE statement to string
 */
std::string CreateTableStatement::to_string() const {
    std::string result = "CREATE TABLE " + table_name_ + " (";

    bool first = true;
    for (const auto& col : columns_) {
        if (!first) {
            result += ", ";
        }
        result += col.name_ + " " + col.type_;
        if (col.is_primary_key_) {
            result += " PRIMARY KEY";
        }
        if (col.is_not_null_) {
            result += " NOT NULL";
        }
        if (col.is_unique_) {
            result += " UNIQUE";
        }
        first = false;
    }

    result += ")";
    return result;
}

}  // namespace cloudsql::parser
