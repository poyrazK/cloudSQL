/**
 * @file types.hpp
 * @brief C++ type definitions for SQL Executor
 */

#ifndef CLOUDSQL_EXECUTOR_TYPES_HPP
#define CLOUDSQL_EXECUTOR_TYPES_HPP

#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include "common/value.hpp"

namespace cloudsql {
namespace executor {

/**
 * @brief Tuple (row) structure
 */
class Tuple {
private:
    std::vector<common::Value> values_;
    
public:
    Tuple() = default;
    explicit Tuple(std::vector<common::Value> values) : values_(std::move(values)) {}
    
    Tuple(const Tuple& other) : values_(other.values_) {}
    Tuple(Tuple&& other) noexcept : values_(std::move(other.values_)) {}
    Tuple& operator=(const Tuple& other) { values_ = other.values_; return *this; }
    Tuple& operator=(Tuple&& other) noexcept { values_ = std::move(other.values_); return *this; }
    
    const common::Value& get(size_t index) const { return values_.at(index); }
    void set(size_t index, const common::Value& value) { 
        if (values_.size() <= index) values_.resize(index + 1);
        values_[index] = value; 
    }
    
    size_t size() const { return values_.size(); }
    bool empty() const { return values_.empty(); }
    
    const auto& values() const { return values_; }
    auto& values() { return values_; }
    
    std::string to_string() const;
};

/**
 * @brief Column metadata
 */
class ColumnMeta {
private:
    std::string name_;
    common::ValueType type_;
    bool nullable_ = true;
    
public:
    ColumnMeta() = default;
    ColumnMeta(std::string name, common::ValueType type, bool nullable = true)
        : name_(std::move(name)), type_(type), nullable_(nullable) {}
    
    const std::string& name() const { return name_; }
    common::ValueType type() const { return type_; }
    bool nullable() const { return nullable_; }
    
    void set_name(std::string name) { name_ = std::move(name); }
    void set_type(common::ValueType type) { type_ = type; }
    void set_nullable(bool nullable) { nullable_ = nullable; }
    
    bool operator==(const ColumnMeta& other) const {
        return name_ == other.name_ && type_ == other.type_ && nullable_ == other.nullable_;
    }
    bool operator!=(const ColumnMeta& other) const { return !(*this == other); }
};

/**
 * @brief Schema definition
 */
class Schema {
private:
    std::vector<ColumnMeta> columns_;
    
public:
    Schema() = default;
    explicit Schema(std::vector<ColumnMeta> columns) : columns_(std::move(columns)) {}
    
    void add_column(const ColumnMeta& col) { columns_.push_back(col); }
    void add_column(std::string name, common::ValueType type, bool nullable = true) {
        columns_.emplace_back(std::move(name), type, nullable);
    }
    
    size_t column_count() const { return columns_.size(); }
    const ColumnMeta& get_column(size_t index) const { return columns_.at(index); }
    size_t find_column(const std::string& name) const {
        /* 1. Try exact match */
        for (size_t i = 0; i < columns_.size(); i++) {
            if (columns_[i].name() == name) {
                return i;
            }
        }
        
        /* 2. Try suffix match (for unqualified names in joined schemas) */
        if (name.find('.') == std::string::npos) {
            std::string suffix = "." + name;
            for (size_t i = 0; i < columns_.size(); i++) {
                const std::string& col_name = columns_[i].name();
                if (col_name.size() > suffix.size() && 
                    col_name.compare(col_name.size() - suffix.size(), suffix.size(), suffix) == 0) {
                    return i;
                }
            }
        }
        
        return static_cast<size_t>(-1);
    }
    
    const auto& columns() const { return columns_; }
    auto& columns() { return columns_; }
    
    bool operator==(const Schema& other) const { return columns_ == other.columns_; }
};

/**
 * @brief Query execution result
 */
class QueryResult {
private:
    Schema schema_;
    std::vector<Tuple> rows_;
    uint64_t execution_time_us_ = 0;
    uint64_t rows_affected_ = 0;
    std::string error_message_;
    bool has_error_ = false;
    
public:
    QueryResult() = default;
    
    bool success() const { return !has_error_; }
    const std::string& error() const { return error_message_; }
    void set_error(const std::string& msg) { error_message_ = msg; has_error_ = true; }
    
    void set_schema(const Schema& schema) { schema_ = schema; }
    const Schema& schema() const { return schema_; }
    
    void add_row(const Tuple& row) { rows_.push_back(row); }
    void add_rows(const std::vector<Tuple>& new_rows) { 
        rows_.insert(rows_.end(), new_rows.begin(), new_rows.end());
    }
    
    size_t row_count() const { return rows_.size(); }
    const auto& rows() const { return rows_; }
    auto& rows() { return rows_; }
    
    uint64_t execution_time() const { return execution_time_us_; }
    void set_execution_time(uint64_t us) { execution_time_us_ = us; }
    
    uint64_t rows_affected() const { return rows_affected_; }
    void set_rows_affected(uint64_t count) { rows_affected_ = count; }
};

}  // namespace executor
}  // namespace cloudsql

inline std::string cloudsql::executor::Tuple::to_string() const {
    std::string result = "(";
    for (size_t i = 0; i < values_.size(); i++) {
        if (i > 0) result += ", ";
        result += values_[i].to_string();
    }
    result += ")";
    return result;
}

#endif  // CLOUDSQL_EXECUTOR_TYPES_HPP
