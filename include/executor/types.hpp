/**
 * @file types.hpp
 * @brief core type definitions and data structures for the SQL execution engine.
 *
 * This file defines the fundamental building blocks for both row-based (Volcano)
 * and vectorized execution models, including Schema metadata, Tuple storage,
 * and high-performance ColumnVector buffers.
 */

#ifndef CLOUDSQL_EXECUTOR_TYPES_HPP
#define CLOUDSQL_EXECUTOR_TYPES_HPP

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/value.hpp"

namespace cloudsql::executor {

/**
 * @brief Represents the lifecycle state of a query operator.
 */
enum class ExecState : uint8_t { Init, Open, Executing, Done, Error };

/**
 * @brief Supported aggregation functions for analytical queries.
 */
enum class AggregateType : uint8_t { Count, Sum, Avg, Min, Max };

/**
 * @brief Metadata for a single column, including type and nullability.
 */
class ColumnMeta {
   private:
    std::string name_;
    common::ValueType type_ = common::ValueType::TYPE_NULL;
    bool nullable_ = true;

   public:
    ColumnMeta() = default;
    ColumnMeta(std::string name, common::ValueType type, bool nullable = true)
        : name_(std::move(name)), type_(type), nullable_(nullable) {}

    [[nodiscard]] const std::string& name() const { return name_; }
    [[nodiscard]] common::ValueType type() const { return type_; }
    [[nodiscard]] bool nullable() const { return nullable_; }

    void set_name(std::string name) { name_ = std::move(name); }
    void set_type(common::ValueType type) { type_ = type; }
    void set_nullable(bool nullable) { nullable_ = nullable; }

    [[nodiscard]] bool operator==(const ColumnMeta& other) const {
        return name_ == other.name_ && type_ == other.type_ && nullable_ == other.nullable_;
    }
    [[nodiscard]] bool operator!=(const ColumnMeta& other) const { return !(*this == other); }
};

/**
 * @brief Defines the structure of a relation (table or intermediate result).
 */
class Schema {
   private:
    std::vector<ColumnMeta> columns_;

   public:
    Schema() = default;
    explicit Schema(std::vector<ColumnMeta> columns) : columns_(std::move(columns)) {}

    /**
     * @brief Appends a column definition to the schema.
     */
    void add_column(const ColumnMeta& col) { columns_.push_back(col); }
    void add_column(std::string name, common::ValueType type, bool nullable = true) {
        columns_.emplace_back(std::move(name), type, nullable);
    }

    [[nodiscard]] size_t column_count() const { return columns_.size(); }
    [[nodiscard]] const ColumnMeta& get_column(size_t index) const { return columns_.at(index); }

    /**
     * @brief Resolves a column index by its name using exact or suffix matching.
     * @return The 0-based index of the column, or static_cast<size_t>(-1) if not found.
     */
    [[nodiscard]] size_t find_column(const std::string& name) const {
        /* 1. Precise match */
        for (size_t i = 0; i < columns_.size(); i++) {
            if (columns_[i].name() == name) {
                return i;
            }
        }

        /* 2. Suffix match for unqualified identifiers in joined relations */
        if (name.find('.') == std::string::npos) {
            const std::string suffix = "." + name;
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

    [[nodiscard]] const std::vector<ColumnMeta>& columns() const { return columns_; }
    [[nodiscard]] std::vector<ColumnMeta>& columns() { return columns_; }

    [[nodiscard]] bool operator==(const Schema& other) const { return columns_ == other.columns_; }
};

/**
 * @brief A single data row used in the row-oriented (Volcano) execution model.
 */
class Tuple {
   private:
    std::vector<common::Value> values_;

   public:
    Tuple() = default;
    explicit Tuple(std::vector<common::Value> values) : values_(std::move(values)) {}

    Tuple(const Tuple& other) = default;
    Tuple(Tuple&& other) noexcept = default;
    Tuple& operator=(const Tuple& other) = default;
    Tuple& operator=(Tuple&& other) noexcept = default;
    ~Tuple() = default;

    /**
     * @brief Retrieves a value by its index. Returns NULL if the index is out of bounds.
     */
    [[nodiscard]] const common::Value& get(size_t index) const {
        if (index >= values_.size()) {
            static const common::Value null_val = common::Value::make_null();
            return null_val;
        }
        return values_[index];
    }

    /**
     * @brief Updates or appends a value at the specified index.
     */
    void set(size_t index, const common::Value& value) {
        if (values_.size() <= index) {
            values_.resize(index + 1);
        }
        values_[index] = value;
    }

    [[nodiscard]] size_t size() const { return values_.size(); }
    [[nodiscard]] bool empty() const { return values_.empty(); }

    [[nodiscard]] const std::vector<common::Value>& values() const { return values_; }
    [[nodiscard]] std::vector<common::Value>& values() { return values_; }

    [[nodiscard]] std::string to_string() const;
};

/**
 * @brief Abstract base class for contiguous column storage in vectorized execution.
 */
class ColumnVector {
   protected:
    common::ValueType type_;
    size_t size_ = 0;
    std::vector<bool> null_bitmap_;

   public:
    explicit ColumnVector(common::ValueType type) : type_(type) {}
    virtual ~ColumnVector() = default;

    [[nodiscard]] common::ValueType type() const { return type_; }
    [[nodiscard]] size_t size() const { return size_; }

    /**
     * @brief Returns true if the value at the specified index is NULL.
     */
    [[nodiscard]] bool is_null(size_t index) const {
        if (index >= size_) {
            return true;
        }
        return null_bitmap_[index];
    }

    /**
     * @brief Updates the nullability status of an existing element.
     */
    virtual void set_null(size_t index, bool is_null) {
        if (index < size_) {
            null_bitmap_[index] = is_null;
        }
    }

    /**
     * @brief Appends a single Value to the end of the vector.
     */
    virtual void append(const common::Value& val) = 0;

    /**
     * @brief Materializes a common::Value for the element at the given index.
     */
    virtual common::Value get(size_t index) const = 0;

    /**
     * @brief Resets the vector, clearing all data and nullability information.
     */
    virtual void clear() {
        size_ = 0;
        null_bitmap_.clear();
    }
};

/**
 * @brief High-performance template for storing fixed-width numeric and boolean columns.
 */
template <typename T>
class NumericVector : public ColumnVector {
   private:
    using InternalType = std::conditional_t<std::is_same_v<T, bool>, uint8_t, T>;
    std::vector<InternalType> data_;

   public:
    explicit NumericVector(common::ValueType type) : ColumnVector(type) {}

    /**
     * @brief Appends a Value, handling type conversions and nullability.
     */
    void append(const common::Value& val) override {
        if (val.is_null()) {
            null_bitmap_.push_back(true);
            data_.push_back(InternalType{});
        } else {
            null_bitmap_.push_back(false);
            if constexpr (std::is_same_v<T, int64_t>) {
                data_.push_back(val.to_int64());
            } else if constexpr (std::is_same_v<T, double>) {
                data_.push_back(val.to_float64());
            } else if constexpr (std::is_same_v<T, bool>) {
                data_.push_back(static_cast<uint8_t>(val.as_bool()));
            }
        }
        size_++;
    }

    /**
     * @brief Materializes a common::Value for the element at the specified index.
     */
    common::Value get(size_t index) const override {
        if (index >= size_ || null_bitmap_[index]) return common::Value::make_null();
        if constexpr (std::is_same_v<T, int64_t>) return common::Value::make_int64(data_[index]);
        if constexpr (std::is_same_v<T, double>) return common::Value::make_float64(data_[index]);
        if constexpr (std::is_same_v<T, bool>)
            return common::Value::make_bool(static_cast<bool>(data_[index]));
        return common::Value::make_null();
    }

    /**
     * @brief Directly sets the value at a specific index.
     * Resizes null_bitmap_ if necessary to accommodate the index.
     */
    void set(size_t index, T val) {
        if (index >= size_) {
            resize(index + 1);
        }
        if constexpr (std::is_same_v<T, bool>) {
            data_[index] = static_cast<uint8_t>(val);
        } else {
            data_[index] = val;
        }
        null_bitmap_[index] = false;
    }

    /**
     * @brief Provides read-only access to the underlying raw data buffer.
     */
    const InternalType* raw_data() const { return data_.data(); }

    /**
     * @brief Provides mutable access to the underlying raw data buffer.
     */
    InternalType* raw_data_mut() { return data_.data(); }

    /**
     * @brief Resizes the underlying buffers to the specified capacity.
     */
    void resize(size_t new_size) {
        data_.resize(new_size);
        null_bitmap_.resize(new_size, false);
        size_ = new_size;
    }

    void clear() override {
        ColumnVector::clear();
        data_.clear();
    }
};

/**
 * @brief Represents a set of data blocks (batches) in a columnar format for vectorized processing.
 */
class VectorBatch {
   private:
    std::vector<std::unique_ptr<ColumnVector>> columns_;
    size_t row_count_ = 0;

   public:
    VectorBatch() = default;

    /**
     * @brief Adds a pre-allocated column vector to the batch.
     */
    void add_column(std::unique_ptr<ColumnVector> col) { columns_.push_back(std::move(col)); }
    [[nodiscard]] size_t column_count() const { return columns_.size(); }
    [[nodiscard]] size_t row_count() const { return row_count_; }

    /**
     * @brief Retrieves a mutable reference to a column by its index.
     */
    ColumnVector& get_column(size_t index) { return *columns_.at(index); }

    void set_row_count(size_t count) { row_count_ = count; }

    /**
     * @brief Initializes the batch's column structure based on the provided schema.
     * @param schema The schema to match.
     * @throws std::runtime_error if an unsupported column type is encountered.
     */
    void init_from_schema(const Schema& schema) {
        clear();
        columns_.clear();
        for (const auto& col : schema.columns()) {
            switch (col.type()) {
                case common::ValueType::TYPE_INT8:
                case common::ValueType::TYPE_INT16:
                case common::ValueType::TYPE_INT32:
                case common::ValueType::TYPE_INT64:
                    add_column(std::make_unique<NumericVector<int64_t>>(col.type()));
                    break;
                case common::ValueType::TYPE_FLOAT32:
                case common::ValueType::TYPE_FLOAT64:
                    add_column(std::make_unique<NumericVector<double>>(col.type()));
                    break;
                case common::ValueType::TYPE_BOOL:
                    add_column(std::make_unique<NumericVector<bool>>(col.type()));
                    break;
                case common::ValueType::TYPE_TEXT:
                    throw std::runtime_error("Vectorized StringVector implementation is pending.");
                default:
                    throw std::runtime_error("Unsupported column type for vectorized execution: " +
                                             std::to_string(static_cast<int>(col.type())));
            }
        }
    }

    /**
     * @brief Factory method to create a VectorBatch matching a schema definition.
     */
    static std::unique_ptr<VectorBatch> create(const Schema& schema) {
        auto batch = std::make_unique<VectorBatch>();
        batch->init_from_schema(schema);
        return batch;
    }

    /**
     * @brief Appends row data from a Tuple to the corresponding column vectors.
     * @throws std::runtime_error if the tuple size does not match the column count.
     */
    void append_tuple(const Tuple& tuple) {
        if (tuple.size() != columns_.size()) {
            throw std::runtime_error("VectorBatch dimensionality mismatch: Tuple size (" +
                                     std::to_string(tuple.size()) + ") vs Column count (" +
                                     std::to_string(columns_.size()) + ")");
        }
        for (size_t i = 0; i < tuple.size(); ++i) {
            columns_[i]->append(tuple.get(i));
        }
        row_count_++;
    }

    /**
     * @brief Resets all column vectors and the row count to zero.
     */
    void clear() {
        for (auto& col : columns_) col->clear();
        row_count_ = 0;
    }
};

/**
 * @brief Encapsulates the results of a query execution, including metadata and row data.
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

    [[nodiscard]] bool success() const { return !has_error_; }
    [[nodiscard]] const std::string& error() const { return error_message_; }
    void set_error(const std::string& msg) {
        error_message_ = msg;
        has_error_ = true;
    }

    void set_schema(const Schema& schema) { schema_ = schema; }
    [[nodiscard]] const Schema& schema() const { return schema_; }

    void add_row(const Tuple& row) { rows_.push_back(row); }
    void add_row(Tuple&& row) { rows_.push_back(std::move(row)); }
    void add_rows(const std::vector<Tuple>& new_rows) {
        rows_.insert(rows_.end(), new_rows.begin(), new_rows.end());
    }

    [[nodiscard]] size_t row_count() const { return rows_.size(); }
    [[nodiscard]] const std::vector<Tuple>& rows() const { return rows_; }
    [[nodiscard]] std::vector<Tuple>& rows() { return rows_; }

    [[nodiscard]] uint64_t execution_time() const { return execution_time_us_; }
    void set_execution_time(uint64_t us) { execution_time_us_ = us; }

    [[nodiscard]] uint64_t rows_affected() const { return rows_affected_; }
    void set_rows_affected(uint64_t count) { rows_affected_ = count; }
};

}  // namespace cloudsql::executor

inline std::string cloudsql::executor::Tuple::to_string() const {
    std::string result = "(";
    for (size_t i = 0; i < values_.size(); i++) {
        if (i > 0) {
            result += ", ";
        }
        result += values_[i].to_string();
    }
    result += ")";
    return result;
}

#endif  // CLOUDSQL_EXECUTOR_TYPES_HPP
