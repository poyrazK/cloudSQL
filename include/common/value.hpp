/**
 * @file value.hpp
 * @brief Type-safe Value class for cloudSQL
 */

#ifndef CLOUDSQL_COMMON_VALUE_HPP
#define CLOUDSQL_COMMON_VALUE_HPP

#include <array>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <variant>

namespace cloudsql::common {

/**
 * @file value.hpp
 * @brief Value types supported by the database
 */
enum class ValueType : uint8_t {
    TYPE_NULL = 0,
    TYPE_BOOL = 1,
    TYPE_INT8 = 2,
    TYPE_INT16 = 3,
    TYPE_INT32 = 4,
    TYPE_INT64 = 5,
    TYPE_FLOAT32 = 6,
    TYPE_FLOAT64 = 7,
    TYPE_DECIMAL = 8,
    TYPE_CHAR = 9,
    TYPE_VARCHAR = 10,
    TYPE_TEXT = 11,
    TYPE_DATE = 12,
    TYPE_TIME = 13,
    TYPE_TIMESTAMP = 14,
    TYPE_JSON = 15,
    TYPE_BLOB = 16
};

/**
 * @brief Type-safe Value class with std::variant
 */
class Value {
   private:
    ValueType type_;
    std::variant<std::monostate, bool, int64_t, double, std::string> data_;

   public:
    Value();
    explicit Value(ValueType type);
    explicit Value(bool v);
    explicit Value(int8_t v);
    explicit Value(int16_t v);
    explicit Value(int32_t v);
    explicit Value(int64_t v);
    explicit Value(float v);
    explicit Value(double v);
    explicit Value(const std::string& v);
    explicit Value(const char* v);

    ~Value() = default;
    Value(const Value& other) = default;
    Value(Value&& other) noexcept = default;
    Value& operator=(const Value& other) = default;
    Value& operator=(Value&& other) noexcept = default;

    /* Comparison operators */
    [[nodiscard]] bool operator==(const Value& other) const;
    [[nodiscard]] bool operator!=(const Value& other) const { return !(*this == other); }
    [[nodiscard]] bool operator<(const Value& other) const;
    [[nodiscard]] bool operator<=(const Value& other) const { return !(other < *this); }
    [[nodiscard]] bool operator>(const Value& other) const { return other < *this; }
    [[nodiscard]] bool operator>=(const Value& other) const { return !(*this < other); }

    [[nodiscard]] static Value make_null();
    [[nodiscard]] static Value make_bool(bool v);
    [[nodiscard]] static Value make_int64(int64_t v);
    [[nodiscard]] static Value make_float64(double v);
    [[nodiscard]] static Value make_text(const std::string& v);

    [[nodiscard]] ValueType type() const { return type_; }
    [[nodiscard]] bool is_null() const { return type_ == ValueType::TYPE_NULL; }
    [[nodiscard]] bool is_numeric() const;

    [[nodiscard]] bool as_bool() const;
    [[nodiscard]] int8_t as_int8() const;
    [[nodiscard]] int16_t as_int16() const;
    [[nodiscard]] int32_t as_int32() const;
    [[nodiscard]] int64_t as_int64() const;
    [[nodiscard]] float as_float32() const;
    [[nodiscard]] double as_float64() const;
    [[nodiscard]] const std::string& as_text() const;

    [[nodiscard]] int64_t to_int64() const;
    [[nodiscard]] double to_float64() const;
    [[nodiscard]] std::string to_string() const;
    [[nodiscard]] std::string to_debug_string() const;

    void swap(Value& other) noexcept {
        std::swap(type_, other.type_);
        std::swap(data_, other.data_);
    }

    struct Hash {
        [[nodiscard]] std::size_t operator()(const Value& v) const noexcept;
    };
};

// Constructors
inline Value::Value() : type_(ValueType::TYPE_NULL), data_(std::monostate{}) {}

inline Value::Value(ValueType type) : type_(type), data_(std::monostate{}) {
    if (type != ValueType::TYPE_NULL) {
        /* Initialize with default values based on type */
        switch (type) {
            case ValueType::TYPE_BOOL:
                data_ = false;
                break;
            case ValueType::TYPE_INT8:
            case ValueType::TYPE_INT16:
            case ValueType::TYPE_INT32:
            case ValueType::TYPE_INT64:
                data_ = int64_t(0);
                break;
            case ValueType::TYPE_FLOAT32:
            case ValueType::TYPE_FLOAT64:
                data_ = 0.0;
                break;
            case ValueType::TYPE_TEXT:
            case ValueType::TYPE_VARCHAR:
            case ValueType::TYPE_CHAR:
                data_ = std::string("");
                break;
            default:
                break;
        }
    }
}

inline Value::Value(bool v) : type_(ValueType::TYPE_BOOL), data_(v) {}

inline Value::Value(int8_t v) : type_(ValueType::TYPE_INT8), data_(static_cast<int64_t>(v)) {}

inline Value::Value(int16_t v) : type_(ValueType::TYPE_INT16), data_(static_cast<int64_t>(v)) {}

inline Value::Value(int32_t v) : type_(ValueType::TYPE_INT32), data_(static_cast<int64_t>(v)) {}

inline Value::Value(int64_t v) : type_(ValueType::TYPE_INT64), data_(v) {}

inline Value::Value(float v) : type_(ValueType::TYPE_FLOAT32), data_(static_cast<double>(v)) {}

inline Value::Value(double v) : type_(ValueType::TYPE_FLOAT64), data_(v) {}

inline Value::Value(const std::string& v) : type_(ValueType::TYPE_TEXT), data_(v) {}

inline Value::Value(const char* v) : type_(ValueType::TYPE_TEXT), data_(std::string(v)) {}

// Factory methods
inline Value Value::make_null() {
    return {};
}
inline Value Value::make_bool(bool v) {
    return Value(v);
}
inline Value Value::make_int64(int64_t v) {
    return Value(v);
}
inline Value Value::make_float64(double v) {
    return Value(v);
}
inline Value Value::make_text(const std::string& v) {
    return Value(v);
}

// Type queries
inline bool Value::is_numeric() const {
    return type_ == ValueType::TYPE_INT8 || type_ == ValueType::TYPE_INT16 || 
           type_ == ValueType::TYPE_INT32 || type_ == ValueType::TYPE_INT64 || 
           type_ == ValueType::TYPE_FLOAT32 || type_ == ValueType::TYPE_FLOAT64 ||
           type_ == ValueType::TYPE_DECIMAL;
}

// Accessors
inline bool Value::as_bool() const {
    if (type_ != ValueType::TYPE_BOOL) {
        throw std::runtime_error("Value is not bool");
    }
    return std::get<bool>(data_);
}

inline int8_t Value::as_int8() const {
    if (type_ != ValueType::TYPE_INT8) {
        throw std::runtime_error("Value is not int8");
    }
    return static_cast<int8_t>(std::get<int64_t>(data_));
}

inline int16_t Value::as_int16() const {
    if (type_ != ValueType::TYPE_INT16) {
        throw std::runtime_error("Value is not int16");
    }
    return static_cast<int16_t>(std::get<int64_t>(data_));
}

inline int32_t Value::as_int32() const {
    if (type_ != ValueType::TYPE_INT32) {
        throw std::runtime_error("Value is not int32");
    }
    return static_cast<int32_t>(std::get<int64_t>(data_));
}

inline int64_t Value::as_int64() const {
    if (type_ != ValueType::TYPE_INT64) {
        throw std::runtime_error("Value is not int64");
    }
    return std::get<int64_t>(data_);
}

inline float Value::as_float32() const {
    if (type_ != ValueType::TYPE_FLOAT32) {
        throw std::runtime_error("Value is not float32");
    }
    return static_cast<float>(std::get<double>(data_));
}

inline double Value::as_float64() const {
    if (type_ != ValueType::TYPE_FLOAT64) {
        throw std::runtime_error("Value is not float64");
    }
    return std::get<double>(data_);
}

inline const std::string& Value::as_text() const {
    if (type_ != ValueType::TYPE_TEXT && type_ != ValueType::TYPE_VARCHAR && type_ != ValueType::TYPE_CHAR) {
        throw std::runtime_error("Value is not text-based");
    }
    return std::get<std::string>(data_);
}

// Conversions
inline int64_t Value::to_int64() const {
    if (std::holds_alternative<int64_t>(data_)) {
        return std::get<int64_t>(data_);
    }
    if (std::holds_alternative<double>(data_)) {
        return static_cast<int64_t>(std::get<double>(data_));
    }
    if (std::holds_alternative<bool>(data_)) {
        return std::get<bool>(data_) ? 1 : 0;
    }
    return 0;
}

inline double Value::to_float64() const {
    if (std::holds_alternative<double>(data_)) {
        return std::get<double>(data_);
    }
    if (std::holds_alternative<int64_t>(data_)) {
        return static_cast<double>(std::get<int64_t>(data_));
    }
    if (std::holds_alternative<bool>(data_)) {
        return std::get<bool>(data_) ? 1.0 : 0.0;
    }
    return 0.0;
}

inline std::string Value::to_string() const {
    if (std::holds_alternative<std::monostate>(data_)) {
        return "NULL";
    }
    if (std::holds_alternative<bool>(data_)) {
        return std::get<bool>(data_) ? "TRUE" : "FALSE";
    }
    if (std::holds_alternative<int64_t>(data_)) {
        return std::to_string(std::get<int64_t>(data_));
    }
    if (std::holds_alternative<double>(data_)) {
        static constexpr int STRING_BUF_SIZE = 64;
        std::array<char, STRING_BUF_SIZE> buf{};
        std::snprintf(buf.data(), buf.size(), "%.10g", std::get<double>(data_));
        return buf.data();
    }
    if (std::holds_alternative<std::string>(data_)) {
        return std::get<std::string>(data_);
    }
    return "<unknown>";
}

inline bool Value::operator==(const Value& other) const {
    if (type_ != other.type_) {
        if (is_numeric() && other.is_numeric()) {
            return to_float64() == other.to_float64();
        }
        return false;
    }
    return data_ == other.data_;
}

inline bool Value::operator<(const Value& other) const {
    if (is_numeric() && other.is_numeric()) {
        return to_float64() < other.to_float64();
    }
    if (std::holds_alternative<std::string>(data_) &&
        std::holds_alternative<std::string>(other.data_)) {
        return std::get<std::string>(data_) < std::get<std::string>(other.data_);
    }
    return false;
}

inline std::string Value::to_debug_string() const {
    return "Value(type=" + std::to_string(static_cast<int>(type_)) + ", data=" + to_string() + ")";
}

inline std::size_t Value::Hash::operator()(const Value& v) const noexcept {
    try {
        std::size_t h = std::hash<int>{}(static_cast<int>(v.type_));
        static constexpr std::size_t GOLDEN_RATIO = 0x9e3779b9U;
        static constexpr unsigned int SHIFT_L = 6;
        static constexpr unsigned int SHIFT_R = 2;

        std::visit(
            [&h](auto&& arg) {
                using T = std::decay_t<decltype(arg)>;
                if constexpr (!std::is_same_v<T, std::monostate>) {
                    h ^= std::hash<T>{}(arg) + GOLDEN_RATIO + (h << SHIFT_L) + (h >> SHIFT_R);
                }
            },
            v.data_);
        return h;
    } catch (...) {
        /* This should not happen as we maintain type invariants and use standard hashes. */
        return 0;
    }
}

}  // namespace cloudsql::common

#endif  // CLOUDSQL_COMMON_VALUE_HPP
