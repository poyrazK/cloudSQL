/**
 * @file value.hpp
 * @brief Type-safe Value class for cloudSQL
 */

#ifndef CLOUDSQL_COMMON_VALUE_HPP
#define CLOUDSQL_COMMON_VALUE_HPP

#include <cstdint>
#include <string>
#include <cstdio>
#include <stdexcept>
#include <functional>

namespace cloudsql {
namespace common {

/**
 * @brief Value types supported by the database
 */
enum ValueType {
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
 * @brief Type-safe Value class with RAII
 */
class Value {
private:
    ValueType type_;
    bool bool_value_;
    int64_t int64_value_;
    double float64_value_;
    std::string* string_value_;

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
    
    ~Value();
    Value(const Value& other);
    Value(Value&& other) noexcept;
    Value& operator=(const Value& other);
    Value& operator=(Value&& other) noexcept;

    // Comparison operators
    bool operator==(const Value& other) const;
    bool operator!=(const Value& other) const { return !(*this == other); }
    bool operator<(const Value& other) const;
    bool operator<=(const Value& other) const { return !(other < *this); }
    bool operator>(const Value& other) const { return other < *this; }
    bool operator>=(const Value& other) const { return !(*this < other); }

    static Value make_null();
    static Value make_bool(bool v);
    static Value make_int64(int64_t v);
    static Value make_float64(double v);
    static Value make_text(const std::string& v);

    ValueType type() const { return type_; }
    bool is_null() const { return type_ == TYPE_NULL; }
    bool is_numeric() const;

    bool as_bool() const;
    int8_t as_int8() const;
    int16_t as_int16() const;
    int32_t as_int32() const;
    int64_t as_int64() const;
    float as_float32() const;
    double as_float64() const;
    const std::string& as_text() const;

    int64_t to_int64() const;
    double to_float64() const;
    std::string to_string() const;
    std::string to_debug_string() const;

    void swap(Value& other) noexcept;

    struct Hash {
        std::size_t operator()(const Value& v) const noexcept;
    };
};

// Constructors
inline Value::Value() 
    : type_(TYPE_NULL), bool_value_(false), int64_value_(0), 
      float64_value_(0.0), string_value_(nullptr) {}

inline Value::Value(ValueType type) 
    : type_(type), bool_value_(false), int64_value_(0), 
      float64_value_(0.0), string_value_(nullptr) {}

inline Value::Value(bool v) 
    : type_(TYPE_BOOL), bool_value_(v), int64_value_(0), 
      float64_value_(0.0), string_value_(nullptr) {}

inline Value::Value(int8_t v) 
    : type_(TYPE_INT8), bool_value_(false), int64_value_(static_cast<int64_t>(v)), 
      float64_value_(0.0), string_value_(nullptr) {}

inline Value::Value(int16_t v) 
    : type_(TYPE_INT16), bool_value_(false), int64_value_(static_cast<int64_t>(v)), 
      float64_value_(0.0), string_value_(nullptr) {}

inline Value::Value(int32_t v) 
    : type_(TYPE_INT32), bool_value_(false), int64_value_(static_cast<int64_t>(v)), 
      float64_value_(0.0), string_value_(nullptr) {}

inline Value::Value(int64_t v) 
    : type_(TYPE_INT64), bool_value_(false), int64_value_(v), 
      float64_value_(0.0), string_value_(nullptr) {}

inline Value::Value(float v) 
    : type_(TYPE_FLOAT32), bool_value_(false), int64_value_(0), 
      float64_value_(static_cast<double>(v)), string_value_(nullptr) {}

inline Value::Value(double v) 
    : type_(TYPE_FLOAT64), bool_value_(false), int64_value_(0), 
      float64_value_(v), string_value_(nullptr) {}

inline Value::Value(const std::string& v) 
    : type_(TYPE_TEXT), bool_value_(false), int64_value_(0), 
      float64_value_(0.0), string_value_(new std::string(v)) {}

inline Value::Value(const char* v) 
    : type_(TYPE_TEXT), bool_value_(false), int64_value_(0), 
      float64_value_(0.0), string_value_(new std::string(v)) {}

// Destructor
inline Value::~Value() {
    delete string_value_;
}

// Copy constructor
inline Value::Value(const Value& other)
    : type_(other.type_), bool_value_(other.bool_value_), 
      int64_value_(other.int64_value_), float64_value_(other.float64_value_),
      string_value_(nullptr) {
    if (other.string_value_) {
        string_value_ = new std::string(*other.string_value_);
    }
}

// Move constructor
inline Value::Value(Value&& other) noexcept
    : type_(other.type_), bool_value_(other.bool_value_), 
      int64_value_(other.int64_value_), float64_value_(other.float64_value_),
      string_value_(other.string_value_) {
    other.string_value_ = nullptr;
    other.type_ = TYPE_NULL;
}

// Copy assignment
inline Value& Value::operator=(const Value& other) {
    if (this != &other) {
        Value temp(other);
        swap(temp);
    }
    return *this;
}

// Move assignment
inline Value& Value::operator=(Value&& other) noexcept {
    if (this != &other) {
        delete string_value_;
        type_ = other.type_;
        bool_value_ = other.bool_value_;
        int64_value_ = other.int64_value_;
        float64_value_ = other.float64_value_;
        string_value_ = other.string_value_;
        other.string_value_ = nullptr;
        other.type_ = TYPE_NULL;
    }
    return *this;
}

// Swap
inline void Value::swap(Value& other) noexcept {
    std::swap(type_, other.type_);
    std::swap(bool_value_, other.bool_value_);
    std::swap(int64_value_, other.int64_value_);
    std::swap(float64_value_, other.float64_value_);
    std::swap(string_value_, other.string_value_);
}

// Factory methods
inline Value Value::make_null() { return Value(); }
inline Value Value::make_bool(bool v) { return Value(v); }
inline Value Value::make_int64(int64_t v) { return Value(v); }
inline Value Value::make_float64(double v) { return Value(v); }
inline Value Value::make_text(const std::string& v) { return Value(v); }

// Type queries
inline bool Value::is_numeric() const {
    return type_ == TYPE_INT8 || type_ == TYPE_INT16 || type_ == TYPE_INT32 ||
           type_ == TYPE_INT64 || type_ == TYPE_FLOAT32 || type_ == TYPE_FLOAT64 ||
           type_ == TYPE_DECIMAL;
}

// Accessors
inline bool Value::as_bool() const { 
    if (type_ != TYPE_BOOL) throw std::runtime_error("Value is not bool");
    return bool_value_; 
}

inline int8_t Value::as_int8() const { 
    if (type_ != TYPE_INT8) throw std::runtime_error("Value is not int8");
    return static_cast<int8_t>(int64_value_); 
}

inline int16_t Value::as_int16() const { 
    if (type_ != TYPE_INT16) throw std::runtime_error("Value is not int16");
    return static_cast<int16_t>(int64_value_); 
}

inline int32_t Value::as_int32() const { 
    if (type_ != TYPE_INT32) throw std::runtime_error("Value is not int32");
    return static_cast<int32_t>(int64_value_); 
}

inline int64_t Value::as_int64() const { 
    if (type_ != TYPE_INT64) throw std::runtime_error("Value is not int64");
    return int64_value_; 
}

inline float Value::as_float32() const { 
    if (type_ != TYPE_FLOAT32) throw std::runtime_error("Value is not float32");
    return static_cast<float>(float64_value_); 
}

inline double Value::as_float64() const { 
    if (type_ != TYPE_FLOAT64) throw std::runtime_error("Value is not float64");
    return float64_value_; 
}

inline const std::string& Value::as_text() const { 
    if (type_ != TYPE_TEXT) throw std::runtime_error("Value is not text");
    if (!string_value_) throw std::runtime_error("Value string is null");
    return *string_value_; 
}

// Conversions
inline int64_t Value::to_int64() const {
    switch (type_) {
        case TYPE_BOOL: return bool_value_ ? 1 : 0;
        case TYPE_INT8: case TYPE_INT16: case TYPE_INT32: case TYPE_INT64:
            return int64_value_;
        case TYPE_FLOAT32: case TYPE_FLOAT64:
            return static_cast<int64_t>(float64_value_);
        default: return 0;
    }
}

inline double Value::to_float64() const {
    switch (type_) {
        case TYPE_BOOL: return bool_value_ ? 1.0 : 0.0;
        case TYPE_INT8: case TYPE_INT16: case TYPE_INT32: case TYPE_INT64:
            return static_cast<double>(int64_value_);
        case TYPE_FLOAT32: case TYPE_FLOAT64:
            return float64_value_;
        default: return 0.0;
    }
}

inline std::string Value::to_string() const {
    char buf[64];
    
    switch (type_) {
        case TYPE_NULL: return "NULL";
        case TYPE_BOOL: return bool_value_ ? "TRUE" : "FALSE";
        case TYPE_INT8: 
        case TYPE_INT16: 
        case TYPE_INT32: 
            snprintf(buf, sizeof(buf), "%ld", (long)int64_value_);
            return buf;
        case TYPE_INT64: 
            snprintf(buf, sizeof(buf), "%lld", (long long)int64_value_);
            return buf;
        case TYPE_FLOAT32:
            snprintf(buf, sizeof(buf), "%.6g", (double)float64_value_);
            return buf;
        case TYPE_FLOAT64:
            snprintf(buf, sizeof(buf), "%.10g", float64_value_);
            return buf;
        case TYPE_TEXT:
            return string_value_ ? *string_value_ : "";
        default: return "<unknown>";
    }
}

inline bool Value::operator==(const Value& other) const {
    if (type_ != other.type_) {
        if (is_numeric() && other.is_numeric()) {
            return to_float64() == other.to_float64();
        }
        return false;
    }
    
    switch (type_) {
        case TYPE_NULL: return true;
        case TYPE_BOOL: return bool_value_ == other.bool_value_;
        case TYPE_INT8: case TYPE_INT16: case TYPE_INT32: case TYPE_INT64:
            return int64_value_ == other.int64_value_;
        case TYPE_FLOAT32: case TYPE_FLOAT64:
            return float64_value_ == other.float64_value_;
        case TYPE_TEXT: 
            return string_value_ && other.string_value_ && *string_value_ == *other.string_value_;
        default: return false;
    }
}

inline bool Value::operator<(const Value& other) const {
    if (is_numeric() && other.is_numeric()) {
        return to_float64() < other.to_float64();
    }
    if (type_ == TYPE_TEXT && other.type_ == TYPE_TEXT) {
        std::string s1 = string_value_ ? *string_value_ : "";
        std::string s2 = other.string_value_ ? *other.string_value_ : "";
        return s1 < s2;
    }
    return false;
}

inline std::string Value::to_debug_string() const {
    return "Value(type=" + std::to_string(type_) + ", data=" + to_string() + ")";
}

inline std::size_t Value::Hash::operator()(const Value& v) const noexcept {
    std::size_t hash = std::hash<int>{}(v.type_);
    
    switch (v.type_) {
        case TYPE_BOOL:
            hash ^= std::hash<bool>{}(v.bool_value_) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
            break;
        case TYPE_INT8: case TYPE_INT16: case TYPE_INT32: case TYPE_INT64:
            hash ^= std::hash<int64_t>{}(v.int64_value_) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
            break;
        case TYPE_FLOAT32: case TYPE_FLOAT64:
            hash ^= std::hash<double>{}(v.float64_value_) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
            break;
        case TYPE_TEXT:
            if (v.string_value_) {
                hash ^= std::hash<std::string>{}(*v.string_value_) + 0x9e3779b9 + (hash << 6) + (hash >> 2);
            }
            break;
        default: break;
    }
    return hash;
}

}  // namespace common
}  // namespace cloudsql

#endif  // CLOUDSQL_COMMON_VALUE_HPP
