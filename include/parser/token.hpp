/**
 * @file token.hpp
 * @brief Token class for SQL Lexer
 */

#ifndef CLOUDSQL_PARSER_TOKEN_HPP
#define CLOUDSQL_PARSER_TOKEN_HPP

#include <cstdint>
#include <string>
#include <variant>

namespace cloudsql::parser {

/**
 * @brief Token types for SQL
 */
enum class TokenType : uint8_t {
    End = 0,

    /* Keywords */
    Select,
    From,
    Where,
    Insert,
    Into,
    Values,
    Update,
    Set,
    Delete,
    Create,
    Table,
    Drop,
    Index,
    On,
    And,
    Or,
    Not,
    In,
    Like,
    Is,
    Null,
    True,
    False,
    Primary,
    Key,
    Foreign,
    References,
    Join,
    Left,
    Right,
    Inner,
    Outer,
    Order,
    By,
    Asc,
    Desc,
    Group,
    Having,
    Limit,
    Offset,
    As,
    Distinct,
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Begin,
    Commit,
    Rollback,
    If,
    Exists,
    Unique,
    Check,
    Default,
    
    /* Data Types */
    TypeInt,
    TypeBigInt,
    TypeFloat,
    TypeDouble,
    TypeText,
    TypeVarchar,
    TypeChar,
    TypeBool,

    /* Identifiers and literals */
    Identifier,
    String,
    Number,
    Param,

    /* Operators */
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Concat,

    /* Delimiters */
    LParen,
    RParen,
    Comma,
    Semicolon,
    Dot,
    Colon,

    /* Error */
    Error
};

/**
 * @brief Token class with std::variant
 */
class Token {
   private:
    TokenType type_;
    std::string lexeme_;
    uint32_t line_;
    uint32_t column_;

    /* Type-safe value storage */
    std::variant<std::monostate, bool, int64_t, double, std::string> value_;

   public:
    Token();
    explicit Token(TokenType type);
    Token(TokenType type, std::string lexeme);
    Token(TokenType type, const char* lexeme);
    Token(TokenType type, std::string lexeme, uint32_t line, uint32_t column);
    Token(TokenType type, bool value);
    Token(TokenType type, int64_t value);
    Token(TokenType type, double value);
    Token(TokenType type, std::string value, bool is_string);

    ~Token() = default;
    Token(const Token& other) = default;
    Token(Token&& other) noexcept = default;
    Token& operator=(const Token& other) = default;
    Token& operator=(Token&& other) noexcept = default;

    /* Accessors */
    [[nodiscard]] TokenType type() const { return type_; }
    [[nodiscard]] const std::string& lexeme() const { return lexeme_; }
    [[nodiscard]] uint32_t line() const { return line_; }
    [[nodiscard]] uint32_t column() const { return column_; }

    void set_type(TokenType type) { type_ = type; }
    void set_position(uint32_t line, uint32_t column) {
        line_ = line;
        column_ = column;
    }

    /* Value accessors */
    [[nodiscard]] bool as_bool() const {
        if (std::holds_alternative<bool>(value_)) {
            return std::get<bool>(value_);
        }
        return false;
    }
    [[nodiscard]] int64_t as_int64() const {
        if (std::holds_alternative<int64_t>(value_)) {
            return std::get<int64_t>(value_);
        }
        return 0;
    }
    [[nodiscard]] double as_double() const {
        if (std::holds_alternative<double>(value_)) {
            return std::get<double>(value_);
        }
        if (std::holds_alternative<int64_t>(value_)) {
            return static_cast<double>(std::get<int64_t>(value_));
        }
        return 0.0;
    }
    [[nodiscard]] const std::string& as_string() const {
        if (std::holds_alternative<std::string>(value_)) {
            return std::get<std::string>(value_);
        }
        static const std::string empty;
        return empty;
    }

    /* Type queries */
    [[nodiscard]] bool is_keyword() const;
    [[nodiscard]] bool is_literal() const;
    [[nodiscard]] bool is_operator() const;
    [[nodiscard]] bool is_identifier() const;

    [[nodiscard]] std::string to_string() const;
};

inline Token::Token()
    : type_(TokenType::End), line_(0), column_(0), value_(std::monostate{}) {}

inline Token::Token(TokenType type)
    : type_(type), line_(0), column_(0), value_(std::monostate{}) {}

inline Token::Token(TokenType type, std::string lexeme)
    : type_(type), lexeme_(std::move(lexeme)), line_(0), column_(0), value_(std::monostate{}) {}

inline Token::Token(TokenType type, const char* lexeme)
    : type_(type), lexeme_(lexeme), line_(0), column_(0), value_(std::monostate{}) {}

inline Token::Token(TokenType type, std::string lexeme, uint32_t line, uint32_t column)
    : type_(type), lexeme_(std::move(lexeme)), line_(line), column_(column), value_(std::monostate{}) {}

inline Token::Token(TokenType type, bool value)
    : type_(type), lexeme_(value ? "TRUE" : "FALSE"), line_(0), column_(0), value_(value) {}

inline Token::Token(TokenType type, int64_t value)
    : type_(type), lexeme_(std::to_string(value)), line_(0), column_(0), value_(value) {}

inline Token::Token(TokenType type, double value)
    : type_(type), lexeme_(std::to_string(value)), line_(0), column_(0), value_(value) {}

inline Token::Token(TokenType type, std::string value, bool is_string)
    : type_(type),
      lexeme_(is_string ? "'" + value + "'" : value),
      line_(0),
      column_(0),
      value_(is_string ? std::variant<std::monostate, bool, int64_t, double, std::string>(std::move(value))
                       : std::monostate{}) {}

inline bool Token::is_keyword() const {
    return type_ >= TokenType::Select && type_ <= TokenType::TypeBool;
}

inline bool Token::is_literal() const {
    return type_ == TokenType::String || type_ == TokenType::Number || type_ == TokenType::Param;
}

inline bool Token::is_operator() const {
    return type_ >= TokenType::Eq && type_ <= TokenType::Concat;
}

inline bool Token::is_identifier() const {
    return type_ == TokenType::Identifier;
}

inline std::string Token::to_string() const {
    return "Token(type=" + std::to_string(static_cast<int>(type_)) + ", lexeme='" + lexeme_ + "')";
}

}  // namespace cloudsql::parser

#endif  // CLOUDSQL_PARSER_TOKEN_HPP
