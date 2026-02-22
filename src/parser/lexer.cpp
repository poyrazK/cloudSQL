/**
 * @file lexer.cpp
 * @brief SQL Lexer implementation
 */

#include "parser/lexer.hpp"

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <utility>

#include "parser/token.hpp"

namespace cloudsql::parser {

// Static member initialization
const std::map<std::string, TokenType> Lexer::keywords_ = Lexer::init_keywords();

Lexer::Lexer(std::string input) : input_(std::move(input)) {
    if (!input_.empty()) {
        current_char_ = input_[0];
    }
}

/**
 * @brief Initialize SQL keywords map
 */
std::map<std::string, TokenType> Lexer::init_keywords() {
    return {{"SELECT", TokenType::Select},
            {"FROM", TokenType::From},
            {"WHERE", TokenType::Where},
            {"INSERT", TokenType::Insert},
            {"INTO", TokenType::Into},
            {"VALUES", TokenType::Values},
            {"DELETE", TokenType::Delete},
            {"UPDATE", TokenType::Update},
            {"SET", TokenType::Set},
            {"CREATE", TokenType::Create},
            {"TABLE", TokenType::Table},
            {"INDEX", TokenType::Index},
            {"DROP", TokenType::Drop},
            {"AND", TokenType::And},
            {"OR", TokenType::Or},
            {"NOT", TokenType::Not},
            {"IN", TokenType::In},
            {"LIKE", TokenType::Like},
            {"IS", TokenType::Is},
            {"NULL", TokenType::Null},
            {"TRUE", TokenType::True},
            {"FALSE", TokenType::False},
            {"JOIN", TokenType::Join},
            {"ON", TokenType::On},
            {"LEFT", TokenType::Left},
            {"RIGHT", TokenType::Right},
            {"INNER", TokenType::Inner},
            {"OUTER", TokenType::Outer},
            {"GROUP", TokenType::Group},
            {"BY", TokenType::By},
            {"ORDER", TokenType::Order},
            {"ASC", TokenType::Asc},
            {"DESC", TokenType::Desc},
            {"LIMIT", TokenType::Limit},
            {"OFFSET", TokenType::Offset},
            {"BEGIN", TokenType::Begin},
            {"COMMIT", TokenType::Commit},
            {"ROLLBACK", TokenType::Rollback},
            {"IF", TokenType::If},
            {"EXISTS", TokenType::Exists},
            {"UNIQUE", TokenType::Unique},
            {"INT", TokenType::TypeInt},
            {"INTEGER", TokenType::TypeInt},
            {"BIGINT", TokenType::TypeBigInt},
            {"FLOAT", TokenType::TypeFloat},
            {"DOUBLE", TokenType::TypeDouble},
            {"TEXT", TokenType::TypeText},
            {"VARCHAR", TokenType::TypeVarchar},
            {"CHAR", TokenType::TypeChar},
            {"BOOL", TokenType::TypeBool},
            {"BOOLEAN", TokenType::TypeBool},
            {"DISTINCT", TokenType::Distinct}};
}

Token Lexer::next_token() {
    while (true) {
        skip_whitespace();

        if (position_ >= input_.length()) {
            return {TokenType::End, ""};
        }

        if (current_char_ == '-' && position_ + 1 < input_.length() && input_[position_ + 1] == '-') {
            skip_comment();
            continue;
        }
        break;
    }

    const char c = current_char_;

    /* Identifiers and Keywords */
    if (std::isalpha(c) || c == '_') {
        auto tok = read_identifier();
        tok.set_position(line_, column_ - static_cast<uint32_t>(tok.lexeme().length()));
        return tok;
    }

    /* Numbers */
    if (std::isdigit(c)) {
        auto tok = read_number();
        tok.set_position(line_, column_ - static_cast<uint32_t>(tok.lexeme().length()));
        return tok;
    }

    /* Strings */
    if (c == '\'') {
        auto tok = read_string();
        tok.set_position(line_, column_ - static_cast<uint32_t>(tok.lexeme().length()));
        return tok;
    }

    /* Operators and Punctuation */
    auto tok = read_operator();
    tok.set_position(line_, column_ - static_cast<uint32_t>(tok.lexeme().length()));
    return tok;
}

void Lexer::advance() {
    position_++;
    if (position_ >= input_.length()) {
        current_char_ = '\0';
    } else {
        current_char_ = input_[position_];
    }
    column_++;
}

void Lexer::skip_whitespace() {
    while (position_ < input_.length() && std::isspace(current_char_)) {
        if (current_char_ == '\n') {
            line_++;
            column_ = 0;
        }
        advance();
    }
}

void Lexer::skip_comment() {
    // Skip '--'
    advance();
    advance();
    while (position_ < input_.length() && current_char_ != '\n') {
        advance();
    }
}

Token Lexer::read_identifier() {
    const size_t start = position_;
    while (position_ < input_.length() && (std::isalnum(current_char_) || current_char_ == '_')) {
        advance();
    }

    const std::string text = input_.substr(start, position_ - start);
    std::string upper_text = text;
    std::transform(upper_text.begin(), upper_text.end(), upper_text.begin(),
                   [](unsigned char val) { return static_cast<char>(std::toupper(val)); });

    auto it = keywords_.find(upper_text);
    if (it != keywords_.end()) {
        if (it->second == TokenType::True) {
            return {it->second, true};
        }
        if (it->second == TokenType::False) {
            return {it->second, false};
        }
        return {it->second, text};
    }

    return {TokenType::Identifier, text};
}

Token Lexer::read_number() {
    const size_t start = position_;
    bool is_float = false;

    while (position_ < input_.length() && (std::isdigit(current_char_) || current_char_ == '.')) {
        if (current_char_ == '.') {
            if (is_float) {
                break;
            }
            is_float = true;
        }
        advance();
    }

    const std::string text = input_.substr(start, position_ - start);
    if (is_float) {
        try {
            const double v = std::stod(text);
            return {TokenType::Number, v};
        } catch (...) {
            static_cast<void>(0);
            return {TokenType::Error, text};
        }
    } else {
        try {
            const int64_t v = std::stoll(text);
            return {TokenType::Number, v};
        } catch (...) {
            /* If it overflows int64, try as double */
            try {
                const double v = std::stod(text);
                return {TokenType::Number, v};
            } catch (...) {
                static_cast<void>(0);
                return {TokenType::Error, text};
            }
        }
    }
}

Token Lexer::read_string() {
    advance(); /* Skip opening quote */
    const size_t start = position_;

    while (position_ < input_.length() && current_char_ != '\'') {
        advance();
    }

    if (position_ >= input_.length()) {
        return {TokenType::Error, input_.substr(start)};
    }

    const std::string text = input_.substr(start, position_ - start);
    advance(); /* Skip closing quote */
    return {TokenType::String, text, true};
}

Token Lexer::read_operator() {
    const char c = current_char_;
    advance();
    switch (c) {
        case '(':
            return {TokenType::LParen, "("};
        case ')':
            return {TokenType::RParen, ")"};
        case ',':
            return {TokenType::Comma, ","};
        case '.':
            return {TokenType::Dot, "."};
        case ';':
            return {TokenType::Semicolon, ";"};
        case '*':
            return {TokenType::Star, "*"};
        case '+':
            return {TokenType::Plus, "+"};
        case '-':
            return {TokenType::Minus, "-"};
        case '/':
            return {TokenType::Slash, "/"};
        case '=':
            return {TokenType::Eq, "="};
        case '<':
            if (current_char_ == '>') {
                advance();
                return {TokenType::Ne, "<>"};
            }
            if (current_char_ == '=') {
                advance();
                return {TokenType::Le, "<="};
            }
            return {TokenType::Lt, "<"};
        case '>':
            if (current_char_ == '=') {
                advance();
                return {TokenType::Ge, ">="};
            }
            return {TokenType::Gt, ">"};
        case '!':
            if (current_char_ == '=') {
                advance();
                return {TokenType::Ne, "!="};
            }
            return {TokenType::Error, "!"};
        default:
            return {TokenType::Error, std::string(1, c)};
    }
}

Token Lexer::peek_token() {
    const size_t old_pos = position_;
    const uint32_t old_line = line_;
    const uint32_t old_col = column_;
    const char old_char = current_char_;

    Token tok = next_token();

    position_ = old_pos;
    line_ = old_line;
    column_ = old_col;
    current_char_ = old_char;

    return tok;
}

bool Lexer::is_at_end() const {
    return position_ >= input_.length();
}

}  // namespace cloudsql::parser
