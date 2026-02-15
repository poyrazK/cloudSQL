/**
 * @file lexer.cpp
 * @brief SQL Lexer implementation
 *
 * @defgroup lexer Lexer Implementation
 * @{
 */

#include "parser/lexer.hpp"
#include <iostream>
#include <algorithm>

namespace cloudsql {
namespace parser {

/* Static keyword initialization */
const std::map<std::string, TokenType> Lexer::keywords_ = Lexer::init_keywords();

std::map<std::string, TokenType> Lexer::init_keywords() {
    return {
        {"SELECT", TokenType::Select},
        {"FROM", TokenType::From},
        {"WHERE", TokenType::Where},
        {"INSERT", TokenType::Insert},
        {"INTO", TokenType::Into},
        {"VALUES", TokenType::Values},
        {"UPDATE", TokenType::Update},
        {"SET", TokenType::Set},
        {"DELETE", TokenType::Delete},
        {"CREATE", TokenType::Create},
        {"TABLE", TokenType::Table},
        {"DROP", TokenType::Drop},
        {"INDEX", TokenType::Index},
        {"ON", TokenType::On},
        {"AND", TokenType::And},
        {"OR", TokenType::Or},
        {"NOT", TokenType::Not},
        {"IN", TokenType::In},
        {"LIKE", TokenType::Like},
        {"IS", TokenType::Is},
        {"NULL", TokenType::Null},
        {"PRIMARY", TokenType::Primary},
        {"KEY", TokenType::Key},
        {"FOREIGN", TokenType::Foreign},
        {"REFERENCES", TokenType::References},
        {"JOIN", TokenType::Join},
        {"LEFT", TokenType::Left},
        {"RIGHT", TokenType::Right},
        {"INNER", TokenType::Inner},
        {"OUTER", TokenType::Outer},
        {"ORDER", TokenType::Order},
        {"BY", TokenType::By},
        {"ASC", TokenType::Asc},
        {"DESC", TokenType::Desc},
        {"GROUP", TokenType::Group},
        {"HAVING", TokenType::Having},
        {"LIMIT", TokenType::Limit},
        {"OFFSET", TokenType::Offset},
        {"AS", TokenType::As},
        {"DISTINCT", TokenType::Distinct},
        {"COUNT", TokenType::Count},
        {"SUM", TokenType::Sum},
        {"AVG", TokenType::Avg},
        {"MIN", TokenType::Min},
        {"MAX", TokenType::Max},
        {"BEGIN", TokenType::Begin},
        {"COMMIT", TokenType::Commit},
        {"ROLLBACK", TokenType::Rollback},
        {"TRUNCATE", TokenType::Truncate},
        {"ALTER", TokenType::Alter},
        {"ADD", TokenType::Add},
        {"COLUMN", TokenType::Column},
        {"TYPE", TokenType::Type},
        {"CONSTRAINT", TokenType::Constraint},
        {"UNIQUE", TokenType::Unique},
        {"CHECK", TokenType::Check},
        {"DEFAULT", TokenType::Default},
        {"EXISTS", TokenType::Exists},
        {"IF", TokenType::If},
        {"VARCHAR", TokenType::Varchar}
    };
}

/**
 * @brief Construct a lexer
 */
Lexer::Lexer(const std::string& input)
    : input_(input), position_(0), line_(1), column_(1) {
    if (!input_.empty()) {
        current_char_ = input_[0];
    } else {
        current_char_ = '\0';
    }
}

/**
 * @brief Advance to the next character
 */
void Lexer::advance() {
    if (position_ < input_.size()) {
        if (current_char_ == '\n') {
            line_++;
            column_ = 1;
        } else {
            column_++;
        }
        position_++;
        if (position_ < input_.size()) {
            current_char_ = input_[position_];
        } else {
            current_char_ = '\0';
        }
    }
}

/**
 * @brief Skip whitespace characters and comments
 */
void Lexer::skip_whitespace() {
    while (!is_at_end()) {
        if (current_char_ == ' ' || current_char_ == '\t' || 
            current_char_ == '\n' || current_char_ == '\r') {
            advance();
        } else if (current_char_ == '-') {
            // Check for single-line comment (--)
            if (position_ + 1 < input_.size() && input_[position_ + 1] == '-') {
                skip_comment();
            } else {
                break;
            }
        } else {
            break;
        }
    }
}

/**
 * @brief Skip a single-line comment
 */
void Lexer::skip_comment() {
    while (!is_at_end() && current_char_ != '\n') {
        advance();
    }
}

bool Lexer::is_letter() const {
    return (current_char_ >= 'a' && current_char_ <= 'z') ||
           (current_char_ >= 'A' && current_char_ <= 'Z') ||
           current_char_ == '_';
}

bool Lexer::is_digit() const {
    return current_char_ >= '0' && current_char_ <= '9';
}

bool Lexer::is_identifier_char() const {
    return is_letter() || is_digit();
}

bool Lexer::is_at_end() const {
    return position_ >= input_.size();
}

Token Lexer::make_token(TokenType type) {
    return Token(type, "", line_, column_);
}

Token Lexer::make_error(const std::string& message) {
    Token error(TokenType::Error, message);
    error.set_position(line_, column_);
    return error;
}

/**
 * @brief Read a number literal
 */
Token Lexer::read_number() {
    uint32_t start_line = line_;
    uint32_t start_col = column_;
    
    std::string number_str;
    while (is_digit()) {
        number_str += current_char_;
        advance();
    }
    
    // Check for decimal point
    if (current_char_ == '.' && position_ + 1 < input_.size() && (input_[position_ + 1] >= '0' && input_[position_ + 1] <= '9')) {
        // std::cerr << "DEBUG Lexer: Entering decimal block for " << number_str << std::endl;
        number_str += current_char_;
        advance();
        while (is_digit()) {
            number_str += current_char_;
            advance();
        }
        
        // Check for exponent
        if (current_char_ == 'e' || current_char_ == 'E') {
            number_str += current_char_;
            advance();
            if (current_char_ == '+' || current_char_ == '-') {
                number_str += current_char_;
                advance();
            }
            while (is_digit()) {
                number_str += current_char_;
                advance();
            }
        }
        
        try {
            double value = std::stod(number_str);
            Token tok(TokenType::Number, value);
            tok.set_position(start_line, start_col);
            return tok;
        } catch (...) {
            // Fallback
        }
    }
    
    try {
        int64_t value = std::stoll(number_str);
        Token tok(TokenType::Number, value);
        tok.set_position(start_line, start_col);
        return tok;
    } catch (...) {
        return make_error("Invalid number literal: " + number_str);
    }
}

/**
 * @brief Read a string literal
 */
Token Lexer::read_string() {
    uint32_t start_line = line_;
    uint32_t start_col = column_;
    
    char quote_char = current_char_;
    advance(); // Skip opening quote
    
    std::string value;
    while (!is_at_end() && current_char_ != quote_char) {
        if (current_char_ == '\\' && position_ + 1 < input_.size()) {
            advance();
            switch (current_char_) {
                case 'n': value += '\n'; break;
                case 't': value += '\t'; break;
                case 'r': value += '\r'; break;
                case '\'': value += '\''; break;
                case '"': value += '"'; break;
                case '\\': value += '\\'; break;
                default: value += current_char_; break;
            }
        } else {
            value += current_char_;
        }
        advance();
    }
    
    // Skip closing quote
    if (!is_at_end() && current_char_ == quote_char) {
        advance();
    }
    
    Token tok(TokenType::String, value, true);
    tok.set_position(start_line, start_col);
    return tok;
}

/**
 * @brief Read an identifier or keyword
 */
Token Lexer::read_identifier() {
    uint32_t start_line = line_;
    uint32_t start_col = column_;
    
    std::string identifier;
    while (is_identifier_char()) {
        identifier += current_char_;
        advance();
    }
    
    // Check if it's a keyword (case-insensitive lookup usually but our map is uppercase)
    std::string lookup = identifier;
    std::transform(lookup.begin(), lookup.end(), lookup.begin(), ::toupper);
    
    auto it = keywords_.find(lookup);
    if (it != keywords_.end()) {
        Token tok(it->second, identifier);
        tok.set_position(start_line, start_col);
        return tok;
    }
    
    // It's an identifier
    Token tok(TokenType::Identifier, identifier);
    tok.set_position(start_line, start_col);
    return tok;
}

/**
 * @brief Read an operator
 */
Token Lexer::read_operator() {
    uint32_t start_line = line_;
    uint32_t start_col = column_;
    
    char c = current_char_;
    advance();
    
    // Two-character operators
    if (position_ < input_.size()) {
        char next = current_char_;
        
        if (c == '=' && next == '=') {
            advance();
            Token tok(TokenType::Eq, "==");
            tok.set_position(start_line, start_col);
            return tok;
        }
        if (c == '<' && next == '>') {
            advance();
            Token tok(TokenType::Ne, "<>");
            tok.set_position(start_line, start_col);
            return tok;
        }
        if (c == '<' && next == '=') {
            advance();
            Token tok(TokenType::Le, "<=");
            tok.set_position(start_line, start_col);
            return tok;
        }
        if (c == '>' && next == '=') {
            advance();
            Token tok(TokenType::Ge, ">=");
            tok.set_position(start_line, start_col);
            return tok;
        }
        if (c == '|' && next == '|') {
            advance();
            Token tok(TokenType::Concat, "||");
            tok.set_position(start_line, start_col);
            return tok;
        }
    }
    
    // Single-character operators
    std::string op(1, c);
    TokenType type;
    
    switch (c) {
        case '=': type = TokenType::Eq; break;
        case '<': type = TokenType::Lt; break;
        case '>': type = TokenType::Gt; break;
        case '+': type = TokenType::Plus; break;
        case '-': type = TokenType::Minus; break;
        case '*': type = TokenType::Star; break;
        case '/': type = TokenType::Slash; break;
        case '%': type = TokenType::Percent; break;
        case '(': type = TokenType::LParen; break;
        case ')': type = TokenType::RParen; break;
        case ',': type = TokenType::Comma; break;
        case ';': type = TokenType::Semicolon; break;
        case '.': type = TokenType::Dot; break;
        case ':': type = TokenType::Colon; break;
        default:
            return make_error("Unknown operator: " + op);
    }
    
    Token tok(type, op);
    tok.set_position(start_line, start_col);
    return tok;
}

/**
 * @brief Get the next token
 */
Token Lexer::next_token() {
    skip_whitespace();
    
    if (is_at_end()) {
        return Token(TokenType::End, "", line_, column_);
    }
    
    char c = current_char_;
    
    // Number
    if (is_digit()) {
        return read_number();
    }
    
    // String
    if (c == '\'' || c == '"') {
        return read_string();
    }
    
    // Identifier or keyword
    if (is_letter()) {
        return read_identifier();
    }
    
    // Operator
    return read_operator();
}

/**
 * @brief Peek at the next token without consuming it
 */
Token Lexer::peek_token() {
    if (is_at_end()) {
        return Token(TokenType::End, "", line_, column_);
    }
    
    // Save state
    size_t saved_pos = position_;
    uint32_t saved_line = line_;
    uint32_t saved_col = column_;
    char saved_char = current_char_;
    
    // Get next token
    Token tok = next_token();
    
    // Restore state
    position_ = saved_pos;
    line_ = saved_line;
    column_ = saved_col;
    current_char_ = saved_char;
    
    return tok;
}

}  // namespace parser
}  // namespace cloudsql

/** @} */ /* lexer */
