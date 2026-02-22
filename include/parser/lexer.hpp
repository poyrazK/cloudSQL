/**
 * @file lexer.hpp
 * @brief SQL Lexer class for tokenizing SQL statements
 */

#ifndef CLOUDSQL_PARSER_LEXER_HPP
#define CLOUDSQL_PARSER_LEXER_HPP

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "parser/token.hpp"

namespace cloudsql::parser {

/**
 * @brief SQL Lexer class
 *
 * Converts SQL text into a stream of tokens.
 */
class Lexer {
   private:
    std::string input_;
    size_t position_ = 0;
    uint32_t line_ = 1;
    uint32_t column_ = 1;
    char current_char_ = '\0';

    // Keyword lookup table
    static const std::map<std::string, TokenType> keywords_;

    // Initialize keywords
    [[nodiscard]] static std::map<std::string, TokenType> init_keywords();

   public:
    /**
     * @brief Construct a lexer
     * @param input SQL input string
     */
    explicit Lexer(std::string input);

    /**
     * @brief Get the next token
     * @return Next token in the stream
     */
    Token next_token();

    /**
     * @brief Peek at the next token without consuming it
     * @return Next token (or End token if at end)
     */
    [[nodiscard]] Token peek_token();

    /**
     * @brief Check if at end of input
     * @return true if at end
     */
    [[nodiscard]] bool is_at_end() const;

    /**
     * @brief Get current line number
     * @return Current line (1-indexed)
     */
    [[nodiscard]] uint32_t line() const { return line_; }

    /**
     * @brief Get current column number
     * @return Current column (1-indexed)
     */
    [[nodiscard]] uint32_t column() const { return column_; }

   private:
    /**
     * @brief Advance to the next character
     */
    void advance();

    /**
     * @brief Skip whitespace characters
     */
    void skip_whitespace();

    /**
     * @brief Skip a comment
     */
    void skip_comment();

    /**
     * @brief Check if current character is a letter
     */
    [[nodiscard]] bool is_letter() const;

    /**
     * @brief Check if current character is a digit
     */
    [[nodiscard]] bool is_digit() const;

    /**
     * @brief Check if current character is part of an identifier
     */
    [[nodiscard]] bool is_identifier_char() const;

    /**
     * @brief Read a number literal
     * @return Number token
     */
    [[nodiscard]] Token read_number();

    /**
     * @brief Read a string literal
     * @return String token
     */
    [[nodiscard]] Token read_string();

    /**
     * @brief Read an identifier or keyword
     * @return Identifier or keyword token
     */
    [[nodiscard]] Token read_identifier();

    /**
     * @brief Read an operator
     * @return Operator token
     */
    [[nodiscard]] Token read_operator();

    /**
     * @brief Create a token with current position
     * @param type Token type
     * @return Token with position set
     */
    [[nodiscard]] Token make_token(TokenType type) const;

    /**
     * @brief Create an error token
     * @param message Error message
     * @return Error token
     */
    [[nodiscard]] Token make_error(const std::string& message) const;
};



}  // namespace cloudsql::parser

#endif  // CLOUDSQL_PARSER_LEXER_HPP
