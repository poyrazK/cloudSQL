#ifndef SQL_ENGINE_TEST_UTILS_HPP
#define SQL_ENGINE_TEST_UTILS_HPP

#include <iostream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

namespace cloudsql::tests {

inline int tests_passed = 0;
inline int tests_failed = 0;

/**
 * @brief Helper functions for test expectations to avoid macros with do-while
 */
namespace detail {
inline void expect_true(bool condition, const char* expr) {
    if (!condition) {
        throw std::runtime_error(std::string("Condition failed: ") + expr);
    }
}

/**
 * @brief String conversion helper for error messages
 */
template <typename T>
std::string to_string_safe(const T& val) {
    if constexpr (std::is_same_v<T, std::string>) {
        return val;
    } else if constexpr (std::is_convertible_v<T, const char*>) {
        return std::string(val);
    } else if constexpr (std::is_arithmetic_v<T>) {
        return std::to_string(val);
    } else {
        return "unknown_type";
    }
}

template <typename T, typename U>
void expect_eq(const T& a, const U& b, const char* expr_a, const char* expr_b) {
    if (!(a == b)) {
        throw std::runtime_error(std::string("Equality failed: ") + expr_a + " (" +
                                 to_string_safe(a) + ") != " + expr_b + " (" + to_string_safe(b) +
                                 ")");
    }
}

// Specialization for types that don't support std::to_string
inline void expect_eq_str(const std::string& a, const std::string& b, const char* expr_a,
                          const char* expr_b) {
    if (a != b) {
        throw std::runtime_error(std::string("Equality failed: ") + expr_a + " (\"" + a +
                                 "\") != " + expr_b + " (\"" + b + "\")");
    }
}

template <typename T, typename U>
void expect_gt(const T& a, const U& b, const char* expr_a, const char* expr_b) {
    if (!(a > b)) {
        throw std::runtime_error(std::string("Greater-than failed: ") + expr_a + " > " + expr_b);
    }
}

template <typename T, typename U>
void expect_lt(const T& a, const U& b, const char* expr_a, const char* expr_b) {
    if (!(a < b)) {
        throw std::runtime_error(std::string("Less-than failed: ") + expr_a + " < " + expr_b);
    }
}

inline void expect_double_eq(double a, double b, const char* expr_a, const char* expr_b) {
    const double diff = a - b;
    const double abs_diff = (diff < 0) ? -diff : diff;
    if (abs_diff > 1e-9) {
        throw std::runtime_error(std::string("Double equality failed: ") + expr_a +
                                 " != " + expr_b);
    }
}

inline void expect_ptr_eq(const void* a, const void* b, const char* expr_a, const char* expr_b) {
    if (a != b) {
        throw std::runtime_error(std::string("Pointer equality failed: ") + expr_a +
                                 " != " + expr_b);
    }
}
}  // namespace detail

#define TEST(name) static void test_##name()

#define RUN_TEST(name)                                    \
    {                                                     \
        std::cout << "  " << #name << "... ";             \
        try {                                             \
            test_##name();                                \
            std::cout << "PASSED\n";                      \
            tests_passed++;                               \
        } catch (const std::exception& e) {               \
            std::cout << "FAILED (" << e.what() << ")\n"; \
            tests_failed++;                               \
        } catch (...) {                                   \
            std::cout << "FAILED (unknown exception)\n";  \
            tests_failed++;                               \
        }                                                 \
    }

#define EXPECT_TRUE(a) ::cloudsql::tests::detail::expect_true((a), #a)

#define EXPECT_FALSE(a) ::cloudsql::tests::detail::expect_true(!(a), "!(" #a ")")

#define EXPECT_EQ(a, b) ::cloudsql::tests::detail::expect_eq((a), (b), #a, #b)

#define EXPECT_GT(a, b) ::cloudsql::tests::detail::expect_gt((a), (b), #a, #b)

#define EXPECT_LT(a, b) ::cloudsql::tests::detail::expect_lt((a), (b), #a, #b)

#define EXPECT_STREQ(a, b) \
    ::cloudsql::tests::detail::expect_eq_str(std::string(a), std::string(b), #a, #b)

#define EXPECT_DOUBLE_EQ(a, b) ::cloudsql::tests::detail::expect_double_eq((a), (b), #a, #b)

#define EXPECT_PTR_EQ(a, b)                                               \
    ::cloudsql::tests::detail::expect_ptr_eq(static_cast<const void*>(a), \
                                             static_cast<const void*>(b), #a, #b)

#define EXPECT_THROW(expr, ex_type)                                                       \
    try {                                                                                 \
        expr;                                                                             \
        throw std::runtime_error(std::string("Expected exception not thrown: ") + #expr); \
    } catch (const ex_type&) {                                                            \
        /* Success */                                                                     \
    }

}  // namespace cloudsql::tests

#endif  // SQL_ENGINE_TEST_UTILS_HPP
