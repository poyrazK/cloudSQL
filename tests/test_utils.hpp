#ifndef CLOUDSQL_TESTS_TEST_UTILS_HPP
#define CLOUDSQL_TESTS_TEST_UTILS_HPP

#include <gtest/gtest.h>
#include <string>
#include <type_traits>

namespace cloudsql::tests {

/**
 * @brief Helper functions for test expectations
 */
namespace detail {

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
}  // namespace detail

// Custom macro for pointer equality that was used in the project
#ifndef EXPECT_PTR_EQ
#define EXPECT_PTR_EQ(a, b) EXPECT_EQ(static_cast<const void*>(a), static_cast<const void*>(b))
#endif

// Legacy compatibility (no-op)
#define RUN_TEST(name)

}  // namespace cloudsql::tests

#endif  // CLOUDSQL_TESTS_TEST_UTILS_HPP
