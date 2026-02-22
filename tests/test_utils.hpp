#ifndef SQL_ENGINE_TEST_UTILS_HPP
#define SQL_ENGINE_TEST_UTILS_HPP

#include <iostream>
#include <string>
#include <vector>
#include <stdexcept>

namespace cloudsql::tests {

inline int tests_passed = 0;
inline int tests_failed = 0;

#define TEST(name) static void test_##name()

#define RUN_TEST(name)                                                \
    do { /* NOLINT(cppcoreguidelines-avoid-do-while) */       \
        std::cout << "  " << #name << "... ";                         \
        try {                                                         \
            test_##name();                                    \
            std::cout << "PASSED\n";                                  \
            tests_passed++;                                           \
        } catch (const std::exception& e) {                           \
            std::cout << "FAILED (" << e.what() << ")\n";             \
            tests_failed++;                                           \
        } catch (...) {                                               \
            std::cout << "FAILED (unknown exception)\n";              \
            tests_failed++;                                           \
        }                                                             \
    } while (0) /* NOLINT(cppcoreguidelines-avoid-do-while) */

#define EXPECT_TRUE(a)                                               \
    do { /* NOLINT(cppcoreguidelines-avoid-do-while) */      \
        if (!(a)) {                                                  \
            throw std::runtime_error("Condition failed: " #a);       \
        }                                                            \
    } while (0) /* NOLINT(cppcoreguidelines-avoid-do-while) */

#define EXPECT_FALSE(a) EXPECT_TRUE(!(a))

#define EXPECT_EQ(a, b)                                                                         \
    do { /* NOLINT(cppcoreguidelines-avoid-do-while) */                                 \
        const auto& _a = (a);                                                                   \
        const auto& _b = (b);                                                                   \
        if (_a != _b) {                                                                         \
            throw std::runtime_error("Equality failed: " #a " != " #b);                         \
        }                                                                                       \
    } while (0) /* NOLINT(cppcoreguidelines-avoid-do-while) */

#define EXPECT_GT(a, b)                                                                         \
    do { /* NOLINT(cppcoreguidelines-avoid-do-while) */                                 \
        const auto& _a = (a);                                                                   \
        const auto& _b = (b);                                                                   \
        if (!(_a > _b)) {                                                                       \
            throw std::runtime_error("Greater-than failed: " #a " > " #b);                      \
        }                                                                                       \
    } while (0) /* NOLINT(cppcoreguidelines-avoid-do-while) */

#define EXPECT_LT(a, b)                                                                         \
    do { /* NOLINT(cppcoreguidelines-avoid-do-while) */                                 \
        const auto& _a = (a);                                                                   \
        const auto& _b = (b);                                                                   \
        if (!(_a < _b)) {                                                                       \
            throw std::runtime_error("Less-than failed: " #a " < " #b);                         \
        }                                                                                       \
    } while (0) /* NOLINT(cppcoreguidelines-avoid-do-while) */

#define EXPECT_STREQ(a, b)                                                                      \
    do { /* NOLINT(cppcoreguidelines-avoid-do-while) */                                 \
        if (std::string(a) != std::string(b)) {                                                 \
            throw std::runtime_error("String equality failed: " #a " != " #b);                  \
        }                                                                                       \
    } while (0) /* NOLINT(cppcoreguidelines-avoid-do-while) */

#define EXPECT_DOUBLE_EQ(a, b)                                                                  \
    do { /* NOLINT(cppcoreguidelines-avoid-do-while) */                                 \
        const double _a = (a);                                                                  \
        const double _b = (b);                                                                  \
        const double diff = _a - _b;                                                            \
        const double abs_diff = (diff < 0) ? -diff : diff;                                      \
        if (abs_diff > 1e-9) {                                                                  \
            throw std::runtime_error("Double equality failed: " #a " != " #b);                  \
        }                                                                                       \
    } while (0) /* NOLINT(cppcoreguidelines-avoid-do-while) */

/* Helper for comparing raw pointers without triggering integer conversion warnings */
#define EXPECT_PTR_EQ(a, b)                                                                     \
    do { /* NOLINT(cppcoreguidelines-avoid-do-while) */                                 \
        const void* const _a = static_cast<const void*>(a);                                     \
        const void* const _b = static_cast<const void*>(b);                                     \
        if (_a != _b) {                                                                         \
            throw std::runtime_error("Pointer equality failed: " #a " != " #b);                 \
        }                                                                                       \
    } while (0) /* NOLINT(cppcoreguidelines-avoid-do-while) */

} // namespace cloudsql::tests

#endif // SQL_ENGINE_TEST_UTILS_HPP
