/**
 * @file utils_coverage_tests.cpp
 * @brief Targeted unit tests to increase coverage of Common Values and Network Client
 */

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "common/value.hpp"
#include "network/rpc_client.hpp"

using namespace cloudsql::common;
using namespace cloudsql::network;

namespace {

/**
 * @brief Tests Value accessor failure paths and boundary conversions.
 */
TEST(UtilsCoverageTests, ValueEdgeCases) {
    // 1. Accessor failure paths
    Value v_int(ValueType::TYPE_INT32);  // Default 0
    EXPECT_THROW(v_int.as_bool(), std::runtime_error);
    EXPECT_THROW(v_int.as_float64(), std::runtime_error);
    EXPECT_THROW(v_int.as_text(), std::runtime_error);

    Value v_bool(true);
    EXPECT_THROW(v_bool.as_int64(), std::runtime_error);

    Value v_text("123");
    EXPECT_THROW(v_text.as_int32(), std::runtime_error);

    // 2. boundary to_* conversions
    EXPECT_EQ(v_text.to_int64(), 0);  // Text doesn't auto-convert to int in to_int64()
    EXPECT_EQ(v_text.to_float64(), 0.0);

    Value v_null = Value::make_null();
    EXPECT_EQ(v_null.to_int64(), 0);
    EXPECT_EQ(v_null.to_float64(), 0.0);
    EXPECT_STREQ(v_null.to_string().c_str(), "NULL");

    Value v_f(1.23);
    EXPECT_EQ(v_f.to_int64(), 1);

    // 3. Numeric check
    EXPECT_TRUE(v_int.is_numeric());
    EXPECT_TRUE(v_f.is_numeric());
    EXPECT_FALSE(v_text.is_numeric());
    EXPECT_FALSE(v_bool.is_numeric());
}

/**
 * @brief Tests complex Value comparisons including mixed types and NULLs.
 */
TEST(UtilsCoverageTests, ValueComparisons) {
    Value v_null = Value::make_null();
    Value v_int(10);
    Value v_float(10.0);
    Value v_text("A");

    // Equality
    EXPECT_TRUE(v_int == v_float);  // Numeric equality
    EXPECT_FALSE(v_int == v_text);
    EXPECT_FALSE(v_int == v_null);

    // Less than
    EXPECT_FALSE(v_null < v_int);  // NULL is not less than anything
    EXPECT_TRUE(v_int < v_null);   // non-NULL is less than NULL (by convention in this impl)

    Value v_int_small(5);
    EXPECT_TRUE(v_int_small < v_float);
    EXPECT_FALSE(v_float < v_int_small);

    Value v_text_b("B");
    EXPECT_TRUE(v_text < v_text_b);

    // Mixed numeric/non-numeric comparison
    EXPECT_FALSE(v_int < v_text);
}

/**
 * @brief Tests RpcClient behavior on connection failures.
 */
TEST(UtilsCoverageTests, RpcClientFailure) {
    // Attempt to connect to an unreachable port on localhost
    RpcClient client("127.0.0.1", 1);  // Port 1 is usually privileged and closed

    EXPECT_FALSE(client.connect());
    EXPECT_FALSE(client.is_connected());

    std::vector<uint8_t> resp;
    EXPECT_FALSE(client.call(RpcType::AppendEntries, {1, 2, 3}, resp));
    EXPECT_FALSE(client.send_only(RpcType::AppendEntries, {1, 2, 3}));
}

/**
 * @brief Tests Value Hashing.
 */
TEST(UtilsCoverageTests, ValueHash) {
    Value::Hash hasher;
    Value v1(10);
    Value v2(10);
    Value v3("10");
    Value v_null = Value::make_null();

    EXPECT_EQ(hasher(v1), hasher(v2));
    EXPECT_NE(hasher(v1), hasher(v3));
    EXPECT_NE(hasher(v1), hasher(v_null));
}

}  // namespace
