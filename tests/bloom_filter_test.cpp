/**
 * @file bloom_filter_test.cpp
 * @brief Unit tests for BloomFilter implementation
 */

#include "common/bloom_filter.hpp"

#include <gtest/gtest.h>

#include <vector>

#include "common/cluster_manager.hpp"
#include "common/value.hpp"
#include "executor/types.hpp"
#include "network/rpc_message.hpp"

using namespace cloudsql::common;
using namespace cloudsql::network;
using namespace cloudsql::cluster;

namespace {

/**
 * @brief Tests basic bloom filter insertion and membership.
 */
TEST(BloomFilterTests, BasicInsertAndQuery) {
    BloomFilter bf(100);  // Expect 100 elements

    Value v1 = Value::make_int64(42);
    Value v2 = Value::make_int64(100);
    Value v3 = Value::make_text("hello");

    bf.insert(v1);
    bf.insert(v2);
    bf.insert(v3);

    // All inserted values should be found
    EXPECT_TRUE(bf.might_contain(v1));
    EXPECT_TRUE(bf.might_contain(v2));
    EXPECT_TRUE(bf.might_contain(v3));

    // Non-inserted values might or might not be found (false positive possible)
    // But with 100 elements in a properly sized filter, probability is low
}

/**
 * @brief Tests that values not inserted return false.
 */
TEST(BloomFilterTests, NonInsertedValues) {
    BloomFilter bf(1000);  // Large filter, low false positive rate

    Value v1 = Value::make_int64(999);
    Value v2 = Value::make_text("nonexistent");

    // Not inserted, should definitely not be found
    EXPECT_FALSE(bf.might_contain(v1));
    EXPECT_FALSE(bf.might_contain(v2));
}

/**
 * @brief Tests serialization and deserialization.
 */
TEST(BloomFilterTests, SerializationRoundTrip) {
    BloomFilter bf(50);

    // Insert some values
    for (int i = 0; i < 25; ++i) {
        bf.insert(Value::make_int64(i));
    }
    for (int i = 100; i < 125; ++i) {
        bf.insert(Value::make_text("text_" + std::to_string(i)));
    }

    // Serialize
    std::vector<uint8_t> data = bf.serialize();
    EXPECT_FALSE(data.empty());

    // Deserialize
    BloomFilter bf2(data.data(), data.size());

    // Check metadata
    EXPECT_EQ(bf.num_hashes(), bf2.num_hashes());

    // Check inserted values are found
    for (int i = 0; i < 25; ++i) {
        EXPECT_TRUE(bf2.might_contain(Value::make_int64(i)));
    }
    for (int i = 100; i < 125; ++i) {
        EXPECT_TRUE(bf2.might_contain(Value::make_text("text_" + std::to_string(i))));
    }
}

/**
 * @brief Tests false positive rate with many insertions.
 */
TEST(BloomFilterTests, FalsePositiveRate) {
    BloomFilter bf(1000);  // 1000 expected elements

    // Insert 500 values
    for (int i = 0; i < 500; ++i) {
        bf.insert(Value::make_int64(i));
    }

    // Check 1000 non-inserted values and count false positives
    int false_positives = 0;
    for (int i = 500; i < 1500; ++i) {
        if (bf.might_contain(Value::make_int64(i))) {
            ++false_positives;
        }
    }

    // With 1% target FPR, we expect roughly 10 false positives out of 1000
    // Allow some margin - shouldn't be more than 5% (50)
    EXPECT_LT(false_positives, 50);
}

/**
 * @brief Tests empty bloom filter.
 */
TEST(BloomFilterTests, EmptyFilter) {
    BloomFilter bf(1);  // Minimal filter

    // Nothing inserted, nothing should be found
    EXPECT_FALSE(bf.might_contain(Value::make_int64(1)));
    EXPECT_FALSE(bf.might_contain(Value::make_text("test")));
}

/**
 * @brief Tests that duplicate insertions don't cause issues.
 */
TEST(BloomFilterTests, DuplicateInsertions) {
    BloomFilter bf(100);

    Value v = Value::make_int64(42);

    bf.insert(v);
    bf.insert(v);
    bf.insert(v);

    // Should still be found
    EXPECT_TRUE(bf.might_contain(v));
}

/**
 * @brief Tests different value types.
 */
TEST(BloomFilterTests, DifferentValueTypes) {
    BloomFilter bf(1000);  // Large filter to minimize false positives

    bf.insert(Value::make_int64(1));
    bf.insert(Value::make_int64(2));
    bf.insert(Value::make_float64(3.14));
    bf.insert(Value::make_text("string"));
    bf.insert(Value::make_bool(true));

    // Verify no-false-negative: inserted values must be found
    EXPECT_TRUE(bf.might_contain(Value::make_int64(1)));
    EXPECT_TRUE(bf.might_contain(Value::make_int64(2)));
    EXPECT_TRUE(bf.might_contain(Value::make_float64(3.14)));
    EXPECT_TRUE(bf.might_contain(Value::make_text("string")));
    EXPECT_TRUE(bf.might_contain(Value::make_bool(true)));
}

/**
 * @brief Tests BloomFilterArgs serialization round-trip.
 */
TEST(BloomFilterTests, BloomFilterArgsSerialization) {
    // Create a real bloom filter and use its serialized form
    BloomFilter original(50);
    original.insert(Value::make_int64(10));
    original.insert(Value::make_int64(20));
    original.insert(Value::make_text("hello"));
    std::vector<uint8_t> real_filter_data = original.serialize();

    BloomFilterArgs args;
    args.context_id = "ctx_123";
    args.build_table = "users";
    args.probe_table = "orders";
    args.probe_key_col = "user_id";
    args.filter_data = real_filter_data;
    args.expected_elements = original.expected_elements();
    args.num_hashes = original.num_hashes();

    auto serialized = args.serialize();
    auto deserialized = BloomFilterArgs::deserialize(serialized);

    EXPECT_EQ(args.context_id, deserialized.context_id);
    EXPECT_EQ(args.build_table, deserialized.build_table);
    EXPECT_EQ(args.probe_table, deserialized.probe_table);
    EXPECT_EQ(args.probe_key_col, deserialized.probe_key_col);
    EXPECT_EQ(args.expected_elements, deserialized.expected_elements);
    EXPECT_EQ(args.num_hashes, deserialized.num_hashes);
    ASSERT_EQ(args.filter_data.size(), deserialized.filter_data.size());
    EXPECT_EQ(args.filter_data, deserialized.filter_data);

    // Reconstruct bloom filter from deserialized data and verify it works
    BloomFilter reconstructed(deserialized.filter_data.data(), deserialized.filter_data.size());
    EXPECT_EQ(reconstructed.expected_elements(), original.expected_elements());
    EXPECT_EQ(reconstructed.num_hashes(), original.num_hashes());
    EXPECT_TRUE(reconstructed.might_contain(Value::make_int64(10)));
    EXPECT_TRUE(reconstructed.might_contain(Value::make_int64(20)));
    EXPECT_TRUE(reconstructed.might_contain(Value::make_text("hello")));
}

/**
 * @brief Tests ClusterManager bloom filter storage operations.
 */
TEST(BloomFilterTests, ClusterManagerBloomFilterStorage) {
    ClusterManager cm(nullptr);

    // Create a real bloom filter and serialize it
    BloomFilter original(100);
    original.insert(Value::make_int64(10));
    original.insert(Value::make_int64(20));
    auto filter_data = original.serialize();

    // Test set_bloom_filter and has_bloom_filter
    cm.set_bloom_filter("ctx1", "table_build", "table_probe", "key_col", filter_data,
                        original.expected_elements(), original.num_hashes());
    EXPECT_TRUE(cm.has_bloom_filter("ctx1"));

    // Test get_bloom_filter reconstructs correctly
    auto bf = cm.get_bloom_filter("ctx1");
    EXPECT_EQ(bf.expected_elements(), original.expected_elements());
    EXPECT_EQ(bf.num_hashes(), original.num_hashes());

    // Test that inserted values are found in reconstructed filter
    EXPECT_TRUE(bf.might_contain(Value::make_int64(10)));
    EXPECT_TRUE(bf.might_contain(Value::make_int64(20)));

    // Test non-existent context
    EXPECT_FALSE(cm.has_bloom_filter("nonexistent"));

    // Test get_probe_table and get_probe_key_col
    cm.set_bloom_filter("ctx2", "build_t", "probe_t", "col_x", filter_data, 500, 3);
    EXPECT_EQ(cm.get_probe_table("ctx2"), "probe_t");
    EXPECT_EQ(cm.get_probe_key_col("ctx2"), "col_x");

    // Test clear_bloom_filter
    cm.clear_bloom_filter("ctx1");
    EXPECT_FALSE(cm.has_bloom_filter("ctx1"));
}

/**
 * @brief Tests bloom filter application logic (simulates PushData handler behavior).
 */
TEST(BloomFilterTests, BloomFilterApplicationLogic) {
    // Build bloom filter with known keys
    BloomFilter bf(100);
    bf.insert(Value::make_int64(10));
    bf.insert(Value::make_int64(20));
    bf.insert(Value::make_int64(30));

    // Verify no-false-negative: inserted values must be found via might_contain
    EXPECT_TRUE(bf.might_contain(Value::make_int64(10)));
    EXPECT_TRUE(bf.might_contain(Value::make_int64(20)));
    EXPECT_TRUE(bf.might_contain(Value::make_int64(30)));

    // Simulate tuple filtering (as done in PushData handler)
    std::vector<cloudsql::executor::Tuple> tuples;
    tuples.push_back(
        cloudsql::executor::Tuple(std::initializer_list<Value>{Value::make_int64(10)}));  // match
    tuples.push_back(cloudsql::executor::Tuple(
        std::initializer_list<Value>{Value::make_int64(15)}));  // no match
    tuples.push_back(
        cloudsql::executor::Tuple(std::initializer_list<Value>{Value::make_int64(20)}));  // match
    tuples.push_back(cloudsql::executor::Tuple(
        std::initializer_list<Value>{Value::make_int64(99)}));  // no match

    std::vector<cloudsql::executor::Tuple> filtered;
    for (auto& row : tuples) {
        if (bf.might_contain(row.get(0))) {
            filtered.push_back(std::move(row));
        }
    }

    // Verify found values in filtered list
    bool found_10 = false;
    bool found_20 = false;
    for (auto& row : filtered) {
        if (row.get(0) == Value::make_int64(10)) found_10 = true;
        if (row.get(0) == Value::make_int64(20)) found_20 = true;
    }
    EXPECT_TRUE(found_10);  // Inserted value must be found
    EXPECT_TRUE(found_20);  // Inserted value must be found
}

}  // namespace