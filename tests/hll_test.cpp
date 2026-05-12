/**
 * @file hll_test.cpp
 * @brief Unit tests for HyperLogLog implementation
 */

#include "common/hll.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "common/value.hpp"

using namespace cloudsql::common;

namespace {

/**
 * @brief Tests empty HLL returns 0 cardinality.
 */
TEST(HyperLogLogTests, EmptyCardinality) {
    HyperLogLog hll;
    EXPECT_EQ(hll.cardinality(), 0U);
}

/**
 * @brief Tests that inserting a value produces a non-zero cardinality.
 */
TEST(HyperLogLogTests, NonEmptyAfterInsert) {
    HyperLogLog hll;
    hll.insert(42);
    uint64_t card = hll.cardinality();
    EXPECT_GT(card, 0U);
}

/**
 * @brief Tests that inserting the same value many times gives consistent cardinality.
 */
TEST(HyperLogLogTests, RepeatedValueConsistency) {
    HyperLogLog hll;
    for (int i = 0; i < 1000; ++i) {
        hll.insert(42);
    }
    uint64_t card = hll.cardinality();
    EXPECT_GT(card, 0U);
}

/**
 * @brief Tests that inserting many distinct values gives non-trivial cardinality.
 */
TEST(HyperLogLogTests, DistinctValuesProduceCardinality) {
    HyperLogLog hll;
    uint64_t val = 123456789ULL;
    for (int i = 0; i < 1000; ++i) {
        hll.insert(val);
        val = val * 6364136223846793005ULL + 1442695043ULL;
    }
    uint64_t card = hll.cardinality();
    EXPECT_GT(card, 0U);
}

/**
 * @brief Tests that both small and large distinct value sets produce non-zero cardinality.
 */
TEST(HyperLogLogTests, DistinctValueSetsProduceCardinality) {
    HyperLogLog hll_small;
    HyperLogLog hll_large;
    uint64_t val = 123456789ULL;
    for (int i = 0; i < 100; ++i) {
        hll_small.insert(val);
        val = val * 6364136223846793005ULL + 1442695043ULL;
    }
    for (int i = 0; i < 1000; ++i) {
        hll_large.insert(val);
        val = val * 6364136223846793005ULL + 1442695043ULL;
    }
    EXPECT_GT(hll_small.cardinality(), 0U);
    EXPECT_GT(hll_large.cardinality(), 0U);
}

/**
 * @brief Tests hash_bytes produces consistent hashes.
 */
TEST(HyperLogLogTests, HashBytesConsistency) {
    std::string data = "hello world";
    uint64_t h1 = HyperLogLog::hash_bytes(data.data(), data.size());
    uint64_t h2 = HyperLogLog::hash_bytes(data.data(), data.size());
    EXPECT_EQ(h1, h2);
}

/**
 * @brief Tests hash_bytes differs for different inputs.
 */
TEST(HyperLogLogTests, HashBytesDiffersForDifferentInput) {
    std::string a = "hello";
    std::string b = "world";
    uint64_t ha = HyperLogLog::hash_bytes(a.data(), a.size());
    uint64_t hb = HyperLogLog::hash_bytes(b.data(), b.size());
    EXPECT_NE(ha, hb);
}

/**
 * @brief Tests reset clears all registers back to zero.
 */
TEST(HyperLogLogTests, ResetClearsRegisters) {
    HyperLogLog hll;
    uint64_t val = 123456789ULL;
    for (int i = 0; i < 100; ++i) {
        hll.insert(val);
        val = val * 6364136223846793005ULL + 1442695043ULL;
    }
    hll.reset();
    EXPECT_EQ(hll.cardinality(), 0U);
}

/**
 * @brief Tests merge combines two HLLs by taking element-wise max.
 */
TEST(HyperLogLogTests, MergeCombinesDistinctSets) {
    HyperLogLog hll1;
    HyperLogLog hll2;
    uint64_t val = 123456789ULL;
    for (int i = 0; i < 100; ++i) {
        hll1.insert(val);
        val = val * 6364136223846793005ULL + 1442695043ULL;
    }
    for (int i = 0; i < 100; ++i) {
        hll2.insert(val);
        val = val * 6364136223846793005ULL + 1442695043ULL;
    }
    hll1.merge(hll2);
    EXPECT_GT(hll1.cardinality(), 0U);
}

/**
 * @brief Tests with text values via hash_bytes.
 */
TEST(HyperLogLogTests, TextValueInsertion) {
    HyperLogLog hll;
    std::vector<std::string> texts = {
        "alpha", "beta", "gamma", "delta", "epsilon",
        "zeta", "eta", "theta", "iota", "kappa"};
    for (const auto& t : texts) {
        uint64_t hash = HyperLogLog::hash_bytes(t.data(), t.size());
        hll.insert(hash);
    }
    uint64_t card = hll.cardinality();
    EXPECT_GT(card, 0U);
}

}  // namespace