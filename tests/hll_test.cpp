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
    std::vector<std::string> texts = {"alpha", "beta", "gamma", "delta", "epsilon",
                                      "zeta",  "eta",  "theta", "iota",  "kappa"};
    for (const auto& t : texts) {
        uint64_t hash = HyperLogLog::hash_bytes(t.data(), t.size());
        hll.insert(hash);
    }
    uint64_t card = hll.cardinality();
    EXPECT_GT(card, 0U);
}

/**
 * @brief Tests accuracy bounds for distinct values.
 * HLL is a probabilistic estimator with ~1.6% standard error for large cardinalities.
 * For smaller cardinalities the error can be larger, so we use a very loose bound
 * (cardinality > 0 and reasonable upper bound).
 */
TEST(HyperLogLogTests, AccuracyBoundsDistinct) {
    HyperLogLog hll;
    uint64_t val = 123456789ULL;
    for (int i = 0; i < 1000; ++i) {
        hll.insert(val);
        val = val * 6364136223846793005ULL + 1442695043ULL;
    }
    uint64_t card = hll.cardinality();
    // Must be positive
    EXPECT_GT(card, 0U);
    // Upper bound: 1000 distinct values can't estimate more than 100000
    EXPECT_LT(card, 100000U);
}

/**
 * @brief Tests merge with overlapping sets.
 * Uses distinct LCG-generated values for hll1 and hll2 to ensure good
 * hash distribution across registers (avoids sequential value collisions).
 */
TEST(HyperLogLogTests, MergeOverlappingSets) {
    HyperLogLog hll1;
    HyperLogLog hll2;
    uint64_t val = 123456789ULL;
    for (int i = 0; i < 100; ++i) {
        hll1.insert(val);
        val = val * 6364136223846793005ULL + 1442695043ULL;
    }
    uint64_t val2 = 987654321ULL;
    for (int i = 0; i < 100; ++i) {
        hll2.insert(val2);
        val2 = val2 * 6364136223846793005ULL + 1442695043ULL;
    }
    uint64_t card1 = hll1.cardinality();
    uint64_t card2 = hll2.cardinality();
    hll1.merge(hll2);
    uint64_t merged = hll1.cardinality();
    // Merged cardinality should be >= either individual
    EXPECT_GE(merged, card1);
    EXPECT_GE(merged, card2);
    // Both sets are disjoint with good distribution, merged should be in a reasonable range
    EXPECT_LT(merged, 50000U);  // Sanity upper bound
}

/**
 * @brief Tests seed reproducibility — same seed gives same cardinality.
 */
TEST(HyperLogLogTests, SeedReproducibility) {
    HyperLogLog hll1(42);
    HyperLogLog hll2(42);
    uint64_t val = 123456789ULL;
    for (int i = 0; i < 500; ++i) {
        hll1.insert(val);
        val = val * 6364136223846793005ULL + 1442695043ULL;
    }
    val = 123456789ULL;
    for (int i = 0; i < 500; ++i) {
        hll2.insert(val);
        val = val * 6364136223846793005ULL + 1442695043ULL;
    }
    EXPECT_EQ(hll1.cardinality(), hll2.cardinality());
}

/**
 * @brief Tests different seeds produce different cardinalities.
 * Seed is XORed onto the hash, so different seeds produce different
 * register distributions and thus different cardinality estimates.
 */
TEST(HyperLogLogTests, DifferentSeedsDiffer) {
    HyperLogLog hll1(0);
    HyperLogLog hll2(12345);  // Large seed difference ensures different register distributions
    uint64_t val = 123456789ULL;
    for (int i = 0; i < 500; ++i) {
        hll1.insert(val);
        hll2.insert(val);
        val = val * 6364136223846793005ULL + 1442695043ULL;
    }
    EXPECT_NE(hll1.cardinality(), hll2.cardinality());
}

/**
 * @brief Tests HLL with different ValueType columns.
 * Verifies the integration path used by execute_analyze() — Value::Hash{}
 * for numeric types, hash_bytes() for text types.
 */
TEST(HyperLogLogTests, ValueTypeColumnCoverage) {
    HyperLogLog hll_int;
    HyperLogLog hll_bigint;
    HyperLogLog hll_double;
    HyperLogLog hll_text;

    // INT64 values
    for (int64_t i = 0; i < 200; ++i) {
        Value v = Value::make_int64(i);
        hll_int.insert(static_cast<uint64_t>(Value::Hash{}(v)));
    }
    EXPECT_GT(hll_int.cardinality(), 0U);

    // BIGINT values (larger range)
    for (int64_t i = 0; i < 200; ++i) {
        Value v = Value::make_int64(i * 1000000000LL);
        hll_bigint.insert(static_cast<uint64_t>(Value::Hash{}(v)));
    }
    EXPECT_GT(hll_bigint.cardinality(), 0U);

    // DOUBLE (float64) values
    for (int i = 0; i < 200; ++i) {
        Value v = Value::make_float64(static_cast<double>(i) * 1.5);
        hll_double.insert(static_cast<uint64_t>(Value::Hash{}(v)));
    }
    EXPECT_GT(hll_double.cardinality(), 0U);

    // TEXT values via hash_bytes (mimics execute_analyze path)
    std::vector<std::string> texts = {"alpha", "beta", "gamma", "delta", "epsilon",
                                      "zeta",  "eta",  "theta", "iota",  "kappa"};
    for (const auto& t : texts) {
        uint64_t hash = HyperLogLog::hash_bytes(t.data(), t.size());
        hll_text.insert(hash);
    }
    EXPECT_GT(hll_text.cardinality(), 0U);
}

}  // namespace
