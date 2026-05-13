/**
 * @file hll.hpp
 * @brief HyperLogLog probabilistic cardinality estimator
 */

#pragma once

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>

namespace cloudsql {
namespace common {

/**
 * @brief HyperLogLog — memory-bounded NDV estimator
 *
 * Uses a fixed register array of 2048 bytes (~12KB total) regardless of
 * cardinality. Provides probabilistic cardinality estimates with ~1.6%
 * standard error for cardinalities >> kNumRegisters.
 *
 * Algorithm (Flajolet et al. HyperLogLog):
 * - For each item, hash to 64 bits
 * - Register index: BOTTOM kIndexBits (p=11 for m=2048)
 * - Register value: count of trailing zeros in remaining upper bits + 1
 * - Final cardinality: m * log2(m / sum(2^(-reg_i)))
 *
 * For small cardinalities (<< kNumRegisters), uses linear counting
 * fallback to avoid HLL's systematic overestimation.
 */
class HyperLogLog {
   public:
    static constexpr size_t kNumRegisters = 2048;   // 2^11 for 11-bit index
    static constexpr double kPowBase = 2.0;          // base for 2^(-reg) computation
    static constexpr int kIndexBits = 11;            // bits used for register index

    // Linear counting fallback: when nonzero registers < m / kLinearCountingThreshold,
    // raw HLL formula overestimates severely (e.g., 3 distinct values → 23k estimate).
    // Linear counting: E[n] ≈ -m * log(V/m) where V = empty fraction.
    static constexpr double kLinearCountingThreshold = 20.0;

    // Bias correction: when raw_estimate <= kBiasCorrectionBoundary * m, apply correction.
    // Empirical testing shows HLL systematically overestimates for small cardinalities.
    static constexpr double kBiasCorrectionBoundary = 2.5;

    // Bias adjustment: bias = -0.5 * (m / kBiasAdjustmentFactor).
    static constexpr double kBiasAdjustmentFactor = 10.0;

    /**
     * @brief Construct with optional seed for reproducible hashing
     */
    explicit HyperLogLog(int seed = 0) noexcept : seed_(seed), registers_({}) {}

    /**
     * @brief Insert a pre-hashed 64-bit value
     */
    void insert(uint64_t hash) noexcept {
        hash ^= static_cast<uint64_t>(seed_);

        // Register index from BOTTOM kIndexBits of hash
        int idx = static_cast<int>(hash & (kNumRegisters - 1));

        // Count trailing zeros in the UPPER bits (after index bits)
        // These are the bits from position kIndexBits to 63
        uint64_t remaining = hash >> kIndexBits;
        int zeros = count_trailing_zeros(remaining) + 1;

        // Clamp to uint8_t max
        uint8_t new_val = static_cast<uint8_t>(std::min(zeros, 255));
        registers_.at(idx) = std::max(registers_.at(idx), new_val);
    }

    /**
     * @brief Estimate cardinality using HyperLogLog formula
     */
    [[nodiscard]] uint64_t cardinality() const noexcept {
        double sum = 0.0;
        int nonzero_count = 0;
        for (uint8_t reg : registers_) {
            if (reg != 0) {
                ++nonzero_count;
                sum += std::pow(kPowBase, -static_cast<double>(reg));
            }
        }

        // Empty HLL → cardinality 0
        if (nonzero_count == 0) {
            return 0;
        }

        double m = static_cast<double>(kNumRegisters);
        int empty_count = static_cast<int>(m) - nonzero_count;

        // For sparse data (few registers used), use linear counting to avoid
        // HLL's extreme overestimation. When registers are sparse (nonzero < m/kLinearCountingThreshold),
        // the HLL raw formula gives wildly incorrect results.
        if (nonzero_count < static_cast<int>(m / kLinearCountingThreshold)) {
            // Linear counting: E[n] ≈ -m * log(V/m) where V = empty fraction
            double linear_est = -m * std::log2(static_cast<double>(empty_count) / m);
            return static_cast<uint64_t>(std::max(1.0, linear_est));
        }

        // Standard HLL formula for moderate to large cardinalities
        double raw_estimate = m * std::log2(m / sum);

        // Bias correction for small cardinalities
        double bias = 0.0;
        if (raw_estimate <= kBiasCorrectionBoundary * m) {
            bias = -0.5 * (m / kBiasAdjustmentFactor);
        }

        double estimate = raw_estimate + bias;

        if (estimate < 0) {
            return 0;
        }
        if (estimate > static_cast<double>(kMaxCardinality)) {
            return kMaxCardinality;
        }
        return static_cast<uint64_t>(estimate);
    }

    /**
     * @brief Reset all registers to zero
     */
    void reset() noexcept { registers_.fill(0); }

    /**
     * @brief Merge another HLL into this one (element-wise max of registers)
     */
    void merge(const HyperLogLog& other) noexcept {
        for (size_t i = 0; i < kNumRegisters; ++i) {
            registers_.at(i) = std::max(registers_.at(i), other.registers_.at(i));
        }
    }

    /**
     * @brief Hash a byte buffer to uint64_t (FNV-1a hash)
     *
     * FNV-1a is used instead of djb2 because djb2 doesn't distribute
     * upper bits well for strings with common prefixes.
     */
    [[nodiscard]] static uint64_t hash_bytes(const void* data, size_t len) noexcept {
        static constexpr uint64_t kFnvOffsetBasis = 14695981039346656037ULL;
        static constexpr uint64_t kFnvPrime = 1099511628211ULL;

        const uint8_t* bytes = static_cast<const uint8_t*>(data);
        uint64_t hash = kFnvOffsetBasis;
        for (size_t i = 0; i < len; ++i) {
            hash ^= bytes[i];
            hash *= kFnvPrime;
        }
        return hash;
    }

   private:
    static constexpr uint64_t kMaxCardinality = UINT64_MAX;

    std::array<uint8_t, kNumRegisters> registers_;
    int seed_;

    /**
     * @brief Count trailing zero bits in a 64-bit value
     */
    [[nodiscard]] static int count_trailing_zeros(uint64_t v) noexcept {
        if (v == 0) {
            return 64;
        }
        return __builtin_ctzll(v);
    }
};

}  // namespace common
}  // namespace cloudsql
