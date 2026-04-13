/**
 * @file bloom_filter.hpp
 * @brief Bloom filter implementation for distributed join optimization
 */

#ifndef SQL_ENGINE_COMMON_BLOOM_FILTER_HPP
#define SQL_ENGINE_COMMON_BLOOM_FILTER_HPP

#include <cstdint>
#include <cstring>
#include <vector>

#include "value.hpp"

namespace cloudsql {
namespace common {

/**
 * @brief Bloom filter for probabilistic membership testing
 *
 * Used in distributed joins to filter tuples that cannot possibly
 * match before network transmission.
 */
class BloomFilter {
   public:
    /**
     * @brief Construct a bloom filter with expected elements and false positive rate
     * @param expected_elements Number of elements expected to be inserted
     * @param false_positive_rate Target false positive rate (default 0.01 = 1%)
     */
    explicit BloomFilter(size_t expected_elements, double false_positive_rate = 0.01);

    /**
     * @brief Construct from serialized data
     */
    BloomFilter(const uint8_t* data, size_t size);

    /**
     * @brief Insert a value into the bloom filter
     */
    void insert(const Value& key);

    /**
     * @brief Check if a value might be in the bloom filter
     * @return true if possibly present, false if definitely not present
     */
    [[nodiscard]] bool might_contain(const Value& key) const;

    /**
     * @brief Serialize the bloom filter for network transmission
     */
    [[nodiscard]] std::vector<uint8_t> serialize() const;

    /**
     * @brief Get the bit array size in bytes
     */
    [[nodiscard]] size_t bit_size() const { return (num_bits_ + 7) / 8; }

    /**
     * @brief Get number of hash functions used
     */
    [[nodiscard]] size_t num_hashes() const { return num_hashes_; }

    /**
     * @brief Get expected elements
     */
    [[nodiscard]] size_t expected_elements() const { return expected_elements_; }

   private:
    size_t num_bits_;
    size_t num_hashes_;
    size_t expected_elements_;
    std::vector<uint8_t> bits_;

    size_t get_bit_position(size_t hash, size_t i) const;
    size_t murmur3_hash(const Value& key) const;
    size_t murmur3_hash(const uint8_t* data, size_t len, size_t seed) const;
};

}  // namespace common
}  // namespace cloudsql

#endif  // SQL_ENGINE_COMMON_BLOOM_FILTER_HPP