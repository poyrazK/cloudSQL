/**
 * @file bloom_filter.cpp
 * @brief Bloom filter implementation
 */

#include "common/bloom_filter.hpp"

#include <cmath>
#include <cstdint>

#if defined(__APPLE__)
#include <libkern/OSByteOrder.h>
#define bswap64(x) OSSwapInt64(x)
#else
#include <endian.h>
#define bswap64(x) __builtin_bswap64(x)
#endif

namespace cloudsql::common {

BloomFilter::BloomFilter(size_t expected_elements, double false_positive_rate)
    : expected_elements_(expected_elements) {
    // Handle zero expected_elements as empty filter
    if (expected_elements == 0) {
        num_bits_ = 0;
        num_hashes_ = 0;
        return;
    }

    // Clamp false_positive_rate to safe range [0.001, 0.99]
    double p = false_positive_rate;
    if (p <= 0.0 || p >= 1.0) {
        p = 0.01;  // Safe default
    }

    // m = -n * ln(p) / (ln(2)^2)
    // k = m/n * ln(2)
    double n = static_cast<double>(expected_elements);

    double m = -n * std::log(p) / (std::log(2) * std::log(2));
    double k = (m / n) * std::log(2);

    num_bits_ = static_cast<size_t>(std::ceil(m));
    num_hashes_ = static_cast<size_t>(std::ceil(k));

    // Ensure minimum sizes
    if (num_bits_ < 64) {
        num_bits_ = 64;
    }
    if (num_hashes_ < 2) {
        num_hashes_ = 2;
    }
    if (num_hashes_ > 16) {
        num_hashes_ = 16;  // Cap for performance
    }

    bits_.resize((num_bits_ + 7) / 8, 0);
}

BloomFilter::BloomFilter(const uint8_t* data, size_t size) {
    // Minimum size: 3 x uint64_t header + at least 1 byte of bits
    if (size < sizeof(uint64_t) * 3 + 1) {
        return;  // Invalid data
    }

    size_t offset = 0;

    // Read with fixed-width uint64_t and proper byte-order conversion
    uint64_t tmp_num_bits = 0;
    std::memcpy(&tmp_num_bits, data + offset, sizeof(uint64_t));
    tmp_num_bits = bswap64(tmp_num_bits);
    num_bits_ = static_cast<size_t>(tmp_num_bits);
    offset += sizeof(uint64_t);

    uint64_t tmp_num_hashes = 0;
    std::memcpy(&tmp_num_hashes, data + offset, sizeof(uint64_t));
    tmp_num_hashes = bswap64(tmp_num_hashes);
    num_hashes_ = static_cast<size_t>(tmp_num_hashes);
    offset += sizeof(uint64_t);

    uint64_t tmp_expected = 0;
    std::memcpy(&tmp_expected, data + offset, sizeof(uint64_t));
    tmp_expected = bswap64(tmp_expected);
    expected_elements_ = static_cast<size_t>(tmp_expected);
    offset += sizeof(uint64_t);

    // Validate header fields before using them
    constexpr size_t MAX_BITS = (1ULL << 40);      // ~1TB max, reasonable upper bound
    constexpr size_t MAX_HASHES = 64;              // reasonable upper bound
    constexpr size_t MAX_EXPECTED = (1ULL << 30);  // ~1B elements max

    if (num_bits_ == 0 || num_bits_ > MAX_BITS) {
        num_bits_ = 0;
        num_hashes_ = 0;
        expected_elements_ = 0;
        bits_.clear();
        return;
    }
    if (num_hashes_ > MAX_HASHES) {
        num_bits_ = 0;
        num_hashes_ = 0;
        expected_elements_ = 0;
        bits_.clear();
        return;
    }
    if (expected_elements_ > MAX_EXPECTED) {
        num_bits_ = 0;
        num_hashes_ = 0;
        expected_elements_ = 0;
        bits_.clear();
        return;
    }

    // Validate bit array size and overflow safety
    size_t bit_bytes = 0;
    if (num_bits_ > (SIZE_MAX - 7) / 8) {
        num_bits_ = 0;
        num_hashes_ = 0;
        expected_elements_ = 0;
        bits_.clear();
        return;
    }
    bit_bytes = (num_bits_ + 7) / 8;

    // Check that bit_bytes fits in remaining payload
    if (bit_bytes > size || offset > size || bit_bytes > size - offset) {
        num_bits_ = 0;
        num_hashes_ = 0;
        expected_elements_ = 0;
        bits_.clear();
        return;
    }

    bits_.resize(bit_bytes);
    std::memcpy(bits_.data(), data + offset, bit_bytes);
}

size_t BloomFilter::murmur3_hash(const Value& key) const {
    std::string s = key.to_string();
    return murmur3_hash(reinterpret_cast<const uint8_t*>(s.data()), s.size(), 0xdeadbeef);
}

size_t BloomFilter::murmur3_hash(const uint8_t* data, size_t len, size_t seed) const {
    // MurmurHash3 32-bit finalizer
    size_t h = seed ^ (len * 0x9e3779b9U);
    h ^= h >> 16;
    h *= 0x85ebca6bU;
    h ^= h >> 13;
    h *= 0xc2b2ae35U;
    h ^= h >> 16;

    // Mix in the data
    for (size_t i = 0; i < len; ++i) {
        h ^= data[i];
        h *= 0x9e3779b9U;
        h ^= h >> 15;
    }

    return h;
}

size_t BloomFilter::get_bit_position(size_t hash, size_t i) const {
    // Double hashing technique: h(i) = h1 + i * h2
    // Make h2 key-dependent by rehashing the input hash with a different seed
    size_t h1 = hash;
    size_t h2 = murmur3_hash(reinterpret_cast<const uint8_t*>(&hash), sizeof(hash), 0xcafebabe);

    // Ensure h2 is non-zero to avoid degenerate probing
    if (h2 == 0) {
        h2 = 1;
    }

    return (h1 + i * h2) % num_bits_;
}

void BloomFilter::insert(const Value& key) {
    if (num_bits_ == 0) return;  // Empty filter

    size_t base_hash = murmur3_hash(key);

    for (size_t i = 0; i < num_hashes_; ++i) {
        size_t bit_pos = get_bit_position(base_hash, i);
        size_t byte_idx = bit_pos / 8;
        size_t bit_offset = bit_pos % 8;
        bits_[byte_idx] |= (1 << bit_offset);
    }
}

bool BloomFilter::might_contain(const Value& key) const {
    if (num_bits_ == 0) return false;  // Empty filter

    size_t base_hash = murmur3_hash(key);

    for (size_t i = 0; i < num_hashes_; ++i) {
        size_t bit_pos = get_bit_position(base_hash, i);
        size_t byte_idx = bit_pos / 8;
        size_t bit_offset = bit_pos % 8;

        if ((bits_[byte_idx] & (1 << bit_offset)) == 0) {
            return false;
        }
    }

    return true;
}

std::vector<uint8_t> BloomFilter::serialize() const {
    std::vector<uint8_t> out;

    // Store metadata using fixed-width uint64_t with byte-order conversion
    out.resize(sizeof(uint64_t) * 3);
    size_t offset = 0;

    uint64_t tmp_num_bits = bswap64(static_cast<uint64_t>(num_bits_));
    std::memcpy(out.data() + offset, &tmp_num_bits, sizeof(uint64_t));
    offset += sizeof(uint64_t);

    uint64_t tmp_num_hashes = bswap64(static_cast<uint64_t>(num_hashes_));
    std::memcpy(out.data() + offset, &tmp_num_hashes, sizeof(uint64_t));
    offset += sizeof(uint64_t);

    uint64_t tmp_expected = bswap64(static_cast<uint64_t>(expected_elements_));
    std::memcpy(out.data() + offset, &tmp_expected, sizeof(uint64_t));
    offset += sizeof(uint64_t);

    // Store bits
    size_t bit_bytes = (num_bits_ + 7) / 8;
    out.insert(out.end(), bits_.begin(), bits_.end());

    return out;
}

}  // namespace cloudsql::common