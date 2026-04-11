/**
 * @file bloom_filter.cpp
 * @brief Bloom filter implementation
 */

#include "common/bloom_filter.hpp"

#include <cmath>

namespace cloudsql::common {

BloomFilter::BloomFilter(size_t expected_elements, double false_positive_rate)
    : expected_elements_(expected_elements) {
    // m = -n * ln(p) / (ln(2)^2)
    // k = m/n * ln(2)
    double n = static_cast<double>(expected_elements);
    double p = false_positive_rate;

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
    if (size < sizeof(size_t) * 3) {
        return;  // Invalid data
    }

    size_t offset = 0;
    std::memcpy(&num_bits_, data + offset, sizeof(size_t));
    offset += sizeof(size_t);

    std::memcpy(&num_hashes_, data + offset, sizeof(size_t));
    offset += sizeof(size_t);

    std::memcpy(&expected_elements_, data + offset, sizeof(size_t));
    offset += sizeof(size_t);

    size_t bit_bytes = (num_bits_ + 7) / 8;
    if (size >= offset + bit_bytes) {
        bits_.resize(bit_bytes);
        std::memcpy(bits_.data(), data + offset, bit_bytes);
    }
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
    // Use two different hash seeds
    size_t h1 = hash;
    size_t h2 = murmur3_hash(reinterpret_cast<const uint8_t*>("salt"), 4, 0xcafebabe);

    return (h1 + i * h2) % num_bits_;
}

void BloomFilter::insert(const Value& key) {
    size_t base_hash = murmur3_hash(key);

    for (size_t i = 0; i < num_hashes_; ++i) {
        size_t bit_pos = get_bit_position(base_hash, i);
        size_t byte_idx = bit_pos / 8;
        size_t bit_offset = bit_pos % 8;
        bits_[byte_idx] |= (1 << bit_offset);
    }
}

bool BloomFilter::might_contain(const Value& key) const {
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

    // Store metadata
    out.resize(sizeof(size_t) * 3);
    size_t offset = 0;
    std::memcpy(out.data() + offset, &num_bits_, sizeof(size_t));
    offset += sizeof(size_t);

    std::memcpy(out.data() + offset, &num_hashes_, sizeof(size_t));
    offset += sizeof(size_t);

    std::memcpy(out.data() + offset, &expected_elements_, sizeof(size_t));
    offset += sizeof(size_t);

    // Store bits
    size_t bit_bytes = (num_bits_ + 7) / 8;
    out.insert(out.end(), bits_.begin(), bits_.end());

    return out;
}

}  // namespace cloudsql::common