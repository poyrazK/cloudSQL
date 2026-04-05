/**
 * @file lru_replacer.cpp
 * @brief Least Recently Used (LRU) tracking implementation using CLOCK algorithm
 */

#include "storage/lru_replacer.hpp"

#include <cstddef>
#include <cstdint>
#include <mutex>

namespace cloudsql::storage {

LRUReplacer::LRUReplacer(size_t num_pages)
    : capacity_(num_pages),
      in_replacer_(num_pages, false),
      referenced_(num_pages, false),
      clock_hand_(0),
      current_size_(0) {}

bool LRUReplacer::victim(uint32_t* frame_id) {
    const std::scoped_lock<std::mutex> lock(latch_);

    if (current_size_ == 0) {
        return false;
    }

    while (true) {
        if (in_replacer_[clock_hand_]) {
            if (referenced_[clock_hand_]) {
                referenced_[clock_hand_] = false;
            } else {
                // Found a victim
                in_replacer_[clock_hand_] = false;
                *frame_id = static_cast<uint32_t>(clock_hand_);
                current_size_--;

                // Move hand forward before returning
                clock_hand_ = (clock_hand_ + 1) % capacity_;
                return true;
            }
        }
        clock_hand_ = (clock_hand_ + 1) % capacity_;
    }
}

void LRUReplacer::pin(uint32_t frame_id) {
    const std::scoped_lock<std::mutex> lock(latch_);

    if (frame_id >= capacity_) {
        return;
    }

    if (in_replacer_[frame_id]) {
        in_replacer_[frame_id] = false;
        current_size_--;
    }
}

void LRUReplacer::unpin(uint32_t frame_id) {
    const std::scoped_lock<std::mutex> lock(latch_);

    if (frame_id >= capacity_) {
        return;
    }

    if (!in_replacer_[frame_id]) {
        in_replacer_[frame_id] = true;
        referenced_[frame_id] = true;
        current_size_++;
    }
}

size_t LRUReplacer::size() const {
    const std::scoped_lock<std::mutex> lock(latch_);
    return current_size_;
}

}  // namespace cloudsql::storage
