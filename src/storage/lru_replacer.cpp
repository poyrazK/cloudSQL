/**
 * @file lru_replacer.cpp
 * @brief Least Recently Used (LRU) tracking implementation
 */

#include "storage/lru_replacer.hpp"

#include <cstddef>
#include <cstdint>
#include <mutex>

namespace cloudsql::storage {

LRUReplacer::LRUReplacer(size_t num_pages) : capacity_(num_pages) {}

bool LRUReplacer::victim(uint32_t* frame_id) {
    const std::scoped_lock<std::mutex> lock(latch_);

    if (lru_list_.empty()) {
        return false;
    }

    *frame_id = lru_list_.back();
    lru_list_.pop_back();
    static_cast<void>(lru_map_.erase(*frame_id));
    return true;
}

void LRUReplacer::pin(uint32_t frame_id) {
    const std::scoped_lock<std::mutex> lock(latch_);

    const auto it = lru_map_.find(frame_id);
    if (it != lru_map_.end()) {
        static_cast<void>(lru_list_.erase(it->second));
        static_cast<void>(lru_map_.erase(it));
    }
}

void LRUReplacer::unpin(uint32_t frame_id) {
    const std::scoped_lock<std::mutex> lock(latch_);

    if (lru_map_.count(frame_id) != 0) {
        return;
    }

    if (lru_list_.size() >= capacity_) {
        return;
    }

    lru_list_.push_front(frame_id);
    lru_map_[frame_id] = lru_list_.begin();
}

size_t LRUReplacer::size() const {
    const std::scoped_lock<std::mutex> lock(latch_);
    return lru_list_.size();
}

}  // namespace cloudsql::storage
