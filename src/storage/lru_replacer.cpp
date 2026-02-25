/**
 * @file lru_replacer.cpp
 * @brief Implementation of the LRU Replacer
 */

#include "storage/lru_replacer.hpp"

namespace cloudsql::storage {

LRUReplacer::LRUReplacer(size_t num_pages) : capacity_(num_pages){}

bool LRUReplacer::victim(uint32_t* frame_id) {
    std::lock_guard<std::mutex> lock(latch_);
    if (lru_list_.empty()) {
        return false;
    }

    // The back of the list is the least recently used
    *frame_id = lru_list_.back();
    lru_map_.erase(*frame_id);
    lru_list_.pop_back();

    return true;
}

void LRUReplacer::pin(uint32_t frame_id) {
    std::lock_guard<std::mutex> lock(latch_);
    auto it = lru_map_.find(frame_id);
    if (it != lru_map_.end()) {
        // Remove it from the tracker because it's currently pinned/in-use
        lru_list_.erase(it->second);
        lru_map_.erase(it);
    }
}

void LRUReplacer::unpin(uint32_t frame_id) {
    std::lock_guard<std::mutex> lock(latch_);
    if (lru_map_.find(frame_id) != lru_map_.end()) {
        // Already in the replacer's candidate list
        return;
    }

    // Add to the front of the list (most recently used)
    lru_list_.push_front(frame_id);
    lru_map_[frame_id] = lru_list_.begin();

    // Enforce capacity constraint (shouldn't happen strictly if used with BufferPool properly, but
    // safe)
    if (lru_list_.size() > capacity_) {
        uint32_t lru_frame = lru_list_.back();
        lru_map_.erase(lru_frame);
        lru_list_.pop_back();
    }
}

size_t LRUReplacer::size() const {
    std::lock_guard<std::mutex> lock(latch_);
    return lru_list_.size();
}

}  // namespace cloudsql::storage
