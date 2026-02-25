/**
 * @file lru_replacer.hpp
 * @brief Least Recently Used (LRU) tracking for buffer pool
 */

#ifndef CLOUDSQL_STORAGE_LRU_REPLACER_HPP
#define CLOUDSQL_STORAGE_LRU_REPLACER_HPP

#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace cloudsql::storage {

/**
 * @class LRUReplacer
 * @brief Tracks page usage and determines which page to evict
 *
 * Implements a thread-safe LRU policy. Pages that are pinned are
 * removed from the replacer. When unpinned, they are added back.
 */
class LRUReplacer {
   public:
    /**
     * @brief Create a new LRUReplacer
     * @param num_pages Maximum number of pages the replacer will track
     */
    explicit LRUReplacer(size_t num_pages);

    ~LRUReplacer() = default;

    // Disable copy/move
    LRUReplacer(const LRUReplacer&) = delete;
    LRUReplacer& operator=(const LRUReplacer&) = delete;
    LRUReplacer(LRUReplacer&&) = delete;
    LRUReplacer& operator=(LRUReplacer&&) = delete;

    /**
     * @brief Find the least recently used frame and remove it from the replacer
     * @param[out] frame_id The ID of the evicted frame
     * @return true if a frame was evicted, false if no frames are available
     */
    bool victim(uint32_t* frame_id);

    /**
     * @brief Pin a frame, removing it from the replacer
     * @param frame_id The ID of the frame to pin
     */
    void pin(uint32_t frame_id);

    /**
     * @brief Unpin a frame, adding it to the replacer
     * @param frame_id The ID of the frame to unpin (becomes a candidate for eviction)
     */
    void unpin(uint32_t frame_id);

    /**
     * @brief Get the number of frames currently in the replacer
     * @return Size of the replacer
     */
    [[nodiscard]] size_t size() const;

   private:
    size_t capacity_;
    mutable std::mutex latch_;
    std::list<uint32_t> lru_list_;
    std::unordered_map<uint32_t, std::list<uint32_t>::iterator> lru_map_;
};

}  // namespace cloudsql::storage

#endif  // CLOUDSQL_STORAGE_LRU_REPLACER_HPP
