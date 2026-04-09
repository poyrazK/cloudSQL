/**
 * @file arena_allocator.hpp
 * @brief High-performance bump allocator for execution-scoped data
 */

#ifndef CLOUDSQL_COMMON_ARENA_ALLOCATOR_HPP
#define CLOUDSQL_COMMON_ARENA_ALLOCATOR_HPP

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <vector>

namespace cloudsql::common {

/**
 * @class ArenaAllocator
 * @brief Manages memory chunks and provides fast, contiguous allocations.
 *
 * Implements std::pmr::memory_resource for compatibility with standard
 * containers like std::pmr::vector.
 */
class ArenaAllocator : public std::pmr::memory_resource {
   public:
    static constexpr size_t DEFAULT_CHUNK_SIZE = 65536;  // 64KB

    explicit ArenaAllocator(size_t chunk_size = DEFAULT_CHUNK_SIZE)
        : chunk_size_(chunk_size), current_chunk_idx_(0), current_offset_(0) {}

    ~ArenaAllocator() override {
        for (auto* chunk : chunks_) {
            delete[] chunk;
        }
    }

    // Disable copy
    ArenaAllocator(const ArenaAllocator&) = delete;
    ArenaAllocator& operator=(const ArenaAllocator&) = delete;

    /**
     * @brief Reset the arena, reclaiming all memory for reuse.
     *
     * Keeps all allocated chunks but resets pointers so they can be overwritten.
     * This is an O(1) or O(N_chunks) operation with zero heap overhead.
     */
    void reset() {
        current_chunk_idx_ = 0;
        current_offset_ = 0;
    }

   protected:
    /**
     * @brief Internal allocation logic for PMR
     */
    void* do_allocate(size_t bytes, size_t alignment) override {
        if (bytes == 0) return nullptr;

        // Align the offset
        size_t mask = alignment - 1;

        // Try current chunk
        if (current_chunk_idx_ < chunks_.size()) {
            size_t aligned_offset = (current_offset_ + mask) & ~mask;
            if (aligned_offset + bytes <= chunk_size_) {
                void* result = chunks_[current_chunk_idx_] + aligned_offset;
                current_offset_ = aligned_offset + bytes;
                return result;
            }

            // Move to next existing chunk if possible
            current_chunk_idx_++;
            current_offset_ = 0;
            return do_allocate(bytes, alignment);
        }

        // Need a new chunk
        if (bytes > chunk_size_) {
            auto* large_chunk = new uint8_t[bytes];
            chunks_.push_back(large_chunk);
            // We don't make this the "current" chunk for small allocations
            // to avoid wasting space. We just return it.
            return large_chunk;
        }

        allocate_new_chunk();
        return do_allocate(bytes, alignment);
    }

    /**
     * @brief PMR deallocate is a no-op for bump allocators (we reset the whole arena)
     */
    void do_deallocate(void* p, size_t bytes, size_t alignment) override {
        // No-op
        (void)p;
        (void)bytes;
        (void)alignment;
    }

    bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override {
        return this == &other;
    }

   private:
    void allocate_new_chunk() {
        chunks_.push_back(new uint8_t[chunk_size_]);
        // Don't change current_chunk_idx_ here, let the recursive call handle it
    }

    size_t chunk_size_;
    std::vector<uint8_t*> chunks_;
    size_t current_chunk_idx_;
    size_t current_offset_;
};

}  // namespace cloudsql::common

#endif  // CLOUDSQL_COMMON_ARENA_ALLOCATOR_HPP
