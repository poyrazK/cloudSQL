/**
 * @file memory_mapped_column.hpp
 * @brief Memory-mapped column data for zero-copy reads
 */

#pragma once

#include <cstdint>
#include <string>

namespace cloudsql::storage {

/**
 * @brief Memory-mapped column data for zero-copy reads.
 *
 * Maps a column's `.data.bin` and `.nulls.bin` files into memory.
 * For fixed-width types, provides O(1) access to any row via pointer arithmetic.
 */
class MemoryMappedColumn {
   public:
    struct MappedRegion {
        void* addr;   // mmap'd base address
        size_t size;  // mapped size in bytes
        int fd;       // file descriptor (for munmap)
    };

   private:
    MappedRegion data_region_{nullptr, 0, -1};
    MappedRegion null_region_{nullptr, 0, -1};
    size_t element_size_ = 0;  // stride for fixed-width columns
    bool is_fixed_width_ = false;
    size_t row_count_ = 0;

   public:
    ~MemoryMappedColumn() { unmap(); }

    // Map a column's data and nulls files. Returns true on success.
    bool map(const std::string& data_path, const std::string& null_path, size_t element_size,
             size_t row_count);

    // Unmap and release resources
    void unmap();

    // Direct pointer to element at row index (fixed-width only)
    const void* data_at(size_t row_idx) const {
        if (!data_region_.addr) return nullptr;
        return static_cast<char*>(data_region_.addr) + row_idx * element_size_;
    }

    // Direct pointer to null bit at row index
    const uint8_t* null_at(size_t row_idx) const {
        if (!null_region_.addr) return nullptr;
        return static_cast<uint8_t*>(null_region_.addr) + row_idx;
    }

    bool is_mapped() const { return data_region_.addr != nullptr; }
    size_t row_count() const { return row_count_; }
};

}  // namespace cloudsql::storage
