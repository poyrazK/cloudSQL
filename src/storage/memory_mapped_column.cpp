/**
 * @file memory_mapped_column.cpp
 * @brief Memory-mapped column implementation
 */

#include "storage/memory_mapped_column.hpp"
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>

namespace cloudsql::storage {

bool MemoryMappedColumn::map(const std::string& data_path, const std::string& null_path,
                              size_t element_size, size_t row_count) {
    // Map data file
    int data_fd = ::open(data_path.c_str(), O_RDONLY);
    if (data_fd < 0) return false;

    struct stat st;
    if (fstat(data_fd, &st) < 0) {
        ::close(data_fd);
        return false;
    }

    void* data_ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, data_fd, 0);
    if (data_ptr == MAP_FAILED) {
        ::close(data_fd);
        return false;
    }

    data_region_ = {data_ptr, static_cast<size_t>(st.st_size), data_fd};

    // Map nulls file
    int null_fd = ::open(null_path.c_str(), O_RDONLY);
    if (null_fd < 0) {
        unmap();
        return false;
    }

    if (fstat(null_fd, &st) < 0) {
        ::close(null_fd);
        unmap();
        return false;
    }

    void* null_ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, null_fd, 0);
    if (null_ptr == MAP_FAILED) {
        ::close(null_fd);
        unmap();
        return false;
    }

    null_region_ = {null_ptr, static_cast<size_t>(st.st_size), null_fd};

    element_size_ = element_size;
    is_fixed_width_ = true;
    row_count_ = row_count;
    return true;
}

void MemoryMappedColumn::unmap() {
    if (data_region_.addr) {
        munmap(data_region_.addr, data_region_.size);
        ::close(data_region_.fd);
        data_region_ = {nullptr, 0, -1};
    }
    if (null_region_.addr) {
        munmap(null_region_.addr, null_region_.size);
        ::close(null_region_.fd);
        null_region_ = {nullptr, 0, -1};
    }
}

}  // namespace cloudsql::storage