/**
 * @file storage_manager.cpp
 * @brief Storage manager implementation
 *
 * @defgroup storage Storage Manager
 * @{
 */

#include "storage/storage_manager.hpp"

#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <ios>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>
#include <utility>

namespace cloudsql::storage {

/**
 * @brief Construct a new Storage Manager
 */
StorageManager::StorageManager(std::string data_dir) : data_dir_(std::move(data_dir)) {
    static_cast<void>(create_dir_if_not_exists());
}

/**
 * @brief Destroy the Storage Manager and close all files
 */
StorageManager::~StorageManager() {
    // Note: write_page uses raw POSIX I/O, so no cleanup needed here
}

/**
 * @brief Open a database file
 */
bool StorageManager::open_file(const std::string& filename) {
    if (open_files_.find(filename) != open_files_.end()) {
        auto& file = open_files_[filename];
        if (file->is_open()) {
            return true;
        }
        static_cast<void>(open_files_.erase(filename));
    }

    const std::string filepath = data_dir_ + "/" + filename;
    auto* file = new std::fstream();

    /* Open for read/write in binary mode. */
    file->open(filepath, std::ios::in | std::ios::out | std::ios::binary);

    if (!file->is_open()) {
        /* Create empty file then reopen */
        file->open(filepath, std::ios::out | std::ios::binary);
        if (!file->is_open()) {
            delete file;
            return false;
        }
        file->close();
        file->open(filepath, std::ios::in | std::ios::out | std::ios::binary);
    }

    if (!file->is_open()) {
        delete file;
        return false;
    }

    open_files_[filename] = file;
    static_cast<void>(stats_.files_opened.fetch_add(1));
    return true;
}

/**
 * @brief Close a database file
 */
bool StorageManager::close_file(const std::string& filename) {
    auto it = open_files_.find(filename);
    if (it == open_files_.end()) {
        return false;
    }

    it->second->close();
    static_cast<void>(open_files_.erase(it));
    return true;
}

/**
 * @brief Read a page from storage
 */
bool StorageManager::read_page(const std::string& filename, uint32_t page_num, char* buffer) {
    const std::string filepath = data_dir_ + "/" + filename;

    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        return false;
    }

    off_t seek_result = lseek(fd, static_cast<off_t>(page_num) * PAGE_SIZE, SEEK_SET);
    if (seek_result < 0) {
        close(fd);
        return false;
    }

    ssize_t bytes_read = read(fd, buffer, PAGE_SIZE);
    close(fd);

    if (bytes_read < 0) {
        return false;
    }

    if (static_cast<std::size_t>(bytes_read) < PAGE_SIZE) {
        std::fill(std::next(buffer, bytes_read), std::next(buffer, static_cast<std::ptrdiff_t>(PAGE_SIZE)), 0);
    }

    static_cast<void>(stats_.pages_read.fetch_add(1));
    static_cast<void>(stats_.bytes_read.fetch_add(PAGE_SIZE));
    return true;
}

/**
 * @brief Write a page to storage
 */
bool StorageManager::write_page(const std::string& filename, uint32_t page_num,
                                const char* buffer) {
    const std::string filepath = data_dir_ + "/" + filename;

    // Ensure file is open via open_files_ map (which uses fstream)
    if (open_files_.find(filename) == open_files_.end()) {
        if (!open_file(filename)) {
            return false;
        }
    }

    auto* file = open_files_[filename];
    file->clear();
    file->seekp(static_cast<std::streamoff>(page_num) * static_cast<std::streamoff>(PAGE_SIZE),
                std::ios::beg);

    if (file->fail()) {
        return false;
    }

    static_cast<void>(file->write(buffer, PAGE_SIZE));
    if (file->fail()) {
        return false;
    }

    file->flush();

    // Force sync to disk using raw syscall
    int sync_fd = open(filepath.c_str(), O_RDWR);
    if (sync_fd >= 0) {
        fsync(sync_fd);
        close(sync_fd);
    }

    // Update file size tracker - track actual written pages
    {
        std::scoped_lock<std::mutex> lock(file_sizes_mutex_);
        auto it = file_sizes_.find(filename);
        std::streamoff tracked_size = (it != file_sizes_.end()) ? it->second : 0;
        std::streamoff page_end = static_cast<std::streamoff>(page_num + 1) * static_cast<std::streamoff>(PAGE_SIZE);
        if (it == file_sizes_.end()) {
            file_sizes_[filename] = page_end;
        } else if (tracked_size < page_end) {
            it->second = page_end;
        }
    }

    static_cast<void>(stats_.pages_written.fetch_add(1));
    static_cast<void>(stats_.bytes_written.fetch_add(PAGE_SIZE));
    return true;
}

/**
 * @brief Allocate a new page in the database file
 */
uint32_t StorageManager::allocate_page(const std::string& filename) {
    // First check in-memory tracker
    auto it = file_sizes_.find(filename);
    if (it != file_sizes_.end()) {
        return static_cast<uint32_t>(static_cast<uint64_t>(it->second) / PAGE_SIZE);
    }

    // Fallback: check actual file size via stat
    const std::string filepath = data_dir_ + "/" + filename;
    struct stat st {};
    if (stat(filepath.c_str(), &st) == 0) {
        file_sizes_[filename] = st.st_size;
        return static_cast<uint32_t>(static_cast<uint64_t>(st.st_size) / PAGE_SIZE);
    }

    return 0;
}

/**
 * @brief Deallocate a page
 */
void StorageManager::deallocate_page(const std::string& filename, uint32_t page_num) {
    (void)filename;
    (void)page_num;
}

/**
 * @brief Resolves the full filesystem path for a given filename.
 */
std::string StorageManager::get_full_path(const std::string& filename) const {
    return data_dir_ + "/" + filename;
}

/**
 * @brief Check if a file exists on disk.
 */
bool StorageManager::file_exists(const std::string& filename) const {
    struct stat st {};
    return stat(get_full_path(filename).c_str(), &st) == 0;
}

/**
 * @brief Create data directory if it doesn't exist
 */
bool StorageManager::create_dir_if_not_exists() {
    struct stat st {};
    if (stat(data_dir_.c_str(), &st) != 0) {
        if (mkdir(data_dir_.c_str(), DEFAULT_DIR_MODE) != 0) {
            return false;
        }
    }
    return true;
}

/**
 * @brief Delete a file from storage
 */
bool StorageManager::delete_file(const std::string& filename) {
    // Close any open handle first to avoid stale fstream issues
    auto it = open_files_.find(filename);
    if (it != open_files_.end()) {
        if (it->second->is_open()) {
            it->second->close();
        }
        open_files_.erase(it);
    }
    const std::string filepath = get_full_path(filename);
    return std::remove(filepath.c_str()) == 0;
}

}  // namespace cloudsql::storage

/** @} */