/**
 * @file storage_manager.hpp
 * @brief Storage Manager for persistent database files
 */

#ifndef CLOUDSQL_STORAGE_STORAGE_MANAGER_HPP
#define CLOUDSQL_STORAGE_STORAGE_MANAGER_HPP

#include <atomic>
#include <fstream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace cloudsql::storage {

/**
 * @brief Manages low-level disk I/O and page-level access
 */
class StorageManager {
   public:
    static constexpr uint32_t PAGE_SIZE = 4096;
    static constexpr int DEFAULT_DIR_MODE = 0755;

    struct Stats {
        std::atomic<uint64_t> pages_read{0};
        std::atomic<uint64_t> pages_written{0};
        std::atomic<uint64_t> bytes_read{0};
        std::atomic<uint64_t> bytes_written{0};
        std::atomic<uint32_t> files_opened{0};
    };

    explicit StorageManager(std::string data_dir);
    ~StorageManager();

    // Disable copy/move for storage manager (due to atomic stats)
    StorageManager(const StorageManager&) = delete;
    StorageManager& operator=(const StorageManager&) = delete;
    StorageManager(StorageManager&&) = delete;
    StorageManager& operator=(StorageManager&&) = delete;

    /**
     * @brief Get storage statistics
     */
    [[nodiscard]] const Stats& get_stats() const { return stats_; }

    /**
     * @brief Open a database file
     */
    bool open_file(const std::string& filename);

    /**
     * @brief Close a database file
     */
    bool close_file(const std::string& filename);

    /**
     * @brief Read a page from disk into buffer
     * @param filename Name of the database file
     * @param page_num Page index
     * @param buffer Pre-allocated buffer of at least PAGE_SIZE
     * @return true on success
     */
    bool read_page(const std::string& filename, uint32_t page_num, char* buffer);

    /**
     * @brief Write a page from buffer to disk
     * @param filename Name of the database file
     * @param page_num Page index
     * @param buffer Buffer containing data to write
     * @return true on success
     */
    bool write_page(const std::string& filename, uint32_t page_num, const char* buffer);

    /**
     * @brief Check if a file exists
     */
    [[nodiscard]] bool file_exists(const std::string& filename) const;

    /**
     * @brief Create data directory if not exists
     */
    bool create_dir_if_not_exists();

   private:
    std::string data_dir_;
    std::unordered_map<std::string, std::unique_ptr<std::fstream>> open_files_;
    Stats stats_;
};

}  // namespace cloudsql::storage

#endif  // CLOUDSQL_STORAGE_STORAGE_MANAGER_HPP
