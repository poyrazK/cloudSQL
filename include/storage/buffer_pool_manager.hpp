/**
 * @file buffer_pool_manager.hpp
 * @brief Manages pages in memory and coordinates with the StorageManager and LogManager
 */

#ifndef CLOUDSQL_STORAGE_BUFFER_POOL_MANAGER_HPP
#define CLOUDSQL_STORAGE_BUFFER_POOL_MANAGER_HPP

#include <list>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "storage/lru_replacer.hpp"
#include "storage/page.hpp"
#include "storage/storage_manager.hpp"

namespace cloudsql::recovery {
class LogManager;
}

namespace cloudsql::storage {

/**
 * @class BufferPoolManager
 * @brief Wraps StorageManager to provide an in-memory cache of disk pages
 */
class BufferPoolManager {
   public:
    /**
     * @brief Creates a new Buffer Pool Manager
     * @param pool_size Size of the buffer pool in number of pages
     * @param storage_manager Reference to the underlying storage manager
     * @param log_manager Pointer to the log manager (can be null if WAL is disabled)
     */
    BufferPoolManager(size_t pool_size, StorageManager& storage_manager,
                      recovery::LogManager* log_manager = nullptr);

    ~BufferPoolManager();

    // Disable copy/move
    BufferPoolManager(const BufferPoolManager&) = delete;
    BufferPoolManager& operator=(const BufferPoolManager&) = delete;
    BufferPoolManager(BufferPoolManager&&) = delete;
    BufferPoolManager& operator=(BufferPoolManager&&) = delete;

    /**
     * @brief Fetch the requested page from the buffer pool
     * @param file_name The file the page belongs to
     * @param page_id The id of the page
     * @return Pointer to the Page, or nullptr if cannot fetch
     */
    Page* fetch_page(const std::string& file_name, uint32_t page_id);

    /**
     * @brief Unpin the target page
     * @param file_name The file the page belongs to
     * @param page_id The id of the page
     * @param is_dirty true if the page was modified
     * @return true if successfully unpinned, false otherwise
     */
    bool unpin_page(const std::string& file_name, uint32_t page_id, bool is_dirty);

    /**
     * @brief Flush a single page to disk
     * @param file_name The file the page belongs to
     * @param page_id The id of the page
     * @return true if successfully flushed, false otherwise
     */
    bool flush_page(const std::string& file_name, uint32_t page_id);

    /**
     * @brief Create a new page in the buffer pool
     * @param file_name The file the page belongs to
     * @param[out] page_id Output param for the id of the created page
     * @return Pointer to the new Page, or nullptr if cannot be created
     */
    Page* new_page(const std::string& file_name, const uint32_t* page_id);

    /**
     * @brief Delete a page
     * @param file_name The file the page belongs to
     * @param page_id The id of the page to delete
     * @return true if successfully deleted, false otherwise
     */
    bool delete_page(const std::string& file_name, uint32_t page_id);

    bool open_file(const std::string& file_name) { return storage_manager_.open_file(file_name); }

    bool close_file(const std::string& file_name) { return storage_manager_.close_file(file_name); }

    /**
     * @brief Flush all pages in the pool to disk
     */
    void flush_all_pages();

   private:
    /**
     * @brief Generates a unique string key for file and page mapping
     */
    static std::string make_page_key(const std::string& file_name, uint32_t page_id) {
        return file_name + "_" + std::to_string(page_id);
    }

    size_t pool_size_;
    StorageManager& storage_manager_;
    recovery::LogManager* log_manager_;

    // To protect concurrent accesses to page_table_ and replacer_
    std::mutex latch_;

    // The actual array of pages
    std::vector<Page> pages_;

    // Replacer instance
    LRUReplacer replacer_;

    // List of free frame IDs
    std::list<uint32_t> free_list_;

    // Maps page keys (file+pageId) to frame IDs
    std::unordered_map<std::string, uint32_t> page_table_;
};

}  // namespace cloudsql::storage

#endif  // CLOUDSQL_STORAGE_BUFFER_POOL_MANAGER_HPP
