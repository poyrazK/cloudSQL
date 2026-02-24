/**
 * @file page.hpp
 * @brief Base page class for the buffer pool manager
 */

#ifndef CLOUDSQL_STORAGE_PAGE_HPP
#define CLOUDSQL_STORAGE_PAGE_HPP

#include <array>
#include <cstring>
#include <shared_mutex>
#include <string>

namespace cloudsql::storage {

/**
 * @class Page
 * @brief Represents a single page in memory managed by the Buffer Pool
 */
class Page {
   public:
    static constexpr uint32_t PAGE_SIZE = 4096;

    Page() { reset_memory(); }

    Page(const Page&) = delete;
    Page& operator=(const Page&) = delete;
    Page(Page&&) = delete;
    Page& operator=(Page&&) = delete;
    virtual ~Page() = default;

    // Output raw pointer
    [[nodiscard]] char* get_data() { return data_.data(); }

    [[nodiscard]] uint32_t get_page_id() const { return page_id_; }

    [[nodiscard]] int get_pin_count() const { return pin_count_; }

    [[nodiscard]] bool is_dirty() const { return is_dirty_; }

    [[nodiscard]] std::string get_file_name() const { return file_name_; }

    // Read/Write Latch for concurrent access
    void r_lock() { rwlatch_.lock_shared(); }
    void r_unlock() { rwlatch_.unlock_shared(); }
    void w_lock() { rwlatch_.lock(); }
    void w_unlock() { rwlatch_.unlock(); }

    // Get Page LSN (for recovery/WAL)
    // Assuming page layout reserves first 8 bytes (or 4 bytes) for PageLSN depending on
    // implementation We will store LSN explicitly here to avoid parsing the header if not strictly
    // needed, but typically it's written in the first 4 bytes of data_.
    [[nodiscard]] int32_t get_lsn() const { return lsn_; }
    void set_lsn(int32_t lsn) { lsn_ = lsn; }

   private:
    friend class BufferPoolManager;

    void reset_memory() { data_.fill(0); }

    std::array<char, PAGE_SIZE> data_{};  // Fixed size page array
    uint32_t page_id_ = 0;                // The logical page id within the file
    std::string file_name_;               // File this page belongs to

    int pin_count_ = 0;      // Number of concurrent accesses
    bool is_dirty_ = false;  // Whether page has been modified
    int32_t lsn_ = -1;       // Page LSN, last modified operation

    std::shared_mutex rwlatch_;
};

}  // namespace cloudsql::storage

#endif  // CLOUDSQL_STORAGE_PAGE_HPP
