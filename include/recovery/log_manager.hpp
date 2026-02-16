/**
 * @file log_manager.hpp
 * @brief Log Manager for Write-Ahead Logging
 */

#ifndef CLOUDSQL_RECOVERY_LOG_MANAGER_HPP
#define CLOUDSQL_RECOVERY_LOG_MANAGER_HPP

#include <atomic>
#include <condition_variable>
#include <fstream>
#include <mutex>
#include <thread>
#include <vector>

#include "recovery/log_record.hpp"

namespace cloudsql {
namespace recovery {

static constexpr lsn_t INVALID_LSN = -1;

/**
 * @brief Manages the WAL buffer and flushing to disk
 */
class LogManager {
   public:
    explicit LogManager(const std::string& log_file_path);
    ~LogManager();

    /**
     * @brief Start the flush thread
     */
    void run_flush_thread();

    /**
     * @brief Stop the flush thread
     */
    void stop_flush_thread();

    /**
     * @brief Append a log record to the buffer
     * @param log_record The record to append (LSN will be set)
     * @return The LSN of the appended record
     */
    lsn_t append_log_record(LogRecord& log_record);

    /**
     * @brief Flush log buffer to disk
     * @param force If true, force flush even if buffer is not full
     */
    void flush(bool force = false);

    /**
     * @brief Get the persistent LSN (flushed to disk)
     */
    lsn_t get_persistent_lsn() { return persistent_lsn_; }

    /**
     * @brief Get the next LSN to be assigned
     */
    lsn_t get_next_lsn() { return next_lsn_; }

   private:
    std::string log_file_path_;
    std::ofstream log_stream_;

    char* log_buffer_;
    uint32_t log_buffer_size_ = 4096 * 16;  // 64KB buffer
    uint32_t log_buffer_offset_ = 0;

    std::mutex latch_;
    std::thread flush_thread_;
    std::condition_variable cv_;
    std::atomic<bool> enable_flushing_{false};
    std::atomic<bool> stop_flush_thread_{false};

    std::atomic<lsn_t> next_lsn_{0};
    std::atomic<lsn_t> persistent_lsn_{INVALID_LSN};

    void flush_thread_loop();
};

}  // namespace recovery
}  // namespace cloudsql

#endif  // CLOUDSQL_RECOVERY_LOG_MANAGER_HPP
