/**
 * @file log_manager.cpp
 * @brief Log Manager implementation
 */

#include "recovery/log_manager.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <iterator>
#include <mutex>
#include <string>
#include <utility>

#include "recovery/log_record.hpp"

namespace cloudsql::recovery {

namespace {
constexpr std::chrono::milliseconds FLUSH_TIMEOUT(30);
}  // anonymous namespace

LogManager::LogManager(std::string log_file_path)
    : log_file_path_(std::move(log_file_path)),
      log_buffer_(new char[log_buffer_size_]) {
    // Open log file in binary append mode
    log_stream_.open(log_file_path_, std::ios::binary | std::ios::app | std::ios::out);
    if (!log_stream_.is_open()) {
        std::cerr << "Error: Could not open log file: " << log_file_path_ << "\n";
    }
}

LogManager::~LogManager() {
    stop_flush_thread();
    if (log_stream_.is_open()) {
        log_stream_.close();
    }
    delete[] log_buffer_;
}

void LogManager::run_flush_thread() {
    if (enable_flushing_) {
        return;
    }

    enable_flushing_ = true;
    stop_flush_thread_flag_ = false;
    flush_thread_ = std::thread(&LogManager::flush_thread_loop, this);
}

void LogManager::stop_flush_thread() {
    if (!enable_flushing_) {
        return;
    }

    enable_flushing_ = false;
    stop_flush_thread_flag_ = true;
    cv_.notify_one();

    if (flush_thread_.joinable()) {
        flush_thread_.join();
    }
}

lsn_t LogManager::append_log_record(LogRecord& log_record) {
    const std::unique_lock<std::mutex> lock(latch_);

    // If record size > buffer size, flush first
    const uint32_t record_size = log_record.get_size();
    if (log_buffer_offset_ + record_size > log_buffer_size_) {
        flush(true);
    }

    // Assign LSN
    const lsn_t lsn = next_lsn_++;
    log_record.lsn_ = lsn;

    // Serialize to buffer
    static_cast<void>(log_record.serialize(std::next(log_buffer_, static_cast<std::ptrdiff_t>(log_buffer_offset_))));
    log_buffer_offset_ += record_size;

    return lsn;
}

void LogManager::flush(bool force) {
    (void)force;
    const std::unique_lock<std::mutex> lock(latch_);

    if (log_buffer_offset_ > 0) {
        log_stream_.write(log_buffer_, static_cast<std::streamsize>(log_buffer_offset_));
        log_stream_.flush();
        persistent_lsn_ = next_lsn_.load() - 1;
        log_buffer_offset_ = 0;
    }
}

void LogManager::flush_thread_loop() {
    while (!stop_flush_thread_flag_) {
        std::unique_lock<std::mutex> lock(latch_);
        static_cast<void>(cv_.wait_for(lock, FLUSH_TIMEOUT, [this] {
            return stop_flush_thread_flag_.load() || log_buffer_offset_ > 0;
        }));

        if (log_buffer_offset_ > 0) {
            log_stream_.write(log_buffer_, static_cast<std::streamsize>(log_buffer_offset_));
            log_stream_.flush();
            persistent_lsn_ = next_lsn_.load() - 1;
            log_buffer_offset_ = 0;
        }
    }
}

}  // namespace cloudsql::recovery
