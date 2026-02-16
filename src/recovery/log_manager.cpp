/**
 * @file log_manager.cpp
 * @brief Log Manager implementation
 */

#include "recovery/log_manager.hpp"
#include <chrono>
#include <cstring>
#include <iostream>

namespace cloudsql {
namespace recovery {

LogManager::LogManager(const std::string& log_file_path)
    : log_file_path_(log_file_path) {
    log_buffer_ = new char[log_buffer_size_];
    
    // Open log file in binary append mode
    log_stream_.open(log_file_path_, std::ios::binary | std::ios::app | std::ios::out);
    if (!log_stream_.is_open()) {
        std::cerr << "Error: Could not open log file: " << log_file_path_ << std::endl;
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
    if (enable_flushing_) return;
    
    enable_flushing_ = true;
    stop_flush_thread_ = false;
    flush_thread_ = std::thread(&LogManager::flush_thread_loop, this);
}

void LogManager::stop_flush_thread() {
    if (!enable_flushing_) return;
    
    enable_flushing_ = false;
    stop_flush_thread_ = true;
    cv_.notify_one();
    
    if (flush_thread_.joinable()) {
        flush_thread_.join();
    }
    
    flush(true); // Final flush
}

lsn_t LogManager::append_log_record(LogRecord& log_record) {
    std::unique_lock<std::mutex> lock(latch_);
    
    // Assign LSN
    log_record.lsn_ = next_lsn_++;
    
    // Calculate and set size (important before serialization)
    // We invalidate cached size first to ensure it includes the new LSN
    log_record.size_ = 0; 
    log_record.size_ = log_record.get_size();
    
    // Check if buffer has space
    if (log_buffer_offset_ + log_record.size_ > log_buffer_size_) {
        // Buffer full, force flush
        // We simply call flush() which will write current buffer to disk
        // Note: For high concurrency, we would swap buffers here.
        log_stream_.write(log_buffer_, log_buffer_offset_);
        log_stream_.flush();
        
        persistent_lsn_ = next_lsn_ - 1; // Assuming all previous are flushed
        log_buffer_offset_ = 0;
    }
    
    // Serialize to buffer
    log_record.serialize(log_buffer_ + log_buffer_offset_);
    log_buffer_offset_ += log_record.size_;
    
    return log_record.lsn_;
}

void LogManager::flush(bool force) {
    std::unique_lock<std::mutex> lock(latch_);
    
    if (log_buffer_offset_ > 0) {
        log_stream_.write(log_buffer_, log_buffer_offset_);
        log_stream_.flush();
        
        // Update persistent LSN
        // The last LSN written is effectively the one before next_lsn_ 
        // (if we assume no gaps, which is true for this sequential appender)
        // A more precise way would be to track 'last_lsn_in_buffer'
        persistent_lsn_ = next_lsn_ - 1;
        
        log_buffer_offset_ = 0;
    }
}

void LogManager::flush_thread_loop() {
    while (enable_flushing_) {
        std::unique_lock<std::mutex> lock(latch_);
        
        // Wait for timeout or stop signal
        cv_.wait_for(lock, std::chrono::milliseconds(30), [this] { 
            return stop_flush_thread_.load(); 
        });
        
        if (log_buffer_offset_ > 0) {
            log_stream_.write(log_buffer_, log_buffer_offset_);
            log_stream_.flush();
            
            persistent_lsn_ = next_lsn_ - 1;
            log_buffer_offset_ = 0;
        }
        
        if (stop_flush_thread_) break;
    }
}

} // namespace recovery
} // namespace cloudsql
