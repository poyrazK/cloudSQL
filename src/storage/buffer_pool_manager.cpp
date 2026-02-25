/**
 * @file buffer_pool_manager.cpp
 * @brief Implementation of the Buffer Pool Manager
 */

#include "storage/buffer_pool_manager.hpp"

#include "recovery/log_manager.hpp"

namespace cloudsql::storage {

BufferPoolManager::BufferPoolManager(size_t pool_size, StorageManager& storage_manager,
                                     recovery::LogManager* log_manager)
    : pool_size_(pool_size),
      storage_manager_(storage_manager),
      log_manager_(log_manager),
      pages_(pool_size_),
      replacer_(pool_size_) {
    for (size_t i = 0; i < pool_size_; i++) {
        free_list_.push_back(static_cast<uint32_t>(i));
    }
}

BufferPoolManager::~BufferPoolManager() {
    flush_all_pages();
}

Page* BufferPoolManager::fetch_page(const std::string& file_name, uint32_t page_id) {
    std::lock_guard<std::mutex> lock(latch_);
    std::string key = make_page_key(file_name, page_id);

    // 1. If page is already in the buffer pool
    if (page_table_.find(key) != page_table_.end()) {
        uint32_t frame_id = page_table_[key];
        Page* page = &pages_[frame_id];
        page->pin_count_++;
        replacer_.pin(frame_id);
        return page;
    }

    // 2. Page is not in the pool. Find a victim or free frame.
    uint32_t frame_id = 0;
    if (!free_list_.empty()) {
        frame_id = free_list_.front();
        free_list_.pop_front();
    } else {
        if (!replacer_.victim(&frame_id)) {
            // Buffer pool is full and everything is pinned
            return nullptr;
        }
        // Write back dirty page
        Page* victim_page = &pages_[frame_id];
        if (victim_page->is_dirty_) {
            // Check WAL requirements before flushing
            if (log_manager_ != nullptr && victim_page->lsn_ != -1) {
                if (victim_page->lsn_ > log_manager_->get_persistent_lsn()) {
                    log_manager_->flush(true);
                }
            }
            storage_manager_.write_page(victim_page->file_name_, victim_page->page_id_,
                                        victim_page->get_data());
        }

        // Remove from page table
        page_table_.erase(make_page_key(victim_page->file_name_, victim_page->page_id_));
    }

    // 3. Read the page from disk
    Page* new_page_ptr = &pages_[frame_id];
    new_page_ptr->reset_memory();

    // storage_manager_.read_page populates the buffer or zero-fills if it doesn't exist
    if (!storage_manager_.read_page(file_name, page_id, new_page_ptr->get_data())) {
        // If it really failed (e.g., IO error), we should return the frame
        free_list_.push_back(frame_id);
        return nullptr;
    }

    // 4. Update metadata
    new_page_ptr->page_id_ = page_id;
    new_page_ptr->file_name_ = file_name;
    new_page_ptr->pin_count_ = 1;
    new_page_ptr->is_dirty_ = false;
    new_page_ptr->lsn_ = -1;

    page_table_[key] = frame_id;
    replacer_.pin(frame_id);

    return new_page_ptr;
}

bool BufferPoolManager::unpin_page(const std::string& file_name, uint32_t page_id, bool is_dirty) {
    std::lock_guard<std::mutex> lock(latch_);
    std::string key = make_page_key(file_name, page_id);

    if (page_table_.find(key) == page_table_.end()) {
        return false;
    }

    uint32_t frame_id = page_table_[key];
    Page* page = &pages_[frame_id];

    if (page->pin_count_ <= 0) {
        return false;
    }

    if (is_dirty) {
        page->is_dirty_ = true;
    }

    page->pin_count_--;
    if (page->pin_count_ == 0) {
        replacer_.unpin(frame_id);
    }

    return true;
}

bool BufferPoolManager::flush_page(const std::string& file_name, uint32_t page_id) {
    std::lock_guard<std::mutex> lock(latch_);
    std::string key = make_page_key(file_name, page_id);

    if (page_table_.find(key) == page_table_.end()) {
        return false;
    }

    uint32_t frame_id = page_table_[key];
    Page* page = &pages_[frame_id];

    // Check WAL requirements before flushing
    if (log_manager_ != nullptr && page->lsn_ != -1) {
        if (page->lsn_ > log_manager_->get_persistent_lsn()) {
            log_manager_->flush(true);
        }
    }

    storage_manager_.write_page(page->file_name_, page->page_id_, page->get_data());
    page->is_dirty_ = false;
    return true;
}

Page* BufferPoolManager::new_page(const std::string& file_name, uint32_t* page_id) {
    std::lock_guard<std::mutex> lock(latch_);

    // We need to determine the new page ID. In our basic layout, we'll
    // assume the caller knows the ID, but wait, the signature expects us to
    // assign it. For simplicity in cloudSQL, typically the table asks for a specific
    // page by ID, or it just reads page N until it fails.
    // If the caller calls new_page, we assume page_id was pre-filled with the desired ID
    // or we have a way to know the next ID.
    // Wait, the interface says `Output param for the id of the created page`. Currently
    // let's just use the passed in page_id as the requested page ID to create.
    uint32_t target_page_id = *page_id;
    std::string key = make_page_key(file_name, target_page_id);

    // If already exists, return
    if (page_table_.find(key) != page_table_.end()) {
        return nullptr;
    }

    uint32_t frame_id = 0;
    if (!free_list_.empty()) {
        frame_id = free_list_.front();
        free_list_.pop_front();
    } else {
        if (!replacer_.victim(&frame_id)) {
            return nullptr;
        }
        Page* victim_page = &pages_[frame_id];
        if (victim_page->is_dirty_) {
            if (log_manager_ != nullptr && victim_page->lsn_ != -1) {
                if (victim_page->lsn_ > log_manager_->get_persistent_lsn()) {
                    log_manager_->flush(true);
                }
            }
            storage_manager_.write_page(victim_page->file_name_, victim_page->page_id_,
                                        victim_page->get_data());
        }
        page_table_.erase(make_page_key(victim_page->file_name_, victim_page->page_id_));
    }

    Page* new_page_ptr = &pages_[frame_id];
    new_page_ptr->reset_memory();

    // Explicitly write a blank page to storage to instantiate it
    storage_manager_.write_page(file_name, target_page_id, new_page_ptr->get_data());

    new_page_ptr->page_id_ = target_page_id;
    new_page_ptr->file_name_ = file_name;
    new_page_ptr->pin_count_ = 1;
    new_page_ptr->is_dirty_ = false;
    new_page_ptr->lsn_ = -1;

    page_table_[key] = frame_id;
    replacer_.pin(frame_id);

    return new_page_ptr;
}

bool BufferPoolManager::delete_page(const std::string& file_name, uint32_t page_id) {
    std::lock_guard<std::mutex> lock(latch_);
    std::string key = make_page_key(file_name, page_id);

    if (page_table_.find(key) == page_table_.end()) {
        return true;
    }

    uint32_t frame_id = page_table_[key];
    Page* page = &pages_[frame_id];

    if (page->pin_count_ > 0) {
        return false;
    }

    page_table_.erase(key);
    free_list_.push_back(frame_id);
    page->reset_memory();
    page->page_id_ = 0;
    page->file_name_.clear();
    page->is_dirty_ = false;
    page->lsn_ = -1;

    return true;
}

void BufferPoolManager::flush_all_pages() {
    std::lock_guard<std::mutex> lock(latch_);
    for (size_t i = 0; i < pool_size_; i++) {
        Page* page = &pages_[i];
        if (!page->file_name_.empty() && page->is_dirty_) {
            if (log_manager_ != nullptr && page->lsn_ != -1) {
                if (page->lsn_ > log_manager_->get_persistent_lsn()) {
                    log_manager_->flush(true);
                }
            }
            storage_manager_.write_page(page->file_name_, page->page_id_, page->get_data());
            page->is_dirty_ = false;
        }
    }
}

}  // namespace cloudsql::storage
