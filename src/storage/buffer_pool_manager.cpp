/**
 * @file buffer_pool_manager.cpp
 * @brief Buffer pool manager implementation
 */

#include "storage/buffer_pool_manager.hpp"

#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>

#include "storage/page.hpp"
#include "storage/storage_manager.hpp"

namespace cloudsql::storage {

BufferPoolManager::BufferPoolManager(size_t pool_size, StorageManager& storage_manager,
                                     recovery::LogManager* log_manager)
    : pool_size_(pool_size),
      storage_manager_(storage_manager),
      log_manager_(log_manager),
      pages_(std::make_unique<Page[]>(pool_size)),
      replacer_(pool_size) {
    for (size_t i = 0; i < pool_size_; ++i) {
        free_list_.push_back(static_cast<uint32_t>(i));
    }
}

BufferPoolManager::~BufferPoolManager() {
    try {
        flush_all_pages();
    } catch (const std::exception& e) {
        // Log error to stderr; avoid throwing from destructor to prevent std::terminate
        std::cerr << "[Error] Exception in BufferPoolManager destructor during flush_all_pages: "
                  << e.what() << std::endl;
    } catch (...) {
        std::cerr
            << "[Error] Unknown exception in BufferPoolManager destructor during flush_all_pages"
            << std::endl;
    }
}

Page* BufferPoolManager::fetch_page(const std::string& file_name, uint32_t page_id) {
    const std::scoped_lock<std::mutex> lock(latch_);

    const std::string key = make_page_key(file_name, page_id);
    if (page_table_.find(key) != page_table_.end()) {
        const uint32_t frame_id = page_table_[key];
        Page* const page = &pages_[frame_id];
        page->pin_count_++;
        replacer_.pin(frame_id);
        return page;
    }

    uint32_t frame_id = 0;
    if (!free_list_.empty()) {
        frame_id = free_list_.back();
        free_list_.pop_back();
    } else if (!replacer_.victim(&frame_id)) {
        return nullptr;
    }

    Page* const page = &pages_[frame_id];
    if (page->is_dirty_) {
        storage_manager_.write_page(page->file_name_, page->page_id_, page->get_data());
    }

    page_table_.erase(make_page_key(page->file_name_, page->page_id_));
    page_table_[key] = frame_id;

    page->page_id_ = page_id;
    page->file_name_ = file_name;
    page->pin_count_ = 1;
    page->is_dirty_ = false;

    if (!storage_manager_.read_page(file_name, page_id, page->get_data())) {
        // If read fails (e.g. file too short), initialize with zeros
        std::memset(page->get_data(), 0, Page::PAGE_SIZE);
    }

    replacer_.pin(frame_id);
    return page;
}

bool BufferPoolManager::unpin_page(const std::string& file_name, uint32_t page_id, bool is_dirty) {
    const std::scoped_lock<std::mutex> lock(latch_);

    const std::string key = make_page_key(file_name, page_id);
    if (page_table_.find(key) == page_table_.end()) {
        return false;
    }

    const uint32_t frame_id = page_table_[key];
    Page* const page = &pages_[frame_id];

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
    const std::scoped_lock<std::mutex> lock(latch_);

    const std::string key = make_page_key(file_name, page_id);
    if (page_table_.find(key) == page_table_.end()) {
        return false;
    }

    const uint32_t frame_id = page_table_[key];
    Page* const page = &pages_[frame_id];
    storage_manager_.write_page(file_name, page_id, page->get_data());
    page->is_dirty_ = false;

    return true;
}

Page* BufferPoolManager::new_page(const std::string& file_name, const uint32_t* page_id) {
    const std::scoped_lock<std::mutex> lock(latch_);

    const uint32_t target_page_id = storage_manager_.allocate_page(file_name);
    if (page_id != nullptr) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
        const_cast<uint32_t&>(*page_id) = target_page_id;
    }
    const std::string key = make_page_key(file_name, target_page_id);

    uint32_t frame_id = 0;
    if (!free_list_.empty()) {
        frame_id = free_list_.back();
        free_list_.pop_back();
    } else if (!replacer_.victim(&frame_id)) {
        return nullptr;
    }

    Page* const page = &pages_[frame_id];
    if (page->is_dirty_) {
        storage_manager_.write_page(page->file_name_, page->page_id_, page->get_data());
    }

    page_table_.erase(make_page_key(page->file_name_, page->page_id_));
    page_table_[key] = frame_id;

    page->page_id_ = target_page_id;
    page->file_name_ = file_name;
    page->pin_count_ = 1;
    page->is_dirty_ = false;
    std::memset(page->get_data(), 0, Page::PAGE_SIZE);

    replacer_.pin(frame_id);
    return page;
}

bool BufferPoolManager::delete_page(const std::string& file_name, uint32_t page_id) {
    const std::scoped_lock<std::mutex> lock(latch_);

    const std::string key = make_page_key(file_name, page_id);
    if (page_table_.find(key) != page_table_.end()) {
        const uint32_t frame_id = page_table_[key];
        Page* const page = &pages_[frame_id];
        if (page->pin_count_ > 0) {
            return false;
        }

        page_table_.erase(key);
        replacer_.pin(frame_id);
        page->page_id_ = 0;
        page->file_name_ = "";
        page->pin_count_ = 0;
        page->is_dirty_ = false;
        free_list_.push_back(frame_id);
    }

    StorageManager::deallocate_page(file_name, page_id);
    return true;
}

void BufferPoolManager::flush_all_pages() {
    const std::scoped_lock<std::mutex> lock(latch_);

    for (auto const& [key, frame_id] : page_table_) {
        Page* const page = &pages_[frame_id];
        if (page->is_dirty_) {
            storage_manager_.write_page(page->file_name_, page->page_id_, page->get_data());
            page->is_dirty_ = false;
        }
    }
}

}  // namespace cloudsql::storage
