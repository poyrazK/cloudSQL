/**
 * @file heap_table.cpp
 * @brief Heap table storage implementation with MVCC support
 *
 * @defgroup storage Storage Engine
 * @{
 */

#include "storage/heap_table.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/value.hpp"
#include "executor/types.hpp"
#include "storage/buffer_pool_manager.hpp"
#include "storage/page.hpp"

namespace cloudsql::storage {

namespace {
constexpr uint16_t DEFAULT_SLOT_COUNT = 64;
}  // anonymous namespace

HeapTable::HeapTable(std::string table_name, BufferPoolManager& bpm, executor::Schema schema)
    : table_name_(std::move(table_name)),
      filename_(table_name_ + ".heap"),
      bpm_(bpm),
      schema_(std::move(schema)),
      last_page_id_(0) {}

/* --- Iterator Implementation --- */

HeapTable::Iterator::Iterator(HeapTable& table) : table_(table), next_id_(0, 0), last_id_(0, 0) {}

bool HeapTable::Iterator::next(executor::Tuple& out_tuple) {
    TupleMeta meta;
    while (next_meta(meta)) {
        if (meta.xmax == 0) {
            out_tuple = std::move(meta.tuple);
            return true;
        }
    }
    return false;
}

/**
 * @brief Fetches next versioned record from scan
 */
bool HeapTable::Iterator::next_meta(TupleMeta& out_meta) {
    if (eof_) {
        return false;
    }

    while (true) {
        if (table_.get_meta(next_id_, out_meta)) {
            /* Record successfully retrieved */
            last_id_ = next_id_;

            /* Prepare for next call: advance slot index */
            next_id_.slot_num++;
            return true;
        }

        /* Check if the current page has more slots to explore */
        std::array<char, Page::PAGE_SIZE> buf{};
        if (table_.read_page(next_id_.page_num, buf.data())) {
            PageHeader header{};
            std::memcpy(&header, buf.data(), sizeof(PageHeader));
            if (next_id_.slot_num < header.num_slots) {
                /* Current slot is empty/deleted; skip to the next */
                next_id_.slot_num++;
                continue;
            }
        }

        /* Move to the beginning of the next physical page */
        next_id_.page_num++;
        next_id_.slot_num = 0;

        /* If the next page cannot be read, end of file is reached */
        if (!table_.read_page(next_id_.page_num, buf.data())) {
            eof_ = true;
            return false;
        }

        /* Validate that the page has been initialized */
        PageHeader next_header{};
        std::memcpy(&next_header, buf.data(), sizeof(PageHeader));
        if (next_header.free_space_offset == 0) {
            eof_ = true;
            return false;
        }
    }
}

/* --- HeapTable Methods --- */

HeapTable::TupleId HeapTable::insert(const executor::Tuple& tuple, uint64_t xmin) {
    uint32_t page_num = last_page_id_;

    /* Pre-serialize tuple to binary to determine size and avoid repeat work */
    std::vector<uint8_t> payload;
    payload.reserve(16 + (tuple.size() * 8)); 
    
    uint64_t xmax = 0;
    payload.resize(16);
    std::memcpy(payload.data(), &xmin, 8);
    std::memcpy(payload.data() + 8, &xmax, 8);

    for (const auto& val : tuple.values()) {
        auto type = static_cast<uint8_t>(val.type());
        payload.push_back(type);
        if (val.is_null()) continue;

        if (val.is_numeric()) {
            double v = val.to_float64();
            size_t off = payload.size();
            payload.resize(off + 8);
            std::memcpy(payload.data() + off, &v, 8);
        } else {
            const std::string& s = val.to_string();
            uint32_t len = static_cast<uint32_t>(s.size());
            size_t off = payload.size();
            payload.resize(off + 4 + len);
            std::memcpy(payload.data() + off, &len, 4);
            std::memcpy(payload.data() + off + 4, s.data(), len);
        }
    }

    const auto required = static_cast<uint16_t>(payload.size());

    while (true) {
        Page* page = bpm_.fetch_page(filename_, page_num);
        if (!page) {
            page = bpm_.new_page(filename_, &page_num);
            if (!page) return {0, 0};
        }

        auto* buffer = page->get_data();
        PageHeader header{};
        std::memcpy(&header, buffer, sizeof(PageHeader));
        
        if (header.free_space_offset == 0) {
            header.free_space_offset =
                static_cast<uint16_t>(sizeof(PageHeader) + (DEFAULT_SLOT_COUNT * sizeof(uint16_t)));
            header.num_slots = 0;
        }

        /* Check for sufficient free space in the current page */
        if (header.free_space_offset + required < Page::PAGE_SIZE &&
            header.num_slots < DEFAULT_SLOT_COUNT) {
            const uint16_t offset = header.free_space_offset;
            
            // Copy binary payload directly to page buffer
            std::memcpy(buffer + offset, payload.data(), payload.size());

            /* Update slot directory */
            std::memcpy(buffer + sizeof(PageHeader) + (header.num_slots * sizeof(uint16_t)),
                        &offset, sizeof(uint16_t));

            TupleId tid(page_num, header.num_slots);
            header.num_slots++;
            header.free_space_offset += required;

            std::memcpy(buffer, &header, sizeof(PageHeader));
            bpm_.unpin_page(filename_, page_num, true);
            last_page_id_ = page_num;
            return tid;
        }

        /* Page is full; attempt insertion in the next page */
        bpm_.unpin_page(filename_, page_num, false);
        page_num++;
    }
}

/**
 * @brief Logical deletion: update xmax field in the record blob
 */
bool HeapTable::remove(const TupleId& tuple_id, uint64_t xmax) {
    Page* page = bpm_.fetch_page(filename_, tuple_id.page_num);
    if (!page) return false;

    auto* buffer = page->get_data();
    PageHeader header{};
    std::memcpy(&header, buffer, sizeof(PageHeader));
    if (header.free_space_offset == 0 || tuple_id.slot_num >= header.num_slots) {
        bpm_.unpin_page(filename_, tuple_id.page_num, false);
        return false;
    }

    uint16_t offset = 0;
    std::memcpy(&offset, buffer + sizeof(PageHeader) + (tuple_id.slot_num * sizeof(uint16_t)),
                sizeof(uint16_t));
    if (offset == 0) {
        bpm_.unpin_page(filename_, tuple_id.page_num, false);
        return false;
    }

    /* In binary format, xmax is at offset + 8 */
    std::memcpy(buffer + offset + 8, &xmax, 8);

    bpm_.unpin_page(filename_, tuple_id.page_num, true);
    return true;
}

/**
 * @brief Physical deletion: zero out slot offset (rollback only)
 */
bool HeapTable::physical_remove(const TupleId& tuple_id) {
    Page* page = bpm_.fetch_page(filename_, tuple_id.page_num);
    if (!page) return false;

    auto* buffer = page->get_data();
    PageHeader header{};
    std::memcpy(&header, buffer, sizeof(PageHeader));
    if (header.free_space_offset == 0 || tuple_id.slot_num >= header.num_slots) {
        bpm_.unpin_page(filename_, tuple_id.page_num, false);
        return false;
    }

    const uint16_t zero = 0;
    std::memcpy(buffer + sizeof(PageHeader) + (tuple_id.slot_num * sizeof(uint16_t)),
                &zero, sizeof(uint16_t));

    bpm_.unpin_page(filename_, tuple_id.page_num, true);
    return true;
}

/**
 * @brief Reset xmax to 0 (used for rollback of a DELETE)
 */
bool HeapTable::undo_remove(const TupleId& tuple_id) {
    return remove(tuple_id, 0);
}

bool HeapTable::update(const TupleId& tuple_id, const executor::Tuple& tuple, uint64_t txn_id) {
    if (!remove(tuple_id, txn_id)) {
        return false;
    }
    static_cast<void>(insert(tuple, txn_id));
    return true;
}

bool HeapTable::get_meta(const TupleId& tuple_id, TupleMeta& out_meta) const {
    Page* page = bpm_.fetch_page(filename_, tuple_id.page_num);
    if (!page) return false;

    auto* buffer = page->get_data();
    PageHeader header{};
    std::memcpy(&header, buffer, sizeof(PageHeader));
    if (header.free_space_offset == 0 || tuple_id.slot_num >= header.num_slots) {
        bpm_.unpin_page(filename_, tuple_id.page_num, false);
        return false;
    }

    uint16_t offset = 0;
    std::memcpy(&offset, buffer + sizeof(PageHeader) + (tuple_id.slot_num * sizeof(uint16_t)),
                sizeof(uint16_t));
    if (offset == 0) {
        bpm_.unpin_page(filename_, tuple_id.page_num, false);
        return false;
    }

    const uint8_t* const data = reinterpret_cast<const uint8_t*>(buffer + offset);
    
    // Read MVCC Header
    std::memcpy(&out_meta.xmin, data, 8);
    std::memcpy(&out_meta.xmax, data + 8, 8);
    
    size_t cursor = 16;
    std::vector<common::Value> values;
    values.reserve(schema_.column_count());
    
    for (size_t i = 0; i < schema_.column_count(); ++i) {
        auto type = static_cast<common::ValueType>(data[cursor++]);
        if (type == common::ValueType::TYPE_NULL) {
            values.push_back(common::Value::make_null());
            continue;
        }

        if (type == common::ValueType::TYPE_BOOL || 
            type == common::ValueType::TYPE_INT8 || type == common::ValueType::TYPE_INT16 ||
            type == common::ValueType::TYPE_INT32 || type == common::ValueType::TYPE_INT64 ||
            type == common::ValueType::TYPE_FLOAT32 || type == common::ValueType::TYPE_FLOAT64) {
            
            double v;
            std::memcpy(&v, data + cursor, 8);
            cursor += 8;
            
            if (type == common::ValueType::TYPE_BOOL) values.push_back(common::Value::make_bool(v != 0));
            else if (type == common::ValueType::TYPE_FLOAT32 || type == common::ValueType::TYPE_FLOAT64) 
                values.push_back(common::Value::make_float64(v));
            else values.push_back(common::Value::make_int64(static_cast<int64_t>(v)));
        } else {
            uint32_t len;
            std::memcpy(&len, data + cursor, 4);
            cursor += 4;
            std::string s(reinterpret_cast<const char*>(data + cursor), len);
            cursor += len;
            values.push_back(common::Value::make_text(s));
        }
    }

    out_meta.tuple = executor::Tuple(std::move(values));
    bpm_.unpin_page(filename_, tuple_id.page_num, false);
    return true;
}

bool HeapTable::get(const TupleId& tuple_id, executor::Tuple& out_tuple) const {
    TupleMeta meta;
    if (get_meta(tuple_id, meta)) {
        out_tuple = std::move(meta.tuple);
        return true;
    }
    return false;
}

uint64_t HeapTable::tuple_count() const {
    uint64_t count = 0;
    uint32_t page_num = 0;
    while (true) {
        Page* page = bpm_.fetch_page(filename_, page_num);
        if (!page) break;

        auto* buffer = page->get_data();
        PageHeader header{};
        std::memcpy(&header, buffer, sizeof(PageHeader));
        if (header.free_space_offset == 0) {
            bpm_.unpin_page(filename_, page_num, false);
            break;
        }

        for (uint16_t i = 0; i < header.num_slots; ++i) {
            uint16_t offset = 0;
            std::memcpy(&offset, buffer + sizeof(PageHeader) + (i * sizeof(uint16_t)), sizeof(uint16_t));
            if (offset != 0) {
                // In binary format, xmax is at offset + 8
                uint64_t xmax = 0;
                std::memcpy(&xmax, buffer + offset + 8, 8);
                if (xmax == 0) count++;
            }
        }
        bpm_.unpin_page(filename_, page_num, false);
        page_num++;
    }
    return count;
}

bool HeapTable::create() {
    if (!bpm_.open_file(filename_)) {
        return false;
    }

    std::array<char, Page::PAGE_SIZE> buffer{};
    std::memset(buffer.data(), 0, Page::PAGE_SIZE);
    PageHeader header{};
    header.free_space_offset =
        static_cast<uint16_t>(sizeof(PageHeader) + (DEFAULT_SLOT_COUNT * sizeof(uint16_t)));
    header.num_slots = 0;
    std::memcpy(buffer.data(), &header, sizeof(PageHeader));

    return write_page(0, buffer.data());
}

bool HeapTable::drop() {
    static_cast<void>(bpm_.close_file(filename_));
    return (std::remove(filename_.c_str()) == 0);
}

bool HeapTable::read_page(uint32_t page_num, char* buffer) const {
    Page* page = bpm_.fetch_page(filename_, page_num);
    if (!page) {
        return false;
    }
    std::memcpy(buffer, page->get_data(), Page::PAGE_SIZE);
    bpm_.unpin_page(filename_, page_num, false);
    return true;
}

bool HeapTable::write_page(uint32_t page_num, const char* buffer) {
    Page* page = bpm_.fetch_page(filename_, page_num);
    if (!page) {
        page = bpm_.new_page(filename_, &page_num);
        if (!page) {
            return false;
        }
    }
    std::memcpy(page->get_data(), buffer, Page::PAGE_SIZE);
    bpm_.unpin_page(filename_, page_num, true);
    return true;
}

}  // namespace cloudsql::storage
