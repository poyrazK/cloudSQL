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
      last_page_id_(0) {
    file_id_ = bpm_.get_file_id(filename_);
}

HeapTable::~HeapTable() {
    // Note: In some tests, the BufferPoolManager might be destroyed before the HeapTable
    // causing this to potentially access a dangling reference if we are not careful.
    if (cached_page_ != nullptr) {
        try {
            bpm_.unpin_page_by_id(file_id_, cached_page_id_, true);
        } catch (...) {
            // Ignore errors during destruction if BPM is already gone
        }
        cached_page_ = nullptr;
    }
}

/* --- Iterator Implementation --- */

common::Value HeapTable::TupleView::get_value(size_t col_index) const {
    if (materialized_tuple) {
        if (col_index < materialized_tuple->values().size()) {
            return materialized_tuple->values()[col_index];
        }
        return common::Value::make_null();
    }

    if (!payload_data || !table_schema) return common::Value::make_null();

    // Finding 4: Translate logical index to physical index via mapping
    size_t target_physical_index = col_index;
    if (column_mapping) {
        if (col_index >= column_mapping->size()) return common::Value::make_null();
        target_physical_index = (*column_mapping)[col_index];
    }

    size_t cursor = 0;
    const auto& columns = table_schema->columns();
    for (size_t i = 0; i < columns.size(); ++i) {
        const auto& col = columns[i];
        common::ValueType type = col.type();

        if (type == common::ValueType::TYPE_NULL) {
            if (i == target_physical_index) return common::Value::make_null();
            continue;
        }

        if (type != common::ValueType::TYPE_TEXT && type != common::ValueType::TYPE_VARCHAR) {
            if (cursor + 8 > payload_len) return common::Value::make_null();

            if (i == target_physical_index) {
                if (type == common::ValueType::TYPE_FLOAT32 ||
                    type == common::ValueType::TYPE_FLOAT64) {
                    double v;
                    std::memcpy(&v, payload_data + cursor, 8);
                    return common::Value::make_float64(v);
                } else {
                    int64_t v;
                    std::memcpy(&v, payload_data + cursor, 8);
                    if (type == common::ValueType::TYPE_BOOL)
                        return common::Value::make_bool(v != 0);
                    else
                        return common::Value::make_int64(v);
                }
            }
            cursor += 8;
        } else {
            // Text-based
            if (cursor + 4 > payload_len) return common::Value::make_null();
            uint32_t len;
            std::memcpy(&len, payload_data + cursor, 4);
            cursor += 4;

            if (cursor + len > payload_len) return common::Value::make_null();

            if (i == target_physical_index) {
                std::string s(reinterpret_cast<const char*>(payload_data + cursor), len);
                return common::Value::make_text(s);
            }
            cursor += len;
        }
        
        if (i >= target_physical_index) break;
    }
    return common::Value::make_null();
}

HeapTable::Iterator::Iterator(HeapTable& table, std::pmr::memory_resource* mr)
    : table_(table),
      next_id_(0, 0),
      last_id_(0, 0),
      eof_(false),
      mr_(mr ? mr : std::pmr::new_delete_resource()) {}

HeapTable::Iterator::~Iterator() {
    if (current_page_) {
        table_.bpm_.unpin_page_by_id(table_.file_id_, current_page_num_, false);
    }
}

HeapTable::Iterator::Iterator(Iterator&& other) noexcept
    : table_(other.table_),
      next_id_(other.next_id_),
      last_id_(other.last_id_),
      eof_(other.eof_),
      mr_(other.mr_),
      current_page_(other.current_page_),
      current_page_num_(other.current_page_num_),
      cached_buffer_(other.cached_buffer_),
      cached_header_(other.cached_header_) {
    other.current_page_ = nullptr;
    other.cached_buffer_ = nullptr;
}

HeapTable::Iterator& HeapTable::Iterator::operator=(Iterator&& other) noexcept {
    if (this != &other) {
        if (&table_ != &other.table_) {
            if (other.current_page_) {
                other.table_.bpm_.unpin_page_by_id(other.table_.file_id_, other.current_page_num_,
                                                   false);
                other.current_page_ = nullptr;
            }
            return *this;
        }

        if (current_page_) {
            table_.bpm_.unpin_page_by_id(table_.file_id_, current_page_num_, false);
        }
        next_id_ = other.next_id_;
        last_id_ = other.last_id_;
        eof_ = other.eof_;
        mr_ = other.mr_;
        current_page_ = other.current_page_;
        current_page_num_ = other.current_page_num_;
        cached_buffer_ = other.cached_buffer_;
        cached_header_ = other.cached_header_;
        other.current_page_ = nullptr;
        other.cached_buffer_ = nullptr;
    }
    return *this;
}

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
        if (!current_page_) {
            current_page_ =
                table_.bpm_.fetch_page_by_id(table_.file_id_, table_.filename_, next_id_.page_num);
            current_page_num_ = next_id_.page_num;
            if (!current_page_) {
                eof_ = true;
                return false;
            }

            // Cache page header and buffer pointer (Phase 2 optimization)
            cached_buffer_ = reinterpret_cast<const uint8_t*>(current_page_->get_data());
            std::memcpy(&cached_header_, cached_buffer_, sizeof(PageHeader));
        }

        if (cached_header_.free_space_offset == 0) {
            table_.bpm_.unpin_page_by_id(table_.file_id_, current_page_num_, false);
            current_page_ = nullptr;
            cached_buffer_ = nullptr;
            eof_ = true;
            return false;
        }

        /* Scan slots in the current page starting from next_id_.slot_num */
        while (next_id_.slot_num < cached_header_.num_slots) {
            uint16_t offset = 0;
            std::memcpy(
                &offset,
                cached_buffer_ + sizeof(PageHeader) + (next_id_.slot_num * sizeof(uint16_t)),
                sizeof(uint16_t));

            if (offset != 0) {
                /* Found a record: Deserialize it in-place from the pinned buffer */
                const uint8_t* const data = cached_buffer_ + offset;

                // Read Tuple Length (first 2 bytes)
                uint16_t tuple_data_len;
                std::memcpy(&tuple_data_len, data, 2);

                const size_t record_len = static_cast<size_t>(tuple_data_len);
                if (record_len < 18) {  // 2 len + 8 xmin + 8 xmax
                    table_.bpm_.unpin_page_by_id(table_.file_id_, current_page_num_, false);
                    current_page_ = nullptr;
                    return false;
                }

                // Read MVCC Header
                std::memcpy(&out_meta.xmin, data + 2, 8);
                std::memcpy(&out_meta.xmax, data + 10, 8);

                size_t cursor = 18;
                std::pmr::vector<common::Value> values(mr_);
                values.reserve(table_.schema_.column_count());

                for (size_t i = 0; i < table_.schema_.column_count(); ++i) {
                    if (cursor >= record_len) break;
                    auto type = static_cast<common::ValueType>(data[cursor++]);
                    if (type == common::ValueType::TYPE_NULL) {
                        values.push_back(common::Value::make_null());
                        continue;
                    }

                    if (type == common::ValueType::TYPE_BOOL ||
                        type == common::ValueType::TYPE_INT8 ||
                        type == common::ValueType::TYPE_INT16 ||
                        type == common::ValueType::TYPE_INT32 ||
                        type == common::ValueType::TYPE_INT64 ||
                        type == common::ValueType::TYPE_FLOAT32 ||
                        type == common::ValueType::TYPE_FLOAT64) {
                        if (cursor + 8 > record_len) break;

                        if (type == common::ValueType::TYPE_FLOAT32 ||
                            type == common::ValueType::TYPE_FLOAT64) {
                            double v;
                            std::memcpy(&v, data + cursor, 8);
                            values.push_back(common::Value::make_float64(v));
                        } else {
                            int64_t v;
                            std::memcpy(&v, data + cursor, 8);
                            if (type == common::ValueType::TYPE_BOOL)
                                values.push_back(common::Value::make_bool(v != 0));
                            else
                                values.push_back(common::Value::make_int64(v));
                        }
                        cursor += 8;
                    } else {
                        if (cursor + 4 > record_len) break;
                        uint32_t len;
                        std::memcpy(&len, data + cursor, 4);
                        cursor += 4;
                        if (cursor + len > record_len) break;
                        std::string s(reinterpret_cast<const char*>(data + cursor), len);
                        cursor += len;
                        values.push_back(common::Value::make_text(s));
                    }
                }

                out_meta.tuple = executor::Tuple(std::move(values));
                last_id_ = next_id_;
                next_id_.slot_num++;
                // Do not unpin here so the page is reused for the next record
                return true;
            }
            next_id_.slot_num++;
        }

        /* Move to the beginning of the next physical page */
        table_.bpm_.unpin_page_by_id(table_.file_id_, current_page_num_, false);
        current_page_ = nullptr;
        next_id_.page_num++;
        next_id_.slot_num = 0;
    }
}

/* --- HeapTable Methods --- */

HeapTable::TupleId HeapTable::insert(const executor::Tuple& tuple, uint64_t xmin) {
    /* Optimization: Use stack buffer for serialization to avoid heap allocations */
    std::array<uint8_t, 1024> stack_buf{};
    std::vector<uint8_t> heap_payload;
    uint8_t* payload_ptr = stack_buf.data();
    size_t payload_capacity = stack_buf.size();
    size_t payload_size = 0;

    auto ensure_capacity = [&](size_t needed) {
        if (payload_size + needed > payload_capacity) {
            if (heap_payload.empty()) {
                heap_payload.assign(stack_buf.begin(), stack_buf.begin() + payload_size);
            }
            heap_payload.resize(payload_size + needed + 256);
            payload_ptr = heap_payload.data();
            payload_capacity = heap_payload.size();
        }
    };

    uint64_t xmax = 0;
    payload_size = 18;  // 2 len + 8 xmin + 8 xmax
    // placeholder for length
    std::memset(payload_ptr, 0, 2);
    std::memcpy(payload_ptr + 2, &xmin, 8);
    std::memcpy(payload_ptr + 10, &xmax, 8);

    for (const auto& val : tuple.values()) {
        ensure_capacity(1);
        auto type = static_cast<uint8_t>(val.type());
        payload_ptr[payload_size++] = type;
        if (val.is_null()) continue;

        if (val.is_numeric()) {
            ensure_capacity(8);
            if (val.is_integer()) {
                int64_t v = val.to_int64();
                std::memcpy(payload_ptr + payload_size, &v, 8);
            } else {
                double v = val.to_float64();
                std::memcpy(payload_ptr + payload_size, &v, 8);
            }
            payload_size += 8;
        } else if (val.type() == common::ValueType::TYPE_BOOL) {
            ensure_capacity(8);
            int64_t v = val.as_bool() ? 1 : 0;
            std::memcpy(payload_ptr + payload_size, &v, 8);
            payload_size += 8;
        } else {
            const std::string& s = val.as_text();
            uint32_t len = static_cast<uint32_t>(s.size());
            ensure_capacity(4 + len);
            std::memcpy(payload_ptr + payload_size, &len, 4);
            std::memcpy(payload_ptr + payload_size + 4, s.data(), len);
            payload_size += 4 + len;
        }
    }

    const auto required = static_cast<uint16_t>(payload_size);
    std::memcpy(payload_ptr, &required, 2);  // set final length

    while (true) {
        // Use cached page if available
        if (cached_page_ == nullptr || cached_page_id_ != last_page_id_) {
            if (cached_page_ != nullptr) {
                bpm_.unpin_page_by_id(file_id_, cached_page_id_, true);
            }
            cached_page_id_ = last_page_id_;
            cached_page_ = bpm_.fetch_page_by_id(file_id_, filename_, cached_page_id_);
            if (!cached_page_) {
                cached_page_ = bpm_.new_page(filename_, &cached_page_id_);
                if (!cached_page_) {
                    return {0, 0};  // Buffer pool full or allocation failed
                }
                last_page_id_ = cached_page_id_;
            }
        }

        auto* buffer = cached_page_->get_data();
        PageHeader header{};
        std::memcpy(&header, buffer, sizeof(PageHeader));

        // Initialize header if it's a new page
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
            std::memcpy(buffer + offset, payload_ptr, payload_size);

            /* Update slot directory */
            std::memcpy(buffer + sizeof(PageHeader) + (header.num_slots * sizeof(uint16_t)),
                        &offset, sizeof(uint16_t));

            TupleId tid(cached_page_id_, header.num_slots);
            header.num_slots++;
            header.free_space_offset += required;

            std::memcpy(buffer, &header, sizeof(PageHeader));
            // Keep page pinned for next insertion
            return tid;
        }

        /* Page is full; unpin and move to next */
        bpm_.unpin_page_by_id(file_id_, cached_page_id_, true);
        cached_page_ = nullptr;
        last_page_id_++;
    }
}

/**
 * @brief Logical deletion: update xmax field in the record blob
 */
bool HeapTable::remove(const TupleId& tuple_id, uint64_t xmax) {
    // If target page is currently cached, we must use it or flush it
    if (cached_page_ != nullptr && cached_page_id_ == tuple_id.page_num) {
        auto* buffer = cached_page_->get_data();
        PageHeader header{};
        std::memcpy(&header, buffer, sizeof(PageHeader));

        uint16_t offset = 0;
        std::memcpy(&offset, buffer + sizeof(PageHeader) + (tuple_id.slot_num * sizeof(uint16_t)),
                    sizeof(uint16_t));
        if (offset != 0) {
            std::memcpy(buffer + offset + 10, &xmax, 8);
            return true;
        }
        return false;
    }

    Page* page = bpm_.fetch_page_by_id(file_id_, filename_, tuple_id.page_num);
    if (!page) return false;

    auto* buffer = page->get_data();
    PageHeader header{};
    std::memcpy(&header, buffer, sizeof(PageHeader));
    if (header.free_space_offset == 0 || tuple_id.slot_num >= header.num_slots) {
        bpm_.unpin_page_by_id(file_id_, tuple_id.page_num, false);
        return false;
    }

    uint16_t offset = 0;
    std::memcpy(&offset, buffer + sizeof(PageHeader) + (tuple_id.slot_num * sizeof(uint16_t)),
                sizeof(uint16_t));
    if (offset == 0) {
        bpm_.unpin_page_by_id(file_id_, tuple_id.page_num, false);
        return false;
    }

    /* In binary format, xmax is at offset + 10 (2 len + 8 xmin) */
    std::memcpy(buffer + offset + 10, &xmax, 8);

    bpm_.unpin_page_by_id(file_id_, tuple_id.page_num, true);
    return true;
}

/**
 * @brief Physical deletion: zero out slot offset (rollback only)
 */
bool HeapTable::physical_remove(const TupleId& tuple_id) {
    if (cached_page_ != nullptr && cached_page_id_ == tuple_id.page_num) {
        auto* buffer = cached_page_->get_data();
        const uint16_t zero = 0;
        std::memcpy(buffer + sizeof(PageHeader) + (tuple_id.slot_num * sizeof(uint16_t)), &zero,
                    sizeof(uint16_t));
        return true;
    }

    Page* page = bpm_.fetch_page_by_id(file_id_, filename_, tuple_id.page_num);
    if (!page) return false;

    auto* buffer = page->get_data();
    PageHeader header{};
    std::memcpy(&header, buffer, sizeof(PageHeader));
    if (header.free_space_offset == 0 || tuple_id.slot_num >= header.num_slots) {
        bpm_.unpin_page_by_id(file_id_, tuple_id.page_num, false);
        return false;
    }

    const uint16_t zero = 0;
    std::memcpy(buffer + sizeof(PageHeader) + (tuple_id.slot_num * sizeof(uint16_t)), &zero,
                sizeof(uint16_t));

    bpm_.unpin_page_by_id(file_id_, tuple_id.page_num, true);
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
    if (cached_page_ != nullptr && cached_page_id_ == tuple_id.page_num) {
        auto* buffer = cached_page_->get_data();
        uint16_t offset = 0;
        std::memcpy(&offset, buffer + sizeof(PageHeader) + (tuple_id.slot_num * sizeof(uint16_t)),
                    sizeof(uint16_t));
        if (offset == 0) return false;

        const uint8_t* const data = reinterpret_cast<const uint8_t*>(buffer + offset);

        uint16_t tuple_data_len;
        std::memcpy(&tuple_data_len, data, 2);
        const size_t record_len = static_cast<size_t>(tuple_data_len);
        if (record_len < 18) return false;

        std::memcpy(&out_meta.xmin, data + 2, 8);
        std::memcpy(&out_meta.xmax, data + 10, 8);

        size_t cursor = 18;
        std::vector<common::Value> values;
        values.reserve(schema_.column_count());

        for (size_t i = 0; i < schema_.column_count(); ++i) {
            if (cursor >= record_len) break;
            auto type = static_cast<common::ValueType>(data[cursor++]);
            if (type == common::ValueType::TYPE_NULL) {
                values.push_back(common::Value::make_null());
                continue;
            }

            if (type == common::ValueType::TYPE_BOOL || type == common::ValueType::TYPE_INT8 ||
                type == common::ValueType::TYPE_INT16 || type == common::ValueType::TYPE_INT32 ||
                type == common::ValueType::TYPE_INT64 || type == common::ValueType::TYPE_FLOAT32 ||
                type == common::ValueType::TYPE_FLOAT64) {
                if (cursor + 8 > record_len) break;
                if (type == common::ValueType::TYPE_FLOAT32 ||
                    type == common::ValueType::TYPE_FLOAT64) {
                    double v;
                    std::memcpy(&v, data + cursor, 8);
                    values.push_back(common::Value::make_float64(v));
                } else {
                    int64_t v;
                    std::memcpy(&v, data + cursor, 8);
                    if (type == common::ValueType::TYPE_BOOL)
                        values.push_back(common::Value::make_bool(v != 0));
                    else
                        values.push_back(common::Value::make_int64(v));
                }
                cursor += 8;
            } else {
                if (cursor + 4 > record_len) break;
                uint32_t len;
                std::memcpy(&len, data + cursor, 4);
                cursor += 4;
                if (cursor + len > record_len) break;
                std::string s(reinterpret_cast<const char*>(data + cursor), len);
                cursor += len;
                values.push_back(common::Value::make_text(s));
            }
        }
        out_meta.tuple = executor::Tuple(std::move(values));
        return true;
    }

    Page* page = bpm_.fetch_page_by_id(file_id_, filename_, tuple_id.page_num);
    if (!page) return false;

    auto* buffer = page->get_data();
    PageHeader header{};
    std::memcpy(&header, buffer, sizeof(PageHeader));
    if (header.free_space_offset == 0 || tuple_id.slot_num >= header.num_slots) {
        bpm_.unpin_page_by_id(file_id_, tuple_id.page_num, false);
        return false;
    }

    uint16_t offset = 0;
    std::memcpy(&offset, buffer + sizeof(PageHeader) + (tuple_id.slot_num * sizeof(uint16_t)),
                sizeof(uint16_t));
    if (offset == 0) {
        bpm_.unpin_page_by_id(file_id_, tuple_id.page_num, false);
        return false;
    }

    const uint8_t* const data = reinterpret_cast<const uint8_t*>(buffer + offset);

    uint16_t tuple_data_len;
    std::memcpy(&tuple_data_len, data, 2);
    const size_t record_len = static_cast<size_t>(tuple_data_len);
    if (record_len < 18) {
        bpm_.unpin_page_by_id(file_id_, tuple_id.page_num, false);
        return false;
    }

    // Read MVCC Header
    std::memcpy(&out_meta.xmin, data + 2, 8);
    std::memcpy(&out_meta.xmax, data + 10, 8);

    size_t cursor = 18;
    std::vector<common::Value> values;
    values.reserve(schema_.column_count());

    for (size_t i = 0; i < schema_.column_count(); ++i) {
        if (cursor >= record_len) break;
        auto type = static_cast<common::ValueType>(data[cursor++]);
        if (type == common::ValueType::TYPE_NULL) {
            values.push_back(common::Value::make_null());
            continue;
        }

        if (type == common::ValueType::TYPE_BOOL || type == common::ValueType::TYPE_INT8 ||
            type == common::ValueType::TYPE_INT16 || type == common::ValueType::TYPE_INT32 ||
            type == common::ValueType::TYPE_INT64 || type == common::ValueType::TYPE_FLOAT32 ||
            type == common::ValueType::TYPE_FLOAT64) {
            if (cursor + 8 > record_len) break;

            if (type == common::ValueType::TYPE_FLOAT32 ||
                type == common::ValueType::TYPE_FLOAT64) {
                double v;
                std::memcpy(&v, data + cursor, 8);
                values.push_back(common::Value::make_float64(v));
            } else {
                int64_t v;
                std::memcpy(&v, data + cursor, 8);
                if (type == common::ValueType::TYPE_BOOL)
                    values.push_back(common::Value::make_bool(v != 0));
                else
                    values.push_back(common::Value::make_int64(v));
            }
            cursor += 8;
        } else {
            if (cursor + 4 > record_len) break;
            uint32_t len;
            std::memcpy(&len, data + cursor, 4);
            cursor += 4;
            if (cursor + len > record_len) break;
            std::string s(reinterpret_cast<const char*>(data + cursor), len);
            cursor += len;
            values.push_back(common::Value::make_text(s));
        }
    }

    out_meta.tuple = executor::Tuple(std::move(values));
    bpm_.unpin_page_by_id(file_id_, tuple_id.page_num, false);
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
        Page* page = bpm_.fetch_page_by_id(file_id_, filename_, page_num);
        if (!page) break;

        auto* buffer = page->get_data();
        PageHeader header{};
        std::memcpy(&header, buffer, sizeof(PageHeader));
        if (header.free_space_offset == 0) {
            bpm_.unpin_page_by_id(file_id_, page_num, false);
            break;
        }

        for (uint16_t i = 0; i < header.num_slots; ++i) {
            uint16_t offset = 0;
            std::memcpy(&offset, buffer + sizeof(PageHeader) + (i * sizeof(uint16_t)),
                        sizeof(uint16_t));
            if (offset != 0) {
                uint64_t xmax = 0;
                std::memcpy(&xmax, buffer + offset + 10, 8);  // 2 len + 8 xmin
                if (xmax == 0) count++;
            }
        }
        bpm_.unpin_page_by_id(file_id_, page_num, false);
        page_num++;
    }
    return count;
}

bool HeapTable::create() {
    if (!bpm_.open_file(filename_)) {
        return false;
    }

    uint32_t page_num = 0;
    Page* page = bpm_.new_page(filename_, &page_num);
    if (!page) return false;

    auto* buffer = page->get_data();
    std::memset(buffer, 0, Page::PAGE_SIZE);
    PageHeader header{};
    header.free_space_offset =
        static_cast<uint16_t>(sizeof(PageHeader) + (DEFAULT_SLOT_COUNT * sizeof(uint16_t)));
    header.num_slots = 0;
    std::memcpy(buffer, &header, sizeof(PageHeader));

    bpm_.unpin_page_by_id(file_id_, page_num, true);
    last_page_id_ = 0;
    return true;
}

bool HeapTable::drop() {
    if (cached_page_ != nullptr) {
        bpm_.unpin_page_by_id(file_id_, cached_page_id_, false);
        cached_page_ = nullptr;
    }
    static_cast<void>(bpm_.close_file(filename_));
    return (std::remove(filename_.c_str()) == 0);
}

bool HeapTable::read_page(uint32_t page_num, char* buffer) const {
    if (cached_page_ != nullptr && cached_page_id_ == page_num) {
        std::memcpy(buffer, cached_page_->get_data(), Page::PAGE_SIZE);
        return true;
    }
    Page* page = bpm_.fetch_page_by_id(file_id_, filename_, page_num);
    if (!page) return false;
    std::memcpy(buffer, page->get_data(), Page::PAGE_SIZE);
    bpm_.unpin_page_by_id(file_id_, page_num, false);
    return true;
}

bool HeapTable::write_page(uint32_t page_num, const char* buffer) {
    if (cached_page_ != nullptr && cached_page_id_ == page_num) {
        std::memcpy(cached_page_->get_data(), buffer, Page::PAGE_SIZE);
        return true;
    }
    Page* page = bpm_.fetch_page_by_id(file_id_, filename_, page_num);
    if (!page) {
        page = bpm_.new_page(filename_, &page_num);
        if (!page) return false;
    }
    std::memcpy(page->get_data(), buffer, Page::PAGE_SIZE);
    bpm_.unpin_page_by_id(file_id_, page_num, true);
    return true;
}

bool HeapTable::Iterator::next_view(TupleView& out_view) {
    if (eof_) {
        return false;
    }

    while (true) {
        if (!current_page_) {
            current_page_ =
                table_.bpm_.fetch_page_by_id(table_.file_id_, table_.filename_, next_id_.page_num);
            current_page_num_ = next_id_.page_num;
            if (!current_page_) {
                eof_ = true;
                return false;
            }

            // Cache page header and buffer pointer (Phase 2 optimization)
            cached_buffer_ = reinterpret_cast<const uint8_t*>(current_page_->get_data());
            std::memcpy(&cached_header_, cached_buffer_, sizeof(PageHeader));
        }

        if (cached_header_.free_space_offset == 0) {
            table_.bpm_.unpin_page_by_id(table_.file_id_, current_page_num_, false);
            current_page_ = nullptr;
            cached_buffer_ = nullptr;
            eof_ = true;
            return false;
        }

        /* Scan slots in the current page starting from next_id_.slot_num */
        while (next_id_.slot_num < cached_header_.num_slots) {
            uint16_t offset = 0;
            std::memcpy(
                &offset,
                cached_buffer_ + sizeof(PageHeader) + (next_id_.slot_num * sizeof(uint16_t)),
                sizeof(uint16_t));

            if (offset != 0) {
                const uint8_t* const data = cached_buffer_ + offset;

                // Read Tuple Length (first 2 bytes)
                uint16_t tuple_data_len;
                std::memcpy(&tuple_data_len, data, 2);

                const size_t record_len = static_cast<size_t>(tuple_data_len);
                if (record_len < 18) {  // 2 len + 8 xmin + 8 xmax
                    std::cerr << "next_view failed: record_len < 18, it is " << record_len << "\n";
                    table_.bpm_.unpin_page_by_id(table_.file_id_, current_page_num_, false);
                    current_page_ = nullptr;
                    cached_buffer_ = nullptr;
                    return false;
                }

                // Read MVCC Header
                std::memcpy(&out_view.xmin, data + 2, 8);
                std::memcpy(&out_view.xmax, data + 10, 8);

                out_view.table_schema = &table_.schema_;
                out_view.payload_data = data + 18;
                out_view.payload_len = record_len - 18;

                last_id_ = next_id_;
                next_id_.slot_num++;
                // Do not unpin here so the page is reused for the next record
                return true;
            }
            next_id_.slot_num++;
        }

        /* Move to the next page */
        table_.bpm_.unpin_page_by_id(table_.file_id_, current_page_num_, false);
        current_page_ = nullptr;
        cached_buffer_ = nullptr;

        next_id_.page_num++;
        next_id_.slot_num = 0;
    }
}


executor::Tuple HeapTable::TupleView::materialize(std::pmr::memory_resource* mr) const {
    if (materialized_tuple) {
        return *materialized_tuple;
    }
    
    const executor::Schema* output_schema = schema ? schema : table_schema;
    if (!output_schema) return executor::Tuple();
    
    std::pmr::vector<common::Value> values(mr ? mr : std::pmr::get_default_resource());
    values.reserve(output_schema->column_count());
    
    for (size_t i = 0; i < output_schema->column_count(); ++i) {
        values.push_back(get_value(i));
    }
    
    return executor::Tuple(std::move(values));
}
}  // namespace cloudsql::storage
