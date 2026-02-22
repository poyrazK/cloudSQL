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
#include <cstring>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "common/value.hpp"
#include "executor/types.hpp"
#include "storage/storage_manager.hpp"

namespace cloudsql::storage {

namespace {
constexpr uint16_t DEFAULT_SLOT_COUNT = 64;
}  // anonymous namespace

HeapTable::HeapTable(std::string table_name, StorageManager& storage_manager,
                     executor::Schema schema)
    : table_name_(std::move(table_name)),
      filename_(table_name_ + ".heap"),
      storage_manager_(storage_manager),
      schema_(std::move(schema)) {}

/* --- Iterator Implementation --- */

HeapTable::Iterator::Iterator(HeapTable& table)
    : table_(table), next_id_(0, 0), last_id_(0, 0) {}

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
        std::array<char, StorageManager::PAGE_SIZE> buf{};
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
    uint32_t page_num = 0;
    std::array<char, StorageManager::PAGE_SIZE> buffer{};

    while (true) {
        /* Read existing page or initialize a new one */
        if (!read_page(page_num, buffer.data())) {
            std::memset(buffer.data(), 0, StorageManager::PAGE_SIZE);
            PageHeader header{};
            header.free_space_offset = static_cast<uint16_t>(sizeof(PageHeader) + (DEFAULT_SLOT_COUNT * sizeof(uint16_t)));
            header.num_slots = 0;
            std::memcpy(buffer.data(), &header, sizeof(PageHeader));
            static_cast<void>(write_page(page_num, buffer.data()));
        }

        PageHeader header{};
        std::memcpy(&header, buffer.data(), sizeof(PageHeader));
        if (header.free_space_offset == 0) {
            header.free_space_offset = static_cast<uint16_t>(sizeof(PageHeader) + (DEFAULT_SLOT_COUNT * sizeof(uint16_t)));
            header.num_slots = 0;
        }

        /* Serialize tuple data prefixed by MVCC header (xmin|xmax|) */
        std::string data_str = std::to_string(xmin) + "|0|";
        for (const auto& val : tuple.values()) {
            data_str += val.to_string() + "|";
        }

        const auto required = static_cast<uint16_t>(data_str.size() + 1);
        const auto slot_array_end = static_cast<uint16_t>(sizeof(PageHeader) + ((header.num_slots + 1) * sizeof(uint16_t)));

        /* Check for sufficient free space in the current page */
        if (header.free_space_offset + required < StorageManager::PAGE_SIZE &&
            slot_array_end < header.free_space_offset) {
            const uint16_t offset = header.free_space_offset;
            std::memcpy(std::next(buffer.data(), static_cast<std::ptrdiff_t>(offset)), data_str.c_str(), data_str.size() + 1);

            /* Update slot directory */
            std::memcpy(std::next(buffer.data(), static_cast<std::ptrdiff_t>(sizeof(PageHeader) + (header.num_slots * sizeof(uint16_t)))), &offset, sizeof(uint16_t));

            TupleId tid(page_num, header.num_slots);
            header.num_slots++;
            header.free_space_offset += required;

            std::memcpy(buffer.data(), &header, sizeof(PageHeader));
            static_cast<void>(write_page(page_num, buffer.data()));
            return tid;
        }

        /* Page is full; attempt insertion in the next page */
        page_num++;
    }
}

/**
 * @brief Logical deletion: update xmax field in the record blob
 */
bool HeapTable::remove(const TupleId& tuple_id, uint64_t xmax) {
    std::array<char, StorageManager::PAGE_SIZE> buffer{};
    if (!read_page(tuple_id.page_num, buffer.data())) {
        return false;
    }

    PageHeader header{};
    std::memcpy(&header, buffer.data(), sizeof(PageHeader));
    if (header.free_space_offset == 0) {
        return false;
    }
    if (tuple_id.slot_num >= header.num_slots) {
        return false;
    }

    uint16_t offset = 0;
    std::memcpy(&offset, std::next(buffer.data(), static_cast<std::ptrdiff_t>(sizeof(PageHeader) + (tuple_id.slot_num * sizeof(uint16_t)))), sizeof(uint16_t));
    if (offset == 0) {
        return false;
    }

    const char* const data_ptr = std::next(buffer.data(), static_cast<std::ptrdiff_t>(offset));
    const std::string raw_data(data_ptr);

    std::stringstream ss(raw_data);
    std::string segment;
    std::vector<std::string> parts;
    while (std::getline(ss, segment, '|')) {
        parts.push_back(segment);
    }

    if (parts.size() < 2) {
        return false;
    }

    /* Update xmax field */
    parts[1] = std::to_string(xmax);

    /* Reconstruct record blob */
    std::string new_data;
    for (const auto& p : parts) {
        new_data += p + "|";
    }

    const auto old_len = raw_data.size() + 1;
    const auto new_len = new_data.size() + 1;

    if (new_len <= old_len) {
        std::memcpy(std::next(buffer.data(), static_cast<std::ptrdiff_t>(offset)), new_data.c_str(), new_len);
        return write_page(tuple_id.page_num, buffer.data());
    }

    /* Reorganize page to accommodate potentially longer xmax string */
    std::vector<std::string> all_tuples;
    for (uint16_t i = 0; i < header.num_slots; ++i) {
        uint16_t slot_off = 0;
        std::memcpy(&slot_off, std::next(buffer.data(), static_cast<std::ptrdiff_t>(sizeof(PageHeader) + (i * sizeof(uint16_t)))), sizeof(uint16_t));
        if (slot_off == 0) {
            all_tuples.emplace_back("");
            continue;
        }
        if (i == tuple_id.slot_num) {
            all_tuples.push_back(new_data);
        } else {
            all_tuples.emplace_back(std::next(buffer.data(), static_cast<std::ptrdiff_t>(slot_off)));
        }
    }

    std::memset(buffer.data(), 0, StorageManager::PAGE_SIZE);
    header.free_space_offset = static_cast<uint16_t>(sizeof(PageHeader) + (DEFAULT_SLOT_COUNT * sizeof(uint16_t)));
    header.num_slots = 0;

    for (const auto& t_data : all_tuples) {
        if (t_data.empty()) {
            const uint16_t zero = 0;
            std::memcpy(std::next(buffer.data(), static_cast<std::ptrdiff_t>(sizeof(PageHeader) + (header.num_slots * sizeof(uint16_t)))), &zero, sizeof(uint16_t));
            header.num_slots++;
            continue;
        }

        const auto req = static_cast<uint16_t>(t_data.size() + 1);
        if (header.free_space_offset + req > StorageManager::PAGE_SIZE) {
            return false;
        }

        const uint16_t off = header.free_space_offset;
        std::memcpy(std::next(buffer.data(), static_cast<std::ptrdiff_t>(off)), t_data.c_str(), req);
        std::memcpy(std::next(buffer.data(), static_cast<std::ptrdiff_t>(sizeof(PageHeader) + (header.num_slots * sizeof(uint16_t)))), &off, sizeof(uint16_t));
        header.num_slots++;
        header.free_space_offset += req;
    }

    std::memcpy(buffer.data(), &header, sizeof(PageHeader));
    return write_page(tuple_id.page_num, buffer.data());
}

/**
 * @brief Physical deletion: zero out slot offset (rollback only)
 */
bool HeapTable::physical_remove(const TupleId& tuple_id) {
    std::array<char, StorageManager::PAGE_SIZE> buffer{};
    if (!read_page(tuple_id.page_num, buffer.data())) {
        return false;
    }

    PageHeader header{};
    std::memcpy(&header, buffer.data(), sizeof(PageHeader));
    if (header.free_space_offset == 0) {
        return false;
    }
    if (tuple_id.slot_num >= header.num_slots) {
        return false;
    }

    const uint16_t zero = 0;
    std::memcpy(std::next(buffer.data(), static_cast<std::ptrdiff_t>(sizeof(PageHeader) + (tuple_id.slot_num * sizeof(uint16_t)))), &zero, sizeof(uint16_t));

    return write_page(tuple_id.page_num, buffer.data());
}

bool HeapTable::update(const TupleId& tuple_id, const executor::Tuple& tuple, uint64_t txn_id) {
    if (!remove(tuple_id, txn_id)) {
        return false;
    }
    static_cast<void>(insert(tuple, txn_id));
    return true;
}

bool HeapTable::get_meta(const TupleId& tuple_id, TupleMeta& out_meta) const {
    std::array<char, StorageManager::PAGE_SIZE> buffer{};
    if (!read_page(tuple_id.page_num, buffer.data())) {
        return false;
    }

    PageHeader header{};
    std::memcpy(&header, buffer.data(), sizeof(PageHeader));
    if (header.free_space_offset == 0) {
        return false;
    }
    if (tuple_id.slot_num >= header.num_slots) {
        return false;
    }

    uint16_t offset = 0;
    std::memcpy(&offset, std::next(buffer.data(), static_cast<std::ptrdiff_t>(sizeof(PageHeader) + (tuple_id.slot_num * sizeof(uint16_t)))), sizeof(uint16_t));
    if (offset == 0) {
        return false;
    }

    const char* const data = std::next(buffer.data(), static_cast<std::ptrdiff_t>(offset));
    const std::string s(data);
    std::stringstream ss(s);
    std::string item;

    /* Parse MVCC Header */
    if (!std::getline(ss, item, '|')) {
        return false;
    }
    try {
        out_meta.xmin = std::stoull(item);
    } catch (...) {
        out_meta.xmin = 0;
    }

    if (!std::getline(ss, item, '|')) {
        return false;
    }
    try {
        out_meta.xmax = std::stoull(item);
    } catch (...) {
        out_meta.xmax = 0;
    }

    /* Parse Column Values */
    std::vector<common::Value> values;
    values.reserve(schema_.column_count());
    for (size_t i = 0; i < schema_.column_count(); ++i) {
        if (!std::getline(ss, item, '|')) {
            break;
        }

        const auto& col = schema_.get_column(i);
        try {
            switch (col.type()) {
                case common::ValueType::TYPE_INT8:
                case common::ValueType::TYPE_INT16:
                case common::ValueType::TYPE_INT32:
                case common::ValueType::TYPE_INT64:
                    values.push_back(common::Value::make_int64(std::stoll(item)));
                    break;
                case common::ValueType::TYPE_FLOAT32:
                case common::ValueType::TYPE_FLOAT64:
                    values.push_back(common::Value::make_float64(std::stod(item)));
                    break;
                case common::ValueType::TYPE_BOOL:
                    values.push_back(common::Value::make_bool(item == "TRUE" || item == "1"));
                    break;
                default:
                    values.push_back(common::Value::make_text(item));
                    break;
            }
        } catch (...) {
            values.push_back(common::Value::make_null());
        }
    }

    out_meta.tuple = executor::Tuple(std::move(values));
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
    std::array<char, StorageManager::PAGE_SIZE> buffer{};
    while (read_page(page_num, buffer.data())) {
        PageHeader header{};
        std::memcpy(&header, buffer.data(), sizeof(PageHeader));
        if (header.free_space_offset == 0) {
            break;
        }

        for (uint16_t i = 0; i < header.num_slots; ++i) {
            TupleMeta meta;
            if (get_meta(TupleId(page_num, i), meta)) {
                if (meta.xmax == 0) {
                    count++;
                }
            }
        }
        page_num++;
    }
    return count;
}

bool HeapTable::create() {
    if (!storage_manager_.open_file(filename_)) {
        return false;
    }

    std::array<char, StorageManager::PAGE_SIZE> buffer{};
    std::memset(buffer.data(), 0, StorageManager::PAGE_SIZE);
    PageHeader header{};
    header.free_space_offset = static_cast<uint16_t>(sizeof(PageHeader) + (DEFAULT_SLOT_COUNT * sizeof(uint16_t)));
    header.num_slots = 0;
    std::memcpy(buffer.data(), &header, sizeof(PageHeader));

    return write_page(0, buffer.data());
}

bool HeapTable::drop() {
    return storage_manager_.close_file(filename_);
}

bool HeapTable::read_page(uint32_t page_num, char* buffer) const {
    return storage_manager_.read_page(filename_, page_num, buffer);
}

bool HeapTable::write_page(uint32_t page_num, const char* buffer) {
    return storage_manager_.write_page(filename_, page_num, buffer);
}


}  // namespace cloudsql::storage
