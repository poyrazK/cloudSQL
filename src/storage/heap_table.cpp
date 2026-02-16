/**
 * @file heap_table.cpp
 * @brief Heap table storage implementation with MVCC support
 *
 * @defgroup storage Storage Engine
 * @{
 */

#include "storage/heap_table.hpp"

#include <cstring>
#include <iostream>
#include <sstream>

namespace cloudsql {
namespace storage {

HeapTable::HeapTable(std::string table_name, StorageManager& storage_manager,
                     executor::Schema schema)
    : table_name_(std::move(table_name)),
      filename_(table_name_ + ".heap"),
      storage_manager_(storage_manager),
      schema_(std::move(schema)) {}

/* --- Iterator Implementation --- */

HeapTable::Iterator::Iterator(HeapTable& table)
    : table_(table), next_id_(0, 0), last_id_(0, 0), eof_(false) {}

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
    if (eof_) return false;

    while (true) {
        if (table_.get_meta(next_id_, out_meta)) {
            /* Record successfully retrieved */
            last_id_ = next_id_;

            /* Prepare for next call: advance slot index */
            next_id_.slot_num++;
            return true;
        }

        /* Check if the current page has more slots to explore */
        char buf[StorageManager::PAGE_SIZE];
        if (table_.read_page(next_id_.page_num, buf)) {
            PageHeader* header = reinterpret_cast<PageHeader*>(buf);
            if (next_id_.slot_num < header->num_slots) {
                /* Current slot is empty/deleted; skip to the next */
                next_id_.slot_num++;
                continue;
            }
        }

        /* Move to the beginning of the next physical page */
        next_id_.page_num++;
        next_id_.slot_num = 0;

        /* If the next page cannot be read, end of file is reached */
        if (!table_.read_page(next_id_.page_num, buf)) {
            eof_ = true;
            return false;
        }

        /* Validate that the page has been initialized */
        PageHeader* next_header = reinterpret_cast<PageHeader*>(buf);
        if (next_header->free_space_offset == 0) {
            eof_ = true;
            return false;
        }
    }
}

/* --- HeapTable Methods --- */

HeapTable::TupleId HeapTable::insert(const executor::Tuple& tuple, uint64_t xmin) {
    uint32_t page_num = 0;
    char buffer[StorageManager::PAGE_SIZE];

    while (true) {
        /* Read existing page or initialize a new one */
        if (!read_page(page_num, buffer)) {
            std::memset(buffer, 0, StorageManager::PAGE_SIZE);
            PageHeader* header = reinterpret_cast<PageHeader*>(buffer);
            header->free_space_offset = sizeof(PageHeader) + (64 * sizeof(uint16_t));
            header->num_slots = 0;
            write_page(page_num, buffer);
        }

        PageHeader* header = reinterpret_cast<PageHeader*>(buffer);
        if (header->free_space_offset == 0) {
            header->free_space_offset = sizeof(PageHeader) + (64 * sizeof(uint16_t));
            header->num_slots = 0;
        }

        /* Serialize tuple data prefixed by MVCC header (xmin|xmax|) */
        std::string data = std::to_string(xmin) + "|0|";
        for (const auto& val : tuple.values()) {
            data += val.to_string() + "|";
        }

        uint16_t required = static_cast<uint16_t>(data.size() + 1);
        uint16_t slot_array_end = sizeof(PageHeader) + ((header->num_slots + 1) * sizeof(uint16_t));

        /* Check for sufficient free space in the current page */
        if (header->free_space_offset + required < StorageManager::PAGE_SIZE &&
            slot_array_end < header->free_space_offset) {
            uint16_t offset = header->free_space_offset;
            std::memcpy(buffer + offset, data.c_str(), data.size() + 1);

            /* Update slot directory */
            uint16_t* slots = reinterpret_cast<uint16_t*>(buffer + sizeof(PageHeader));
            slots[header->num_slots] = offset;

            TupleId tid(page_num, header->num_slots);
            header->num_slots++;
            header->free_space_offset += required;

            write_page(page_num, buffer);
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
    char buffer[StorageManager::PAGE_SIZE];
    if (!read_page(tuple_id.page_num, buffer)) return false;

    PageHeader* header = reinterpret_cast<PageHeader*>(buffer);
    if (header->free_space_offset == 0) return false;
    if (tuple_id.slot_num >= header->num_slots) return false;

    uint16_t* slots = reinterpret_cast<uint16_t*>(buffer + sizeof(PageHeader));
    uint16_t offset = slots[tuple_id.slot_num];
    if (offset == 0) return false;

    char* data_ptr = buffer + offset;
    std::string raw_data(data_ptr);

    std::stringstream ss(raw_data);
    std::string segment;
    std::vector<std::string> parts;
    while (std::getline(ss, segment, '|')) {
        parts.push_back(segment);
    }

    if (parts.size() < 2) return false;

    /* Update xmax field */
    parts[1] = std::to_string(xmax);

    /* Reconstruct record blob */
    std::string new_data;
    for (const auto& p : parts) {
        new_data += p + "|";
    }

    size_t old_len = raw_data.size() + 1;
    size_t new_len = new_data.size() + 1;

    if (new_len <= old_len) {
        std::memcpy(data_ptr, new_data.c_str(), new_len);
        return write_page(tuple_id.page_num, buffer);
    }

    /* Reorganize page to accommodate potentially longer xmax string */
    std::vector<std::string> all_tuples;
    for (int i = 0; i < header->num_slots; ++i) {
        if (slots[i] == 0) {
            all_tuples.push_back("");
            continue;
        }
        if (i == (int)tuple_id.slot_num) {
            all_tuples.push_back(new_data);
        } else {
            all_tuples.push_back(std::string(buffer + slots[i]));
        }
    }

    std::memset(buffer, 0, StorageManager::PAGE_SIZE);
    header = reinterpret_cast<PageHeader*>(buffer);
    header->free_space_offset = sizeof(PageHeader) + (64 * sizeof(uint16_t));
    header->num_slots = 0;
    slots = reinterpret_cast<uint16_t*>(buffer + sizeof(PageHeader));

    for (const auto& t_data : all_tuples) {
        if (t_data.empty()) {
            slots[header->num_slots] = 0;
            header->num_slots++;
            continue;
        }

        uint16_t req = static_cast<uint16_t>(t_data.size() + 1);
        if (header->free_space_offset + req > StorageManager::PAGE_SIZE) return false;

        uint16_t off = header->free_space_offset;
        std::memcpy(buffer + off, t_data.c_str(), req);
        slots[header->num_slots] = off;
        header->num_slots++;
        header->free_space_offset += req;
    }

    return write_page(tuple_id.page_num, buffer);
}

/**
 * @brief Physical deletion: zero out slot offset (rollback only)
 */
bool HeapTable::physical_remove(const TupleId& tuple_id) {
    char buffer[StorageManager::PAGE_SIZE];
    if (!read_page(tuple_id.page_num, buffer)) return false;

    PageHeader* header = reinterpret_cast<PageHeader*>(buffer);
    if (header->free_space_offset == 0) return false;
    if (tuple_id.slot_num >= header->num_slots) return false;

    uint16_t* slots = reinterpret_cast<uint16_t*>(buffer + sizeof(PageHeader));
    slots[tuple_id.slot_num] = 0;

    return write_page(tuple_id.page_num, buffer);
}

bool HeapTable::update(const TupleId& tuple_id, const executor::Tuple& tuple, uint64_t txn_id) {
    if (!remove(tuple_id, txn_id)) return false;
    insert(tuple, txn_id);
    return true;
}

bool HeapTable::get_meta(const TupleId& tuple_id, TupleMeta& out_meta) const {
    char buffer[StorageManager::PAGE_SIZE];
    if (!read_page(tuple_id.page_num, buffer)) return false;

    PageHeader* header = reinterpret_cast<PageHeader*>(buffer);
    if (header->free_space_offset == 0) return false;
    if (tuple_id.slot_num >= header->num_slots) return false;

    uint16_t* slots = reinterpret_cast<uint16_t*>(buffer + sizeof(PageHeader));
    uint16_t offset = slots[tuple_id.slot_num];
    if (offset == 0) return false;

    const char* data = buffer + offset;
    std::string s(data);
    std::stringstream ss(s);
    std::string item;

    /* Parse MVCC Header */
    if (!std::getline(ss, item, '|')) return false;
    try {
        out_meta.xmin = std::stoull(item);
    } catch (...) {
        out_meta.xmin = 0;
    }

    if (!std::getline(ss, item, '|')) return false;
    try {
        out_meta.xmax = std::stoull(item);
    } catch (...) {
        out_meta.xmax = 0;
    }

    /* Parse Column Values */
    std::vector<common::Value> values;
    for (size_t i = 0; i < schema_.column_count(); ++i) {
        if (!std::getline(ss, item, '|')) break;

        const auto& col = schema_.get_column(i);
        try {
            switch (col.type()) {
                case common::TYPE_INT8:
                case common::TYPE_INT16:
                case common::TYPE_INT32:
                case common::TYPE_INT64:
                    values.push_back(common::Value::make_int64(std::stoll(item)));
                    break;
                case common::TYPE_FLOAT32:
                case common::TYPE_FLOAT64:
                    values.push_back(common::Value::make_float64(std::stod(item)));
                    break;
                case common::TYPE_BOOL:
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
    char buffer[StorageManager::PAGE_SIZE];
    while (read_page(page_num, buffer)) {
        PageHeader* header = reinterpret_cast<PageHeader*>(buffer);
        if (header->free_space_offset == 0) break;

        for (int i = 0; i < header->num_slots; ++i) {
            TupleMeta meta;
            if (get_meta(TupleId(page_num, i), meta)) {
                if (meta.xmax == 0) count++;
            }
        }
        page_num++;
    }
    return count;
}

bool HeapTable::exists() const {
    return true;
}

bool HeapTable::create() {
    if (!storage_manager_.open_file(filename_)) return false;

    char buffer[StorageManager::PAGE_SIZE];
    std::memset(buffer, 0, StorageManager::PAGE_SIZE);
    PageHeader* header = reinterpret_cast<PageHeader*>(buffer);
    header->free_space_offset = sizeof(PageHeader) + (64 * sizeof(uint16_t));
    header->num_slots = 0;

    return write_page(0, buffer);
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

}  // namespace storage
}  // namespace cloudsql

/** @} */
