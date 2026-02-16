/**
 * @file btree_index.cpp
 * @brief B-tree index implementation
 */

#include "storage/btree_index.hpp"

#include <algorithm>
#include <cstring>
#include <sstream>

namespace cloudsql {
namespace storage {

BTreeIndex::BTreeIndex(std::string index_name, StorageManager& storage_manager,
                       common::ValueType key_type)
    : index_name_(std::move(index_name)),
      filename_(index_name_ + ".idx"),
      storage_manager_(storage_manager),
      key_type_(key_type) {}

/**
 * @brief Iterator implementation
 */
BTreeIndex::Iterator::Iterator(BTreeIndex& index, uint32_t page, uint16_t slot)
    : index_(index), current_page_(page), current_slot_(slot), eof_(false) {}

bool BTreeIndex::Iterator::next(Entry& out_entry) {
    if (eof_) return false;

    char buffer[StorageManager::PAGE_SIZE];
    if (!index_.read_page(current_page_, buffer)) {
        eof_ = true;
        return false;
    }

    NodeHeader* header = reinterpret_cast<NodeHeader*>(buffer);
    if (current_slot_ >= header->num_keys) {
        /* Move to next leaf if exists */
        if (header->next_leaf != 0) {
            current_page_ = header->next_leaf;
            current_slot_ = 0;
            return next(out_entry);
        }
        eof_ = true;
        return false;
    }

    /* Deserialize entry (crude implementation) */
    const char* data_start = buffer + sizeof(NodeHeader);
    /* Find the N-th pipe-delimited segment */
    std::string s(data_start);
    std::stringstream ss(s);
    std::string item;
    uint16_t i = 0;
    while (i < current_slot_ && std::getline(ss, item, '|')) {
        // Skip previous entries
        // Each entry is: type|lexeme|page|slot|
        for (int j = 0; j < 3; ++j) std::getline(ss, item, '|');
        i++;
    }

    /* Read our entry */
    std::string type_str, lexeme, page_str, slot_str;
    if (std::getline(ss, type_str, '|') && std::getline(ss, lexeme, '|') &&
        std::getline(ss, page_str, '|') && std::getline(ss, slot_str, '|')) {
        common::Value val;
        if (std::stoi(type_str) == common::TYPE_INT64)
            val = common::Value::make_int64(std::stoll(lexeme));
        else
            val = common::Value::make_text(lexeme);

        out_entry = Entry(std::move(val),
                          HeapTable::TupleId(std::stoul(page_str), (uint16_t)std::stoi(slot_str)));
        current_slot_++;
        return true;
    }

    eof_ = true;
    return false;
}

/**
 * @brief BTreeIndex operations
 */

bool BTreeIndex::create() {
    if (!storage_manager_.open_file(filename_)) return false;

    /* Initialize root page */
    char buffer[StorageManager::PAGE_SIZE];
    std::memset(buffer, 0, StorageManager::PAGE_SIZE);
    NodeHeader* header = reinterpret_cast<NodeHeader*>(buffer);
    header->type = NodeType::Leaf;
    header->num_keys = 0;
    header->parent_page = 0;
    header->next_leaf = 0;

    return write_page(0, buffer);
}

bool BTreeIndex::open() {
    return storage_manager_.open_file(filename_);
}

void BTreeIndex::close() {
    storage_manager_.close_file(filename_);
}

bool BTreeIndex::drop() {
    return storage_manager_.close_file(filename_);
}

bool BTreeIndex::insert(const common::Value& key, HeapTable::TupleId tuple_id) {
    uint32_t leaf_page = find_leaf(key);
    char buffer[StorageManager::PAGE_SIZE];
    if (!read_page(leaf_page, buffer)) return false;

    NodeHeader* header = reinterpret_cast<NodeHeader*>(buffer);

    /* Simple append-style serialization for this phase */
    std::string entry_data = std::to_string(static_cast<int>(key.type())) + "|" + key.to_string() +
                             "|" + std::to_string(tuple_id.page_num) + "|" +
                             std::to_string(tuple_id.slot_num) + "|";

    /* Check space (very crude) */
    char* data_area = buffer + sizeof(NodeHeader);
    size_t existing_len = std::strlen(data_area);
    if (existing_len + entry_data.size() + 1 > StorageManager::PAGE_SIZE - sizeof(NodeHeader)) {
        /* TODO: split_leaf(leaf_page, buffer); */
        return false;
    }

    std::memcpy(data_area + existing_len, entry_data.c_str(), entry_data.size() + 1);
    header->num_keys++;

    return write_page(leaf_page, buffer);
}

bool BTreeIndex::remove(const common::Value& key, HeapTable::TupleId tuple_id) {
    (void)key;
    (void)tuple_id;
    return true;
}

std::vector<HeapTable::TupleId> BTreeIndex::search(const common::Value& key) {
    uint32_t leaf_page = find_leaf(key);
    char buffer[StorageManager::PAGE_SIZE];
    if (!read_page(leaf_page, buffer)) return {};

    std::vector<HeapTable::TupleId> results;

    const char* data = buffer + sizeof(NodeHeader);
    std::string s(data);
    std::stringstream ss(s);
    std::string type_s, val_s, page_s, slot_s;

    while (std::getline(ss, type_s, '|') && std::getline(ss, val_s, '|') &&
           std::getline(ss, page_s, '|') && std::getline(ss, slot_s, '|')) {
        if (val_s == key.to_string()) {
            results.emplace_back(std::stoul(page_s), (uint16_t)std::stoi(slot_s));
        }
    }

    return results;
}

BTreeIndex::Iterator BTreeIndex::scan() {
    return Iterator(*this, root_page_, 0);
}

bool BTreeIndex::exists() const {
    return true;
}

uint32_t BTreeIndex::find_leaf(const common::Value& key) {
    (void)key;
    return root_page_;  // Root is leaf in this simple 1-level tree
}

bool BTreeIndex::read_page(uint32_t page_num, char* buffer) const {
    return storage_manager_.read_page(filename_, page_num, buffer);
}

bool BTreeIndex::write_page(uint32_t page_num, const char* buffer) {
    return storage_manager_.write_page(filename_, page_num, buffer);
}

}  // namespace storage
}  // namespace cloudsql
