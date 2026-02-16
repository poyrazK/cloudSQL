/**
 * @file btree_index.hpp
 * @brief C++ wrapper for B-tree index
 */

#ifndef CLOUDSQL_STORAGE_BTREE_INDEX_HPP
#define CLOUDSQL_STORAGE_BTREE_INDEX_HPP

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/value.hpp"
#include "storage/heap_table.hpp"
#include "storage/storage_manager.hpp"

namespace cloudsql {
namespace storage {

/**
 * @brief B+ Tree index for fast lookups
 */
class BTreeIndex {
   public:
    /**
     * @brief Node types in the B+ Tree
     */
    enum class NodeType : uint8_t { Leaf = 0, Internal = 1 };

    /**
     * @brief Page header for B-tree nodes
     */
    struct NodeHeader {
        NodeType type;
        uint16_t num_keys;
        uint32_t parent_page;
        uint32_t next_leaf;  // For leaf nodes
    };

    /**
     * @brief Index entry (Key + TupleId)
     */
    struct Entry {
        common::Value key;
        HeapTable::TupleId tuple_id;

        Entry() = default;
        Entry(common::Value k, HeapTable::TupleId tid) : key(std::move(k)), tuple_id(tid) {}
    };

    /**
     * @brief Scan iterator for index
     */
    class Iterator {
       private:
        BTreeIndex& index_;
        uint32_t current_page_;
        uint16_t current_slot_;
        bool eof_ = false;

       public:
        Iterator(BTreeIndex& index, uint32_t page, uint16_t slot);

        bool next(Entry& out_entry);
        bool is_done() const { return eof_; }
    };

   private:
    std::string index_name_;
    std::string filename_;
    StorageManager& storage_manager_;
    common::ValueType key_type_;
    uint32_t root_page_ = 0;

   public:
    BTreeIndex(std::string index_name, StorageManager& storage_manager, common::ValueType key_type);

    ~BTreeIndex() = default;

    /* Non-copyable */
    BTreeIndex(const BTreeIndex&) = delete;
    BTreeIndex& operator=(const BTreeIndex&) = delete;

    /* Movable (assignment deleted due to reference member) */
    BTreeIndex(BTreeIndex&&) noexcept = default;
    BTreeIndex& operator=(BTreeIndex&&) noexcept = delete;

    const std::string& index_name() const { return index_name_; }
    common::ValueType key_type() const { return key_type_; }

    bool create();
    bool open();
    void close();
    bool drop();

    bool insert(const common::Value& key, HeapTable::TupleId tuple_id);
    bool remove(const common::Value& key, HeapTable::TupleId tuple_id);

    std::vector<HeapTable::TupleId> search(const common::Value& key);

    Iterator scan();

    bool exists() const;

   private:
    /* Internal B-tree logic */
    uint32_t find_leaf(const common::Value& key);
    void split_leaf(uint32_t page_num, char* buffer);
    // void split_internal(...) // TODO phase 2

    bool read_page(uint32_t page_num, char* buffer) const;
    bool write_page(uint32_t page_num, const char* buffer);
    uint32_t allocate_page();
};

}  // namespace storage
}  // namespace cloudsql

#endif  // CLOUDSQL_STORAGE_BTREE_INDEX_HPP
