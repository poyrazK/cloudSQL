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
#include "storage/buffer_pool_manager.hpp"
#include "storage/heap_table.hpp"

namespace cloudsql::storage {

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
        uint32_t next_leaf;  // For leaf nodes: next leaf page. For internal: rightmost child.
    };

    /**
     * @brief Slot entry — points to an entry in the data area of a page.
     * Slot array grows forward from after NodeHeader.
     * Entry data grows backward from end of page.
     */
    struct SlotEntry {
        uint16_t offset;   // Byte offset from start of page to entry data
        uint16_t length;   // Entry size in bytes
    };

    static constexpr uint16_t kSlotSize = sizeof(SlotEntry);  // 4 bytes per slot
    static constexpr uint16_t kMaxSlots =
        (Page::PAGE_SIZE - sizeof(NodeHeader)) / sizeof(SlotEntry);  // ~1014 slots max

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
        [[nodiscard]] bool is_done() const { return eof_; }
    };

   private:
    std::string index_name_;
    std::string filename_;
    BufferPoolManager& bpm_;
    common::ValueType key_type_;
    uint32_t root_page_ = 0;
    common::Value pending_separator_;

   public:
    BTreeIndex(std::string index_name, BufferPoolManager& bpm, common::ValueType key_type);

    ~BTreeIndex() = default;

    /* Non-copyable */
    BTreeIndex(const BTreeIndex&) = delete;
    BTreeIndex& operator=(const BTreeIndex&) = delete;

    /* Movable (assignment deleted due to reference member) */
    BTreeIndex(BTreeIndex&&) noexcept = default;
    BTreeIndex& operator=(BTreeIndex&&) noexcept = delete;

    [[nodiscard]] const std::string& index_name() const { return index_name_; }
    [[nodiscard]] common::ValueType key_type() const { return key_type_; }
    [[nodiscard]] uint32_t root_page() const { return root_page_; }

    bool create();
    bool open();
    void close();
    bool drop();

    bool insert(const common::Value& key, HeapTable::TupleId tuple_id);
    bool remove(const common::Value& key, HeapTable::TupleId tuple_id);

    [[nodiscard]] std::vector<HeapTable::TupleId> search(const common::Value& key);

    [[nodiscard]] Iterator scan();

   private:
    /* Internal B-tree logic */
    [[nodiscard]] uint32_t find_leaf(const common::Value& key) const;
    [[nodiscard]] uint32_t split_leaf(uint32_t page_num, char* buffer);
    bool split_internal(uint32_t page_num, char* buffer, uint16_t insert_pos,
                        uint32_t& out_right_page);

    bool read_page(uint32_t page_num, char* buffer) const;
    bool write_page(uint32_t page_num, const char* buffer);
    [[nodiscard]] uint32_t allocate_page();

    /* Slot array helpers */
    [[nodiscard]] uint16_t get_data_start_offset(uint16_t num_keys) const;
    [[nodiscard]] uint16_t compute_entry_size(const common::Value& key) const;
    [[nodiscard]] bool get_slot(const char* buffer, uint16_t slot_idx, SlotEntry& out) const;
    bool put_slot(char* buffer, uint16_t slot_idx, const SlotEntry& entry);
    bool append_entry_at(char* buffer, uint16_t slot_idx, const SlotEntry& entry,
                         const common::Value& key, HeapTable::TupleId tuple_id);

    /* Entry serialization */
    [[nodiscard]] bool serialize_entry(const common::Value& key, HeapTable::TupleId tuple_id,
                                      char* out_buf, uint16_t buf_size,
                                      uint16_t& bytes_written) const;
    [[nodiscard]] bool deserialize_entry(const char* buf, uint16_t buf_size,
                                        common::Value& out_key,
                                        HeapTable::TupleId& out_tuple_id) const;

    /* Key comparison */
    [[nodiscard]] int compare_keys(const common::Value& a, const common::Value& b) const;

    /* Internal node navigation */
    [[nodiscard]] uint32_t find_child_for_key(const char* buffer, const common::Value& key, uint16_t num_keys) const;
    [[nodiscard]] uint32_t get_child_page(const char* buffer, uint16_t slot_idx) const;
    [[nodiscard]] int compare_separator(const char* buffer, uint16_t sep_idx, const common::Value& key) const;

    /* Internal node insertion (Phase 4/5) */
    [[nodiscard]] common::Value extract_key_from_entry(const char* entry_ptr, uint16_t entry_length) const;
    [[nodiscard]] bool serialize_internal_entry(const common::Value& key, uint32_t child_page_num,
                                                char* out_buf, uint16_t buf_size,
                                                uint16_t& bytes_written) const;
    bool insert_into_parent(const common::Value& sep_key, uint32_t left_page, uint32_t right_page);
    bool create_new_root(const common::Value& sep_key, uint32_t left_child, uint32_t right_child);
    bool update_child_parent(uint32_t child_page, uint32_t parent_page);
};

}  // namespace cloudsql::storage

#endif  // CLOUDSQL_STORAGE_BTREE_INDEX_HPP
