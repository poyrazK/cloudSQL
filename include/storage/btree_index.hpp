/**
 * @file btree_index.hpp
 * @brief C++ wrapper for B-tree index
 */

#ifndef CLOUDSQL_STORAGE_BTREE_INDEX_HPP
#define CLOUDSQL_STORAGE_BTREE_INDEX_HPP

#include <string>
#include <memory>
#include <vector>
#include <cstdint>
#include "common/value.hpp"
#include "storage/heap_table.hpp"

namespace cloudsql {
namespace storage {

/**
 * @brief B-tree index for fast lookups
 */
class BTreeIndex {
public:
    /**
     * @brief Index entry
     */
    struct Entry {
        common::Value key;
        HeapTable::TupleId tuple_id;
        
        Entry() {}
        Entry(common::Value k, HeapTable::TupleId tid) : key(std::move(k)), tuple_id(tid) {}
    };
    
    /**
     * @brief Scan iterator for index
     */
    class Iterator {
    private:
        BTreeIndex& index_;
        bool eof_ = false;
        
    public:
        explicit Iterator(BTreeIndex& index) : index_(index) {}
        
        bool next(Entry& out_entry);
        bool is_done() const { return eof_; }
    };
    
    /**
     * @brief Scan range bounds
     */
    struct Range {
        std::unique_ptr<common::Value> min;
        std::unique_ptr<common::Value> max;
        
        Range() = default;
        Range(std::unique_ptr<common::Value> min_key, std::unique_ptr<common::Value> max_key)
            : min(std::move(min_key)), max(std::move(max_key)) {}
    };
    
private:
    std::string index_name_;
    std::string table_name_;
    common::ValueType key_type_;
    
public:
    BTreeIndex(std::string index_name, std::string table_name, common::ValueType key_type)
        : index_name_(std::move(index_name)), table_name_(std::move(table_name)), key_type_(key_type) {}
    
    ~BTreeIndex() = default;
    
    // Non-copyable
    BTreeIndex(const BTreeIndex&) = delete;
    BTreeIndex& operator=(const BTreeIndex&) = delete;
    
    // Movable
    BTreeIndex(BTreeIndex&&) noexcept = default;
    BTreeIndex& operator=(BTreeIndex&&) noexcept = default;
    
    const std::string& index_name() const { return index_name_; }
    const std::string& table_name() const { return table_name_; }
    common::ValueType key_type() const { return key_type_; }
    
    bool create();
    bool open();
    void close();
    bool drop();
    
    bool insert(const common::Value& key, HeapTable::TupleId tuple_id);
    bool remove(const common::Value& key, HeapTable::TupleId tuple_id);
    
    std::vector<HeapTable::TupleId> search(const common::Value& key) const;
    
    std::vector<HeapTable::TupleId> range_search(
        const std::unique_ptr<common::Value>& min_key,
        const std::unique_ptr<common::Value>& max_key
    ) const;
    
    Iterator scan() { return Iterator(*this); }
    Iterator range_scan(const Range& range) { (void)range; return Iterator(*this); }
    
    void get_stats(uint64_t& num_entries, int& depth, uint32_t& num_pages) const;
    
    bool verify() const;
    bool exists() const;
};

}  // namespace storage
}  // namespace cloudsql

#endif  // CLOUDSQL_STORAGE_BTREE_INDEX_HPP
