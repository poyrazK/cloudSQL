/**
 * @file heap_table.hpp
 * @brief C++ wrapper for heap file storage
 */

#ifndef CLOUDSQL_STORAGE_HEAP_TABLE_HPP
#define CLOUDSQL_STORAGE_HEAP_TABLE_HPP

#include <string>
#include <memory>
#include <vector>
#include <cstdint>
#include "executor/types.hpp"

namespace cloudsql {
namespace storage {

/**
 * @brief Heap table storage (row-oriented)
 */
class HeapTable {
public:
    struct TupleId {
        uint32_t page_num;
        uint16_t slot_num;
        
        TupleId() : page_num(0), slot_num(0) {}
        TupleId(uint32_t page, uint16_t slot) : page_num(page), slot_num(slot) {}
        
        bool is_null() const { return page_num == 0 && slot_num == 0; }
        
        std::string to_string() const {
            return "(" + std::to_string(page_num) + ", " + std::to_string(slot_num) + ")";
        }
    };
    
    class Iterator {
    private:
        HeapTable& table_;
        TupleId current_id_;
        bool eof_ = false;
        
    public:
        explicit Iterator(HeapTable& table) : table_(table) {}
        
        bool next(executor::Tuple& out_tuple);
        bool is_done() const { return eof_; }
    };
    
private:
    std::string table_name_;
    
public:
    explicit HeapTable(std::string table_name) : table_name_(std::move(table_name)) {}
    
    ~HeapTable() = default;
    
    // Non-copyable
    HeapTable(const HeapTable&) = delete;
    HeapTable& operator=(const HeapTable&) = delete;
    
    // Movable
    HeapTable(HeapTable&&) noexcept = default;
    HeapTable& operator=(HeapTable&&) noexcept = default;
    
    const std::string& table_name() const { return table_name_; }
    
    TupleId insert(const executor::Tuple& tuple);
    bool remove(const TupleId& tuple_id);
    bool update(const TupleId& tuple_id, const executor::Tuple& tuple);
    bool get(const TupleId& tuple_id, executor::Tuple& out_tuple) const;
    
    uint64_t tuple_count() const;
    uint64_t file_size() const;
    
    Iterator scan() { return Iterator(*this); }
    
    bool exists() const;
    bool create();
    bool drop();
    
    int free_space(uint32_t page_num) const;
    uint32_t vacuum();
};

}  // namespace storage
}  // namespace cloudsql

#endif  // CLOUDSQL_STORAGE_HEAP_TABLE_HPP
