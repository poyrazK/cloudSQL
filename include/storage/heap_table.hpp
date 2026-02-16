/**
 * @file heap_table.hpp
 * @brief Slot-based heap file storage for row-oriented data
 *
 * This implementation uses a slotted page structure to manage variable-length
 * records within fixed-size database pages.
 *
 * @defgroup storage Storage Engine
 * @{
 */

#ifndef CLOUDSQL_STORAGE_HEAP_TABLE_HPP
#define CLOUDSQL_STORAGE_HEAP_TABLE_HPP

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "executor/types.hpp"
#include "storage/storage_manager.hpp"

namespace cloudsql {
namespace storage {

/**
 * @class HeapTable
 * @brief Manages a physical heap file containing database records
 */
class HeapTable {
   public:
    /**
     * @struct TupleId
     * @brief Record Identifier (RID) consisting of a page number and slot index
     */
    struct TupleId {
        uint32_t page_num; /**< Physical page index in the file */
        uint16_t slot_num; /**< Logical slot index within the page */

        TupleId() : page_num(0), slot_num(0) {}
        TupleId(uint32_t page, uint16_t slot) : page_num(page), slot_num(slot) {}

        /** @return true if the ID represents a null/invalid record */
        bool is_null() const { return page_num == 0 && slot_num == 0; }

        /** @return Human-readable string representation */
        std::string to_string() const {
            return "(" + std::to_string(page_num) + ", " + std::to_string(slot_num) + ")";
        }

        bool operator==(const TupleId& other) const {
            return page_num == other.page_num && slot_num == other.slot_num;
        }
    };

    /**
     * @struct PageHeader
     * @brief Fixed-size header present at the beginning of every database page
     */
    struct PageHeader {
        uint32_t next_page;         /**< Next page in the heap chain */
        uint16_t num_slots;         /**< Total slots allocated in this page */
        uint16_t free_space_offset; /**< Pointer to the start of free space */
        uint16_t flags;             /**< Page-level metadata flags */
    };

    /**
     * @struct TupleHeader
     * @brief MVCC metadata prepended to every tuple
     */
    struct TupleHeader {
        uint64_t xmin; /**< Transaction ID that created this tuple */
        uint64_t xmax; /**< Transaction ID that deleted this tuple (0 if active) */
    };

    /**
     * @struct TupleMeta
     * @brief Container for tuple data and its MVCC metadata
     */
    struct TupleMeta {
        executor::Tuple tuple;
        uint64_t xmin;
        uint64_t xmax;
    };

    /**
     * @class Iterator
     * @brief Forward-only iterator for scanning heap table records
     */
    class Iterator {
       private:
        HeapTable& table_;
        TupleId next_id_;  /**< ID of the next record to be checked */
        TupleId last_id_;  /**< ID of the record returned by the last next() call */
        bool eof_ = false; /**< End-of-file indicator */

       public:
        explicit Iterator(HeapTable& table);

        /**
         * @brief Fetches the next non-deleted record from the heap
         * @param[out] out_tuple Container for the retrieved record
         * @return true if a record was successfully retrieved, false on EOF
         */
        bool next(executor::Tuple& out_tuple);

        /**
         * @brief Fetches the next record including MVCC metadata
         * @param[out] out_meta Container for the retrieved record and metadata
         * @return true if a record was successfully retrieved, false on EOF
         */
        bool next_meta(TupleMeta& out_meta);

        /** @return true if the scan has reached the end of the table */
        bool is_done() const { return eof_; }

        /** @return RID of the most recently retrieved record */
        const TupleId& current_id() const { return last_id_; }
    };

   private:
    std::string table_name_;
    std::string filename_;
    StorageManager& storage_manager_;
    executor::Schema schema_;

   public:
    /**
     * @brief Constructor
     * @param table_name Logical name of the table
     * @param storage_manager Reference to the global storage manager
     * @param schema Table schema definition
     */
    HeapTable(std::string table_name, StorageManager& storage_manager, executor::Schema schema);

    ~HeapTable() = default;

    /* Disable copy semantics */
    HeapTable(const HeapTable&) = delete;
    HeapTable& operator=(const HeapTable&) = delete;

    /* Enable move semantics (assignment deleted due to reference member) */
    HeapTable(HeapTable&&) noexcept = default;
    HeapTable& operator=(HeapTable&&) noexcept = delete;

    /** @return Logical table name */
    const std::string& table_name() const { return table_name_; }

    /** @return Schema definition */
    const executor::Schema& schema() const { return schema_; }

    /**
     * @brief Inserts a new record into the heap
     * @param tuple The data to insert
     * @param xmin Transaction ID creating this tuple
     * @return Unique identifier assigned to the new record
     */
    TupleId insert(const executor::Tuple& tuple, uint64_t xmin = 0);

    /**
     * @brief Logically deletes a record by setting xmax
     * @param tuple_id The record to delete
     * @param xmax Transaction ID deleting this tuple
     * @return true on success
     */
    bool remove(const TupleId& tuple_id, uint64_t xmax);

    /**
     * @brief Physically removes a record (used for rollback)
     * @return true on success
     */
    bool physical_remove(const TupleId& tuple_id);

    /**
     * @brief Replaces an existing record with new data
     * @param tuple_id The record to update
     * @param tuple The new data
     * @param txn_id ID of the transaction performing the update
     * @return true on success
     */
    bool update(const TupleId& tuple_id, const executor::Tuple& tuple, uint64_t txn_id);

    /**
     * @brief Retrieves a specific record by its ID
     * @return true if the record exists and was retrieved
     */
    bool get(const TupleId& tuple_id, executor::Tuple& out_tuple) const;

    /**
     * @brief Retrieves a specific record with metadata by its ID
     * @return true if the record exists and was retrieved
     */
    bool get_meta(const TupleId& tuple_id, TupleMeta& out_meta) const;

    /** @return Total count of non-deleted records in the table */
    uint64_t tuple_count() const;

    /** @return An iterator starting at the first page */
    Iterator scan() { return Iterator(*this); }

    /** @return true if the physical file exists */
    bool exists() const;

    /** @brief Initializes the physical heap file */
    bool create();

    /** @brief Removes the physical heap file */
    bool drop();

   private:
    bool read_page(uint32_t page_num, char* buffer) const;
    bool write_page(uint32_t page_num, const char* buffer);
};

}  // namespace storage
}  // namespace cloudsql

#endif  // CLOUDSQL_STORAGE_HEAP_TABLE_HPP

/** @} */
