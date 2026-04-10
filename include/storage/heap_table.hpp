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
#include <memory_resource>
#include <string>
#include <vector>

#include "executor/types.hpp"
#include "storage/buffer_pool_manager.hpp"

namespace cloudsql::storage {

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
        [[nodiscard]] bool is_null() const { return page_num == 0 && slot_num == 0; }

        /** @return Human-readable string representation */
        [[nodiscard]] std::string to_string() const {
            return "(" + std::to_string(page_num) + ", " + std::to_string(slot_num) + ")";
        }

        bool operator==(const TupleId& other) const {
            return page_num == other.page_num && slot_num == other.slot_num;
        }

        struct Hash {
            std::size_t operator()(const TupleId& tid) const {
                return (static_cast<size_t>(tid.page_num) << 16) ^
                       static_cast<size_t>(tid.slot_num);
            }
        };
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
        uint64_t xmin = 0;
        uint64_t xmax = 0;
    };

    /**
     * @struct TupleView
     * @brief Zero-allocation view into a serialized tuple residing on a pinned page
     */
    struct TupleView {
        const uint8_t* payload_data = nullptr;
        uint16_t payload_len = 0;
        const executor::Schema* table_schema = nullptr; /**< Physical schema of payload_data */
        const executor::Schema* schema = nullptr;       /**< Logical schema of this view */
        const std::vector<size_t>* column_mapping = nullptr;
        uint64_t xmin = 0;
        uint64_t xmax = 0;

        /**
         * @brief Materialize a common::Value for a specific column index via lazy parsing
         */
        common::Value get_value(size_t col_index) const;

        /**
         * @brief Materialize the entire view into a Tuple
         */
        executor::Tuple materialize(std::pmr::memory_resource* mr = nullptr) const;
    };

    /**
     * @class Iterator
     * @brief Forward-only iterator for scanning heap table records
     */
    class Iterator {
       private:
        HeapTable& table_;
        TupleId next_id_;               /**< ID of the next record to be checked */
        TupleId last_id_;               /**< ID of the record returned by the last next() call */
        bool eof_ = false;              /**< End-of-file indicator */
        std::pmr::memory_resource* mr_; /**< Memory resource for tuple allocations */
        Page* current_page_ = nullptr;
        uint32_t current_page_num_ = 0xFFFFFFFF;
        
        /* Caching for Phase 2 optimization */
        const uint8_t* cached_buffer_ = nullptr;
        PageHeader cached_header_{};

       public:
        explicit Iterator(HeapTable& table, std::pmr::memory_resource* mr = nullptr);
        ~Iterator();

        Iterator(const Iterator&) = delete;
        Iterator& operator=(const Iterator&) = delete;
        Iterator(Iterator&& other) noexcept;
        Iterator& operator=(Iterator&& other) noexcept;
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

        /**
         * @brief Move to the next tuple and return a view into its data.
         * 
         * @note The returned TupleView points into the iterator's currently pinned page and
         * therefore becomes invalid as soon as the iterator advances to a different page,
         * is closed, or is destroyed. Callers must copy data out of the TupleView if they
         * need it beyond the iterator's current position (e.g., during materialization).
         * 
         * @param out_view Output parameter to store the view.
         * @return true if a tuple was found, false if EOF.
         */
        bool next_view(TupleView& out_view);

        /** @return true if the scan has reached the end of the table */
        [[nodiscard]] bool is_done() const { return eof_; }

        /** @return RID of the most recently retrieved record */
        [[nodiscard]] const TupleId& current_id() const { return last_id_; }
    };

   private:
    std::string table_name_;
    std::string filename_;
    BufferPoolManager& bpm_;
    executor::Schema schema_;
    uint32_t last_page_id_ = 0;
    uint32_t file_id_ = 0;

    // Last page cache for fast insertions
    Page* cached_page_ = nullptr;
    uint32_t cached_page_id_ = 0xFFFFFFFF;

   public:
    /**
     * @brief Constructor
     * @param table_name Logical name of the table
     * @param bpm Reference to the global buffer pool manager
     * @param schema Table schema definition
     */
    HeapTable(std::string table_name, BufferPoolManager& bpm, executor::Schema schema);

    ~HeapTable();

    /* Disable copy semantics */
    HeapTable(const HeapTable&) = delete;
    HeapTable& operator=(const HeapTable&) = delete;

    /* Enable move semantics (assignment deleted due to reference member) */
    HeapTable(HeapTable&&) noexcept = default;
    HeapTable& operator=(HeapTable&&) noexcept = delete;

    /** @return Logical table name */
    [[nodiscard]] const std::string& table_name() const { return table_name_; }

    /** @return Schema definition */
    [[nodiscard]] const executor::Schema& schema() const { return schema_; }

    [[nodiscard]] uint32_t file_id() const { return file_id_; }

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
     * @brief Resets xmax to 0 (used for rollback of a DELETE)
     * @return true on success
     */
    bool undo_remove(const TupleId& tuple_id);

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
    [[nodiscard]] uint64_t tuple_count() const;

    /** @return An iterator starting at the first page */
    [[nodiscard]] Iterator scan(std::pmr::memory_resource* mr = nullptr) {
        return Iterator(*this, mr);
    }

    /** @brief Initializes the physical heap file */
    bool create();

    /** @brief Removes the physical heap file */
    bool drop();

   private:
    bool read_page(uint32_t page_num, char* buffer) const;
    bool write_page(uint32_t page_num, const char* buffer);
};

}  // namespace cloudsql::storage

#endif  // CLOUDSQL_STORAGE_HEAP_TABLE_HPP

/** @} */
