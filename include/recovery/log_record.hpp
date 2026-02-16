/**
 * @file log_record.hpp
 * @brief Log record structure for Write-Ahead Logging (WAL)
 */

#ifndef CLOUDSQL_RECOVERY_LOG_RECORD_HPP
#define CLOUDSQL_RECOVERY_LOG_RECORD_HPP

#include <cstdint>
#include <vector>
#include <string>
#include <cstring>
#include <iostream>
#include "executor/types.hpp"
#include "storage/heap_table.hpp"

namespace cloudsql {
namespace recovery {

using lsn_t = int32_t;
using txn_id_t = uint64_t;

/**
 * @brief Types of log records
 */
enum class LogRecordType {
    INVALID = 0,
    INSERT,
    MARK_DELETE,
    APPLY_DELETE,
    ROLLBACK_DELETE,
    UPDATE,
    BEGIN,
    COMMIT,
    ABORT,
    NEW_PAGE
};

/**
 * @brief Header of a log record
 */
struct LogRecordHeader {
    uint32_t size;              // Total size of the record including header
    lsn_t lsn;                  // Log Sequence Number
    lsn_t prev_lsn;             // LSN of the previous record for this transaction
    txn_id_t txn_id;            // Transaction ID
    LogRecordType type;         // Record type
};

/**
 * @brief Represents a single log entry in the WAL
 */
class LogRecord {
public:
    // Header
    uint32_t size_ = 0;
    lsn_t lsn_ = 0;
    lsn_t prev_lsn_ = 0;
    txn_id_t txn_id_ = 0;
    LogRecordType type_ = LogRecordType::INVALID;

    // Body (variable fields depending on type)
    // For tuple operations:
    std::string table_name_;
    storage::HeapTable::TupleId rid_;
    executor::Tuple tuple_;         // Inserted or New tuple
    executor::Tuple old_tuple_;     // Old tuple (for UPDATE/DELETE)

    // For NEW_PAGE:
    uint32_t page_id_;

    /**
     * @brief Default constructor
     */
    LogRecord() = default;

    /**
     * @brief Constructor for transaction control records (BEGIN, COMMIT, ABORT)
     */
    LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType type)
        : prev_lsn_(prev_lsn), txn_id_(txn_id), type_(type) {
        size_ = HEADER_SIZE;
    }

    /**
     * @brief Constructor for single-tuple operations (INSERT, DELETE variants)
     */
    LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType type, 
              const std::string& table_name, const storage::HeapTable::TupleId& rid, 
              const executor::Tuple& tuple_data)
        : prev_lsn_(prev_lsn), txn_id_(txn_id), type_(type), 
          table_name_(table_name), rid_(rid) {
        
        if (type == LogRecordType::INSERT) {
            tuple_ = tuple_data;
        } else {
            old_tuple_ = tuple_data;
        }
    }

    /**
     * @brief Constructor for UPDATE
     */
    LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType type, 
              const std::string& table_name, const storage::HeapTable::TupleId& rid, 
              const executor::Tuple& old_tuple, const executor::Tuple& new_tuple)
        : prev_lsn_(prev_lsn), txn_id_(txn_id), type_(type), 
          table_name_(table_name), rid_(rid), tuple_(new_tuple), old_tuple_(old_tuple) {
    }

    /**
     * @brief Constructor for NEW_PAGE
     */
    LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType type, uint32_t page_id)
        : prev_lsn_(prev_lsn), txn_id_(txn_id), type_(type), page_id_(page_id) {
        size_ = HEADER_SIZE + sizeof(uint32_t);
    }

    // Header size (approximate for C++ struct, actual serialization might vary padding)
    static const uint32_t HEADER_SIZE = 
        sizeof(uint32_t) + // size
        sizeof(lsn_t) * 2 + // lsn, prev_lsn
        sizeof(txn_id_t) + // txn_id
        sizeof(LogRecordType); // type

    /**
     * @return The string representation of the record type
     */
    std::string type_to_string() const {
        switch (type_) {
            case LogRecordType::INVALID: return "INVALID";
            case LogRecordType::INSERT: return "INSERT";
            case LogRecordType::MARK_DELETE: return "MARK_DELETE";
            case LogRecordType::APPLY_DELETE: return "APPLY_DELETE";
            case LogRecordType::ROLLBACK_DELETE: return "ROLLBACK_DELETE";
            case LogRecordType::UPDATE: return "UPDATE";
            case LogRecordType::BEGIN: return "BEGIN";
            case LogRecordType::COMMIT: return "COMMIT";
            case LogRecordType::ABORT: return "ABORT";
            case LogRecordType::NEW_PAGE: return "NEW_PAGE";
            default: return "UNKNOWN";
        }
    }

    /**
     * @brief Print log record for debugging
     */
    friend std::ostream& operator<<(std::ostream& os, const LogRecord& log) {
        os << "Log[" << log.lsn_ << "] Txn: " << log.txn_id_ 
           << " PrevLSN: " << log.prev_lsn_ 
           << " Type: " << log.type_to_string();
        
        if (log.type_ == LogRecordType::INSERT || log.type_ == LogRecordType::UPDATE || 
            log.type_ == LogRecordType::MARK_DELETE || log.type_ == LogRecordType::APPLY_DELETE || 
            log.type_ == LogRecordType::ROLLBACK_DELETE) {
            os << " Table: " << log.table_name_ << " RID: " << log.rid_.to_string();
        }
        
        return os;
    }
    
    /**
     * @brief Serialize log record to buffer
     * @param buffer Output buffer
     * @return Number of bytes written
     */
    uint32_t serialize(char* buffer) const;

    /**
     * @brief Deserialize log record from buffer
     * @param buffer Input buffer
     * @return Deserialized LogRecord
     */
    static LogRecord deserialize(const char* buffer);
    
    /**
     * @brief Get serialized size
     */
    uint32_t get_size() const;
};

} // namespace recovery
} // namespace cloudsql

#endif // CLOUDSQL_RECOVERY_LOG_RECORD_HPP
