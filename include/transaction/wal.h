/**
 * @file wal.h
 * @brief Write-Ahead Log for crash recovery
 *
 * @defgroup wal Write-Ahead Log
 * @{
 */

#ifndef SQL_ENGINE_TRANSACTION_WAL_H
#define SQL_ENGINE_TRANSACTION_WAL_H

#include <stdbool.h>
#include <stdint.h>

#include "common/types.h"

/* WAL constants */
#define WAL_PAGE_SIZE 8192
#define WAL_MAGIC 0x57414C20  /* "WAL " */
#define WAL_VERSION 1

/**
 * @brief WAL record types
 */
typedef enum {
    WAL_RECORD_BEGIN = 1,
    WAL_RECORD_COMMIT = 2,
    WAL_RECORD_ABORT = 3,
    WAL_RECORD_INSERT = 4,
    WAL_RECORD_DELETE = 5,
    WAL_RECORD_UPDATE = 6,
    WAL_RECORD_CHECKPOINT = 7
} wal_record_type_t;

/**
 * @brief WAL record header
 */
typedef struct {
    uint32_t magic;           /* WAL magic number */
    uint32_t version;         /* WAL version */
    uint64_t lsn;           /* Log Sequence Number */
    uint64_t prev_lsn;       /* Previous LSN */
    transaction_id_t transaction_id;
    uint32_t record_length;
    uint16_t record_type;
    uint32_t checksum;
} wal_record_header_t;

/**
 * @brief WAL instance
 */
typedef struct {
    /** WAL file path */
    char wal_path[PATH_MAX];
    
    /** WAL file handle */
    FILE *wal_file;
    
    /** Current LSN */
    uint64_t current_lsn;
    
    /** Log segment number */
    uint32_t segment_num;
    
    /** Log page buffer */
    page_t *page_buffer;
    
    /** Page offset within buffer */
    uint32_t page_offset;
    
    /** Is in recovery */
    bool in_recovery;
    
    /** Oldest active transaction */
    transaction_id_t oldest_active_txn;
} wal_t;

/**
 * @brief Create a new WAL
 * @param wal_path WAL file path
 * @return New WAL or NULL on error
 */
wal_t *wal_create(const char *wal_path);

/**
 * @brief Open existing WAL
 * @param wal_path WAL file path
 * @return New WAL or NULL on error
 */
wal_t *wal_open(const char *wal_path);

/**
 * @brief Close WAL
 * @param wal WAL instance
 */
void wal_close(wal_t *wal);

/**
 * @brief Destroy WAL
 * @param wal WAL instance
 */
void wal_destroy(wal_t *wal);

/**
 * @brief Begin transaction
 * @param wal WAL instance
 * @param transaction_id Transaction ID
 * @return LSN of BEGIN record
 */
uint64_t wal_begin(wal_t *wal, transaction_id_t transaction_id);

/**
 * @brief Commit transaction
 * @param wal WAL instance
 * @param transaction_id Transaction ID
 * @param prev_lsn Previous LSN
 * @return LSN of COMMIT record
 */
uint64_t wal_commit(wal_t *wal, transaction_id_t transaction_id, uint64_t prev_lsn);

/**
 * @brief Abort transaction
 * @param wal WAL instance
 * @param transaction_id Transaction ID
 * @param prev_lsn Previous LSN
 * @return LSN of ABORT record
 */
uint64_t wal_abort(wal_t *wal, transaction_id_t transaction_id, uint64_t prev_lsn);

/**
 * @brief Log insert operation
 * @param wal WAL instance
 * @param transaction_id Transaction ID
 * @param prev_lsn Previous LSN
 * @param table_id Table ID
 * @param tuple_id Tuple ID
 * @return LSN of INSERT record
 */
uint64_t wal_insert(wal_t *wal, transaction_id_t transaction_id, uint64_t prev_lsn,
                    oid_t table_id, tuple_id_t tuple_id);

/**
 * @brief Log delete operation
 * @param wal WAL instance
 * @param transaction_id Transaction ID
 * @param prev_lsn Previous LSN
 * @param table_id Table ID
 * @param tuple_id Tuple ID
 * @return LSN of DELETE record
 */
uint64_t wal_delete(wal_t *wal, transaction_id_t transaction_id, uint64_t prev_lsn,
                    oid_t table_id, tuple_id_t tuple_id);

/**
 * @brief Log update operation
 * @param wal WAL instance
 * @param transaction_id Transaction ID
 * @param prev_lsn Previous LSN
 * @param table_id Table ID
 * @param old_tuple_id Old tuple ID
 * @param new_tuple_id New tuple ID
 * @return LSN of UPDATE record
 */
uint64_t wal_update(wal_t *wal, transaction_id_t transaction_id, uint64_t prev_lsn,
                    oid_t table_id, tuple_id_t old_tuple_id, tuple_id_t new_tuple_id);

/**
 * @brief Write checkpoint
 * @param wal WAL instance
 * @param checkpoint_data Checkpoint data
 * @param data_length Data length
 * @return LSN of CHECKPOINT record
 */
uint64_t wal_checkpoint(wal_t *wal, void *checkpoint_data, uint32_t data_length);

/**
 * @brief Flush WAL to disk
 * @param wal WAL instance
 * @return 0 on success, -1 on error
 */
int wal_flush(wal_t *wal);

/**
 * @brief Get current LSN
 * @param wal WAL instance
 * @return Current LSN
 */
uint64_t wal_get_lsn(wal_t *wal);

/**
 * @brief Recovery - analyze phase
 * @param wal WAL instance
 * @return List of active transactions
 */
transaction_id_t *wal_analyze(wal_t *wal, int *num_transactions);

/**
 * @brief Recovery - redo phase
 * @param wal WAL instance
 * @param oldest_lsn Redo from this LSN
 * @return 0 on success, -1 on error
 */
int wal_redo(wal_t *wal, uint64_t oldest_lsn);

/**
 * @brief Recovery - undo phase
 * @param wal WAL instance
 * @param active_transactions Active transaction IDs
 * @param num_transactions Number of transactions
 * @return 0 on success, -1 on error
 */
int wal_undo(wal_t *wal, transaction_id_t *active_transactions, int num_transactions);

/**
 * @brief Perform full recovery
 * @param wal WAL instance
 * @return 0 on success, -1 on error
 */
int wal_recover(wal_t *wal);

#endif /* SQL_ENGINE_TRANSACTION_WAL_H */

/** @} */ /* wal */
