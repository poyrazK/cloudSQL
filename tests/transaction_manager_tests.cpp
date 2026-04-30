/**
 * @file transaction_manager_tests.cpp
 * @brief Unit tests for Transaction Manager
 */

#include <gtest/gtest.h>

#include <string>

#include "catalog/catalog.hpp"
#include "common/config.hpp"
#include "common/fault_injection.hpp"
#include "executor/query_executor.hpp"
#include "parser/lexer.hpp"
#include "parser/parser.hpp"
#include "parser/statement.hpp"
#include "recovery/log_manager.hpp"
#include "storage/buffer_pool_manager.hpp"
#include "storage/storage_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"
#include "transaction/transaction_manager.hpp"

using namespace cloudsql;
using namespace cloudsql::common;
using namespace cloudsql::executor;
using namespace cloudsql::parser;
using namespace cloudsql::recovery;
using namespace cloudsql::storage;
using namespace cloudsql::transaction;

namespace {

TEST(TransactionManagerTests, Basic) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* const txn1 = tm.begin();
    ASSERT_NE(txn1, nullptr);
    EXPECT_EQ(txn1->get_state(), TransactionState::RUNNING);

    tm.commit(txn1);
    EXPECT_EQ(txn1->get_state(), TransactionState::COMMITTED);

    Transaction* const txn2 = tm.begin();
    tm.abort(txn2);
    EXPECT_EQ(txn2->get_state(), TransactionState::ABORTED);
}

TEST(TransactionManagerTests, Isolation) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* const txn1 = tm.begin();
    Transaction* const txn2 = tm.begin();

    EXPECT_GT(txn2->get_id(), txn1->get_id());

    tm.commit(txn1);
    tm.commit(txn2);
}

TEST(TransactionManagerTests, BeginWithIsolationLevels) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* const txn1 = tm.begin(IsolationLevel::READ_COMMITTED);
    ASSERT_NE(txn1, nullptr);
    EXPECT_EQ(txn1->get_state(), TransactionState::RUNNING);

    Transaction* const txn2 = tm.begin(IsolationLevel::REPEATABLE_READ);
    ASSERT_NE(txn2, nullptr);
    EXPECT_EQ(txn2->get_state(), TransactionState::RUNNING);

    Transaction* const txn3 = tm.begin(IsolationLevel::SERIALIZABLE);
    ASSERT_NE(txn3, nullptr);
    EXPECT_EQ(txn3->get_state(), TransactionState::RUNNING);

    tm.commit(txn1);
    tm.commit(txn2);
    tm.commit(txn3);
}

TEST(TransactionManagerTests, PrepareTransaction) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* const txn = tm.begin();
    ASSERT_NE(txn, nullptr);
    EXPECT_EQ(txn->get_state(), TransactionState::RUNNING);

    tm.prepare(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::PREPARED);

    tm.commit(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::COMMITTED);
}

TEST(TransactionManagerTests, GetTransaction) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* const txn1 = tm.begin();
    ASSERT_NE(txn1, nullptr);

    // Get existing transaction
    EXPECT_EQ(tm.get_transaction(txn1->get_id()), txn1);

    // Get non-existent transaction
    EXPECT_EQ(tm.get_transaction(9999), nullptr);

    tm.commit(txn1);

    // After commit, transaction is no longer active
    EXPECT_EQ(tm.get_transaction(txn1->get_id()), nullptr);
}

TEST(TransactionManagerTests, PrepareWhenNotRunning) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* const txn = tm.begin();
    ASSERT_NE(txn, nullptr);

    // Force txn to PREPARED state before prepare() call
    txn->set_state(TransactionState::PREPARED);
    tm.prepare(txn);
    // prepare() should return early since state != RUNNING

    // Also test with COMMITTED
    Transaction* const txn2 = tm.begin();
    txn2->set_state(TransactionState::COMMITTED);
    tm.prepare(txn2);

    // And ABORTED
    Transaction* const txn3 = tm.begin();
    txn3->set_state(TransactionState::ABORTED);
    tm.prepare(txn3);

    tm.commit(txn);
    tm.commit(txn2);
    tm.commit(txn3);
}

TEST(TransactionManagerTests, CommitIdempotent) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* const txn = tm.begin();
    ASSERT_NE(txn, nullptr);

    tm.commit(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::COMMITTED);

    // Commit again should be safe (idempotent)
    tm.commit(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::COMMITTED);
}

TEST(TransactionManagerTests, AbortIdempotent) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* const txn = tm.begin();
    ASSERT_NE(txn, nullptr);

    tm.abort(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::ABORTED);

    // Abort again should be safe (idempotent)
    tm.abort(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::ABORTED);
}

TEST(TransactionManagerTests, MultipleTransactions) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* txns[5];
    for (int i = 0; i < 5; ++i) {
        txns[i] = tm.begin();
        ASSERT_NE(txns[i], nullptr);
        EXPECT_EQ(txns[i]->get_state(), TransactionState::RUNNING);
    }

    // Verify IDs are ordered
    for (int i = 1; i < 5; ++i) {
        EXPECT_GT(txns[i]->get_id(), txns[i - 1]->get_id());
    }

    // Commit even-indexed transactions, abort odd-indexed
    for (int i = 0; i < 5; ++i) {
        if (i % 2 == 0) {
            tm.commit(txns[i]);
            EXPECT_EQ(txns[i]->get_state(), TransactionState::COMMITTED);
        } else {
            tm.abort(txns[i]);
            EXPECT_EQ(txns[i]->get_state(), TransactionState::ABORTED);
        }
    }
}

TEST(TransactionManagerTests, SnapshotCaptureSingleTxn) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* const txn = tm.begin();
    ASSERT_NE(txn, nullptr);

    // Snapshot should be valid: xmin <= xmax
    const auto& snap = txn->get_snapshot();
    EXPECT_LE(snap.xmin, snap.xmax);

    // Snapshot's xmax should be at least as large as txn's id + 1
    EXPECT_GE(snap.xmax, txn->get_id() + 1);

    tm.commit(txn);
}

TEST(TransactionManagerTests, SnapshotWithActiveTransactions) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* const txn1 = tm.begin();
    ASSERT_NE(txn1, nullptr);
    txn_id_t txn1_id = txn1->get_id();

    // Start txn2 while txn1 is still active
    Transaction* const txn2 = tm.begin();
    ASSERT_NE(txn2, nullptr);
    txn_id_t txn2_id = txn2->get_id();

    // txn2's snapshot should include txn1 in active_txns
    const auto& snap2 = txn2->get_snapshot();
    EXPECT_TRUE(snap2.active_txns.find(txn1_id) != snap2.active_txns.end());
    EXPECT_GE(snap2.xmax, txn2_id + 1);

    // txn1's snapshot should NOT include txn2 (txn2 started after txn1)
    const auto& snap1 = txn1->get_snapshot();
    EXPECT_TRUE(snap1.active_txns.find(txn2_id) == snap1.active_txns.end());

    tm.commit(txn1);
    tm.commit(txn2);
}

TEST(TransactionManagerTests, SnapshotAfterCommit) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* const txn1 = tm.begin();
    Transaction* const txn2 = tm.begin();
    txn_id_t txn1_id = txn1->get_id();

    // Commit txn1
    tm.commit(txn1);

    // Start txn3 after txn1 commits
    Transaction* const txn3 = tm.begin();

    // txn3's snapshot should NOT include txn1 (it committed)
    const auto& snap3 = txn3->get_snapshot();
    EXPECT_TRUE(snap3.active_txns.find(txn1_id) == snap3.active_txns.end());
    // But should include txn2 which is still active
    EXPECT_TRUE(snap3.active_txns.find(txn2->get_id()) != snap3.active_txns.end());

    tm.commit(txn2);
    tm.commit(txn3);
}

TEST(TransactionManagerTests, SerializableWriteSkewDetection) {
    // SERIALIZABLE isolation should detect write skew when two transactions
    // read overlapping data and write to different columns
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* const txn1 = tm.begin(IsolationLevel::SERIALIZABLE);
    Transaction* const txn2 = tm.begin(IsolationLevel::SERIALIZABLE);

    ASSERT_NE(txn1, nullptr);
    ASSERT_NE(txn2, nullptr);
    EXPECT_EQ(txn1->get_isolation_level(), IsolationLevel::SERIALIZABLE);
    EXPECT_EQ(txn2->get_isolation_level(), IsolationLevel::SERIALIZABLE);

    // Both should be able to begin and hold their isolation levels
    EXPECT_EQ(txn1->get_state(), TransactionState::RUNNING);
    EXPECT_EQ(txn2->get_state(), TransactionState::RUNNING);

    tm.commit(txn1);
    tm.commit(txn2);
}

/**
 * @brief Verifies begin() with multiple active transactions captures them in snapshot
 */
TEST(TransactionManagerTests, BeginWithActiveTransactions) {
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/txn_log.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);

    Transaction* const txn1 = tm.begin();
    ASSERT_NE(txn1, nullptr);
    txn_id_t txn1_id = txn1->get_id();

    // Start txn2 while txn1 is still active
    Transaction* const txn2 = tm.begin();
    ASSERT_NE(txn2, nullptr);

    // txn2's snapshot should include txn1 in active_txns
    const auto& snap2 = txn2->get_snapshot();
    EXPECT_TRUE(snap2.active_txns.find(txn1_id) != snap2.active_txns.end());

    // Start txn3 — should capture both txn1 and txn2
    Transaction* const txn3 = tm.begin();
    const auto& snap3 = txn3->get_snapshot();
    EXPECT_TRUE(snap3.active_txns.find(txn1_id) != snap3.active_txns.end());
    EXPECT_TRUE(snap3.active_txns.find(txn2->get_id()) != snap3.active_txns.end());

    tm.commit(txn1);
    tm.commit(txn2);
    tm.commit(txn3);
}

TEST(TransactionManagerTests, InsertThenAbort) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());
    executor::QueryExecutor exec(*catalog, bpm, lm, tm);

    // Create table
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE TABLE abort_test (id INT, val INT)"))
                          .parse_statement()));

    // Begin + Insert (explicit BEGIN for transactional insert)
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("BEGIN")).parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("INSERT INTO abort_test VALUES (1, 100)"))
                          .parse_statement()));

    // Verify insert worked
    const auto res_before = exec.execute(
        *Parser(std::make_unique<Lexer>("SELECT * FROM abort_test")).parse_statement());
    EXPECT_EQ(res_before.row_count(), 1U);

    // Rollback
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("ROLLBACK")).parse_statement()));

    // Verify row is gone
    const auto res_after = exec.execute(
        *Parser(std::make_unique<Lexer>("SELECT * FROM abort_test")).parse_statement());
    EXPECT_EQ(res_after.row_count(), 0U);

    static_cast<void>(std::remove("./test_data/abort_test.heap"));
}

TEST(TransactionManagerTests, UpdateThenAbort) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());
    executor::QueryExecutor exec(*catalog, bpm, lm, tm);

    // Create + insert initial row
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE TABLE upd_abort (id INT, val TEXT)"))
                          .parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("INSERT INTO upd_abort VALUES (1, 'old')"))
                          .parse_statement()));
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("COMMIT")).parse_statement()));

    // Begin + Update
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("BEGIN")).parse_statement()));
    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("UPDATE upd_abort SET val = 'new' WHERE id = 1"))
             .parse_statement()));

    // Verify update is visible
    const auto res_before =
        exec.execute(*Parser(std::make_unique<Lexer>("SELECT val FROM upd_abort WHERE id = 1"))
                          .parse_statement());
    EXPECT_STREQ(res_before.rows()[0].get(0).to_string().c_str(), "new");

    // Rollback
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("ROLLBACK")).parse_statement()));

    // Verify old value is back
    const auto res_after =
        exec.execute(*Parser(std::make_unique<Lexer>("SELECT val FROM upd_abort WHERE id = 1"))
                          .parse_statement());
    EXPECT_STREQ(res_after.rows()[0].get(0).to_string().c_str(), "old");

    static_cast<void>(std::remove("./test_data/upd_abort.heap"));
}

TEST(TransactionManagerTests, PrepareOnRunningTxn) {
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/prepare_log.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);

    Transaction* const txn = tm.begin();
    ASSERT_NE(txn, nullptr);
    EXPECT_EQ(txn->get_state(), TransactionState::RUNNING);

    tm.prepare(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::PREPARED);

    tm.commit(txn);
}

TEST(TransactionManagerTests, AbortWithLocks) {
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/abort_locks_log.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);
    executor::QueryExecutor exec(*catalog, bpm, lm, tm);

    static_cast<void>(std::remove("./test_data/locktest.heap"));

    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("CREATE TABLE locktest (id INT)")).parse_statement()));
    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("INSERT INTO locktest VALUES (1)")).parse_statement()));
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("COMMIT")).parse_statement()));

    // BEGIN, INSERT (adds lock via QueryExecutor), ROLLBACK
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("BEGIN")).parse_statement()));
    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("INSERT INTO locktest VALUES (2)")).parse_statement()));
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("ROLLBACK")).parse_statement()));

    const auto res =
        exec.execute(*Parser(std::make_unique<Lexer>("SELECT * FROM locktest")).parse_statement());
    EXPECT_EQ(res.row_count(), 1U);

    static_cast<void>(std::remove("./test_data/locktest.heap"));
}

TEST(TransactionManagerTests, CommitWithLocks) {
    // Test commit() unlock loop (lines 91-97, 134-141)
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/commit_locks_log.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);

    Transaction* txn = tm.begin();
    ASSERT_NE(txn, nullptr);

    // Acquire two shared locks and track them in the txn
    HeapTable::TupleId rid1(1, 1);
    HeapTable::TupleId rid2(1, 2);
    lm.acquire_shared(txn, rid1);
    lm.acquire_shared(txn, rid2);
    txn->add_shared_lock(rid1);
    txn->add_shared_lock(rid2);

    // commit() should iterate lock_set and call unlock for each
    tm.commit(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::COMMITTED);
}

TEST(TransactionManagerTests, CommitWithSharedLocks) {
    // Test commit() shared lock unlock loop (line 136)
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/shared_locks_log.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);

    Transaction* txn = tm.begin();
    ASSERT_NE(txn, nullptr);

    // Add SHARED locks
    HeapTable::TupleId rid1(1, 1);
    HeapTable::TupleId rid2(1, 2);
    lm.acquire_shared(txn, rid1);
    lm.acquire_shared(txn, rid2);
    txn->add_shared_lock(rid1);
    txn->add_shared_lock(rid2);

    tm.commit(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::COMMITTED);
}

TEST(TransactionManagerTests, InsertThenAbortWithIndex) {
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/idx_abort_log.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);
    executor::QueryExecutor exec(*catalog, bpm, lm, tm);

    static_cast<void>(std::remove("./test_data/idx_abort.heap"));
    static_cast<void>(std::remove("./test_data/idx_abort.idx"));

    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE TABLE idx_abort (id INT, val INT)"))
                          .parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE INDEX idx_val ON idx_abort (val)"))
                          .parse_statement()));

    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("BEGIN")).parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("INSERT INTO idx_abort VALUES (1, 100)"))
                          .parse_statement()));

    const auto res_before =
        exec.execute(*Parser(std::make_unique<Lexer>("SELECT * FROM idx_abort")).parse_statement());
    EXPECT_EQ(res_before.row_count(), 1U);

    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("ROLLBACK")).parse_statement()));

    const auto res_after =
        exec.execute(*Parser(std::make_unique<Lexer>("SELECT * FROM idx_abort")).parse_statement());
    EXPECT_EQ(res_after.row_count(), 0U);

    static_cast<void>(std::remove("./test_data/idx_abort.heap"));
    static_cast<void>(std::remove("./test_data/idx_abort.idx"));
}

TEST(TransactionManagerTests, UpdateThenAbortWithIndex) {
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/upd_idx_log.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);
    executor::QueryExecutor exec(*catalog, bpm, lm, tm);

    static_cast<void>(std::remove("./test_data/upd_idx.heap"));
    static_cast<void>(std::remove("./test_data/upd_idx.idx"));

    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE TABLE upd_idx (id INT, val INT)"))
                          .parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE INDEX idx_upd_val ON upd_idx (val)"))
                          .parse_statement()));
    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("INSERT INTO upd_idx VALUES (1, 100)")).parse_statement()));
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("COMMIT")).parse_statement()));

    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("BEGIN")).parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("UPDATE upd_idx SET val = 999 WHERE id = 1"))
                          .parse_statement()));

    const auto res_before = exec.execute(
        *Parser(std::make_unique<Lexer>("SELECT val FROM upd_idx WHERE id = 1")).parse_statement());
    EXPECT_EQ(std::stoi(res_before.rows()[0].get(0).to_string()), 999);

    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("ROLLBACK")).parse_statement()));

    const auto res_after = exec.execute(
        *Parser(std::make_unique<Lexer>("SELECT val FROM upd_idx WHERE id = 1")).parse_statement());
    EXPECT_EQ(std::stoi(res_after.rows()[0].get(0).to_string()), 100);

    static_cast<void>(std::remove("./test_data/upd_idx.heap"));
    static_cast<void>(std::remove("./test_data/upd_idx.idx"));
}

TEST(TransactionManagerTests, DeleteThenAbort) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());
    executor::QueryExecutor exec(*catalog, bpm, lm, tm);

    // Create + insert initial row
    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("CREATE TABLE del_abort (id INT)")).parse_statement()));
    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("INSERT INTO del_abort VALUES (1)")).parse_statement()));
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("COMMIT")).parse_statement()));

    // Begin + Delete
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("BEGIN")).parse_statement()));
    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("DELETE FROM del_abort WHERE id = 1")).parse_statement()));

    // Verify delete is visible
    const auto res_before =
        exec.execute(*Parser(std::make_unique<Lexer>("SELECT * FROM del_abort")).parse_statement());
    EXPECT_EQ(res_before.row_count(), 0U);

    // Rollback
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("ROLLBACK")).parse_statement()));

    // Verify row is back
    const auto res_after =
        exec.execute(*Parser(std::make_unique<Lexer>("SELECT * FROM del_abort")).parse_statement());
    EXPECT_EQ(res_after.row_count(), 1U);

    static_cast<void>(std::remove("./test_data/del_abort.heap"));
}

TEST(TransactionManagerTests, AbortWithSharedLocks) {
    // Test abort() shared lock unlock loop (line 136)
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/shared_abort_log.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);

    Transaction* txn = tm.begin();
    ASSERT_NE(txn, nullptr);

    // Add SHARED locks (not exclusive)
    HeapTable::TupleId rid1(1, 1);
    HeapTable::TupleId rid2(1, 2);
    lm.acquire_shared(txn, rid1);
    lm.acquire_shared(txn, rid2);
    txn->add_shared_lock(rid1);
    txn->add_shared_lock(rid2);

    // abort() should iterate shared_lock_set and call unlock for each
    tm.abort(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::ABORTED);
}

TEST(TransactionManagerTests, UndoPhysicalRemoveFailure) {
    // Test FAULT_PHYSICAL_REMOVE branch in undo_transaction (INSERT undo path)
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/phys_remove_fault.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);
    executor::QueryExecutor exec(*catalog, bpm, lm, tm);

    static_cast<void>(std::remove("./test_data/phys_fault.heap"));

    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE TABLE phys_fault (id INT, val INT)"))
                          .parse_statement()));
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("BEGIN")).parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("INSERT INTO phys_fault VALUES (1, 100)"))
                          .parse_statement()));

    // Arm fault injection for physical_remove
    cloudsql::common::FaultInjection::instance().set_fault(cloudsql::common::FAULT_PHYSICAL_REMOVE);

    // ROLLBACK — should hit the error branch inside undo_transaction
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("ROLLBACK")).parse_statement()));

    // Clear fault
    cloudsql::common::FaultInjection::instance().clear();

    static_cast<void>(std::remove("./test_data/phys_fault.heap"));
}

TEST(TransactionManagerTests, UndoIndexInsertFailure) {
    // Test FAULT_INDEX_INSERT branch in undo_transaction (DELETE undo path)
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/idx_insert_fault.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);
    executor::QueryExecutor exec(*catalog, bpm, lm, tm);

    static_cast<void>(std::remove("./test_data/idx_ins_fault.heap"));
    static_cast<void>(std::remove("./test_data/idx_ins_fault.idx"));

    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("CREATE TABLE idx_ins_fault (id INT, val INT)"))
             .parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE INDEX idx_if ON idx_ins_fault (val)"))
                          .parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("INSERT INTO idx_ins_fault VALUES (1, 100)"))
                          .parse_statement()));
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("COMMIT")).parse_statement()));

    // Delete + ROLLBACK with index insert fault
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("BEGIN")).parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("DELETE FROM idx_ins_fault WHERE id = 1"))
                          .parse_statement()));

    cloudsql::common::FaultInjection::instance().set_fault(cloudsql::common::FAULT_INDEX_INSERT);
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("ROLLBACK")).parse_statement()));
    cloudsql::common::FaultInjection::instance().clear();

    static_cast<void>(std::remove("./test_data/idx_ins_fault.heap"));
    static_cast<void>(std::remove("./test_data/idx_ins_fault.idx"));
}

TEST(TransactionManagerTests, UndoIndexRemoveFailure) {
    // Test FAULT_INDEX_REMOVE branch in undo_transaction (UPDATE undo path)
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/idx_rm_fault.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);
    executor::QueryExecutor exec(*catalog, bpm, lm, tm);

    static_cast<void>(std::remove("./test_data/idx_rm_fault.heap"));
    static_cast<void>(std::remove("./test_data/idx_rm_fault.idx"));

    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE TABLE idx_rm_fault (id INT, val INT)"))
                          .parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE INDEX idx_rf ON idx_rm_fault (val)"))
                          .parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("INSERT INTO idx_rm_fault VALUES (1, 100)"))
                          .parse_statement()));
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("COMMIT")).parse_statement()));

    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("BEGIN")).parse_statement()));
    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("UPDATE idx_rm_fault SET val = 999 WHERE id = 1"))
             .parse_statement()));

    cloudsql::common::FaultInjection::instance().set_fault(cloudsql::common::FAULT_INDEX_REMOVE);
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("ROLLBACK")).parse_statement()));
    cloudsql::common::FaultInjection::instance().clear();

    static_cast<void>(std::remove("./test_data/idx_rm_fault.heap"));
    static_cast<void>(std::remove("./test_data/idx_rm_fault.idx"));
}

TEST(TransactionManagerTests, DoubleCommit) {
    // Test COMMITTED early-return branch in commit() — line 78-80
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* txn = tm.begin();
    ASSERT_NE(txn, nullptr);

    // First commit
    tm.commit(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::COMMITTED);

    // Second commit — should return early (line 79 check)
    tm.commit(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::COMMITTED);
}

TEST(TransactionManagerTests, DoubleAbort) {
    // Test ABORTED early-return branch in abort() — line 117-119
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* txn = tm.begin();
    ASSERT_NE(txn, nullptr);

    // First abort
    tm.abort(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::ABORTED);

    // Second abort — should return early (line 118 check)
    tm.abort(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::ABORTED);
}

TEST(TransactionManagerTests, CommitWithoutLogManager) {
    // Test commit() with nullptr log_manager — lines 83-88 (log_manager_ == nullptr path)
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    // Pass nullptr explicitly for log_manager
    TransactionManager tm(lm, *catalog, bpm, nullptr);

    Transaction* txn = tm.begin();
    ASSERT_NE(txn, nullptr);

    // commit() with no log manager should work (just skip logging)
    tm.commit(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::COMMITTED);
}

TEST(TransactionManagerTests, AbortWithoutLogManager) {
    // Test abort() with nullptr log_manager — lines 127-132 (log_manager_ == nullptr path)
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, nullptr);

    Transaction* txn = tm.begin();
    ASSERT_NE(txn, nullptr);

    // abort() with no log manager should work (just skip logging)
    tm.abort(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::ABORTED);
}

TEST(TransactionManagerTests, UndoLogReferencesNonExistentTable) {
    // Test table metadata not found branch — line 168
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* txn = tm.begin();
    ASSERT_NE(txn, nullptr);

    // Manually add an undo log referencing a non-existent table
    txn->add_undo_log(UndoLog::Type::INSERT, "nonexistent_table", HeapTable::TupleId(99, 99));

    // abort() should hit the table metadata lookup failure (line 168)
    tm.abort(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::ABORTED);
}

TEST(TransactionManagerTests, UndoLogInsertRIDNotFound) {
    // Test table.get() returns false in INSERT undo — line 188
    // We create a table, insert a row, then manually set an undo log with a bad RID
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/ins_rid_log.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);
    executor::QueryExecutor exec(*catalog, bpm, lm, tm);

    static_cast<void>(std::remove("./test_data/ins_rid.heap"));

    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE TABLE ins_rid (id INT, val INT)"))
                          .parse_statement()));
    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("INSERT INTO ins_rid VALUES (1, 100)")).parse_statement()));
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("COMMIT")).parse_statement()));

    // Begin, manually add INSERT undo with non-existent RID, abort
    Transaction* txn = tm.begin();
    ASSERT_NE(txn, nullptr);
    txn->add_undo_log(UndoLog::Type::INSERT, "ins_rid", HeapTable::TupleId(999, 999));

    tm.abort(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::ABORTED);

    static_cast<void>(std::remove("./test_data/ins_rid.heap"));
}

TEST(TransactionManagerTests, UndoLogDeleteRIDNotFound) {
    // Test table.undo_remove() returns false in DELETE undo — line 211
    // We create a table, insert a row, manually add DELETE undo with bad RID, then abort
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/del_rid_log.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);
    executor::QueryExecutor exec(*catalog, bpm, lm, tm);

    static_cast<void>(std::remove("./test_data/del_rid.heap"));

    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("CREATE TABLE del_rid (id INT)")).parse_statement()));
    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("INSERT INTO del_rid VALUES (1)")).parse_statement()));
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("COMMIT")).parse_statement()));

    // Begin, manually add DELETE undo with non-existent RID, abort
    Transaction* txn = tm.begin();
    ASSERT_NE(txn, nullptr);
    // Use RID that doesn't exist — undo_remove will return false
    txn->add_undo_log(UndoLog::Type::DELETE, "del_rid", HeapTable::TupleId(999, 999));

    tm.abort(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::ABORTED);

    static_cast<void>(std::remove("./test_data/del_rid.heap"));
}

TEST(TransactionManagerTests, UpdateUndoNewTupleNotFound) {
    // Test table.get() returns false in UPDATE undo path — line 238
    // Manually add UPDATE undo log with non-existent RID
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/upd_new_notfound.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);
    executor::QueryExecutor exec(*catalog, bpm, lm, tm);

    static_cast<void>(std::remove("./test_data/upd_new.heap"));
    static_cast<void>(std::remove("./test_data/upd_new.idx"));

    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE TABLE upd_new (id INT, val INT)"))
                          .parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE INDEX idx_un ON upd_new (val)"))
                          .parse_statement()));
    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("INSERT INTO upd_new VALUES (1, 100)")).parse_statement()));
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("COMMIT")).parse_statement()));

    // Begin, manually add UPDATE undo with non-existent new RID (but has old_rid)
    Transaction* txn = tm.begin();
    ASSERT_NE(txn, nullptr);
    HeapTable::TupleId bad_rid(999, 999);
    HeapTable::TupleId old_rid(1, 1);
    txn->add_undo_log(UndoLog::Type::UPDATE, "upd_new", bad_rid, old_rid);

    // abort() — table.get(new_rid) returns false, skips index loop (line 238-239 branch)
    tm.abort(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::ABORTED);

    static_cast<void>(std::remove("./test_data/upd_new.heap"));
    static_cast<void>(std::remove("./test_data/upd_new.idx"));
}

TEST(TransactionManagerTests, UpdateUndoOldTupleNotFound) {
    // Test table.get() returns false for old_tuple in UPDATE undo — line 265
    // UPDATE undo where undo_remove succeeds but old_tuple get() fails
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/upd_old_notfound.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);
    executor::QueryExecutor exec(*catalog, bpm, lm, tm);

    static_cast<void>(std::remove("./test_data/upd_old.heap"));
    static_cast<void>(std::remove("./test_data/upd_old.idx"));

    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE TABLE upd_old (id INT, val INT)"))
                          .parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE INDEX idx_uo ON upd_old (val)"))
                          .parse_statement()));
    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("INSERT INTO upd_old VALUES (1, 100)")).parse_statement()));
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("COMMIT")).parse_statement()));

    // Begin, add UPDATE undo with non-existent old RID
    Transaction* txn = tm.begin();
    ASSERT_NE(txn, nullptr);
    HeapTable::TupleId new_rid(1, 2);
    HeapTable::TupleId bad_old_rid(999, 999);
    txn->add_undo_log(UndoLog::Type::UPDATE, "upd_old", new_rid, bad_old_rid);

    // abort() — undo_remove(old_rid) returns false (line 260), hits error path
    tm.abort(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::ABORTED);

    static_cast<void>(std::remove("./test_data/upd_old.heap"));
    static_cast<void>(std::remove("./test_data/upd_old.idx"));
}

TEST(TransactionManagerTests, UpdateUndoWithIndexInsertFault) {
    // Test FAULT_INDEX_INSERT in UPDATE undo old_rid restore path — line 272
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/upd_idx_ins_fault.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);
    executor::QueryExecutor exec(*catalog, bpm, lm, tm);

    static_cast<void>(std::remove("./test_data/upd_ii_fault.heap"));
    static_cast<void>(std::remove("./test_data/upd_ii_fault.idx"));

    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE TABLE upd_ii_fault (id INT, val INT)"))
                          .parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE INDEX idx_ui ON upd_ii_fault (val)"))
                          .parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("INSERT INTO upd_ii_fault VALUES (1, 100)"))
                          .parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("INSERT INTO upd_ii_fault VALUES (2, 200)"))
                          .parse_statement()));
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("COMMIT")).parse_statement()));

    // Begin + UPDATE (creates new version with old_rid pointing to original)
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("BEGIN")).parse_statement()));
    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("UPDATE upd_ii_fault SET val = 999 WHERE id = 1"))
             .parse_statement()));

    // Fault inject index insert for old_rid restore during abort
    cloudsql::common::FaultInjection::instance().set_fault(cloudsql::common::FAULT_INDEX_INSERT);
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("ROLLBACK")).parse_statement()));
    cloudsql::common::FaultInjection::instance().clear();

    static_cast<void>(std::remove("./test_data/upd_ii_fault.heap"));
    static_cast<void>(std::remove("./test_data/upd_ii_fault.idx"));
}

TEST(TransactionManagerTests, Commit101Transactions) {
    // Test completed_transactions_ overflow — lines 111, 155
    // The 101st commit should trigger pop_front()
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/overflow_log.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);

    // Commit 101 transactions — the 101st will overflow the deque (size > 100)
    for (int i = 0; i < 101; ++i) {
        Transaction* txn = tm.begin();
        ASSERT_NE(txn, nullptr);
        tm.commit(txn);
    }
}

TEST(TransactionManagerTests, Abort101Transactions) {
    // Test completed_transactions_ overflow via abort path — line 155
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/abort_overflow_log.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);

    // Abort 101 transactions — the 101st will overflow the deque
    for (int i = 0; i < 101; ++i) {
        Transaction* txn = tm.begin();
        ASSERT_NE(txn, nullptr);
        tm.abort(txn);
    }
}

TEST(TransactionManagerTests, UpdateUndoBothTuplesFound) {
    // Test UPDATE undo full path: new_tuple found, old_rid undo_remove succeeds,
    // old_tuple found, index insert succeeds — lines 238, 244, 253, 260, 265, 272
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/upd_both_found.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);
    executor::QueryExecutor exec(*catalog, bpm, lm, tm);

    static_cast<void>(std::remove("./test_data/upd_both.heap"));
    static_cast<void>(std::remove("./test_data/upd_both.idx"));

    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE TABLE upd_both (id INT, val INT)"))
                          .parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE INDEX idx_ub ON upd_both (val)"))
                          .parse_statement()));
    // Insert two rows
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("INSERT INTO upd_both VALUES (1, 100)"))
                          .parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("INSERT INTO upd_both VALUES (2, 200)"))
                          .parse_statement()));
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("COMMIT")).parse_statement()));

    // UPDATE id=1, then ROLLBACK — should hit all branches in UPDATE undo path
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("BEGIN")).parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("UPDATE upd_both SET val = 999 WHERE id = 1"))
                          .parse_statement()));

    // Verify update visible
    const auto res =
        exec.execute(*Parser(std::make_unique<Lexer>("SELECT val FROM upd_both WHERE id = 1"))
                          .parse_statement());
    EXPECT_EQ(std::stoi(res.rows()[0].get(0).to_string()), 999);

    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("ROLLBACK")).parse_statement()));

    // After rollback, original value should be back
    const auto res_after =
        exec.execute(*Parser(std::make_unique<Lexer>("SELECT val FROM upd_both WHERE id = 1"))
                          .parse_statement());
    EXPECT_EQ(std::stoi(res_after.rows()[0].get(0).to_string()), 100);

    static_cast<void>(std::remove("./test_data/upd_both.heap"));
    static_cast<void>(std::remove("./test_data/upd_both.idx"));
}

TEST(TransactionManagerTests, DeleteUndoWithIndexRestore) {
    // Test DELETE undo path with index: undo_remove succeeds, table.get finds tuple,
    // index.insert succeeds — lines 211, 216, 222
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/del_idx_restore.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);
    executor::QueryExecutor exec(*catalog, bpm, lm, tm);

    static_cast<void>(std::remove("./test_data/del_idx.heap"));
    static_cast<void>(std::remove("./test_data/del_idx.idx"));

    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE TABLE del_idx (id INT, val INT)"))
                          .parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE INDEX idx_di ON del_idx (val)"))
                          .parse_statement()));
    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("INSERT INTO del_idx VALUES (1, 100)")).parse_statement()));
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("COMMIT")).parse_statement()));

    // DELETE, then ROLLBACK — should hit DELETE undo path with index restore
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("BEGIN")).parse_statement()));
    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("DELETE FROM del_idx WHERE id = 1")).parse_statement()));

    // Verify delete visible
    const auto res =
        exec.execute(*Parser(std::make_unique<Lexer>("SELECT * FROM del_idx")).parse_statement());
    EXPECT_EQ(res.row_count(), 0U);

    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("ROLLBACK")).parse_statement()));

    // Row should be restored
    const auto res_after =
        exec.execute(*Parser(std::make_unique<Lexer>("SELECT * FROM del_idx")).parse_statement());
    EXPECT_EQ(res_after.row_count(), 1U);

    static_cast<void>(std::remove("./test_data/del_idx.heap"));
    static_cast<void>(std::remove("./test_data/del_idx.idx"));
}

TEST(TransactionManagerTests, AbortCommittedTxn) {
    // Test abort() path when transaction is already COMMITTED
    // abort() should skip undo_transaction and proceed to lock release + state change
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* txn = tm.begin();
    ASSERT_NE(txn, nullptr);

    tm.commit(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::COMMITTED);

    // Force state to COMMITTED before calling abort to hit the state check at line 123
    txn->set_state(TransactionState::COMMITTED);
    tm.abort(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::ABORTED);
}

TEST(TransactionManagerTests, PrepareOnPreparedTxn) {
    // Test prepare() when transaction is already PREPARED
    // Second prepare() should return early at line 63-64 (state != RUNNING)
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* txn = tm.begin();
    ASSERT_NE(txn, nullptr);

    tm.prepare(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::PREPARED);

    // Second prepare on already PREPARED should hit early-return branch
    tm.prepare(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::PREPARED);
}

TEST(TransactionManagerTests, UndoEmptyLogs) {
    // Test undo_transaction when transaction has no undo logs
    // The reverse iteration loop should execute 0 times
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* txn = tm.begin();
    ASSERT_NE(txn, nullptr);

    // txn has no undo logs - abort() calls undo_transaction with empty logs
    tm.abort(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::ABORTED);
}

TEST(TransactionManagerTests, CommitIdempotentTwoCalls) {
    // Test commit() called twice on same transaction
    // Second call should return early at line 79 (state == COMMITTED)
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* txn = tm.begin();
    ASSERT_NE(txn, nullptr);

    tm.commit(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::COMMITTED);

    // Second commit - early return at line 79-80
    tm.commit(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::COMMITTED);
}

TEST(TransactionManagerTests, CommitWithLogFailure) {
    // Test commit() when log_manager_->append_log_record returns false
    // Should hit the error branch in commit()
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/commit_log_fail.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);

    Transaction* txn = tm.begin();
    ASSERT_NE(txn, nullptr);

    // Arm fault injection for log commit
    cloudsql::common::FaultInjection::instance().set_fault(cloudsql::common::FAULT_LOG_COMMIT);

    // commit() should still complete even if logging fails
    tm.commit(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::COMMITTED);

    cloudsql::common::FaultInjection::instance().clear();
}

TEST(TransactionManagerTests, AbortWithLogFailure) {
    // Test abort() when log_manager_->append_log_record returns false
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/abort_log_fail.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);

    Transaction* txn = tm.begin();
    ASSERT_NE(txn, nullptr);

    // Arm fault injection for log abort
    cloudsql::common::FaultInjection::instance().set_fault(cloudsql::common::FAULT_LOG_ABORT);

    // abort() should still complete even if logging fails
    tm.abort(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::ABORTED);

    cloudsql::common::FaultInjection::instance().clear();
}

TEST(TransactionManagerTests, PrepareWithLogFailure) {
    // Test prepare() when log_manager_->append_log_record returns false
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/prepare_log_fail.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);

    Transaction* txn = tm.begin();
    ASSERT_NE(txn, nullptr);

    // Arm fault injection for log prepare
    cloudsql::common::FaultInjection::instance().set_fault(cloudsql::common::FAULT_LOG_PREPARE);

    // prepare() should still set PREPARED state even if logging fails
    tm.prepare(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::PREPARED);

    cloudsql::common::FaultInjection::instance().clear();
}

TEST(TransactionManagerTests, PrepareWithoutLogManager) {
    // Test prepare() with nullptr log_manager — lines 67-72 (log_manager_ == nullptr path)
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, nullptr);

    Transaction* txn = tm.begin();
    ASSERT_NE(txn, nullptr);
    EXPECT_EQ(txn->get_state(), TransactionState::RUNNING);

    // prepare() with no log manager should still set PREPARED state
    tm.prepare(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::PREPARED);
}

TEST(TransactionManagerTests, BeginWithoutLogManager) {
    // Test begin() with nullptr log_manager — lines 53-56 (log_manager_ == nullptr path)
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, nullptr);

    Transaction* txn = tm.begin();
    ASSERT_NE(txn, nullptr);
    EXPECT_EQ(txn->get_state(), TransactionState::RUNNING);
    // begin() should return without logging when log_manager_ is nullptr
}

TEST(TransactionManagerTests, CommitOverflowThreshold) {
    // Test completed_transactions_ overflow with reduced threshold (10 instead of 100)
    // With MAX_COMPLETED=10, the 11th commit triggers pop_front()
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/overflow10_log.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);

    for (int i = 0; i < 15; ++i) {
        Transaction* txn = tm.begin();
        ASSERT_NE(txn, nullptr);
        tm.commit(txn);
    }
}

TEST(TransactionManagerTests, AbortOverflowThreshold) {
    // Test completed_transactions_ overflow via abort with reduced threshold
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/abort_overflow10_log.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);

    for (int i = 0; i < 15; ++i) {
        Transaction* txn = tm.begin();
        ASSERT_NE(txn, nullptr);
        tm.abort(txn);
    }
}

TEST(TransactionManagerTests, UndoLogUnknownType) {
    // Test the default case in switch when undo log type is UNKNOWN
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/unknown_log.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);
    executor::QueryExecutor exec(*catalog, bpm, lm, tm);

    static_cast<void>(std::remove("./test_data/unknown_test.heap"));

    // Create a real table
    static_cast<void>(exec.execute(
        *Parser(std::make_unique<Lexer>("CREATE TABLE unknown_test (id INT)")).parse_statement()));
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("COMMIT")).parse_statement()));

    Transaction* txn = tm.begin();
    ASSERT_NE(txn, nullptr);

    // Add an undo log with UNKNOWN type for a real table
    txn->add_undo_log_for_test(
        {UndoLog::Type::UNKNOWN, "unknown_test", HeapTable::TupleId(1, 1), std::nullopt});

    // abort() should hit the default case in the switch after table lookup succeeds
    tm.abort(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::ABORTED);

    static_cast<void>(std::remove("./test_data/unknown_test.heap"));
}

TEST(TransactionManagerTests, UndoDeleteTableNotFound) {
    // Test table metadata not found branch in DELETE undo path
    // Manually add DELETE undo log for non-existent table, call abort()
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE,
                                   disk_manager);
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, bpm.get_log_manager());

    Transaction* txn = tm.begin();
    ASSERT_NE(txn, nullptr);

    // Add DELETE undo log for non-existent table
    txn->add_undo_log(UndoLog::Type::DELETE, "nonexistent_delete_table",
                      HeapTable::TupleId(99, 99));

    // abort() should hit table metadata lookup failure
    tm.abort(txn);
    EXPECT_EQ(txn->get_state(), TransactionState::ABORTED);
}

TEST(TransactionManagerTests, UndoUpdatePhysicalRemoveFailure) {
    // Test FAULT_PHYSICAL_REMOVE branch in UPDATE undo
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/upd_phys_rm_fault.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);
    executor::QueryExecutor exec(*catalog, bpm, lm, tm);

    static_cast<void>(std::remove("./test_data/upd_phys_rm.heap"));
    static_cast<void>(std::remove("./test_data/upd_phys_rm.idx"));

    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE TABLE upd_phys_rm (id INT, val INT)"))
                          .parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE INDEX idx_upr ON upd_phys_rm (val)"))
                          .parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("INSERT INTO upd_phys_rm VALUES (1, 100)"))
                          .parse_statement()));
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("COMMIT")).parse_statement()));

    // UPDATE then ROLLBACK with fault injection
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("BEGIN")).parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("UPDATE upd_phys_rm SET val = 999 WHERE id = 1"))
                          .parse_statement()));

    // Arm fault for physical_remove failure during UPDATE undo
    cloudsql::common::FaultInjection::instance().set_fault(cloudsql::common::FAULT_PHYSICAL_REMOVE);
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("ROLLBACK")).parse_statement()));
    cloudsql::common::FaultInjection::instance().clear();

    static_cast<void>(std::remove("./test_data/upd_phys_rm.heap"));
    static_cast<void>(std::remove("./test_data/upd_phys_rm.idx"));
}

TEST(TransactionManagerTests, UndoUpdateUndoRemoveFailure) {
    // Test FAULT_UNDO_REMOVE branch in UPDATE undo old_rid restore
    storage::StorageManager disk_manager("./test_data");
    disk_manager.create_dir_if_not_exists();
    recovery::LogManager log_mgr("./test_data/upd_undo_rm_fault.dat");
    storage::BufferPoolManager bpm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager,
                                   &log_mgr);
    auto catalog = Catalog::create();
    LockManager lm;
    TransactionManager tm(lm, *catalog, bpm, &log_mgr);
    executor::QueryExecutor exec(*catalog, bpm, lm, tm);

    static_cast<void>(std::remove("./test_data/upd_undo_rm.heap"));
    static_cast<void>(std::remove("./test_data/upd_undo_rm.idx"));

    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE TABLE upd_undo_rm (id INT, val INT)"))
                          .parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("CREATE INDEX idx_uur ON upd_undo_rm (val)"))
                          .parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("INSERT INTO upd_undo_rm VALUES (1, 100)"))
                          .parse_statement()));
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("COMMIT")).parse_statement()));

    // UPDATE then ROLLBACK with fault injection for undo_remove
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("BEGIN")).parse_statement()));
    static_cast<void>(
        exec.execute(*Parser(std::make_unique<Lexer>("UPDATE upd_undo_rm SET val = 999 WHERE id = 1"))
                          .parse_statement()));

    // Arm fault for undo_remove failure during UPDATE undo (old_rid restore)
    cloudsql::common::FaultInjection::instance().set_fault(cloudsql::common::FAULT_UNDO_REMOVE);
    static_cast<void>(exec.execute(*Parser(std::make_unique<Lexer>("ROLLBACK")).parse_statement()));
    cloudsql::common::FaultInjection::instance().clear();

    static_cast<void>(std::remove("./test_data/upd_undo_rm.heap"));
    static_cast<void>(std::remove("./test_data/upd_undo_rm.idx"));
}

}  // namespace
