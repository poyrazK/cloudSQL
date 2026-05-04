/**
 * @file recovery_tests.cpp
 * @brief Unit tests for Write-Ahead Logging and Recovery
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdio>
#include <fstream>
#include <ios>
#include <string>
#include <utility>
#include <vector>

#include "common/value.hpp"
#include "executor/types.hpp"
#include "recovery/log_manager.hpp"
#include "recovery/log_record.hpp"
#include "storage/heap_table.hpp"
#include "test_utils.hpp"

using namespace cloudsql;
using namespace cloudsql::recovery;
using namespace cloudsql::common;
using namespace cloudsql::executor;
using namespace cloudsql::storage;

namespace {

constexpr uint64_t TXN_100 = 100;
constexpr lsn_t PREV_LSN_99 = 99;
constexpr lsn_t CUR_LSN_101 = 101;
constexpr int64_t VAL_42 = 42;
constexpr uint64_t TXN_50 = 50;
constexpr uint64_t TXN_60 = 60;
constexpr uint64_t TXN_70 = 70;
constexpr lsn_t PREV_LSN_49 = 49;
constexpr lsn_t PREV_LSN_59 = 59;
constexpr lsn_t PREV_LSN_69 = 69;
constexpr int8_t INT8_10 = 10;
constexpr int16_t INT16_200 = 200;
constexpr int32_t INT32_3000 = 3000;
constexpr float F32_1_22 = 1.22F;
constexpr float F32_1_23 = 1.23F;
constexpr float F32_1_24 = 1.24F;
constexpr double F64_4_55 = 4.55;
constexpr double F64_4_56 = 4.56;
constexpr double F64_4_57 = 4.57;

// Helper to clean up test files
void cleanup(const std::string& file) {
    static_cast<void>(std::remove(file.c_str()));
}

TEST(RecoveryTests, LogRecordSerialization) {
    // 1. Create a dummy INSERT log record
    std::vector<Value> values;
    values.emplace_back(Value::make_int64(VAL_42));
    values.emplace_back(Value::make_text("test_string"));
    const Tuple tuple(std::move(values));

    LogRecord original(TXN_100, PREV_LSN_99, LogRecordType::INSERT, "test_table",
                       HeapTable::TupleId(1, 2), tuple);
    original.lsn_ = CUR_LSN_101;
    original.size_ = original.get_size();

    // 2. Serialize
    std::vector<char> buffer(original.size_);
    static_cast<void>(original.serialize(buffer.data()));

    // 3. Deserialize
    const LogRecord deserialized = LogRecord::deserialize(buffer.data());

    // 4. Verify
    EXPECT_EQ(deserialized.lsn_, original.lsn_);
    EXPECT_EQ(deserialized.prev_lsn_, original.prev_lsn_);
    EXPECT_EQ(deserialized.txn_id_, original.txn_id_);
    EXPECT_EQ(static_cast<int>(deserialized.type_), static_cast<int>(original.type_));
    EXPECT_EQ(deserialized.table_name_, original.table_name_);
    EXPECT_EQ(deserialized.rid_, original.rid_);

    EXPECT_EQ(deserialized.tuple_.size(), original.tuple_.size());
    EXPECT_EQ(deserialized.tuple_.get(0).to_int64(), VAL_42);
    EXPECT_EQ(deserialized.tuple_.get(1).as_text(), "test_string");
}

TEST(RecoveryTests, LogRecordAllTypes) {
    std::vector<Value> values;
    values.emplace_back(Value::make_bool(true));
    values.emplace_back(static_cast<int8_t>(INT8_10));
    values.emplace_back(static_cast<int16_t>(INT16_200));
    values.emplace_back(static_cast<int32_t>(INT32_3000));
    values.emplace_back(static_cast<float>(F32_1_23));
    values.emplace_back(static_cast<double>(F64_4_56));
    values.emplace_back(Value::make_null());

    const Tuple tuple(std::move(values));
    LogRecord original(TXN_50, PREV_LSN_49, LogRecordType::INSERT, "types_table",
                       HeapTable::TupleId(1, 1), tuple);
    original.size_ = original.get_size();

    std::vector<char> buffer(original.size_);
    static_cast<void>(original.serialize(buffer.data()));

    const LogRecord deserialized = LogRecord::deserialize(buffer.data());

    ASSERT_EQ(deserialized.tuple_.size(), 7U);
    EXPECT_TRUE(deserialized.tuple_.get(0).as_bool());
    EXPECT_EQ(deserialized.tuple_.get(1).as_int8(), INT8_10);
    EXPECT_EQ(deserialized.tuple_.get(2).as_int16(), INT16_200);
    EXPECT_EQ(deserialized.tuple_.get(3).as_int32(), INT32_3000);
    EXPECT_GT(deserialized.tuple_.get(4).as_float32(), F32_1_22);
    EXPECT_LT(deserialized.tuple_.get(4).as_float32(), F32_1_24);
    EXPECT_GT(deserialized.tuple_.get(5).as_float64(), F64_4_55);
    EXPECT_LT(deserialized.tuple_.get(5).as_float64(), F64_4_57);
    EXPECT_TRUE(deserialized.tuple_.get(6).is_null());
}

TEST(RecoveryTests, LogRecordVariants) {
    /* Test BEGIN/COMMIT/ABORT which have no tuple/table */
    {
        LogRecord rec(1, -1, LogRecordType::BEGIN);
        std::vector<char> buf(rec.get_size());
        rec.serialize(buf.data());
        auto d = LogRecord::deserialize(buf.data());
        EXPECT_EQ(d.type_, LogRecordType::BEGIN);
        EXPECT_EQ(d.txn_id_, 1);
        EXPECT_EQ(d.prev_lsn_, -1);
    }
    {
        LogRecord rec(2, 10, LogRecordType::COMMIT);
        std::vector<char> buf(rec.get_size());
        rec.serialize(buf.data());
        auto d = LogRecord::deserialize(buf.data());
        EXPECT_EQ(d.type_, LogRecordType::COMMIT);
        EXPECT_EQ(d.txn_id_, 2);
        EXPECT_EQ(d.prev_lsn_, 10);
    }
    {
        LogRecord rec(3, 20, LogRecordType::ABORT);
        std::vector<char> buf(rec.get_size());
        rec.serialize(buf.data());
        auto d = LogRecord::deserialize(buf.data());
        EXPECT_EQ(d.type_, LogRecordType::ABORT);
        EXPECT_EQ(d.txn_id_, 3);
        EXPECT_EQ(d.prev_lsn_, 20);
    }
}

TEST(RecoveryTests, LogManagerBasic) {
    const std::string log_file = "test_log_basic.log";
    cleanup(log_file);

    {
        LogManager log_manager(log_file);
        log_manager.run_flush_thread();

        // Append a few logs
        LogRecord qlog1(1, -1, LogRecordType::BEGIN);
        ASSERT_TRUE(log_manager.append_log_record(qlog1));
        EXPECT_EQ(qlog1.lsn_, 0);

        LogRecord qlog2(1, qlog1.lsn_, LogRecordType::COMMIT);
        ASSERT_TRUE(log_manager.append_log_record(qlog2));
        EXPECT_EQ(qlog2.lsn_, 1);

        // Wait for flush
        log_manager.flush(true);
        EXPECT_GE(log_manager.get_persistent_lsn(), qlog2.lsn_);
    }

    // Verify file content size roughly
    std::ifstream in(log_file, std::ios::binary | std::ios::ate);
    EXPECT_GT(in.tellg(), 0);

    cleanup(log_file);
}

// ============= LogRecord UPDATE Tests =============

TEST(RecoveryTests, LogRecordUpdateRoundTrip) {
    // UPDATE has both old_tuple_ and tuple_
    std::vector<Value> old_values;
    old_values.emplace_back(Value::make_int64(1));
    old_values.emplace_back(Value::make_text("old_value"));
    const Tuple old_tuple(std::move(old_values));

    std::vector<Value> new_values;
    new_values.emplace_back(Value::make_int64(1));
    new_values.emplace_back(Value::make_text("new_value"));
    const Tuple new_tuple(std::move(new_values));

    LogRecord original(TXN_100, PREV_LSN_99, LogRecordType::UPDATE, "update_table",
                       HeapTable::TupleId(5, 10), old_tuple, new_tuple);
    original.lsn_ = CUR_LSN_101;
    original.size_ = original.get_size();

    std::vector<char> buffer(original.size_);
    static_cast<void>(original.serialize(buffer.data()));

    const LogRecord deserialized = LogRecord::deserialize(buffer.data());

    EXPECT_EQ(deserialized.type_, LogRecordType::UPDATE);
    EXPECT_EQ(deserialized.table_name_, "update_table");
    EXPECT_EQ(deserialized.txn_id_, TXN_100);
    EXPECT_EQ(deserialized.lsn_, CUR_LSN_101);
    // Check old tuple (stored in old_tuple_)
    EXPECT_EQ(deserialized.old_tuple_.get(0).to_int64(), 1);
    EXPECT_EQ(deserialized.old_tuple_.get(1).as_text(), "old_value");
    // Check new tuple (stored in tuple_)
    EXPECT_EQ(deserialized.tuple_.get(0).to_int64(), 1);
    EXPECT_EQ(deserialized.tuple_.get(1).as_text(), "new_value");
}

TEST(RecoveryTests, LogRecordMarkDeleteRoundTrip) {
    std::vector<Value> old_values;
    old_values.emplace_back(Value::make_int64(100));
    old_values.emplace_back(Value::make_text("to_delete"));
    const Tuple old_tuple(std::move(old_values));

    LogRecord original(TXN_50, PREV_LSN_49, LogRecordType::MARK_DELETE, "delete_table",
                       HeapTable::TupleId(3, 7), old_tuple);
    original.size_ = original.get_size();

    std::vector<char> buffer(original.size_);
    static_cast<void>(original.serialize(buffer.data()));

    const LogRecord deserialized = LogRecord::deserialize(buffer.data());

    EXPECT_EQ(deserialized.type_, LogRecordType::MARK_DELETE);
    EXPECT_EQ(deserialized.table_name_, "delete_table");
    // MARK_DELETE stores tuple in old_tuple_ (like all DELETE variants)
    EXPECT_EQ(deserialized.old_tuple_.get(0).to_int64(), 100);
    EXPECT_EQ(deserialized.old_tuple_.get(1).as_text(), "to_delete");
}

TEST(RecoveryTests, LogRecordApplyDeleteRoundTrip) {
    std::vector<Value> old_values;
    old_values.emplace_back(Value::make_int64(200));
    const Tuple old_tuple(std::move(old_values));

    LogRecord original(TXN_60, PREV_LSN_59, LogRecordType::APPLY_DELETE, "apply_table",
                       HeapTable::TupleId(4, 8), old_tuple);
    original.size_ = original.get_size();

    std::vector<char> buffer(original.size_);
    static_cast<void>(original.serialize(buffer.data()));

    const LogRecord deserialized = LogRecord::deserialize(buffer.data());

    EXPECT_EQ(deserialized.type_, LogRecordType::APPLY_DELETE);
    EXPECT_EQ(deserialized.table_name_, "apply_table");
    // DELETE variants store tuple in old_tuple_
    EXPECT_EQ(deserialized.old_tuple_.get(0).to_int64(), 200);
}

TEST(RecoveryTests, LogRecordRollbackDeleteRoundTrip) {
    std::vector<Value> old_values;
    old_values.emplace_back(Value::make_int64(300));
    const Tuple old_tuple(std::move(old_values));

    LogRecord original(TXN_70, PREV_LSN_69, LogRecordType::ROLLBACK_DELETE, "rollback_table",
                       HeapTable::TupleId(5, 9), old_tuple);
    original.size_ = original.get_size();

    std::vector<char> buffer(original.size_);
    static_cast<void>(original.serialize(buffer.data()));

    const LogRecord deserialized = LogRecord::deserialize(buffer.data());

    EXPECT_EQ(deserialized.type_, LogRecordType::ROLLBACK_DELETE);
    EXPECT_EQ(deserialized.table_name_, "rollback_table");
    // DELETE variants store tuple in old_tuple_
    EXPECT_EQ(deserialized.old_tuple_.get(0).to_int64(), 300);
}

TEST(RecoveryTests, LogRecordPrepareRoundTrip) {
    LogRecord original(TXN_100, PREV_LSN_99, LogRecordType::PREPARE);
    original.size_ = original.get_size();

    std::vector<char> buffer(original.size_);
    static_cast<void>(original.serialize(buffer.data()));

    const LogRecord deserialized = LogRecord::deserialize(buffer.data());

    EXPECT_EQ(deserialized.type_, LogRecordType::PREPARE);
    EXPECT_EQ(deserialized.txn_id_, TXN_100);
    EXPECT_EQ(deserialized.prev_lsn_, PREV_LSN_99);
}

TEST(RecoveryTests, LogRecordNewPageRoundTrip) {
    LogRecord original(TXN_100, PREV_LSN_99, LogRecordType::NEW_PAGE, 42);
    original.size_ = original.get_size();

    std::vector<char> buffer(original.size_);
    static_cast<void>(original.serialize(buffer.data()));

    const LogRecord deserialized = LogRecord::deserialize(buffer.data());

    EXPECT_EQ(deserialized.type_, LogRecordType::NEW_PAGE);
    EXPECT_EQ(deserialized.page_id_, 42);
}

TEST(RecoveryTests, LogRecordGetSize_AllTypes) {
    // Test get_size() for all record types
    struct TestCase {
        LogRecordType type;
        bool has_tuple;
    };
    std::vector<TestCase> cases = {
        {LogRecordType::BEGIN, false},
        {LogRecordType::PREPARE, false},
        {LogRecordType::COMMIT, false},
        {LogRecordType::ABORT, false},
        {LogRecordType::NEW_PAGE, false},
        {LogRecordType::INSERT, true},
        {LogRecordType::UPDATE, true},
        {LogRecordType::MARK_DELETE, true},
        {LogRecordType::APPLY_DELETE, true},
        {LogRecordType::ROLLBACK_DELETE, true},
    };

    for (const auto& c : cases) {
        LogRecord rec;
        if (c.has_tuple) {
            std::vector<Value> vals;
            vals.emplace_back(Value::make_int64(42));
            rec = LogRecord(1, 0, c.type, "t", HeapTable::TupleId(1, 1), Tuple(std::move(vals)));
        } else if (c.type == LogRecordType::NEW_PAGE) {
            rec = LogRecord(1, 0, c.type, 99);
        } else {
            rec = LogRecord(1, 0, c.type);
        }
        uint32_t size = rec.get_size();
        EXPECT_GT(size, 0u) << " get_size() returned 0 for type " << static_cast<int>(c.type);
    }
}

TEST(RecoveryTests, LogRecordGetSize_CachedAfterSerialize) {
    std::vector<Value> values;
    values.emplace_back(Value::make_int64(42));
    const Tuple tuple(std::move(values));

    LogRecord rec(TXN_100, PREV_LSN_99, LogRecordType::INSERT, "test_table",
                  HeapTable::TupleId(1, 1), tuple);

    // Before serialize, size_ might be 0, get_size() computes
    uint32_t size_before = rec.get_size();
    EXPECT_GT(size_before, 0u);

    // After serialize, size_ should be set
    std::vector<char> buffer(size_before);
    rec.serialize(buffer.data());
    uint32_t size_after = rec.get_size();

    // Both should be equal
    EXPECT_EQ(size_before, size_after);
}

TEST(RecoveryTests, LogRecordMixedTypesInSequence) {
    // Simulate a transaction sequence: BEGIN -> INSERT -> UPDATE -> COMMIT
    std::vector<char> all_data;

    // BEGIN
    LogRecord begin_rec(1, -1, LogRecordType::BEGIN);
    std::vector<char> begin_buf(begin_rec.get_size());
    begin_rec.serialize(begin_buf.data());
    all_data.insert(all_data.end(), begin_buf.begin(), begin_buf.end());

    // INSERT
    std::vector<Value> vals;
    vals.emplace_back(Value::make_int64(1));
    vals.emplace_back(Value::make_text("test"));
    LogRecord insert_rec(1, begin_rec.lsn_, LogRecordType::INSERT, "t",
                         HeapTable::TupleId(1, 1), Tuple(std::move(vals)));
    std::vector<char> insert_buf(insert_rec.get_size());
    insert_rec.serialize(insert_buf.data());
    all_data.insert(all_data.end(), insert_buf.begin(), insert_buf.end());

    // COMMIT
    LogRecord commit_rec(1, insert_rec.lsn_, LogRecordType::COMMIT);
    std::vector<char> commit_buf(commit_rec.get_size());
    commit_rec.serialize(commit_buf.data());
    all_data.insert(all_data.end(), commit_buf.begin(), commit_buf.end());

    // Deserialize all three
    size_t offset = 0;
    auto d1 = LogRecord::deserialize(all_data.data() + offset);
    offset += d1.get_size();
    auto d2 = LogRecord::deserialize(all_data.data() + offset);
    offset += d2.get_size();
    auto d3 = LogRecord::deserialize(all_data.data() + offset);

    EXPECT_EQ(d1.type_, LogRecordType::BEGIN);
    EXPECT_EQ(d2.type_, LogRecordType::INSERT);
    EXPECT_EQ(d3.type_, LogRecordType::COMMIT);
    EXPECT_EQ(d2.tuple_.get(0).to_int64(), 1);
    EXPECT_EQ(d2.tuple_.get(1).as_text(), "test");
}

}  // namespace
