/**
 * @file recovery_tests.cpp
 * @brief Unit tests for Write-Ahead Logging and Recovery
 */

#include <cassert>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <utility>
#include <cstdint>

#include "common/value.hpp"
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

using cloudsql::tests::tests_passed;
using cloudsql::tests::tests_failed;

constexpr uint64_t TXN_100 = 100;
constexpr lsn_t PREV_LSN_99 = 99;
constexpr lsn_t CUR_LSN_101 = 101;
constexpr int64_t VAL_42 = 42;
constexpr uint64_t TXN_50 = 50;
constexpr lsn_t PREV_LSN_49 = 49;
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

TEST(LogRecordSerialization) {
    // 1. Create a dummy INSERT log record
    std::vector<Value> values;
    values.emplace_back(Value::make_int64(VAL_42));
    values.emplace_back(Value::make_text("test_string"));
    const Tuple tuple(std::move(values));

    LogRecord original(TXN_100, PREV_LSN_99, LogRecordType::INSERT, "test_table", HeapTable::TupleId(1, 2),
                       tuple);
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
    EXPECT_TRUE(deserialized.table_name_ == original.table_name_);
    EXPECT_TRUE(deserialized.rid_ == original.rid_);

    EXPECT_EQ(deserialized.tuple_.size(), original.tuple_.size());
    EXPECT_EQ(deserialized.tuple_.get(0).to_int64(), VAL_42);
    EXPECT_TRUE(deserialized.tuple_.get(1).as_text() == "test_string");
}

TEST(LogRecordAllTypes) {
    std::vector<Value> values;
    values.emplace_back(Value::make_bool(true));
    values.emplace_back(static_cast<int8_t>(INT8_10));
    values.emplace_back(static_cast<int16_t>(INT16_200));
    values.emplace_back(static_cast<int32_t>(INT32_3000));
    values.emplace_back(static_cast<float>(F32_1_23));
    values.emplace_back(static_cast<double>(F64_4_56));
    values.emplace_back(Value::make_null());

    const Tuple tuple(std::move(values));
    LogRecord original(TXN_50, PREV_LSN_49, LogRecordType::INSERT, "types_table", HeapTable::TupleId(1, 1),
                       tuple);
    original.size_ = original.get_size();

    std::vector<char> buffer(original.size_);
    static_cast<void>(original.serialize(buffer.data()));

    const LogRecord deserialized = LogRecord::deserialize(buffer.data());

    EXPECT_EQ(deserialized.tuple_.size(), static_cast<size_t>(7)); // NOLINT(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    EXPECT_TRUE(deserialized.tuple_.get(0).as_bool());
    EXPECT_EQ(deserialized.tuple_.get(1).as_int8(), INT8_10);
    EXPECT_EQ(deserialized.tuple_.get(2).as_int16(), INT16_200);
    EXPECT_EQ(deserialized.tuple_.get(3).as_int32(), INT32_3000);
    EXPECT_TRUE(deserialized.tuple_.get(4).as_float32() > F32_1_22 &&
                deserialized.tuple_.get(4).as_float32() < F32_1_24);
    EXPECT_TRUE(deserialized.tuple_.get(5).as_float64() > F64_4_55 &&
                deserialized.tuple_.get(5).as_float64() < F64_4_57);
    EXPECT_TRUE(deserialized.tuple_.get(6).is_null());
}

TEST(LogManagerBasic) {
    const std::string log_file = "test_log_basic.log";
    cleanup(log_file);

    {
        LogManager log_manager(log_file);
        log_manager.run_flush_thread();

        // Append a few logs
        LogRecord qlog1(1, -1, LogRecordType::BEGIN);
        const lsn_t lsn1 = log_manager.append_log_record(qlog1);
        EXPECT_EQ(lsn1, 0);

        LogRecord qlog2(1, lsn1, LogRecordType::COMMIT);
        const lsn_t lsn2 = log_manager.append_log_record(qlog2);
        EXPECT_EQ(lsn2, 1);

        // Wait for flush
        log_manager.flush(true);
        EXPECT_TRUE(log_manager.get_persistent_lsn() >= lsn2);
    }

    // Verify file content size roughly
    std::ifstream in(log_file, std::ios::binary | std::ios::ate);
    EXPECT_TRUE(in.tellg() > 0);

    cleanup(log_file);
}

} // namespace

int main() {
    std::cout << "cloudSQL Recovery Test Suite" << "\n";
    std::cout << "============================" << "\n";

    RUN_TEST(LogRecordSerialization);
    RUN_TEST(LogRecordAllTypes);
    RUN_TEST(LogManagerBasic);

    std::cout << "\n"
              << "Results: " << tests_passed << " passed, " << tests_failed << " failed"
              << "\n";
    return (tests_failed > 0);
}
