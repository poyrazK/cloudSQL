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

#include "common/value.hpp"
#include "recovery/log_manager.hpp"
#include "recovery/log_record.hpp"
#include "storage/heap_table.hpp"

using namespace cloudsql;
using namespace cloudsql::recovery;
using namespace cloudsql::common;
using namespace cloudsql::executor;
using namespace cloudsql::storage;

// Test framework
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) void test_##name()
#define RUN_TEST(name)                                        \
    do {                                                      \
        std::cout << "  " << #name << "... ";                 \
        try {                                                 \
            test_##name();                                    \
            std::cout << "PASSED" << std::endl;               \
            tests_passed++;                                   \
        } catch (const std::exception& e) {                   \
            std::cout << "FAILED: " << e.what() << std::endl; \
            tests_failed++;                                   \
        }                                                     \
    } while (0)

#define EXPECT_EQ(a, b)                                                                        \
    do {                                                                                       \
        if ((a) != (b)) {                                                                      \
            throw std::runtime_error("Expected " + std::to_string(static_cast<long long>(b)) + \
                                     " but got " + std::to_string(static_cast<long long>(a))); \
        }                                                                                      \
    } while (0)

#define EXPECT_TRUE(a)                                                     \
    do {                                                                   \
        if (!(a)) throw std::runtime_error("Expected true but got false"); \
    } while (0)

// Helper to clean up test files
void cleanup(const std::string& file) {
    std::remove(file.c_str());
}

TEST(LogRecordSerialization) {
    // 1. Create a dummy INSERT log record
    std::vector<Value> values;
    values.push_back(Value::make_int64(42));
    values.push_back(Value::make_text("test_string"));
    Tuple tuple(std::move(values));

    LogRecord original(100, 99, LogRecordType::INSERT, "test_table", HeapTable::TupleId(1, 2),
                       tuple);
    original.lsn_ = 101;
    original.size_ = original.get_size();

    // 2. Serialize
    std::vector<char> buffer(original.size_);
    original.serialize(buffer.data());

    // 3. Deserialize
    LogRecord deserialized = LogRecord::deserialize(buffer.data());

    // 4. Verify
    EXPECT_EQ(deserialized.lsn_, original.lsn_);
    EXPECT_EQ(deserialized.prev_lsn_, original.prev_lsn_);
    EXPECT_EQ(deserialized.txn_id_, original.txn_id_);
    EXPECT_TRUE(deserialized.type_ == original.type_);
    EXPECT_TRUE(deserialized.table_name_ == original.table_name_);
    EXPECT_TRUE(deserialized.rid_ == original.rid_);

    EXPECT_EQ(deserialized.tuple_.size(), original.tuple_.size());
    EXPECT_EQ(deserialized.tuple_.get(0).to_int64(), 42);
    EXPECT_TRUE(deserialized.tuple_.get(1).as_text() == "test_string");
}

TEST(LogRecordAllTypes) {
    std::vector<Value> values;
    values.push_back(Value::make_bool(true));
    values.push_back(Value(static_cast<int8_t>(10)));
    values.push_back(Value(static_cast<int16_t>(200)));
    values.push_back(Value(static_cast<int32_t>(3000)));
    values.push_back(Value(static_cast<float>(1.23f)));
    values.push_back(Value(static_cast<double>(4.56)));
    values.push_back(Value::make_null());

    Tuple tuple(std::move(values));
    LogRecord original(50, 49, LogRecordType::INSERT, "types_table", HeapTable::TupleId(1, 1),
                       tuple);
    original.size_ = original.get_size();

    std::vector<char> buffer(original.size_);
    original.serialize(buffer.data());

    LogRecord deserialized = LogRecord::deserialize(buffer.data());

    EXPECT_EQ(deserialized.tuple_.size(), 7);
    EXPECT_TRUE(deserialized.tuple_.get(0).as_bool());
    EXPECT_EQ(deserialized.tuple_.get(1).as_int8(), 10);
    EXPECT_EQ(deserialized.tuple_.get(2).as_int16(), 200);
    EXPECT_EQ(deserialized.tuple_.get(3).as_int32(), 3000);
    EXPECT_TRUE(deserialized.tuple_.get(4).as_float32() > 1.22f &&
                deserialized.tuple_.get(4).as_float32() < 1.24f);
    EXPECT_TRUE(deserialized.tuple_.get(5).as_float64() > 4.55 &&
                deserialized.tuple_.get(5).as_float64() < 4.57);
    EXPECT_TRUE(deserialized.tuple_.get(6).is_null());
}

TEST(LogManagerBasic) {
    std::string log_file = "test_log_basic.log";
    cleanup(log_file);

    {
        LogManager log_manager(log_file);
        log_manager.run_flush_thread();

        // Append a few logs
        LogRecord log1(1, -1, LogRecordType::BEGIN);
        lsn_t lsn1 = log_manager.append_log_record(log1);
        EXPECT_EQ(lsn1, 0);

        LogRecord log2(1, lsn1, LogRecordType::COMMIT);
        lsn_t lsn2 = log_manager.append_log_record(log2);
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

int main() {
    std::cout << "cloudSQL Recovery Test Suite" << std::endl;
    std::cout << "============================" << std::endl;

    RUN_TEST(LogRecordSerialization);
    RUN_TEST(LogRecordAllTypes);
    RUN_TEST(LogManagerBasic);

    std::cout << std::endl
              << "Results: " << tests_passed << " passed, " << tests_failed << " failed"
              << std::endl;
    return (tests_failed > 0);
}
