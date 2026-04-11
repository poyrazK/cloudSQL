/**
 * @file columnar_table_tests.cpp
 * @brief Unit tests for ColumnarTable - column-oriented storage
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <cstdio>
#include <memory>
#include <vector>

#include "common/value.hpp"
#include "executor/types.hpp"
#include "storage/columnar_table.hpp"
#include "storage/storage_manager.hpp"

using namespace cloudsql;
using namespace cloudsql::storage;
using namespace cloudsql::executor;

namespace {

static void cleanup_table(const std::string& name) {
    // clang-format off
  std::remove(("./test_data/" + name + ".meta.bin").c_str());
  std::remove(("./test_data/" + name + ".col0.nulls.bin").c_str());
  std::remove(("./test_data/" + name + ".col0.data.bin").c_str());
  std::remove(("./test_data/" + name + ".col1.nulls.bin").c_str());
  std::remove(("./test_data/" + name + ".col1.data.bin").c_str());
    // clang-format on
}

class ColumnarTableTests : public ::testing::Test {
   protected:
    void SetUp() override {
        sm_ = std::make_unique<StorageManager>("./test_data");
        sm_->create_dir_if_not_exists();
    }

    void TearDown() override { sm_.reset(); }

    std::unique_ptr<StorageManager> sm_;
};

TEST_F(ColumnarTableTests, BasicInt64Lifecycle) {
    const std::string name = "col_test_int";
    cleanup_table(name);

    Schema schema;
    schema.add_column("id", common::ValueType::TYPE_INT64);
    schema.add_column("val", common::ValueType::TYPE_INT64);

    ColumnarTable table(name, *sm_, schema);

    ASSERT_TRUE(table.create());
    ASSERT_TRUE(table.open());
    ASSERT_EQ(table.row_count(), 0U);

    // Build batch with 2 rows
    auto batch = VectorBatch::create(schema);
    batch->get_column(0).append(common::Value::make_int64(1));
    batch->get_column(0).append(common::Value::make_int64(2));
    batch->get_column(1).append(common::Value::make_int64(100));
    batch->get_column(1).append(common::Value::make_int64(200));
    batch->set_row_count(2);

    ASSERT_TRUE(table.append_batch(*batch));
    ASSERT_EQ(table.row_count(), 2U);

    // Reopen and verify
    ColumnarTable table2(name, *sm_, schema);
    ASSERT_TRUE(table2.open());
    ASSERT_EQ(table2.row_count(), 2U);

    auto read_batch = VectorBatch::create(schema);
    ASSERT_TRUE(table2.read_batch(0, 10, *read_batch));
    ASSERT_EQ(read_batch->row_count(), 2U);
}

TEST_F(ColumnarTableTests, BasicFloat64Lifecycle) {
    const std::string name = "col_test_float";
    cleanup_table(name);

    Schema schema;
    schema.add_column("x", common::ValueType::TYPE_FLOAT64);
    schema.add_column("y", common::ValueType::TYPE_FLOAT64);

    ColumnarTable table(name, *sm_, schema);
    ASSERT_TRUE(table.create());

    auto batch = VectorBatch::create(schema);
    batch->get_column(0).append(common::Value::make_float64(1.5));
    batch->get_column(1).append(common::Value::make_float64(2.7));
    batch->set_row_count(1);
    ASSERT_TRUE(table.append_batch(*batch));
    ASSERT_EQ(table.row_count(), 1U);

    auto out = VectorBatch::create(schema);
    ASSERT_TRUE(table.read_batch(0, 1, *out));
    ASSERT_EQ(out->row_count(), 1U);

    auto& col_x = dynamic_cast<NumericVector<double>&>(out->get_column(0));
    EXPECT_FLOAT_EQ(col_x.get(0).to_float64(), 1.5);
}

TEST_F(ColumnarTableTests, NullValueHandling) {
    const std::string name = "col_test_null";
    cleanup_table(name);

    Schema schema;
    schema.add_column("nullable_col", common::ValueType::TYPE_INT64);

    ColumnarTable table(name, *sm_, schema);
    ASSERT_TRUE(table.create());

    auto batch = VectorBatch::create(schema);
    batch->get_column(0).append(common::Value::make_null());
    batch->get_column(0).append(common::Value::make_int64(42));
    batch->set_row_count(2);
    ASSERT_TRUE(table.append_batch(*batch));

    auto out = VectorBatch::create(schema);
    ASSERT_TRUE(table.read_batch(0, 2, *out));
    ASSERT_EQ(out->row_count(), 2U);

    auto& col = dynamic_cast<NumericVector<int64_t>&>(out->get_column(0));
    EXPECT_TRUE(col.is_null(0));
    EXPECT_FALSE(col.is_null(1));
    EXPECT_EQ(col.get(1).to_int64(), 42);
}

TEST_F(ColumnarTableTests, MultiBatchAppendRead) {
    const std::string name = "col_test_multi";
    cleanup_table(name);

    Schema schema;
    schema.add_column("val", common::ValueType::TYPE_INT64);

    ColumnarTable table(name, *sm_, schema);
    ASSERT_TRUE(table.create());

    // Append 3 batches of 100 rows each
    for (int batch_num = 0; batch_num < 3; ++batch_num) {
        auto batch = VectorBatch::create(schema);
        for (int i = 0; i < 100; ++i) {
            batch->get_column(0).append(common::Value::make_int64(batch_num * 100 + i));
        }
        batch->set_row_count(100);
        ASSERT_TRUE(table.append_batch(*batch));
    }

    ASSERT_EQ(table.row_count(), 300U);

    // Read in pages
    auto out = VectorBatch::create(schema);
    ASSERT_TRUE(table.read_batch(0, 100, *out));
    ASSERT_EQ(out->row_count(), 100U);

    out = VectorBatch::create(schema);
    ASSERT_TRUE(table.read_batch(100, 100, *out));
    ASSERT_EQ(out->row_count(), 100U);

    out = VectorBatch::create(schema);
    ASSERT_TRUE(table.read_batch(200, 100, *out));
    ASSERT_EQ(out->row_count(), 100U);
}

TEST_F(ColumnarTableTests, ReadBatchBeyondEnd) {
    const std::string name = "col_test_beyond";
    cleanup_table(name);

    Schema schema;
    schema.add_column("id", common::ValueType::TYPE_INT64);

    ColumnarTable table(name, *sm_, schema);
    ASSERT_TRUE(table.create());

    auto batch = VectorBatch::create(schema);
    batch->get_column(0).append(common::Value::make_int64(1));
    batch->set_row_count(1);
    ASSERT_TRUE(table.append_batch(*batch));

    auto out = VectorBatch::create(schema);
    EXPECT_FALSE(table.read_batch(5, 10, *out));
    EXPECT_FALSE(table.read_batch(1, 10, *out));
}

TEST_F(ColumnarTableTests, ReadBatchPartial) {
    const std::string name = "col_test_partial";
    cleanup_table(name);

    Schema schema;
    schema.add_column("id", common::ValueType::TYPE_INT64);

    ColumnarTable table(name, *sm_, schema);
    ASSERT_TRUE(table.create());

    auto batch = VectorBatch::create(schema);
    for (int i = 0; i < 10; ++i) {
        batch->get_column(0).append(common::Value::make_int64(i));
    }
    batch->set_row_count(10);
    ASSERT_TRUE(table.append_batch(*batch));

    // Read starting mid-table with batch_size larger than remaining
    auto out = VectorBatch::create(schema);
    ASSERT_TRUE(table.read_batch(8, 100, *out));
    ASSERT_EQ(out->row_count(), 2U);
}

TEST_F(ColumnarTableTests, UnsupportedTypeThrows) {
    const std::string name = "col_test_unsupported";
    cleanup_table(name);

    Schema schema;
    schema.add_column("text_col", common::ValueType::TYPE_TEXT);

    // VectorBatch::create() throws when it sees TYPE_TEXT (unsupported)
    EXPECT_THROW([[maybe_unused]] auto batch = VectorBatch::create(schema), std::runtime_error);
}

TEST_F(ColumnarTableTests, CreateTwice) {
    const std::string name = "col_test_twice";
    cleanup_table(name);

    Schema schema;
    schema.add_column("id", common::ValueType::TYPE_INT64);

    ColumnarTable table(name, *sm_, schema);
    ASSERT_TRUE(table.create());

    // Second create() on existing files - behavior depends on ofstream flags
    // This is a basic sanity check
    ASSERT_TRUE(table.create());
}

TEST_F(ColumnarTableTests, OpenWithoutCreate) {
    const std::string name = "col_test_missing";

    Schema schema;
    schema.add_column("id", common::ValueType::TYPE_INT64);

    ColumnarTable table(name, *sm_, schema);
    ASSERT_FALSE(table.open());
}

TEST_F(ColumnarTableTests, EmptyBatch) {
    const std::string name = "col_test_empty";
    cleanup_table(name);

    Schema schema;
    schema.add_column("id", common::ValueType::TYPE_INT64);

    ColumnarTable table(name, *sm_, schema);
    ASSERT_TRUE(table.create());

    auto batch = VectorBatch::create(schema);
    batch->set_row_count(0);
    ASSERT_TRUE(table.append_batch(*batch));
    ASSERT_EQ(table.row_count(), 0U);

    auto out = VectorBatch::create(schema);
    ASSERT_FALSE(table.read_batch(0, 10, *out));
}

TEST_F(ColumnarTableTests, SchemaAccessor) {
    const std::string name = "col_test_schema";
    cleanup_table(name);

    Schema schema;
    schema.add_column("col1", common::ValueType::TYPE_INT64);
    schema.add_column("col2", common::ValueType::TYPE_FLOAT64);

    ColumnarTable table(name, *sm_, schema);
    ASSERT_TRUE(table.create());

    const auto& retrieved_schema = table.schema();
    ASSERT_EQ(retrieved_schema.column_count(), 2U);
    ASSERT_EQ(retrieved_schema.get_column(0).name(), "col1");
    ASSERT_EQ(retrieved_schema.get_column(1).type(), common::ValueType::TYPE_FLOAT64);
}

}  // namespace
