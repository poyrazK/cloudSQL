/**
 * @file columnar_table.hpp
 * @brief Column-oriented storage for analytical workloads
 */

#ifndef CLOUDSQL_STORAGE_COLUMNAR_TABLE_HPP
#define CLOUDSQL_STORAGE_COLUMNAR_TABLE_HPP

#include <memory>
#include <string>
#include <vector>

#include "executor/types.hpp"
#include "storage/storage_manager.hpp"

namespace cloudsql::storage {

/**
 * @brief A table implementation that stores data by column
 */
class ColumnarTable {
   private:
    std::string name_;
    StorageManager& storage_manager_;
    executor::Schema schema_;
    uint64_t row_count_ = 0;

   public:
    ColumnarTable(std::string name, StorageManager& storage, executor::Schema schema)
        : name_(std::move(name)), storage_manager_(storage), schema_(std::move(schema)) {}

    bool create();
    bool open();

    /**
     * @brief Load a batch of data from the table
     */
    bool read_batch(uint64_t start_row, uint32_t batch_size, executor::VectorBatch& out_batch);

    /**
     * @brief Append a batch of data to the table
     */
    bool append_batch(const executor::VectorBatch& batch);

    [[nodiscard]] uint64_t row_count() const { return row_count_; }
    [[nodiscard]] const executor::Schema& schema() const { return schema_; }
};

}  // namespace cloudsql::storage

#endif  // CLOUDSQL_STORAGE_COLUMNAR_TABLE_HPP
