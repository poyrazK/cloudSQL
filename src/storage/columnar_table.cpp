/**
 * @file columnar_table.cpp
 * @brief Implementation of column-oriented persistent storage.
 *
 * This implementation provides high-performance access to columnar data by
 * storing each column in a separate binary file. It integrates with the
 * StorageManager to ensure all files are correctly rooted in the database
 * data directory.
 */

#include "storage/columnar_table.hpp"

#include <algorithm>
#include <cstring>
#include <fstream>
#include <stdexcept>

namespace cloudsql::storage {

bool ColumnarTable::create() {
    const std::string meta_path = storage_manager_.get_full_path(name_ + ".meta.bin");
    std::ofstream out(meta_path, std::ios::binary);
    if (!out.is_open()) return false;

    uint64_t initial_rows = 0;
    out.write(reinterpret_cast<const char*>(&initial_rows), 8);
    out.close();

    for (size_t i = 0; i < schema_.column_count(); ++i) {
        const std::string base = name_ + ".col" + std::to_string(i);
        std::ofstream n_out(storage_manager_.get_full_path(base + ".nulls.bin"), std::ios::binary);
        std::ofstream d_out(storage_manager_.get_full_path(base + ".data.bin"), std::ios::binary);
        if (!n_out.is_open() || !d_out.is_open()) return false;
    }
    return true;
}

bool ColumnarTable::open() {
    const std::string meta_path = storage_manager_.get_full_path(name_ + ".meta.bin");
    std::ifstream in(meta_path, std::ios::binary);
    if (!in.is_open()) return false;

    in.read(reinterpret_cast<char*>(&row_count_), 8);
    in.close();
    return true;
}

bool ColumnarTable::append_batch(const executor::VectorBatch& batch) {
    for (size_t i = 0; i < schema_.column_count(); ++i) {
        const std::string base = name_ + ".col" + std::to_string(i);
        std::ofstream n_out(storage_manager_.get_full_path(base + ".nulls.bin"),
                            std::ios::binary | std::ios::app);
        std::ofstream d_out(storage_manager_.get_full_path(base + ".data.bin"),
                            std::ios::binary | std::ios::app);
        if (!n_out.is_open() || !d_out.is_open()) return false;

        auto& col_vec = const_cast<executor::VectorBatch&>(batch).get_column(i);

        // Persist nullability information (1 byte per row for simplicity in this POC)
        for (size_t r = 0; r < batch.row_count(); ++r) {
            uint8_t is_null = col_vec.is_null(r) ? 1 : 0;
            n_out.write(reinterpret_cast<const char*>(&is_null), 1);
        }

        // Persist raw binary data
        const auto type = schema_.get_column(i).type();
        if (type == common::ValueType::TYPE_INT64) {
            auto& num_vec = dynamic_cast<executor::NumericVector<int64_t>&>(col_vec);
            d_out.write(reinterpret_cast<const char*>(num_vec.raw_data()), batch.row_count() * 8);
        } else if (type == common::ValueType::TYPE_INT32 || type == common::ValueType::TYPE_INT16 ||
                   type == common::ValueType::TYPE_INT8) {
            // Promote smaller int types to int64_t for storage
            auto& num_vec = dynamic_cast<executor::NumericVector<int64_t>&>(col_vec);
            std::vector<int64_t> promoted(batch.row_count());
            for (size_t r = 0; r < batch.row_count(); ++r) {
                promoted[r] = num_vec.get(r).is_null() ? 0 : num_vec.get(r).to_int64();
            }
            d_out.write(reinterpret_cast<const char*>(promoted.data()), batch.row_count() * 8);
        } else if (type == common::ValueType::TYPE_FLOAT64) {
            auto& num_vec = dynamic_cast<executor::NumericVector<double>&>(col_vec);
            d_out.write(reinterpret_cast<const char*>(num_vec.raw_data()), batch.row_count() * 8);
        } else if (type == common::ValueType::TYPE_FLOAT32 ||
                   type == common::ValueType::TYPE_DECIMAL) {
            // Promote float32/decimal to float64 for storage
            auto& num_vec = dynamic_cast<executor::NumericVector<double>&>(col_vec);
            std::vector<double> promoted(batch.row_count());
            for (size_t r = 0; r < batch.row_count(); ++r) {
                promoted[r] = num_vec.get(r).is_null() ? 0.0 : num_vec.get(r).to_float64();
            }
            d_out.write(reinterpret_cast<const char*>(promoted.data()), batch.row_count() * 8);
        } else if (type == common::ValueType::TYPE_BOOL) {
            auto& num_vec = dynamic_cast<executor::NumericVector<bool>&>(col_vec);
            std::vector<uint8_t> data(batch.row_count());
            for (size_t r = 0; r < batch.row_count(); ++r) {
                data[r] = num_vec.get(r).is_null() ? 0 : (num_vec.get(r).as_bool() ? 1 : 0);
            }
            d_out.write(reinterpret_cast<const char*>(data.data()), batch.row_count());
        } else if (type == common::ValueType::TYPE_TEXT ||
                   type == common::ValueType::TYPE_VARCHAR ||
                   type == common::ValueType::TYPE_CHAR) {
            auto& str_vec = dynamic_cast<executor::StringVector&>(col_vec);
            const auto& data = str_vec.raw_data();
            for (size_t r = 0; r < batch.row_count(); ++r) {
                uint32_t len = static_cast<uint32_t>(data[r].size());
                d_out.write(reinterpret_cast<const char*>(&len), 4);
                d_out.write(data[r].data(), len);
            }
        } else {
            throw std::runtime_error("ColumnarTable::append_batch: Unsupported persistence type " +
                                     std::to_string(static_cast<int>(type)));
        }
    }

    row_count_ += batch.row_count();

    const std::string meta_path = storage_manager_.get_full_path(name_ + ".meta.bin");
    std::ofstream out(meta_path, std::ios::binary | std::ios::in | std::ios::out);
    out.write(reinterpret_cast<const char*>(&row_count_), 8);
    return true;
}

bool ColumnarTable::read_batch(uint64_t start_row, uint32_t batch_size,
                               executor::VectorBatch& out_batch) {
    if (start_row >= row_count_) return false;

    uint32_t actual_rows =
        static_cast<uint32_t>(std::min(static_cast<uint64_t>(batch_size), row_count_ - start_row));

    // Ensure the output batch is correctly structured for the current schema
    out_batch.init_from_schema(schema_);

    for (size_t i = 0; i < schema_.column_count(); ++i) {
        const std::string base = name_ + ".col" + std::to_string(i);
        std::ifstream n_in(storage_manager_.get_full_path(base + ".nulls.bin"), std::ios::binary);
        std::ifstream d_in(storage_manager_.get_full_path(base + ".data.bin"), std::ios::binary);
        if (!n_in.is_open() || !d_in.is_open()) return false;

        auto& target_col = out_batch.get_column(i);
        const auto type = schema_.get_column(i).type();

        if (type == common::ValueType::TYPE_INT64) {
            auto& num_vec = dynamic_cast<executor::NumericVector<int64_t>&>(target_col);

            n_in.seekg(static_cast<std::streamoff>(start_row), std::ios::beg);
            std::vector<uint8_t> nulls(actual_rows);
            n_in.read(reinterpret_cast<char*>(nulls.data()), actual_rows);

            d_in.seekg(static_cast<std::streamoff>(start_row * 8), std::ios::beg);
            std::vector<int64_t> data(actual_rows);
            d_in.read(reinterpret_cast<char*>(data.data()), actual_rows * 8);

            for (uint32_t r = 0; r < actual_rows; ++r) {
                if (nulls[r] != 0U) {
                    num_vec.append(common::Value::make_null());
                } else {
                    num_vec.append(common::Value::make_int64(data[r]));
                }
            }
        } else if (type == common::ValueType::TYPE_INT32 || type == common::ValueType::TYPE_INT16 ||
                   type == common::ValueType::TYPE_INT8) {
            // Read as int64_t and demote to appropriate type
            auto& num_vec = dynamic_cast<executor::NumericVector<int64_t>&>(target_col);

            n_in.seekg(static_cast<std::streamoff>(start_row), std::ios::beg);
            std::vector<uint8_t> nulls(actual_rows);
            n_in.read(reinterpret_cast<char*>(nulls.data()), actual_rows);

            d_in.seekg(static_cast<std::streamoff>(start_row * 8), std::ios::beg);
            std::vector<int64_t> data(actual_rows);
            d_in.read(reinterpret_cast<char*>(data.data()), actual_rows * 8);

            for (uint32_t r = 0; r < actual_rows; ++r) {
                if (nulls[r] != 0U) {
                    num_vec.append(common::Value::make_null());
                } else if (type == common::ValueType::TYPE_INT32) {
                    num_vec.append(common::Value(static_cast<int32_t>(data[r])));
                } else if (type == common::ValueType::TYPE_INT16) {
                    num_vec.append(common::Value(static_cast<int16_t>(data[r])));
                } else {
                    num_vec.append(common::Value(static_cast<int8_t>(data[r])));
                }
            }
        } else if (type == common::ValueType::TYPE_FLOAT64) {
            auto& num_vec = dynamic_cast<executor::NumericVector<double>&>(target_col);

            n_in.seekg(static_cast<std::streamoff>(start_row), std::ios::beg);
            std::vector<uint8_t> nulls(actual_rows);
            n_in.read(reinterpret_cast<char*>(nulls.data()), actual_rows);

            d_in.seekg(static_cast<std::streamoff>(start_row * 8), std::ios::beg);
            std::vector<double> data(actual_rows);
            d_in.read(reinterpret_cast<char*>(data.data()), actual_rows * 8);

            for (uint32_t r = 0; r < actual_rows; ++r) {
                if (nulls[r] != 0U) {
                    num_vec.append(common::Value::make_null());
                } else {
                    num_vec.append(common::Value::make_float64(data[r]));
                }
            }
        } else if (type == common::ValueType::TYPE_TEXT ||
                   type == common::ValueType::TYPE_VARCHAR ||
                   type == common::ValueType::TYPE_CHAR) {
            auto& str_vec = dynamic_cast<executor::StringVector&>(target_col);

            n_in.seekg(static_cast<std::streamoff>(start_row), std::ios::beg);
            std::vector<uint8_t> nulls(actual_rows);
            n_in.read(reinterpret_cast<char*>(nulls.data()), actual_rows);

            // For variable-length strings, skip start_row records first
            // by reading and discarding their length-prefixed data
            if (start_row > 0) {
                for (uint32_t r = 0; r < start_row; ++r) {
                    uint32_t len = 0;
                    if (!d_in.read(reinterpret_cast<char*>(&len), 4)) break;
                    if (len > 0) {
                        d_in.seekg(static_cast<std::streamoff>(len), std::ios::cur);
                    }
                }
            }

            // Now read the actual_rows we want
            for (uint32_t r = 0; r < actual_rows; ++r) {
                uint32_t len = 0;
                d_in.read(reinterpret_cast<char*>(&len), 4);
                std::string s(len, '\0');
                d_in.read(s.data(), len);
                if (nulls[r] != 0U) {
                    str_vec.append(common::Value::make_null());
                } else {
                    str_vec.append(common::Value::make_text(s));
                }
            }
        } else {
            throw std::runtime_error(
                "ColumnarTable::read_batch: Symmetric serialization failure for type " +
                std::to_string(static_cast<int>(type)));
        }
    }
    out_batch.set_row_count(actual_rows);
    return true;
}

}  // namespace cloudsql::storage
