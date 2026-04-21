/**
 * @file vectorized_operator.hpp
 * @brief Base class for vectorized query operators
 */

#ifndef CLOUDSQL_EXECUTOR_VECTORIZED_OPERATOR_HPP
#define CLOUDSQL_EXECUTOR_VECTORIZED_OPERATOR_HPP

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "executor/types.hpp"
#include "parser/expression.hpp"
#include "storage/columnar_table.hpp"

namespace cloudsql::executor {

/**
 * @brief Base class for vectorized operators (Batch-at-a-time)
 */
class VectorizedOperator {
   protected:
    ExecState state_ = ExecState::Init;
    std::string error_message_;
    Schema output_schema_;

   public:
    explicit VectorizedOperator(Schema schema) : output_schema_(std::move(schema)) {}
    virtual ~VectorizedOperator() = default;

    virtual bool init() { return true; }
    virtual bool open() { return true; }

    /**
     * @brief Produce the next batch of results
     * @return true if a batch was produced, false if EOF or error
     */
    virtual bool next_batch(VectorBatch& out_batch) = 0;

    virtual void close() {}

    [[nodiscard]] Schema& output_schema() { return output_schema_; }
    [[nodiscard]] ExecState state() const { return state_; }
    [[nodiscard]] const std::string& error() const { return error_message_; }

   protected:
    void set_error(std::string msg) {
        error_message_ = std::move(msg);
        state_ = ExecState::Error;
    }
};

/**
 * @brief Vectorized sequential scan operator for ColumnarTable
 */
class VectorizedSeqScanOperator : public VectorizedOperator {
   private:
    std::string table_name_;
    std::shared_ptr<storage::ColumnarTable> table_;
    uint64_t current_row_ = 0;
    uint32_t batch_size_ = 1024;

   public:
    VectorizedSeqScanOperator(std::string table_name, std::shared_ptr<storage::ColumnarTable> table)
        : VectorizedOperator(table->schema()),
          table_name_(std::move(table_name)),
          table_(std::move(table)) {}

    bool next_batch(VectorBatch& out_batch) override {
        if (current_row_ >= table_->row_count()) {
            return false;
        }

        if (table_->read_batch(current_row_, batch_size_, out_batch)) {
            current_row_ += out_batch.row_count();
            return true;
        }
        return false;
    }
};

/**
 * @brief Vectorized filter operator
 */
class VectorizedFilterOperator : public VectorizedOperator {
   private:
    std::unique_ptr<VectorizedOperator> child_;
    std::unique_ptr<parser::Expression> condition_;
    std::unique_ptr<VectorBatch> input_batch_;
    std::unique_ptr<ColumnVector> selection_mask_;

   public:
    VectorizedFilterOperator(std::unique_ptr<VectorizedOperator> child,
                             std::unique_ptr<parser::Expression> condition)
        : VectorizedOperator(child->output_schema()),
          child_(std::move(child)),
          condition_(std::move(condition)) {
        input_batch_ = VectorBatch::create(child_->output_schema());
        selection_mask_ = std::make_unique<NumericVector<bool>>(common::ValueType::TYPE_BOOL);
    }

    bool next_batch(VectorBatch& out_batch) override {
        out_batch.clear();

        // Ensure output batch is structured for current schema
        if (out_batch.column_count() == 0) {
            out_batch.init_from_schema(output_schema_);
        }

        // Process child batches until we find matches or exhaust input
        while (child_->next_batch(*input_batch_)) {
            selection_mask_->clear();
            condition_->evaluate_vectorized(*input_batch_, child_->output_schema(),
                                            *selection_mask_);

            std::vector<size_t> selection;
            for (size_t r = 0; r < input_batch_->row_count(); ++r) {
                common::Value val = selection_mask_->get(r);
                if (!val.is_null() && val.as_bool()) {
                    selection.push_back(r);
                }
            }

            if (!selection.empty()) {
                // Batch-level append optimization: iterate columns once
                for (size_t c = 0; c < input_batch_->column_count(); ++c) {
                    auto& src_col = input_batch_->get_column(c);
                    auto& dest_col = out_batch.get_column(c);
                    for (size_t r : selection) {
                        dest_col.append(src_col.get(r));
                    }
                }
                out_batch.set_row_count(selection.size());
                input_batch_->clear();
                return true;  // Return with matches
            }
            input_batch_->clear();
        }

        return false;  // Exhausted without finding matches
    }
};

/**
 * @brief Vectorized project operator
 */
class VectorizedProjectOperator : public VectorizedOperator {
   private:
    std::unique_ptr<VectorizedOperator> child_;
    std::vector<std::unique_ptr<parser::Expression>> expressions_;
    std::unique_ptr<VectorBatch> input_batch_;

   public:
    VectorizedProjectOperator(std::unique_ptr<VectorizedOperator> child, Schema out_schema,
                              std::vector<std::unique_ptr<parser::Expression>> exprs)
        : VectorizedOperator(std::move(out_schema)),
          child_(std::move(child)),
          expressions_(std::move(exprs)) {
        input_batch_ = VectorBatch::create(child_->output_schema());
    }

    bool next_batch(VectorBatch& out_batch) override {
        out_batch.clear();
        if (child_->next_batch(*input_batch_)) {
            // Pre-allocate result columns if out_batch is empty
            if (out_batch.column_count() == 0) {
                out_batch.init_from_schema(output_schema_);
            }

            for (size_t i = 0; i < expressions_.size(); ++i) {
                expressions_[i]->evaluate_vectorized(*input_batch_, child_->output_schema(),
                                                     out_batch.get_column(i));
            }
            out_batch.set_row_count(input_batch_->row_count());
            input_batch_->clear();
            return true;
        }
        return false;
    }
};

/**
 * @brief Aggregate specification for vectorized operator
 */
struct VectorizedAggregateInfo {
    AggregateType type;
    int32_t input_col_idx;  // -1 for COUNT(*)
};

/**
 * @brief Vectorized global aggregate operator (no GROUP BY)
 */
class VectorizedAggregateOperator : public VectorizedOperator {
   private:
    std::unique_ptr<VectorizedOperator> child_;
    std::vector<VectorizedAggregateInfo> aggregates_;
    std::vector<int64_t> results_int_;
    std::vector<double> results_double_;
    std::vector<bool> has_value_;
    std::unique_ptr<VectorBatch> input_batch_;
    bool done_ = false;

   public:
    VectorizedAggregateOperator(std::unique_ptr<VectorizedOperator> child, Schema out_schema,
                                std::vector<VectorizedAggregateInfo> aggregates)
        : VectorizedOperator(std::move(out_schema)),
          child_(std::move(child)),
          aggregates_(std::move(aggregates)) {
        results_int_.assign(aggregates_.size(), 0);
        results_double_.assign(aggregates_.size(), 0.0);
        has_value_.assign(aggregates_.size(), false);
        // COUNT aggregates always have a value (0 for empty input) per SQL spec
        for (size_t i = 0; i < aggregates_.size(); ++i) {
            if (aggregates_[i].type == AggregateType::Count) {
                has_value_[i] = true;
            }
        }
        input_batch_ = VectorBatch::create(child_->output_schema());
    }

    bool next_batch(VectorBatch& out_batch) override {
        if (done_) return false;

        // Process all input batches
        while (child_->next_batch(*input_batch_)) {
            for (size_t i = 0; i < aggregates_.size(); ++i) {
                const auto& agg = aggregates_[i];
                if (agg.type == AggregateType::Count) {
                    results_int_[i] += input_batch_->row_count();
                    has_value_[i] = true;
                } else if (agg.type == AggregateType::Sum && agg.input_col_idx >= 0) {
                    auto& col = input_batch_->get_column(agg.input_col_idx);
                    if (col.type() == common::ValueType::TYPE_INT64) {
                        auto& num_col = dynamic_cast<NumericVector<int64_t>&>(col);
                        const int64_t* raw = num_col.raw_data();
                        for (size_t r = 0; r < input_batch_->row_count(); ++r) {
                            if (!num_col.is_null(r)) {
                                results_int_[i] += raw[r];
                                has_value_[i] = true;
                            }
                        }
                    } else if (col.type() == common::ValueType::TYPE_FLOAT64) {
                        auto& num_col = dynamic_cast<NumericVector<double>&>(col);
                        const double* raw = num_col.raw_data();
                        for (size_t r = 0; r < input_batch_->row_count(); ++r) {
                            if (!num_col.is_null(r)) {
                                results_double_[i] += raw[r];
                                has_value_[i] = true;
                            }
                        }
                    } else {
                        set_error("SUM: Unsupported column type " +
                                  std::to_string(static_cast<int>(col.type())));
                        return false;
                    }
                } else {
                    set_error("Aggregate: Unsupported aggregate type or missing handler");
                    return false;
                }
            }
            input_batch_->clear();
        }

        // Produce final result batch
        out_batch.clear();
        if (out_batch.column_count() == 0) {
            out_batch.init_from_schema(output_schema_);
        }

        for (size_t i = 0; i < aggregates_.size(); ++i) {
            if (!has_value_[i]) {
                out_batch.get_column(i).append(common::Value::make_null());
                continue;
            }

            if (output_schema_.get_column(i).type() == common::ValueType::TYPE_INT64) {
                out_batch.get_column(i).append(common::Value::make_int64(results_int_[i]));
            } else if (output_schema_.get_column(i).type() == common::ValueType::TYPE_FLOAT64) {
                out_batch.get_column(i).append(common::Value::make_float64(results_double_[i]));
            }
        }
        out_batch.set_row_count(1);
        done_ = true;
        return true;
    }
};

/**
 * @brief Group state for vectorized GROUP BY - accumulator data per group
 */
struct VectorizedGroupState {
    std::vector<int64_t> counts;
    std::vector<double> sums;
    std::vector<common::Value> mins;
    std::vector<common::Value> maxes;

    VectorizedGroupState() = default;
    explicit VectorizedGroupState(size_t agg_count) {
        counts.assign(agg_count, 0);
        sums.assign(agg_count, 0.0);
        mins.assign(agg_count, common::Value::make_null());
        maxes.assign(agg_count, common::Value::make_null());
    }
};

/**
 * @brief Vectorized GROUP BY operator with hash-based aggregation
 */
class VectorizedGroupByOperator : public VectorizedOperator {
   private:
    std::unique_ptr<VectorizedOperator> child_;
    std::vector<std::unique_ptr<parser::Expression>> group_by_;
    std::vector<VectorizedAggregateInfo> aggregates_;

    // Hash table: group key string -> group state
    std::unordered_map<std::string, VectorizedGroupState> groups_;
    // Ordered group keys for output iteration
    std::vector<std::string> group_keys_;
    // Group key values for each group
    std::vector<std::vector<common::Value>> group_values_;

    // Current output position
    size_t current_group_idx_ = 0;

    // Processing state
    enum class ProcessState { Input, Output };
    ProcessState state_ = ProcessState::Input;

    // Reusable batch objects
    std::unique_ptr<VectorBatch> input_batch_;
    std::unique_ptr<VectorBatch> group_key_batch_;

   public:
    VectorizedGroupByOperator(std::unique_ptr<VectorizedOperator> child,
                              std::vector<std::unique_ptr<parser::Expression>> group_by,
                              std::vector<VectorizedAggregateInfo> aggregates,
                              Schema output_schema)
        : VectorizedOperator(std::move(output_schema)),
          child_(std::move(child)),
          group_by_(std::move(group_by)),
          aggregates_(std::move(aggregates)) {
        input_batch_ = VectorBatch::create(child_->output_schema());
        // Create schema for group key evaluation
        Schema key_schema;
        for (size_t i = 0; i < group_by_.size(); ++i) {
            key_schema.add_column("gb_key_" + std::to_string(i), common::ValueType::TYPE_TEXT);
        }
        group_key_batch_ = VectorBatch::create(key_schema);
    }

    bool next_batch(VectorBatch& out_batch) override {
        if (state_ == ProcessState::Input) {
            // Phase 1: Consume all input batches and populate hash table
            while (child_->next_batch(*input_batch_)) {
                process_input_batch(*input_batch_);
            }
            state_ = ProcessState::Output;
        }

        // Phase 2: Produce grouped output batches
        return produce_output_batch(out_batch);
    }

   private:
    void process_input_batch(VectorBatch& batch) {
        // For each row, compute hash key directly from source columns
        for (size_t r = 0; r < batch.row_count(); ++r) {
            // Build key from group-by values
            std::string key;
            for (size_t i = 0; i < group_by_.size(); ++i) {
                // Find column index for group-by expression
                const auto& schema = child_->output_schema();
                size_t col_idx = schema.find_column(group_by_[i]->to_string());
                if (col_idx == static_cast<size_t>(-1)) {
                    key += "NULL|";
                } else {
                    key += batch.get_column(col_idx).get(r).to_string();
                    key += "|";
                }
            }

            // Get or create group state
            auto it = groups_.find(key);
            if (it == groups_.end()) {
                // Store group key values for output
                std::vector<common::Value> key_vals;
                for (size_t i = 0; i < group_by_.size(); ++i) {
                    size_t col_idx = child_->output_schema().find_column(group_by_[i]->to_string());
                    if (col_idx == static_cast<size_t>(-1)) {
                        key_vals.push_back(common::Value::make_null());
                    } else {
                        key_vals.push_back(batch.get_column(col_idx).get(r));
                    }
                }
                auto result = groups_.emplace(key, VectorizedGroupState(aggregates_.size()));
                it = result.first;
                group_keys_.push_back(key);
                group_values_.push_back(std::move(key_vals));
            }

            // Update accumulators for this row
            update_accumulators(it->second, batch, r);
        }
        input_batch_->clear();
    }

    void update_accumulators(VectorizedGroupState& state, VectorBatch& batch, size_t row_idx) {
        for (size_t i = 0; i < aggregates_.size(); ++i) {
            const auto& agg = aggregates_[i];

            if (agg.type == AggregateType::Count && agg.input_col_idx < 0) {
                // COUNT(*) - always increment
                state.counts[i]++;
            } else if (agg.type == AggregateType::Sum && agg.input_col_idx >= 0) {
                auto& col = batch.get_column(agg.input_col_idx);
                if (!col.is_null(row_idx)) {
                    if (col.type() == common::ValueType::TYPE_INT64) {
                        auto& num_col = dynamic_cast<NumericVector<int64_t>&>(col);
                        state.sums[i] += num_col.raw_data()[row_idx];
                    } else if (col.type() == common::ValueType::TYPE_FLOAT64) {
                        auto& num_col = dynamic_cast<NumericVector<double>&>(col);
                        state.sums[i] += num_col.raw_data()[row_idx];
                    }
                }
            }
        }
    }

    bool produce_output_batch(VectorBatch& out_batch) {
        if (current_group_idx_ >= group_keys_.size()) {
            return false;  // EOF
        }

        out_batch.clear();
        if (out_batch.column_count() == 0) {
            out_batch.init_from_schema(output_schema_);
        }

        constexpr size_t BATCH_SIZE = 1024;
        size_t output_count = 0;

        while (current_group_idx_ < group_keys_.size() && output_count < BATCH_SIZE) {
            // Append group key columns
            const auto& key_vals = group_values_[current_group_idx_];
            for (size_t i = 0; i < key_vals.size(); ++i) {
                out_batch.get_column(i).append(key_vals[i]);
            }

            // Append aggregate result columns
            const auto& state = groups_.at(group_keys_[current_group_idx_]);
            for (size_t i = 0; i < aggregates_.size(); ++i) {
                size_t col_idx = group_by_.size() + i;
                switch (aggregates_[i].type) {
                    case AggregateType::Count:
                        out_batch.get_column(col_idx).append(
                            common::Value::make_int64(state.counts[i]));
                        break;
                    case AggregateType::Sum:
                        out_batch.get_column(col_idx).append(
                            common::Value::make_float64(state.sums[i]));
                        break;
                    default:
                        out_batch.get_column(col_idx).append(common::Value::make_null());
                        break;
                }
            }
            output_count++;
            current_group_idx_++;
        }

        out_batch.set_row_count(output_count);
        return true;
    }
};

}  // namespace cloudsql::executor

#endif  // CLOUDSQL_EXECUTOR_VECTORIZED_OPERATOR_HPP
