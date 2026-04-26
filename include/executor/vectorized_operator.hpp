/**
 * @file vectorized_operator.hpp
 * @brief Base class for vectorized query operators
 */

#ifndef CLOUDSQL_EXECUTOR_VECTORIZED_OPERATOR_HPP
#define CLOUDSQL_EXECUTOR_VECTORIZED_OPERATOR_HPP

#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
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
    std::vector<int64_t> sums_int64;  // Separate accumulators to avoid precision loss
    std::vector<double> sums_float64;
    std::vector<bool> has_float_value_;  // Tracks whether any float64 values were accumulated
    std::vector<common::Value> mins;
    std::vector<common::Value> maxes;

    VectorizedGroupState() = default;
    explicit VectorizedGroupState(size_t agg_count) {
        counts.assign(agg_count, 0);
        sums_int64.assign(agg_count, 0);
        sums_float64.assign(agg_count, 0.0);
        has_float_value_.assign(agg_count, false);
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

    // Pre-resolved column indices for group-by expressions (computed once in ctor)
    std::vector<size_t> group_by_col_indices_;

    // Hash table: group key string -> group state
    std::unordered_map<std::string, VectorizedGroupState> groups_;
    // Ordered group keys for output iteration
    std::vector<std::string> group_keys_;
    // Group key values for each group
    std::vector<std::vector<common::Value>> group_values_;

    // Current output position
    size_t current_group_idx_ = 0;

    // Processing state
    enum class ProcessPhase { Input, Output };
    ProcessPhase process_phase_ = ProcessPhase::Input;

    // Reusable batch objects
    std::unique_ptr<VectorBatch> input_batch_;
    std::unique_ptr<VectorBatch> group_key_batch_;

   public:
    VectorizedGroupByOperator(std::unique_ptr<VectorizedOperator> child,
                              std::vector<std::unique_ptr<parser::Expression>> group_by,
                              std::vector<VectorizedAggregateInfo> aggregates, Schema output_schema)
        : VectorizedOperator(std::move(output_schema)),
          child_(std::move(child)),
          group_by_(std::move(group_by)),
          aggregates_(std::move(aggregates)) {
        input_batch_ = VectorBatch::create(child_->output_schema());

        // Pre-resolve column indices once in constructor
        const auto& schema = child_->output_schema();
        for (size_t i = 0; i < group_by_.size(); ++i) {
            size_t col_idx = schema.find_column(group_by_[i]->to_string());
            group_by_col_indices_.push_back(col_idx);
        }

        // Create schema for group key evaluation
        Schema key_schema;
        for (size_t i = 0; i < group_by_.size(); ++i) {
            key_schema.add_column("gb_key_" + std::to_string(i), common::ValueType::TYPE_TEXT);
        }
        group_key_batch_ = VectorBatch::create(key_schema);
    }

    bool next_batch(VectorBatch& out_batch) override {
        if (process_phase_ == ProcessPhase::Input) {
            // Phase 1: Consume all input batches and populate hash table
            while (child_->next_batch(*input_batch_)) {
                process_input_batch(*input_batch_);
            }
            process_phase_ = ProcessPhase::Output;
        }

        // Phase 2: Produce grouped output batches
        return produce_output_batch(out_batch);
    }

   private:
    void process_input_batch(VectorBatch& batch) {
        // For each row, compute hash key using collision-safe encoding
        for (size_t r = 0; r < batch.row_count(); ++r) {
            // Build key using length-prefixed, type-tagged encoding
            std::string key;
            for (size_t i = 0; i < group_by_col_indices_.size(); ++i) {
                size_t col_idx = group_by_col_indices_[i];
                if (col_idx == static_cast<size_t>(-1)) {
                    // Column not found in schema - fail fast
                    set_error("GROUP BY: column not found in input schema: " +
                              group_by_[i]->to_string());
                    return;
                }

                const auto& val = batch.get_column(col_idx).get(r);
                if (val.is_null()) {
                    // Use a dedicated NULL marker for null values
                    key.append("\1NULL\0", 6);
                } else {
                    // Length-prefixed value: marker + length (4 bytes) + data
                    std::string val_str = val.to_string();
                    key.push_back('\0');  // non-NULL marker
                    uint32_t len = static_cast<uint32_t>(val_str.size());
                    key.append(reinterpret_cast<const char*>(&len), 4);
                    key.append(val_str);
                }
            }

            // Get or create group state
            auto it = groups_.find(key);
            if (it == groups_.end()) {
                // Store group key values for output
                std::vector<common::Value> key_vals;
                for (size_t i = 0; i < group_by_col_indices_.size(); ++i) {
                    size_t col_idx = group_by_col_indices_[i];
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
                        state.sums_int64[i] += num_col.raw_data()[row_idx];
                    } else if (col.type() == common::ValueType::TYPE_FLOAT64) {
                        auto& num_col = dynamic_cast<NumericVector<double>&>(col);
                        state.sums_float64[i] += num_col.raw_data()[row_idx];
                        state.has_float_value_[i] = true;
                    }
                }
            } else if (agg.type == AggregateType::Min && agg.input_col_idx >= 0) {
                auto& col = batch.get_column(agg.input_col_idx);
                if (!col.is_null(row_idx)) {
                    if (state.mins[i].is_null() || col.get(row_idx) < state.mins[i]) {
                        state.mins[i] = col.get(row_idx);
                    }
                }
            } else if (agg.type == AggregateType::Max && agg.input_col_idx >= 0) {
                auto& col = batch.get_column(agg.input_col_idx);
                if (!col.is_null(row_idx)) {
                    if (state.maxes[i].is_null() || state.maxes[i] < col.get(row_idx)) {
                        state.maxes[i] = col.get(row_idx);
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
                        // Emit based on output column type to preserve precision
                        if (output_schema_.get_column(col_idx).type() ==
                            common::ValueType::TYPE_INT64) {
                            out_batch.get_column(col_idx).append(
                                common::Value::make_int64(state.sums_int64[i]));
                        } else if (output_schema_.get_column(col_idx).type() ==
                                   common::ValueType::TYPE_FLOAT64) {
                            // If we saw any float64 values, use the float64 accumulator
                            // Otherwise convert from int64 accumulator
                            double float_val = state.has_float_value_[i]
                                                   ? state.sums_float64[i]
                                                   : static_cast<double>(state.sums_int64[i]);
                            out_batch.get_column(col_idx).append(
                                common::Value::make_float64(float_val));
                        } else {
                            out_batch.get_column(col_idx).append(common::Value::make_null());
                        }
                        break;
                    case AggregateType::Min:
                        out_batch.get_column(col_idx).append(state.mins[i]);
                        break;
                    case AggregateType::Max:
                        out_batch.get_column(col_idx).append(state.maxes[i]);
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

/**
 * @brief Hash bucket for streaming hash join
 */
struct VectorizedHashBucket {
    std::vector<common::Value> key_values;    // Key column value for each row in this bucket
    std::vector<std::vector<common::Value>> payload_rows;  // Full right row values
};

/**
 * @brief Vectorized hash join operator with streaming/chunked processing
 *        to bound memory usage when handling large right tables.
 */
class VectorizedHashJoinOperator : public VectorizedOperator {
   private:
    std::unique_ptr<VectorizedOperator> left_;
    std::unique_ptr<VectorizedOperator> right_;
    std::unique_ptr<parser::Expression> left_key_;
    std::unique_ptr<parser::Expression> right_key_;

    // Hash bucket for right relation (only one chunk in memory at a time)
    static constexpr size_t NUM_BUCKETS = 64;
    std::vector<VectorizedHashBucket> buckets_;

    // Processing state - streaming/chunked phases
    enum class ProcessPhase {
        LoadLeftBuffer,   // Load all left rows into buffer once
        BuildRightChunk,  // Load next right chunk into hash buckets
        ProbeChunk,       // Probe buffered left rows against current chunk
        EmitUnmatched,    // For LEFT join: emit unmatched rows with NULLs
        Done
    };
    ProcessPhase phase_ = ProcessPhase::LoadLeftBuffer;

    // Reusable batch objects
    std::unique_ptr<VectorBatch> left_batch_;
    std::unique_ptr<VectorBatch> right_batch_;

    // LEFT join: buffer all left rows for repeated probing across chunks
    static constexpr size_t RIGHT_CHUNK_SIZE = 1024;
    static constexpr size_t BATCH_SIZE = 1024;
    std::vector<std::vector<std::vector<common::Value>>> left_rows_buffer_;  // All left rows (all columns)
    std::vector<bool> left_row_matched_;  // Track if left row found any match across chunks
    size_t left_buffer_row_count_ = 0;    // Number of rows in left buffer

    // Probe state
    size_t left_row_idx_ = 0;       // Current row within buffered left rows (for current probing)
    bool right_exhausted_ = false;  // All right consumed

    // For LEFT join: track unmatched rows across all chunks
    std::vector<size_t> unmatched_indices_;

    // Join type
    JoinType join_type_;

    // Key column indices (pre-resolved)
    size_t left_key_col_idx_ = 0;
    size_t right_key_col_idx_ = 0;

    // Output column layout: left columns first, then right columns
    size_t left_col_count_ = 0;
    size_t right_col_count_ = 0;

   public:
    VectorizedHashJoinOperator(std::unique_ptr<VectorizedOperator> left,
                               std::unique_ptr<VectorizedOperator> right,
                               std::unique_ptr<parser::Expression> left_key,
                               std::unique_ptr<parser::Expression> right_key, JoinType join_type,
                               Schema output_schema)
        : VectorizedOperator(std::move(output_schema)),
          left_(std::move(left)),
          right_(std::move(right)),
          left_key_(std::move(left_key)),
          right_key_(std::move(right_key)),
          join_type_(join_type) {
        buckets_.resize(NUM_BUCKETS);
        left_batch_ = VectorBatch::create(left_->output_schema());
        right_batch_ = VectorBatch::create(right_->output_schema());

        // Pre-resolve key column indices
        left_key_col_idx_ = left_->output_schema().find_column(left_key_->to_string());
        right_key_col_idx_ = right_->output_schema().find_column(right_key_->to_string());
        left_col_count_ = left_->output_schema().columns().size();
        right_col_count_ = right_->output_schema().columns().size();

        // Pre-allocate unmatched tracking
        unmatched_indices_.reserve(BATCH_SIZE);
    }

    bool next_batch(VectorBatch& out_batch) override {
        out_batch.clear();
        if (out_batch.column_count() == 0) {
            out_batch.init_from_schema(output_schema_);
        }

        switch (phase_) {
            case ProcessPhase::LoadLeftBuffer:
                load_left_into_buffer();
                if (state_ == ExecState::Error) return false;
                phase_ = ProcessPhase::BuildRightChunk;
                [[fallthrough]];

            case ProcessPhase::BuildRightChunk:
                if (!load_next_right_chunk()) {
                    // Right exhausted - check for unmatched left rows (LEFT join)
                    if (join_type_ == JoinType::Left) {
                        phase_ = ProcessPhase::EmitUnmatched;
                        // Build unmatched list
                        unmatched_indices_.clear();
                        for (size_t i = 0; i < left_buffer_row_count_; ++i) {
                            if (!left_row_matched_[i]) unmatched_indices_.push_back(i);
                        }
                        left_row_idx_ = 0;
                    } else {
                        phase_ = ProcessPhase::Done;
                    }
                    break;
                }
                // Fall through to ProbeChunk to process this chunk
                [[fallthrough]];

            case ProcessPhase::ProbeChunk: {
                if (probe_left_against_chunk(out_batch)) return true;
                // Chunk fully probed - load next chunk or finalize
                if (right_exhausted_) {
                    if (join_type_ == JoinType::Left) {
                        phase_ = ProcessPhase::EmitUnmatched;
                        // Build unmatched list
                        unmatched_indices_.clear();
                        for (size_t i = 0; i < left_buffer_row_count_; ++i) {
                            if (!left_row_matched_[i]) unmatched_indices_.push_back(i);
                        }
                        left_row_idx_ = 0;
                        return true;  // Call again to emit unmatched rows
                    } else {
                        phase_ = ProcessPhase::Done;
                        return false;  // INNER join is done
                    }
                }
                phase_ = ProcessPhase::BuildRightChunk;
                return true;  // Call again to load next chunk
            }

            case ProcessPhase::EmitUnmatched:
                if (emit_unmatched_left_rows(out_batch)) return true;
                phase_ = ProcessPhase::Done;
                return false;

            case ProcessPhase::Done:
            default:
                return false;
        }
        // Fall-through cases: BuildRightChunk or ProbeChunk need to continue processing
        // Return true to indicate more data is available and caller should call again
        return true;
    }

   private:
    void load_left_into_buffer() {
        // Load all left rows into memory buffer for repeated probing across chunks
        while (left_->next_batch(*left_batch_)) {
            for (size_t r = 0; r < left_batch_->row_count(); ++r) {
                std::vector<std::vector<common::Value>> row_values;
                for (size_t c = 0; c < left_batch_->column_count(); ++c) {
                    row_values.push_back({left_batch_->get_column(c).get(r)});
                }
                left_rows_buffer_.push_back(std::move(row_values));
            }
            left_batch_->clear();
        }
        left_buffer_row_count_ = left_rows_buffer_.size();
        left_row_matched_.resize(left_buffer_row_count_, false);
    }

    bool load_next_right_chunk() {
        // Clear previous chunk buckets
        for (auto& bucket : buckets_) {
            bucket.key_values.clear();
            bucket.payload_rows.clear();
        }

        size_t chunk_rows = 0;
        bool more_batches = true;
        while (chunk_rows < RIGHT_CHUNK_SIZE && more_batches) {
            right_batch_->clear();
            more_batches = right_->next_batch(*right_batch_);
            if (!more_batches) break;

            for (size_t r = 0; r < right_batch_->row_count(); ++r) {
                const auto& key_val = right_batch_->get_column(right_key_col_idx_).get(r);

                // NULL keys go to special bucket (cannot match)
                if (key_val.is_null()) {
                    store_in_bucket(NUM_BUCKETS - 1, r);
                } else {
                    size_t bucket_idx = compute_bucket_idx(key_val);
                    store_in_bucket(bucket_idx, r);
                }
                chunk_rows++;
                if (chunk_rows >= RIGHT_CHUNK_SIZE) break;
            }
        }

        // If we loaded a chunk, right is not exhausted yet (there may be more chunks)
        // If we loaded nothing (chunk_rows == 0) AND more_batches is false, right is exhausted
        if (chunk_rows == 0) {
            right_exhausted_ = true;
        }
        return chunk_rows > 0;
    }

    size_t compute_bucket_idx(const common::Value& key_val) {
        std::string key_str = key_val.to_string();
        size_t hash = std::hash<std::string>{}(key_str);
        return hash % (NUM_BUCKETS - 1);
    }

    void store_in_bucket(size_t bucket_idx, size_t row_idx) {
        auto& bucket = buckets_[bucket_idx];

        // Store only the key column value
        common::Value key_val = right_batch_->get_column(right_key_col_idx_).get(row_idx);
        bucket.key_values.push_back(key_val);

        // Store full row for payload
        std::vector<common::Value> payload;
        for (size_t c = 0; c < right_batch_->column_count(); ++c) {
            payload.push_back(right_batch_->get_column(c).get(row_idx));
        }
        bucket.payload_rows.push_back(std::move(payload));
    }

    bool probe_left_against_chunk(VectorBatch& out_batch) {
        for (size_t left_idx = 0; left_idx < left_buffer_row_count_; ++left_idx) {
            if (out_batch.row_count() >= BATCH_SIZE) {
                return true;  // Batch is full
            }

            // For INNER join, skip already matched rows
            if (join_type_ == JoinType::Inner && left_row_matched_[left_idx]) {
                continue;
            }

            const auto& key_val = left_rows_buffer_[left_idx][left_key_col_idx_][0];

            if (key_val.is_null()) {
                continue;  // NULL keys never match
            }

            size_t bucket_idx = compute_bucket_idx(key_val);
            auto& bucket = buckets_[bucket_idx];

            // Search for match in this bucket
            for (size_t i = 0; i < bucket.key_values.size(); ++i) {
                if (out_batch.row_count() >= BATCH_SIZE) {
                    return true;  // Batch is full
                }

                const auto& bucket_key = bucket.key_values[i];
                if (bucket_key == key_val) {
                    emit_joined_row(out_batch, left_idx, bucket.payload_rows[i]);
                    if (join_type_ == JoinType::Left) {
                        left_row_matched_[left_idx] = true;
                    }
                }
            }
        }
        return false;  // Return false so state machine continues to next chunk or unmatched emission
    }

    void emit_joined_row(VectorBatch& out_batch, size_t left_buffer_idx,
                         const std::vector<common::Value>& right_row) {
        // Append left columns from buffer
        for (size_t c = 0; c < left_col_count_; ++c) {
            out_batch.get_column(c).append(left_rows_buffer_[left_buffer_idx][c][0]);
        }
        // Append right columns
        for (size_t c = 0; c < right_row.size(); ++c) {
            out_batch.get_column(left_col_count_ + c).append(right_row[c]);
        }
        out_batch.set_row_count(out_batch.row_count() + 1);
    }

    bool emit_unmatched_left_rows(VectorBatch& out_batch) {
        while (left_row_idx_ < unmatched_indices_.size()) {
            if (out_batch.row_count() >= BATCH_SIZE) {
                return true;  // Batch is full
            }
            size_t idx = unmatched_indices_[left_row_idx_];
            // Append left columns from buffer
            for (size_t c = 0; c < left_col_count_; ++c) {
                out_batch.get_column(c).append(left_rows_buffer_[idx][c][0]);
            }
            // Append NULLs for right columns
            for (size_t c = 0; c < right_col_count_; ++c) {
                out_batch.get_column(left_col_count_ + c).append(common::Value::make_null());
            }
            out_batch.set_row_count(out_batch.row_count() + 1);
            left_row_idx_++;
        }
        return out_batch.row_count() > 0;
    }
};

}  // namespace cloudsql::executor

#endif  // CLOUDSQL_EXECUTOR_VECTORIZED_OPERATOR_HPP
