/**
 * @file vectorized_operator.hpp
 * @brief Base class for vectorized query operators
 */

#ifndef CLOUDSQL_EXECUTOR_VECTORIZED_OPERATOR_HPP
#define CLOUDSQL_EXECUTOR_VECTORIZED_OPERATOR_HPP

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "executor/operator.hpp"
#include "executor/thread_pool.hpp"
#include "executor/types.hpp"
#include "parser/expression.hpp"
#include "storage/columnar_table.hpp"

namespace cloudsql::executor {

/**
 * @brief Base class for vectorized operators (Batch-at-a-time)
 */
class VectorizedOperator : public Operator {
   protected:
    ExecState state_ = ExecState::Init;
    std::string error_message_;
    Schema output_schema_;

   public:
    explicit VectorizedOperator(Schema schema)
        : Operator(OperatorType::Result), output_schema_(std::move(schema)) {}
    virtual ~VectorizedOperator() = default;

    bool init() override { return true; }
    bool open() override { return true; }

    /**
     * @brief Produce the next batch of results
     * @return true if a batch was produced, false if EOF or error
     */
    virtual bool next_batch(VectorBatch& out_batch) = 0;

    void close() override {}

    [[nodiscard]] Schema& output_schema() override { return output_schema_; }
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
    uint32_t batch_size_ = 4096;
    std::shared_ptr<ThreadPool> thread_pool_;
    bool parallel_enabled_ = false;
    size_t num_threads_ = 1;
    std::vector<std::unique_ptr<VectorBatch>> parallel_results_;
    size_t parallel_idx_ = 0;

   public:
    VectorizedSeqScanOperator(std::string table_name, std::shared_ptr<storage::ColumnarTable> table,
                              std::shared_ptr<ThreadPool> thread_pool = nullptr)
        : VectorizedOperator(table->schema()),
          table_name_(std::move(table_name)),
          table_(std::move(table)),
          thread_pool_(std::move(thread_pool)) {
        if (thread_pool_ && thread_pool_->num_threads() > 1) {
            num_threads_ = thread_pool_->num_threads();
            parallel_enabled_ = table_->row_count() > 50000;
        }
    }

    bool next_batch(VectorBatch& out_batch) override {
        if (!parallel_enabled_ || !thread_pool_) {
            return next_batch_sequential(out_batch);
        }
        return next_batch_parallel(out_batch);
    }

   private:
    bool next_batch_sequential(VectorBatch& out_batch) {
        if (current_row_ >= table_->row_count()) {
            return false;
        }

        if (table_->read_batch(current_row_, batch_size_, out_batch)) {
            current_row_ += out_batch.row_count();
            return true;
        }
        return false;
    }

    bool next_batch_parallel(VectorBatch& out_batch) {
        if (parallel_idx_ >= parallel_results_.size()) {
            size_t total_rows = table_->row_count();
            if (current_row_ >= total_rows) {
                return false;
            }

            size_t range_size = (total_rows - current_row_ + num_threads_ - 1) / num_threads_;

            parallel_results_.clear();
            parallel_idx_ = 0;

            std::vector<size_t> task_starts;
            task_starts.reserve(num_threads_);

            for (size_t t = 0; t < num_threads_ && current_row_ < total_rows; ++t) {
                size_t start = current_row_;
                task_starts.push_back(start);
                size_t end = std::min(start + range_size, total_rows);
                current_row_ = end;

                auto batch = VectorBatch::create(output_schema_);
                parallel_results_.push_back(std::move(batch));
            }

            for (size_t t = 0; t < task_starts.size(); ++t) {
                size_t start = task_starts[t];
                size_t rows_to_read = std::min(range_size, total_rows - start);
                if (start >= total_rows) {
                    parallel_results_[t]->set_row_count(0);
                    continue;
                }
                thread_pool_->submit([this, t, start, rows_to_read]() {
                    table_->read_batch(start, static_cast<uint32_t>(rows_to_read),
                                       *parallel_results_[t]);
                });
            }

            thread_pool_->wait();
        }

        if (parallel_idx_ < parallel_results_.size()) {
            auto& src = *parallel_results_[parallel_idx_];
            out_batch.init_from_schema(output_schema_);
            for (size_t c = 0; c < src.column_count(); ++c) {
                out_batch.get_column(c).steal(std::move(src.get_column(c)));
            }
            out_batch.set_row_count(src.row_count());
            parallel_idx_++;
            return out_batch.row_count() > 0;
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
 * @brief Open-addressing hash aggregation for arbitrary GROUP BY keys.
 *
 * Uses linear probing with power-of-2 capacity. Binary key encoding avoids
 * string allocation for common key types. Stores hash to avoid recomputation
 * on collision resolution.
 *
 * Key encoding scheme:
 * [1 byte: type tag] 0x01=NULL, 0x02=INT64, 0x03=FLOAT64, 0x04=STRING
 * [4 bytes: key length (little-endian)]
 * [key data...]
 */
class OpenAddressHashAgg {
   public:
    static constexpr size_t MAX_AGGREGATES = 8;
    static constexpr float kLoadFactor = 0.5f;

   private:
    struct HashBucket {
        bool occupied = false;
        bool is_new = false;  // True if this bucket was just allocated
        uint64_t key_hash = 0;
        int64_t key_int64 = 0;  // Direct storage for int64 keys
        int64_t counts[MAX_AGGREGATES] = {0};
        int64_t sums_int64[MAX_AGGREGATES] = {0};
        double sums_float64[MAX_AGGREGATES / 2] = {0.0};
        bool has_float_value[MAX_AGGREGATES] = {false};
        int64_t mins[MAX_AGGREGATES] = {0};
        int64_t maxes[MAX_AGGREGATES] = {0};
        bool has_mins[MAX_AGGREGATES] = {false};  // Track if initialized
        uint8_t key_type = 0;                     // 0x02=INT64, 0x04=STRING
        uint32_t key_len = 0;                     // For non-int64 keys
        uint8_t key_data[64];                     // Stored key bytes for iteration
    };

    std::vector<HashBucket> buckets_;
    size_t mask_ = 0;
    size_t num_occupied_ = 0;
    size_t max_aggregates_ = 0;
    std::vector<size_t> valid_indices_;  // For iteration

    static constexpr size_t kInitialCapacity = 1024;

   public:
    static uint64_t hash_bytes(const uint8_t* data, size_t len) {
        // FNV-1a 64-bit hash
        uint64_t hash = 14695981039346656037ull;
        for (size_t i = 0; i < len; ++i) {
            hash ^= data[i];
            hash *= 1099511628211ull;
        }
        return hash;
    }

    void init(size_t capacity_hint, size_t max_aggregates) {
        max_aggregates_ = max_aggregates;
        num_occupied_ = 0;
        valid_indices_.clear();

        size_t cap = kInitialCapacity;
        while (cap < capacity_hint) cap *= 2;
        buckets_.assign(cap, HashBucket());
        mask_ = cap - 1;
    }

    HashBucket& find_or_insert(const uint8_t* key, size_t key_len, uint64_t hash) {
        // Grow if load factor exceeded
        if (num_occupied_ >= buckets_.size() * kLoadFactor) {
            grow();
        }

        size_t idx = hash & mask_;
        for (size_t probes = 0; probes < buckets_.size(); ++probes) {
            auto& bucket = buckets_[idx];
            if (!bucket.occupied) {
                bucket.occupied = true;
                bucket.is_new = true;
                bucket.key_hash = hash;
                bucket.key_len = static_cast<uint32_t>(key_len);
                bucket.key_type = key[0];
                std::memcpy(bucket.key_data, key, key_len);
                num_occupied_++;
                valid_indices_.push_back(idx);
                return bucket;
            }
            if (bucket.key_hash == hash && bucket.key_len == key_len && bucket.key_type == key[0] &&
                std::memcmp(bucket.key_data, key, key_len) == 0) {
                bucket.is_new = false;
                return bucket;  // Found
            }
            idx = (idx + 1) & mask_;  // Linear probe
        }
        return buckets_[idx];  // Shouldn't reach here
    }

    HashBucket& find_or_insert_int64(int64_t key, uint64_t hash) {
        if (num_occupied_ >= buckets_.size() * kLoadFactor) {
            grow();
        }

        uint8_t key_buf[sizeof(int64_t) + 1];
        key_buf[0] = 0x02;
        std::memcpy(&key_buf[1], &key, sizeof(int64_t));

        size_t idx = hash & mask_;
        for (size_t probes = 0; probes < buckets_.size(); ++probes) {
            auto& bucket = buckets_[idx];
            if (!bucket.occupied) {
                bucket.occupied = true;
                bucket.is_new = true;
                bucket.key_hash = hash;
                bucket.key_int64 = key;
                bucket.key_type = 0x02;
                bucket.key_len = sizeof(int64_t) + 1;
                std::memcpy(bucket.key_data, key_buf, bucket.key_len);
                num_occupied_++;
                valid_indices_.push_back(idx);
                return bucket;
            }
            if (bucket.key_type == 0x02 && bucket.key_int64 == key) {
                bucket.is_new = false;
                return bucket;
            }
            idx = (idx + 1) & mask_;
        }
        return buckets_[idx];
    }

    void grow() {
        auto old_buckets = std::move(buckets_);
        size_t new_cap = old_buckets.empty() ? kInitialCapacity : old_buckets.size() * 2;
        buckets_.assign(new_cap, HashBucket());
        mask_ = new_cap - 1;
        num_occupied_ = 0;
        valid_indices_.clear();

        for (size_t i = 0; i < old_buckets.size(); ++i) {
            if (old_buckets[i].occupied) {
                if (old_buckets[i].key_type == 0x02) {
                    find_or_insert_int64(old_buckets[i].key_int64, old_buckets[i].key_hash);
                } else {
                    find_or_insert(old_buckets[i].key_data, old_buckets[i].key_len,
                                   old_buckets[i].key_hash);
                }
            }
        }
    }

    size_t group_count() const { return valid_indices_.size(); }
    const std::vector<size_t>& valid_slots() const { return valid_indices_; }
    HashBucket& slot(size_t idx) { return buckets_[idx]; }
    const HashBucket& slot(size_t idx) const { return buckets_[idx]; }
};

/**
 * @brief Direct-indexed aggregation for low-cardinality integer GROUP BY.
 *
 * When the number of distinct GROUP BY values is small, we can use a
 * simple vector indexed by key value rather than a hash table. This avoids:
 * - Hash computation per row
 * - Hash table probing and collision handling
 * - String key allocation and comparison
 *
 * For each row: slot_idx = (key - min_key) where min_key is the
 * minimum key value observed. This gives O(1) direct indexing.
 */
class DirectIndexAgg {
   public:
    static constexpr size_t MAX_AGGREGATES = 8;
    static constexpr size_t MAX_GROUP_KEYS = 2;

   private:
    struct GroupSlot {
        bool valid = false;
        int64_t key1 = 0;
        int64_t key2 = 0;
        int64_t counts[MAX_AGGREGATES] = {0};
        int64_t sums_int64[MAX_AGGREGATES] = {0};
        double sums_float64[MAX_AGGREGATES / 2] = {0.0};
        bool has_float_value[MAX_AGGREGATES] = {false};
    };

    std::vector<GroupSlot> slots_;
    mutable size_t max_aggregates_ = 0;
    mutable size_t max_group_keys_ = 0;
    mutable int64_t min_key_ = INT64_MAX;
    mutable int64_t max_key_ = INT64_MIN;

   public:
    void init(size_t capacity_hint, size_t max_aggregates, size_t max_group_keys = 1) {
        max_aggregates_ = max_aggregates;
        max_group_keys_ = max_group_keys;
        min_key_ = INT64_MAX;
        max_key_ = INT64_MIN;
        slots_.resize(capacity_hint);
    }

    GroupSlot& slot(size_t idx) { return slots_[idx]; }
    const GroupSlot& slot(size_t idx) const { return slots_[idx]; }

    size_t find_or_insert(int64_t key1, int64_t key2 = 0) {
        // Expand if key outside current range
        if (key1 < min_key_ || key1 > max_key_) {
            if (key1 < min_key_) min_key_ = key1;
            if (key1 > max_key_) max_key_ = key1;
            size_t new_size = static_cast<size_t>(max_key_ - min_key_ + 1);
            if (new_size > slots_.size()) {
                size_t alloc_size = 1;
                while (alloc_size < new_size) alloc_size *= 2;
                slots_.resize(alloc_size);
            }
        }
        size_t idx = static_cast<size_t>(key1 - min_key_);
        slots_[idx].valid = true;  // Mark valid on first insertion
        return idx;
    }

    size_t group_count() const {
        size_t count = 0;
        for (const auto& s : slots_) {
            if (s.valid) ++count;
        }
        return count;
    }

    int64_t min_key() const { return min_key_; }
    int64_t max_key() const { return max_key_; }
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

    // Direct-index aggregation (for low-cardinality integer GROUP BY)
    DirectIndexAgg agg_;
    bool is_direct_indexable_ = false;
    std::vector<int64_t> direct_group_keys_;  // Ordered keys for direct index output

    // Open-addressing hash aggregation (for general GROUP BY)
    OpenAddressHashAgg hash_agg_;
    std::vector<std::vector<common::Value>> hash_group_keys_;  // Ordered group keys for iteration

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

        // Check if we can use direct indexing (single integer GROUP BY column)
        bool is_int_key = (group_by_col_indices_[0] != static_cast<size_t>(-1));
        if (is_int_key) {
            auto col_type = schema.get_column(group_by_col_indices_[0]).type();
            is_int_key = (col_type == common::ValueType::TYPE_INT64 ||
                          col_type == common::ValueType::TYPE_INT32 ||
                          col_type == common::ValueType::TYPE_INT16 ||
                          col_type == common::ValueType::TYPE_INT8);
        }
        is_direct_indexable_ = (group_by_.size() == 1 && is_int_key);
        if (is_direct_indexable_) {
            agg_.init(65536, aggregates_.size(), group_by_.size());
        } else {
            hash_agg_.init(65536, aggregates_.size());
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
        if (is_direct_indexable_) {
            process_input_batch_direct(batch);
        } else {
            process_input_batch_open_addressing(batch);
        }
    }

    void process_input_batch_direct(VectorBatch& batch) {
        // Fast path: direct integer key indexing
        const size_t gb_col_idx = group_by_col_indices_[0];
        const auto& gb_col = batch.get_column(gb_col_idx);

        for (size_t r = 0; r < batch.row_count(); ++r) {
            int64_t key = gb_col.get(r).to_int64();
            size_t slot_idx = agg_.find_or_insert(key, 0);
            auto& slot = agg_.slot(slot_idx);

            if (!slot.valid) {
                slot.valid = true;
                slot.key1 = key;
                direct_group_keys_.push_back(key);
            }

            // Update accumulators directly in slot
            for (size_t i = 0; i < aggregates_.size(); ++i) {
                const auto& agg = aggregates_[i];
                if (agg.type == AggregateType::Count && agg.input_col_idx < 0) {
                    slot.counts[i]++;
                } else if ((agg.type == AggregateType::Sum || agg.type == AggregateType::Avg) &&
                           agg.input_col_idx >= 0) {
                    const auto& col = batch.get_column(agg.input_col_idx);
                    if (!col.is_null(r)) {
                        slot.counts[i]++;
                        if (col.type() == common::ValueType::TYPE_INT64) {
                            auto& num_col = dynamic_cast<const NumericVector<int64_t>&>(col);
                            slot.sums_int64[i] += num_col.raw_data()[r];
                        } else if (col.type() == common::ValueType::TYPE_FLOAT64) {
                            auto& num_col = dynamic_cast<const NumericVector<double>&>(col);
                            slot.sums_float64[i] += num_col.raw_data()[r];
                            slot.has_float_value[i] = true;
                        }
                    }
                }
            }
        }
        input_batch_->clear();
    }

    void process_input_batch_open_addressing(VectorBatch& batch) {
        // Fast path: open-addressing hash with binary key encoding
        for (size_t r = 0; r < batch.row_count(); ++r) {
            // Encode key: [type tag][len][data]
            uint8_t key_buf[64];
            uint8_t* key_ptr = key_buf;
            std::vector<uint8_t> heap_key;
            size_t key_len = 0;

            for (size_t i = 0; i < group_by_col_indices_.size(); ++i) {
                size_t col_idx = group_by_col_indices_[i];
                if (col_idx == static_cast<size_t>(-1)) {
                    set_error("GROUP BY: column not found in input schema: " +
                              group_by_[i]->to_string());
                    return;
                }

                const auto& val = batch.get_column(col_idx).get(r);
                if (val.is_null()) {
                    key_ptr[key_len++] = 0x01;  // NULL tag
                } else if (val.type() == common::ValueType::TYPE_INT64) {
                    key_ptr[key_len++] = 0x02;  // INT64 tag
                    int64_t v = val.to_int64();
                    std::memcpy(&key_ptr[key_len], &v, sizeof(int64_t));
                    key_len += sizeof(int64_t);
                } else {
                    key_ptr[key_len++] = 0x04;  // STRING tag
                    std::string val_str = val.as_text();
                    uint32_t len = static_cast<uint32_t>(val_str.size());
                    if (key_len + 4 + val_str.size() > 64) {
                        heap_key.resize(key_len + 4 + val_str.size());
                        std::memcpy(heap_key.data(), key_ptr, key_len);
                        key_ptr = heap_key.data();
                    }
                    std::memcpy(&key_ptr[key_len], &len, 4);
                    key_len += 4;
                    std::memcpy(&key_ptr[key_len], val_str.data(), val_str.size());
                    key_len += val_str.size();
                }
            }

            uint64_t hash = OpenAddressHashAgg::hash_bytes(key_ptr, key_len);
            auto& bucket = hash_agg_.find_or_insert(key_ptr, key_len, hash);

            // Store key for output if first time
            if (bucket.is_new) {
                std::vector<common::Value> key_vals;
                for (size_t i = 0; i < group_by_col_indices_.size(); ++i) {
                    key_vals.push_back(batch.get_column(group_by_col_indices_[i]).get(r));
                }
                hash_group_keys_.push_back(std::move(key_vals));
            }

            // Update accumulators directly in bucket
            for (size_t i = 0; i < aggregates_.size(); ++i) {
                const auto& agg = aggregates_[i];
                if (agg.type == AggregateType::Count && agg.input_col_idx < 0) {
                    bucket.counts[i]++;
                } else if ((agg.type == AggregateType::Sum || agg.type == AggregateType::Avg) &&
                           agg.input_col_idx >= 0) {
                    const auto& col = batch.get_column(agg.input_col_idx);
                    if (!col.is_null(r)) {
                        bucket.counts[i]++;
                        if (col.type() == common::ValueType::TYPE_INT64) {
                            auto& num_col = dynamic_cast<const NumericVector<int64_t>&>(col);
                            bucket.sums_int64[i] += num_col.raw_data()[r];
                        } else if (col.type() == common::ValueType::TYPE_FLOAT64) {
                            auto& num_col = dynamic_cast<const NumericVector<double>&>(col);
                            bucket.sums_float64[i] += num_col.raw_data()[r];
                            bucket.has_float_value[i] = true;
                        }
                    }
                } else if ((agg.type == AggregateType::Min || agg.type == AggregateType::Max) &&
                           agg.input_col_idx >= 0) {
                    const auto& col = batch.get_column(agg.input_col_idx);
                    if (!col.is_null(r)) {
                        auto val = col.get(r).to_int64();
                        if (!bucket.has_mins[i]) {
                            bucket.mins[i] = val;
                            bucket.maxes[i] = val;
                            bucket.has_mins[i] = true;
                        } else {
                            bucket.mins[i] = std::min(bucket.mins[i], val);
                            bucket.maxes[i] = std::max(bucket.maxes[i], val);
                        }
                    }
                }
            }
        }
        input_batch_->clear();
    }

    void update_accumulators(VectorizedGroupState& state, VectorBatch& batch, size_t row_idx) {
        for (size_t i = 0; i < aggregates_.size(); ++i) {
            const auto& agg = aggregates_[i];

            if (agg.type == AggregateType::Count && agg.input_col_idx < 0) {
                // COUNT(*) - always increment
                state.counts[i]++;
            } else if ((agg.type == AggregateType::Sum || agg.type == AggregateType::Avg) &&
                       agg.input_col_idx >= 0) {
                auto& col = batch.get_column(agg.input_col_idx);
                if (!col.is_null(row_idx)) {
                    state.counts[i]++;  // Track count for AVG
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
        if (is_direct_indexable_) {
            return produce_output_batch_direct(out_batch);
        }
        return produce_output_batch_open_addressing(out_batch);
    }

    bool produce_output_batch_direct(VectorBatch& out_batch) {
        if (current_group_idx_ >= direct_group_keys_.size()) {
            return false;  // EOF
        }

        out_batch.clear();
        if (out_batch.column_count() == 0) {
            out_batch.init_from_schema(output_schema_);
        }

        constexpr size_t BATCH_SIZE = 1024;
        size_t output_count = 0;

        // Iterate through direct_group_keys_ and emit groups
        while (current_group_idx_ < direct_group_keys_.size() && output_count < BATCH_SIZE) {
            int64_t key = direct_group_keys_[current_group_idx_];
            size_t slot_idx = static_cast<size_t>(key - agg_.min_key());
            const auto& slot = agg_.slot(slot_idx);

            // Append group key column
            out_batch.get_column(0).append(common::Value::make_int64(key));

            // Append aggregate result columns
            for (size_t i = 0; i < aggregates_.size(); ++i) {
                size_t col_idx = group_by_.size() + i;
                switch (aggregates_[i].type) {
                    case AggregateType::Count:
                        out_batch.get_column(col_idx).append(
                            common::Value::make_int64(slot.counts[i]));
                        break;
                    case AggregateType::Sum:
                        if (output_schema_.get_column(col_idx).type() ==
                            common::ValueType::TYPE_INT64) {
                            out_batch.get_column(col_idx).append(
                                common::Value::make_int64(slot.sums_int64[i]));
                        } else {
                            double float_val = slot.has_float_value[i]
                                                   ? slot.sums_float64[i]
                                                   : static_cast<double>(slot.sums_int64[i]);
                            out_batch.get_column(col_idx).append(
                                common::Value::make_float64(float_val));
                        }
                        break;
                    case AggregateType::Avg:
                        if (slot.counts[i] > 0) {
                            double avg_val =
                                slot.has_float_value[i]
                                    ? slot.sums_float64[i] / static_cast<double>(slot.counts[i])
                                    : static_cast<double>(slot.sums_int64[i]) /
                                          static_cast<double>(slot.counts[i]);
                            out_batch.get_column(col_idx).append(
                                common::Value::make_float64(avg_val));
                        } else {
                            out_batch.get_column(col_idx).append(common::Value::make_null());
                        }
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

    bool produce_output_batch_open_addressing(VectorBatch& out_batch) {
        if (current_group_idx_ >= hash_agg_.group_count()) {
            return false;  // EOF
        }

        out_batch.clear();
        if (out_batch.column_count() == 0) {
            out_batch.init_from_schema(output_schema_);
        }

        constexpr size_t BATCH_SIZE = 1024;
        size_t output_count = 0;

        while (current_group_idx_ < hash_group_keys_.size() && output_count < BATCH_SIZE) {
            size_t slot_idx = hash_agg_.valid_slots()[current_group_idx_];
            const auto& bucket = hash_agg_.slot(slot_idx);

            // Append group key columns
            const auto& key_vals = hash_group_keys_[current_group_idx_];
            for (size_t i = 0; i < key_vals.size(); ++i) {
                out_batch.get_column(i).append(key_vals[i]);
            }

            // Append aggregate result columns
            for (size_t i = 0; i < aggregates_.size(); ++i) {
                size_t col_idx = group_by_.size() + i;
                switch (aggregates_[i].type) {
                    case AggregateType::Count:
                        out_batch.get_column(col_idx).append(
                            common::Value::make_int64(bucket.counts[i]));
                        break;
                    case AggregateType::Sum:
                        if (output_schema_.get_column(col_idx).type() ==
                            common::ValueType::TYPE_INT64) {
                            out_batch.get_column(col_idx).append(
                                common::Value::make_int64(bucket.sums_int64[i]));
                        } else {
                            double float_val = bucket.has_float_value[i]
                                                   ? bucket.sums_float64[i]
                                                   : static_cast<double>(bucket.sums_int64[i]);
                            out_batch.get_column(col_idx).append(
                                common::Value::make_float64(float_val));
                        }
                        break;
                    case AggregateType::Avg:
                        if (bucket.counts[i] > 0) {
                            double avg_val =
                                bucket.has_float_value[i]
                                    ? bucket.sums_float64[i] / static_cast<double>(bucket.counts[i])
                                    : static_cast<double>(bucket.sums_int64[i]) /
                                          static_cast<double>(bucket.counts[i]);
                            out_batch.get_column(col_idx).append(
                                common::Value::make_float64(avg_val));
                        } else {
                            out_batch.get_column(col_idx).append(common::Value::make_null());
                        }
                        break;
                    case AggregateType::Min:
                        if (bucket.has_mins[i]) {
                            out_batch.get_column(col_idx).append(
                                common::Value::make_int64(bucket.mins[i]));
                        } else {
                            out_batch.get_column(col_idx).append(common::Value::make_null());
                        }
                        break;
                    case AggregateType::Max:
                        if (bucket.has_mins[i]) {
                            out_batch.get_column(col_idx).append(
                                common::Value::make_int64(bucket.maxes[i]));
                        } else {
                            out_batch.get_column(col_idx).append(common::Value::make_null());
                        }
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

    bool produce_output_batch_hash(VectorBatch& out_batch) {
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
                    case AggregateType::Avg:
                        if (state.counts[i] > 0) {
                            double avg_val =
                                state.has_float_value_[i]
                                    ? state.sums_float64[i] / static_cast<double>(state.counts[i])
                                    : static_cast<double>(state.sums_int64[i]) /
                                          static_cast<double>(state.counts[i]);
                            out_batch.get_column(col_idx).append(
                                common::Value::make_float64(avg_val));
                        } else {
                            out_batch.get_column(col_idx).append(common::Value::make_null());
                        }
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
 * @brief Hash bucket for graceful hash join
 */
struct VectorizedHashBucket {
    std::vector<std::vector<common::Value>> key_values;    // Key column values per row
    std::vector<std::vector<common::Value>> payload_rows;  // Full right row values
    std::vector<size_t>
        right_row_indices;  // Global indices into right_bucket_rows_ for unmatched tracking
};

/**
 * @brief Vectorized hash join operator with graceful partitioning
 */
class VectorizedHashJoinOperator : public VectorizedOperator {
   private:
    std::unique_ptr<VectorizedOperator> left_;
    std::unique_ptr<VectorizedOperator> right_;
    std::unique_ptr<parser::Expression> left_key_;
    std::unique_ptr<parser::Expression> right_key_;

    // Graceful hash partition buckets (for right relation)
    static constexpr size_t NUM_BUCKETS = 64;
    std::vector<VectorizedHashBucket> buckets_;

    // Processing state
    enum class ProcessPhase { BuildRight, ProbeLeft, Done };
    ProcessPhase phase_ = ProcessPhase::BuildRight;

    // Reusable batch objects
    std::unique_ptr<VectorBatch> left_batch_;
    std::unique_ptr<VectorBatch> right_batch_;

    // Probe state
    size_t left_row_idx_ = 0;       // Current row within left_batch_
    bool right_exhausted_ = false;  // All right consumed
    bool left_exhausted_ = false;   // All left consumed

    // For LEFT join: track matched/unmatched rows
    static constexpr size_t BATCH_SIZE = 1024;
    std::vector<bool> left_matched_in_batch_;
    std::vector<size_t> unmatched_left_indices_;

    // For RIGHT join: track matched right rows during probe
    std::vector<bool> right_matched_;
    std::vector<size_t> unmatched_right_rows_;
    bool emitted_unmatched_right_ = false;

    // Probe state for resumable bucket scanning (prevents batch overflow)
    bool resuming_bucket_scan_ = false;  // True if we're resuming a mid-bucket scan
    size_t resumed_bucket_idx_ = 0;      // Bucket index when resuming
    size_t resumed_entry_idx_ = 0;       // Entry index within bucket when resuming
    common::Value resumed_key_val_;      // Key value being probed when resuming

    // Join type
    JoinType join_type_;

    // Track if we emitted unmatched rows on the last probe call (for LEFT join)
    bool emitted_unmatched_last_probe_ = false;

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

        // Pre-size matched tracking vectors
        left_matched_in_batch_.resize(BATCH_SIZE, false);
        unmatched_left_indices_.reserve(BATCH_SIZE);
    }

    bool next_batch(VectorBatch& out_batch) override {
        out_batch.clear();
        if (out_batch.column_count() == 0) {
            out_batch.init_from_schema(output_schema_);
        }

        switch (phase_) {
            case ProcessPhase::BuildRight:
                build_hash_table();
                if (state_ == ExecState::Error) return false;
                // Resize matched tracking for right rows (needed for RIGHT/FULL joins)
                right_matched_.resize(right_bucket_rows_.size(), false);
                phase_ = ProcessPhase::ProbeLeft;
                [[fallthrough]];
            case ProcessPhase::ProbeLeft:
                if (probe_and_emit(out_batch)) return true;
                // probe_and_emit returned false - all data consumed
                // If we emitted unmatched rows in probe_and_emit (when left exhausted),
                // out_batch already has them, so return true
                phase_ = ProcessPhase::Done;
                [[fallthrough]];
            case ProcessPhase::Done:
                // Emit unmatched right rows for RIGHT/FULL joins
                if (!emitted_unmatched_right_ &&
                    (join_type_ == JoinType::Right || join_type_ == JoinType::Full)) {
                    // Build unmatched_right_rows_ from right_matched_ (unmatched = false)
                    for (size_t i = 0; i < right_matched_.size(); ++i) {
                        if (!right_matched_[i]) {
                            unmatched_right_rows_.push_back(i);
                        }
                    }
                    if (emit_unmatched_right_rows(out_batch)) {
                        return true;  // Batch is full, more to emit later
                    }
                    // We emitted rows but batch wasn't full - return true so caller can process
                    // them
                    if (out_batch.row_count() > 0) {
                        emitted_unmatched_right_ = true;
                        return true;
                    }
                    emitted_unmatched_right_ = true;
                }
                return false;
            default:
                return false;
        }
    }

   private:
    void build_hash_table() {
        // Phase 1: Consume all right batches and partition into hash buckets
        while (right_->next_batch(*right_batch_)) {
            for (size_t r = 0; r < right_batch_->row_count(); ++r) {
                // Get key value
                const auto& key_val = right_batch_->get_column(right_key_col_idx_).get(r);

                // NULL keys go to special bucket (cannot match)
                if (key_val.is_null()) {
                    store_in_bucket(NUM_BUCKETS - 1, r);
                } else {
                    size_t bucket_idx = compute_bucket_idx(key_val);
                    store_in_bucket(bucket_idx, r);
                }
            }
            right_batch_->clear();
        }
    }

    size_t compute_bucket_idx(const common::Value& key_val) {
        // Use string representation for hashing (consistent with GROUP BY)
        std::string key_str = key_val.to_string();
        size_t hash = std::hash<std::string>{}(key_str);
        return hash % (NUM_BUCKETS - 1);  // -1 to leave room for NULL bucket
    }

    void store_in_bucket(size_t bucket_idx, size_t row_idx) {
        auto& bucket = buckets_[bucket_idx];

        // Store key values
        std::vector<common::Value> key_vals;
        for (size_t c = 0; c < right_batch_->column_count(); ++c) {
            key_vals.push_back(right_batch_->get_column(c).get(row_idx));
        }
        bucket.key_values.push_back(std::move(key_vals));

        // Store full row (same data for now, could optimize)
        bucket.payload_rows.push_back(bucket.key_values.back());

        // Track global right row index for RIGHT/FULL join unmatched tracking
        size_t global_idx = right_bucket_rows_.size();
        right_bucket_rows_.push_back(bucket.payload_rows.back());

        // Track this bucket/entry for unmatched right row emission (RIGHT/FULL join)
        if (join_type_ == JoinType::Right || join_type_ == JoinType::Full) {
            bucket.right_row_indices.push_back(global_idx);
        }
    }

    bool probe_and_emit(VectorBatch& out_batch) {
        while (true) {
            // Get next left batch if needed
            if (left_row_idx_ >= left_batch_->row_count()) {
                // For LEFT/FULL join: if there are unmatched rows, emit them FIRST
                if ((join_type_ == JoinType::Left || join_type_ == JoinType::Full) &&
                    !unmatched_left_indices_.empty()) {
                    // First, emit all unmatched rows before any matched rows
                    if (emit_unmatched_left_rows(out_batch)) {
                        return true;  // Batch is full
                    }
                    unmatched_left_indices_.clear();
                }

                left_batch_->clear();
                if (!left_->next_batch(*left_batch_)) {
                    left_exhausted_ = true;
                    right_exhausted_ = true;
                    // If we have data in out_batch (from unmatched emit), return true to give
                    // caller the data
                    if (out_batch.row_count() > 0) {
                        return true;
                    }
                    return false;
                }
                left_row_idx_ = 0;
                // Reset matched tracking for new batch
                std::fill(left_matched_in_batch_.begin(), left_matched_in_batch_.end(), false);
                // Clear resume state when advancing to new batch
                resuming_bucket_scan_ = false;
            }

            // Process rows in current batch
            while (left_row_idx_ < left_batch_->row_count() && out_batch.row_count() < BATCH_SIZE) {
                // Check if we need to resume an interrupted bucket scan
                if (resuming_bucket_scan_) {
                    // We were in the middle of scanning a bucket - resume from saved position
                    const auto& key_val = resumed_key_val_;
                    auto& bucket = buckets_[resumed_bucket_idx_];
                    bool found_match = left_matched_in_batch_[left_row_idx_];

                    // Resume scanning bucket from resumed_entry_idx_
                    for (size_t i = resumed_entry_idx_; i < bucket.key_values.size(); ++i) {
                        if (out_batch.row_count() >= BATCH_SIZE) {
                            // Batch full - save state and return
                            resuming_bucket_scan_ = true;
                            resumed_entry_idx_ = i;
                            resumed_key_val_ = key_val;
                            return true;  // Caller must consume batch before continuing
                        }

                        const auto& bucket_key = bucket.key_values[i][right_key_col_idx_];
                        if (bucket_key == key_val) {
                            emit_joined_row(out_batch, left_row_idx_, bucket.payload_rows[i]);
                            found_match = true;
                            if (join_type_ == JoinType::Left) {
                                left_matched_in_batch_[left_row_idx_] = true;
                            }
                        }
                    }

                    // Finished scanning this bucket
                    resuming_bucket_scan_ = false;

                    // Track unmatched for LEFT/FULL join
                    if ((join_type_ == JoinType::Left || join_type_ == JoinType::Full) &&
                        !found_match) {
                        unmatched_left_indices_.push_back(left_row_idx_);
                    }

                    left_row_idx_++;
                    continue;
                }

                const auto& key_val = left_batch_->get_column(left_key_col_idx_).get(left_row_idx_);

                if (key_val.is_null()) {
                    // NULL keys never match - mark as unmatched for LEFT/FULL join
                    if (join_type_ == JoinType::Left || join_type_ == JoinType::Full) {
                        unmatched_left_indices_.push_back(left_row_idx_);
                    }
                    left_row_idx_++;
                    continue;
                }

                size_t bucket_idx = compute_bucket_idx(key_val);
                auto& bucket = buckets_[bucket_idx];

                // Search for match in this bucket
                bool found_match = false;
                for (size_t i = 0; i < bucket.key_values.size(); ++i) {
                    if (out_batch.row_count() >= BATCH_SIZE) {
                        // Batch full - save state and return
                        resuming_bucket_scan_ = true;
                        resumed_bucket_idx_ = bucket_idx;
                        resumed_entry_idx_ = i;
                        resumed_key_val_ = key_val;
                        return true;  // Caller must consume batch before continuing
                    }

                    const auto& bucket_key = bucket.key_values[i][right_key_col_idx_];
                    if (bucket_key == key_val) {
                        // Match found - emit row
                        emit_joined_row(out_batch, left_row_idx_, bucket.payload_rows[i]);
                        found_match = true;
                        // Mark right row as matched for RIGHT/FULL join
                        if (join_type_ == JoinType::Right || join_type_ == JoinType::Full) {
                            if (i < bucket.right_row_indices.size()) {
                                right_matched_[bucket.right_row_indices[i]] = true;
                            }
                        }
                        if (join_type_ == JoinType::Left) {
                            left_matched_in_batch_[left_row_idx_] = true;
                        }
                        // Continue scanning bucket for all matching right rows
                    }
                }

                // Track unmatched for LEFT/FULL join
                if ((join_type_ == JoinType::Left || join_type_ == JoinType::Full) &&
                    !found_match) {
                    unmatched_left_indices_.push_back(left_row_idx_);
                }

                left_row_idx_++;
            }

            if (out_batch.row_count() > 0) {
                return true;  // Batch is full, return what we have
            }

            if (right_exhausted_ && left_row_idx_ >= left_batch_->row_count()) {
                return false;  // No more data
            }
        }
    }

    void emit_joined_row(VectorBatch& out_batch, size_t left_row_idx,
                         const std::vector<common::Value>& right_row) {
        // Append left columns
        for (size_t c = 0; c < left_col_count_; ++c) {
            out_batch.get_column(c).append(left_batch_->get_column(c).get(left_row_idx));
        }
        // Append right columns
        for (size_t c = 0; c < right_row.size(); ++c) {
            out_batch.get_column(left_col_count_ + c).append(right_row[c]);
        }
        out_batch.set_row_count(out_batch.row_count() + 1);
    }

    bool row_has_match(size_t left_row_idx) {
        const auto& key_val = left_batch_->get_column(left_key_col_idx_).get(left_row_idx);
        if (key_val.is_null()) return false;

        size_t bucket_idx = compute_bucket_idx(key_val);
        auto& bucket = buckets_[bucket_idx];

        for (size_t i = 0; i < bucket.key_values.size(); ++i) {
            const auto& bucket_key = bucket.key_values[i][right_key_col_idx_];
            if (bucket_key == key_val) {
                return true;
            }
        }
        return false;
    }

    bool emit_unmatched_left_rows(VectorBatch& out_batch) {
        constexpr size_t BATCH_SIZE = 1024;

        for (size_t idx : unmatched_left_indices_) {
            if (out_batch.row_count() >= BATCH_SIZE) {
                return true;  // Batch is full
            }
            // Append left columns
            for (size_t c = 0; c < left_col_count_; ++c) {
                out_batch.get_column(c).append(left_batch_->get_column(c).get(idx));
            }
            // Append NULLs for right columns
            for (size_t c = 0; c < right_col_count_; ++c) {
                out_batch.get_column(left_col_count_ + c).append(common::Value::make_null());
            }
            out_batch.set_row_count(out_batch.row_count() + 1);
        }
        unmatched_left_indices_.clear();
        return false;
    }

    bool emit_unmatched_right_rows(VectorBatch& out_batch) {
        constexpr size_t BATCH_SIZE = 1024;

        for (size_t row_idx : unmatched_right_rows_) {
            if (out_batch.row_count() >= BATCH_SIZE) {
                return true;  // Batch is full
            }
            // Append NULLs for left columns
            for (size_t c = 0; c < left_col_count_; ++c) {
                out_batch.get_column(c).append(common::Value::make_null());
            }
            // Append right columns from bucket payload
            const auto& right_row = right_bucket_rows_[row_idx];
            for (size_t c = 0; c < right_col_count_; ++c) {
                out_batch.get_column(left_col_count_ + c).append(right_row[c]);
            }
            out_batch.set_row_count(out_batch.row_count() + 1);
        }
        return false;
    }

    // Storage for unmatched right rows (index into bucket payload)
    std::vector<std::vector<common::Value>> right_bucket_rows_;
};

}  // namespace cloudsql::executor

#endif  // CLOUDSQL_EXECUTOR_VECTORIZED_OPERATOR_HPP
