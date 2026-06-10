/**
 * @file vectorized_operator.hpp
 * @brief Base class for vectorized query operators
 */

#ifndef CLOUDSQL_EXECUTOR_VECTORIZED_OPERATOR_HPP
#define CLOUDSQL_EXECUTOR_VECTORIZED_OPERATOR_HPP

#include <algorithm>
#include <cstdint>
#include <memory>
#include <numeric>
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
            }

            if (out_batch.row_count() > 0) {
                return true;
            }
        }

        return out_batch.row_count() > 0;
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
        double sums_float64[MAX_AGGREGATES] = {0.0};
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

    // Fast path for int64 keys: hash of [0x02][8-byte-int64] without buffer construction
    static uint64_t hash_int64(int64_t key) {
        uint64_t h = 14695981039346656037ull;
        h ^= 0x02;  // type tag for INT64
        h *= 1099511628211ull;
        // XOR each byte of key in big-endian order
        uint64_t v = static_cast<uint64_t>(key);
        for (int i = 7; i >= 0; --i) {
            h ^= (v >> (i * 8)) & 0xFF;
            h *= 1099511628211ull;
        }
        return h;
    }

    void init(size_t capacity_hint, size_t max_aggregates) {
        max_aggregates_ = max_aggregates;
        num_occupied_ = 0;
        valid_indices_.clear();

        // Pre-allocate to avoid grow(): capacity = next power of 2 above (capacity_hint / kLoadFactor)
        // This ensures we never grow for capacity_hint rows at 0.5 load factor
        size_t min_cap = static_cast<size_t>(capacity_hint / kLoadFactor);
        size_t cap = kInitialCapacity;
        while (cap < min_cap) cap *= 2;
        buckets_.assign(cap, HashBucket());
        mask_ = cap - 1;
    }

    HashBucket& find_or_insert(const uint8_t* key, size_t key_len, uint64_t hash) {
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
            if (bucket.key_hash == hash && bucket.key_type == 0x02 && bucket.key_int64 == key) {
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

    const std::vector<size_t>& valid_slots() const { return valid_indices_; }
    HashBucket& slot(size_t idx) { return buckets_[idx]; }
    const HashBucket& slot(size_t idx) const { return buckets_[idx]; }

    /**
     * @brief Merge all entries from another hash table into this one.
     * @param other Source hash table to merge from
     *
     * For existing keys: merges accumulators (sums, counts, mins, maxes)
     * For new keys: copies entire bucket state
     */
    void merge_from(const OpenAddressHashAgg& other) {
        for (size_t src_idx : other.valid_slots()) {
            const auto& src = other.slot(src_idx);

            // Find or create the destination bucket
            // key_type: 0x01=NULL, 0x02=INT64, 0x03=FLOAT64, 0x04=STRING
            // Only 0x02 has direct int64 storage (key_int64); others use key_data
            auto& dst = (src.key_type == 0x02)
                ? find_or_insert_int64(src.key_int64, src.key_hash)
                : find_or_insert(src.key_data, src.key_len, src.key_hash);

            if (!dst.is_new) {
                // Key exists - merge accumulators
                for (size_t i = 0; i < max_aggregates_; ++i) {
                    dst.counts[i] += src.counts[i];
                    dst.sums_int64[i] += src.sums_int64[i];
                    dst.sums_float64[i] += src.sums_float64[i];
                    dst.has_float_value[i] = dst.has_float_value[i] || src.has_float_value[i];
                    if (src.has_mins[i]) {
                        if (!dst.has_mins[i]) {
                            dst.mins[i] = src.mins[i];
                            dst.maxes[i] = src.maxes[i];
                            dst.has_mins[i] = true;
                        } else {
                            dst.mins[i] = std::min(dst.mins[i], src.mins[i]);
                            dst.maxes[i] = std::max(dst.maxes[i], src.maxes[i]);
                        }
                    }
                }
            } else {
                // New key - find_or_insert already populated key fields (key_hash, key_type, key_len, key_data)
                // Just copy accumulators since find_or_insert initialized them to zero
                for (size_t i = 0; i < max_aggregates_; ++i) {
                    dst.counts[i] = src.counts[i];
                    dst.sums_int64[i] = src.sums_int64[i];
                    dst.sums_float64[i] = src.sums_float64[i];
                    dst.has_float_value[i] = src.has_float_value[i];
                    dst.mins[i] = src.mins[i];
                    dst.maxes[i] = src.maxes[i];
                    dst.has_mins[i] = src.has_mins[i];
                }
                // is_new remains true so output phase outputs this group
            }
        }
    }
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
        int64_t counts[MAX_AGGREGATES] = {0};
        int64_t sums_int64[MAX_AGGREGATES] = {0};
        double sums_float64[MAX_AGGREGATES] = {0.0};
        bool has_float_value[MAX_AGGREGATES] = {false};
        int64_t mins[MAX_AGGREGATES] = {0};
        int64_t maxes[MAX_AGGREGATES] = {0};
        bool has_mins[MAX_AGGREGATES] = {false};
    };

    size_t num_aggs_ = 0;
    int64_t min_key_ = 0;
    int64_t max_key_ = 0;
    std::vector<GroupSlot> slots_;
    std::vector<size_t> valid_indices_;
    bool initialized_ = false;

   public:
    void init(size_t capacity_hint, size_t num_aggregates, size_t num_group_keys) {
        num_aggs_ = num_aggregates;
        // For int8/tinyint: use 256 fixed slots (covers full range of int8)
        size_t capacity = 256;
        slots_.assign(capacity, GroupSlot());
        valid_indices_.clear();
        initialized_ = true;
    }

    GroupSlot& get_slot(int64_t key) {
        size_t idx = static_cast<size_t>(key - min_key_);
        return slots_[idx];
    }

    void track_key(int64_t key) {
        if (!initialized_) return;
        // int8 range: -128 to 127
        size_t idx = static_cast<size_t>(static_cast<int8_t>(key));
        if (!slots_[idx].valid) {
            slots_[idx].valid = true;
            valid_indices_.push_back(idx);
        }
    }

    const std::vector<size_t>& valid_slots() const { return valid_indices_; }
    GroupSlot& slot(size_t idx) { return slots_[idx]; }
};

/**
 * @brief Vectorized GROUP BY aggregation operator
 *
 * Supports both hash-based aggregation (OpenAddressHashAgg) for arbitrary keys
 * and direct-indexed aggregation (DirectIndexAgg) for low-cardinality integer keys.
 */
class VectorizedGroupByOperator : public VectorizedOperator {
   private:
    std::unique_ptr<VectorizedOperator> child_;
    std::vector<std::unique_ptr<parser::Expression>> group_by_;
    std::vector<VectorizedAggregateInfo> aggregates_;
    std::unique_ptr<VectorBatch> input_batch_;
    std::vector<size_t> group_by_col_indices_;
    bool is_direct_indexable_ = false;
    DirectIndexAgg agg_;
    OpenAddressHashAgg hash_agg_;
    std::vector<std::vector<common::Value>> hash_group_keys_;
    std::vector<size_t> sorted_indices_;  // Indices sorted by group key for lexicographic output
    // Note: sorted_indices_ is populated after input phase to ensure correct GROUP BY ordering

    // Batch encoding scratch space (Phase 1 optimization)
    static constexpr size_t MAX_BATCH_SIZE = 4096;
    static constexpr size_t MAX_KEY_LEN = 256;
    std::vector<uint8_t> batch_key_buffer_;   // Heap-allocated scratch: MAX_BATCH_SIZE * MAX_KEY_LEN bytes
    std::vector<uint64_t> batch_hashes_;      // batch_size
    std::vector<int64_t> batch_int64_keys_;   // batch_size (for int64-only path)
    std::vector<size_t> batch_key_lens_;      // batch_size
    bool all_int64_keys_ = false;             // True when all GROUP BY cols are INT64

    // Parallel aggregation support (Phase 4)
    std::shared_ptr<ThreadPool> thread_pool_;
    size_t num_threads_ = 1;
    std::vector<OpenAddressHashAgg> thread_hash_aggs_;  // One per thread
    std::vector<std::vector<std::vector<common::Value>>> thread_group_keys_;  // Group keys per thread

   public:
    VectorizedGroupByOperator(std::unique_ptr<VectorizedOperator> child,
                              std::vector<std::unique_ptr<parser::Expression>> group_by,
                              std::vector<VectorizedAggregateInfo> aggregates, Schema output_schema,
                              std::shared_ptr<ThreadPool> thread_pool = nullptr)
        : VectorizedOperator(std::move(output_schema)),
          child_(std::move(child)),
          group_by_(std::move(group_by)),
          aggregates_(std::move(aggregates)),
          thread_pool_(thread_pool) {
        input_batch_ = VectorBatch::create(child_->output_schema());

        // Pre-resolve column indices once in constructor
        for (auto& expr : group_by_) {
            auto* col_expr = dynamic_cast<ColumnExpr*>(expr.get());
            if (col_expr) {
                size_t col_idx = child_->output_schema().get_column_index(col_expr->column_name());
                group_by_col_indices_.push_back(col_idx);
            } else {
                group_by_col_indices_.push_back(static_cast<size_t>(-1));
            }
        }

        // Check if we can use direct indexing (single INT64 column)
        bool is_int_key = (group_by_.size() == 1);
        if (is_int_key) {
            auto& col = child_->output_schema().get_column(group_by_col_indices_[0]);
            auto col_type = col.type();
            is_int_key = (col_type == common::ValueType::TYPE_INT64 ||
                          col_type == common::ValueType::TYPE_INT32 ||
                          col_type == common::ValueType::TYPE_INT16 ||
                          col_type == common::ValueType::TYPE_INT8);
        }
        is_direct_indexable_ = (group_by_.size() == 1 && is_int_key);
        all_int64_keys_ = is_direct_indexable_;  // Can use fast int64 path
        if (is_direct_indexable_) {
            agg_.init(65536, aggregates_.size(), group_by_.size());
        } else {
            hash_agg_.init(65536, aggregates_.size());
        }

        // Initialize parallel aggregation (Phase 4)
        if (thread_pool_ && thread_pool_->num_threads() > 1) {
            num_threads_ = thread_pool_->num_threads();
            thread_hash_aggs_.resize(num_threads_);
            thread_group_keys_.resize(num_threads_);
            for (size_t t = 0; t < num_threads_; ++t) {
                thread_hash_aggs_[t].init(std::max(size_t(8192), 65536 / num_threads_), aggregates_.size());
            }
        }

        // Initialize batch encoding scratch space
        batch_key_buffer_.resize(MAX_BATCH_SIZE * MAX_KEY_LEN);
        batch_hashes_.resize(MAX_BATCH_SIZE);
        batch_int64_keys_.resize(MAX_BATCH_SIZE);
        batch_key_lens_.resize(MAX_BATCH_SIZE);

        // Create schema for group key evaluation
        Schema key_schema;
        for (size_t i = 0; i < group_by_.size(); ++i) {
            key_schema.add_column("key_" + std::to_string(i),
                                  child_->output_schema().get_column(group_by_col_indices_[i]).type(),
                                  false);
        }
    }

    bool next_batch(VectorBatch& out_batch) override {
        if (state_ == ExecState::Error) {
            return false;
        }

        if (state_ == ExecState::Init) {
            state_ = ExecState::Running;
        }

        out_batch.clear();

        // Ensure output batch is structured for current schema
        if (out_batch.column_count() == 0) {
            out_batch.init_from_schema(output_schema_);
        }

        // Process input batches until we produce output or exhaust input
        while (child_->next_batch(*input_batch_)) {
            if (is_direct_indexable_) {
                process_input_batch_direct_index(*input_batch_);
            } else {
                process_input_batch_open_addressing(*input_batch_);
            }

            // Try to produce output after each input batch
            if (is_direct_indexable_) {
                if (produce_output_batch_direct_index(out_batch)) {
                    return true;
                }
            } else {
                if (produce_output_batch_open_addressing(out_batch)) {
                    return true;
                }
            }
        }

        // After exhausting input, try one more output in case there's pending data
        if (is_direct_indexable_) {
            return produce_output_batch_direct_index(out_batch);
        } else {
            return produce_output_batch_open_addressing(out_batch);
        }
    }

    void process_input_batch_direct_index(VectorBatch& batch) {
        const auto& col = batch.get_column(group_by_col_indices_[0]);
        for (size_t r = 0; r < batch.row_count(); ++r) {
            int64_t key = col.get(r).to_int64();
            agg_.track_key(key);
            auto& slot = agg_.get_slot(key);

            for (size_t i = 0; i < aggregates_.size(); ++i) {
                const auto& agg = aggregates_[i];
                if (agg.type == AggregateType::Count && agg.input_col_idx < 0) {
                    slot.counts[i]++;
                } else if ((agg.type == AggregateType::Sum || agg.type == AggregateType::Avg) &&
                           agg.input_col_idx >= 0) {
                    const auto& agg_col = batch.get_column(agg.input_col_idx);
                    if (!agg_col.is_null(r)) {
                        slot.counts[i]++;
                        if (agg_col.type() == common::ValueType::TYPE_INT64) {
                            slot.sums_int64[i] += agg_col.get(r).to_int64();
                        } else if (agg_col.type() == common::ValueType::TYPE_FLOAT64) {
                            slot.sums_float64[i] += agg_col.get(r).to_float64();
                            slot.has_float_value[i] = true;
                        }
                    }
                } else if ((agg.type == AggregateType::Min || agg.type == AggregateType::Max) &&
                           agg.input_col_idx >= 0) {
                    const auto& agg_col = batch.get_column(agg.input_col_idx);
                    if (!agg_col.is_null(r)) {
                        auto val = agg_col.get(r).to_int64();
                        if (!slot.has_mins[i]) {
                            slot.mins[i] = val;
                            slot.maxes[i] = val;
                            slot.has_mins[i] = true;
                        } else {
                            slot.mins[i] = std::min(slot.mins[i], val);
                            slot.maxes[i] = std::max(slot.maxes[i], val);
                        }
                    }
                }
            }
        }
        input_batch_->clear();
    }

    void process_input_batch_open_addressing(VectorBatch& batch) {
        // Phase 1: Batch key encoding & hash precomputation
        size_t n = batch.row_count();

        if (all_int64_keys_) {
            // Fast path: extract int64 keys directly
            const auto& col = batch.get_column(group_by_col_indices_[0]);
            for (size_t r = 0; r < n; ++r) {
                if (col.is_null(r)) {
                    batch_int64_keys_[r] = 0;  // NULL represented as 0
                } else {
                    batch_int64_keys_[r] = col.get(r).to_int64();
                }
            }
            // Batch compute hashes
            for (size_t i = 0; i < n; ++i) {
                batch_hashes_[i] = OpenAddressHashAgg::hash_int64(batch_int64_keys_[i]);
            }
        } else {
            // General path: encode all keys into batch_key_buffer_
            for (size_t r = 0; r < n; ++r) {
                size_t key_offset = r * MAX_KEY_LEN;
                size_t key_len = 0;
                uint8_t* key_ptr = &batch_key_buffer_[key_offset];

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
                        if (key_offset + key_len + 4 + val_str.size() > MAX_BATCH_SIZE * MAX_KEY_LEN) {
                            // Key too large, skip - warn once per operator instance
                            static bool warned = false;
                            if (!warned) {
                                fprintf(stderr, "Warning: String key exceeded MAX_KEY_LEN, treating as NULL\n");
                                warned = true;
                            }
                            key_ptr[key_len++] = 0x01;  // Fallback to NULL
                        } else {
                            std::memcpy(&key_ptr[key_len], &len, 4);
                            key_len += 4;
                            std::memcpy(&key_ptr[key_len], val_str.data(), val_str.size());
                            key_len += val_str.size();
                        }
                    }
                }
                batch_key_lens_[r] = key_len;
                // Compute hash for this key
                batch_hashes_[r] = OpenAddressHashAgg::hash_bytes(key_ptr, key_len);
            }
        }

        // Phase 2: Row-by-row hash table lookup and accumulator updates
        // (Hash computation done in batch above; lookup is fast)
        if (num_threads_ > 1) {
            // Parallel path: partition rows by thread and process in parallel
            std::vector<std::vector<size_t>> thread_row_indices(num_threads_);
            for (size_t r = 0; r < n; ++r) {
                size_t thread_idx = batch_hashes_[r] % num_threads_;
                thread_row_indices[thread_idx].push_back(r);
            }

            // Submit parallel tasks for each thread
            for (size_t t = 0; t < num_threads_; ++t) {
                auto& indices = thread_row_indices[t];
                if (indices.empty()) continue;

                thread_pool_->submit([this, &batch, &indices, t]() {
                    this->process_thread_batch(batch, indices, t);
                });
            }
            thread_pool_->wait();

            // Merge thread results into main hash_agg_
            for (size_t t = 0; t < num_threads_; ++t) {
                hash_agg_.merge_from(thread_hash_aggs_[t]);
                // Also merge group keys
                hash_group_keys_.insert(hash_group_keys_.end(),
                                        thread_group_keys_[t].begin(),
                                        thread_group_keys_[t].end());
            }
        } else {
            // Sequential path (original code)
            for (size_t r = 0; r < n; ++r) {
                auto& bucket =
                    all_int64_keys_
                        ? hash_agg_.find_or_insert_int64(batch_int64_keys_[r], batch_hashes_[r])
                        : hash_agg_.find_or_insert(&batch_key_buffer_[r * MAX_KEY_LEN],
                                                   batch_key_lens_[r], batch_hashes_[r]);

                // Store key for output if first time
                if (bucket.is_new) {
                    std::vector<common::Value> key_vals;
                    for (size_t i = 0; i < group_by_col_indices_.size(); ++i) {
                        key_vals.push_back(batch.get_column(group_by_col_indices_[i]).get(r));
                    }
                    hash_group_keys_.push_back(std::move(key_vals));
                }

                // Update accumulators directly in bucket
                update_bucket_accumulators(bucket, batch, r);
            }
        }
        input_batch_->clear();
    }

    void process_thread_batch(VectorBatch& batch, const std::vector<size_t>& row_indices, size_t thread_idx) {
        auto& hash_agg = thread_hash_aggs_[thread_idx];
        auto& group_keys = thread_group_keys_[thread_idx];

        for (size_t local_idx = 0; local_idx < row_indices.size(); ++local_idx) {
            size_t r = row_indices[local_idx];

            auto& bucket =
                all_int64_keys_
                    ? hash_agg.find_or_insert_int64(batch_int64_keys_[r], batch_hashes_[r])
                    : hash_agg.find_or_insert(&batch_key_buffer_[r * MAX_KEY_LEN],
                                               batch_key_lens_[r], batch_hashes_[r]);

            // Track new groups in this thread's hash table
            if (bucket.is_new) {
                std::vector<common::Value> key_vals;
                for (size_t i = 0; i < group_by_col_indices_.size(); ++i) {
                    key_vals.push_back(batch.get_column(group_by_col_indices_[i]).get(r));
                }
                group_keys.push_back(std::move(key_vals));
            }

            // Update accumulators directly in bucket
            update_bucket_accumulators(bucket, batch, r);
        }
    }

    // Shared helper to update accumulators in a hash bucket from a batch row
    // Note: batch.get_column() is not const-correct in current VectorBatch API.
    // This helper only reads from batch; write access is not needed.
    template<typename Bucket>
    void update_bucket_accumulators(Bucket& bucket, VectorBatch& batch, size_t row_idx) {
        for (size_t i = 0; i < aggregates_.size(); ++i) {
            const auto& agg = aggregates_[i];
            if (agg.type == AggregateType::Count && agg.input_col_idx < 0) {
                bucket.counts[i]++;
            } else if ((agg.type == AggregateType::Sum || agg.type == AggregateType::Avg) &&
                       agg.input_col_idx >= 0) {
                const auto& col = batch.get_column(agg.input_col_idx);
                if (!col.is_null(row_idx)) {
                    bucket.counts[i]++;
                    if (col.type() == common::ValueType::TYPE_INT64) {
                        auto& num_col = dynamic_cast<const NumericVector<int64_t>&>(col);
                        bucket.sums_int64[i] += num_col.raw_data()[row_idx];
                    } else if (col.type() == common::ValueType::TYPE_FLOAT64) {
                        auto& num_col = dynamic_cast<const NumericVector<double>&>(col);
                        bucket.sums_float64[i] += num_col.raw_data()[row_idx];
                        bucket.has_float_value[i] = true;
                    }
                }
            } else if ((agg.type == AggregateType::Min || agg.type == AggregateType::Max) &&
                       agg.input_col_idx >= 0) {
                const auto& col = batch.get_column(agg.input_col_idx);
                if (!col.is_null(row_idx)) {
                    auto val = col.get(row_idx).to_int64();
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
                        state.sums_int64[i] += col.get(row_idx).to_int64();
                    } else if (col.type() == common::ValueType::TYPE_FLOAT64) {
                        state.sums_float64[i] += col.get(row_idx).to_float64();
                        state.has_float_value[i] = true;
                    }
                }
            } else if (agg.type == AggregateType::Min && agg.input_col_idx >= 0) {
                auto& col = batch.get_column(agg.input_col_idx);
                if (!col.is_null(row_idx)) {
                    auto val = col.get(row_idx).to_int64();
                    if (!state.has_min[i]) {
                        state.mins[i] = val;
                        state.has_min[i] = true;
                    } else {
                        state.mins[i] = std::min(state.mins[i], val);
                    }
                }
            } else if (agg.type == AggregateType::Max && agg.input_col_idx >= 0) {
                auto& col = batch.get_column(agg.input_col_idx);
                if (!col.is_null(row_idx)) {
                    auto val = col.get(row_idx).to_int64();
                    if (!state.has_max[i]) {
                        state.maxes[i] = val;
                        state.has_max[i] = true;
                    } else {
                        state.maxes[i] = std::max(state.maxes[i], val);
                    }
                }
            }
        }
    }

    bool produce_output_batch_direct_index(VectorBatch& out_batch) {
        // Find first valid group slot with output pending
        for (size_t idx : agg_.valid_slots()) {
            auto& slot = agg_.slot(idx);
            if (slot.counts[0] > 0 || (slot.valid && aggregates_[0].type == AggregateType::Count &&
                                         aggregates_[0].input_col_idx < 0)) {
                // Found a group with data
                // int8 range: -128 to 127
                int64_t key = static_cast<int64_t>(static_cast<int8_t>(idx));
                out_batch.get_column(0).append(common::Value::make_int64(key));

                for (size_t i = 0; i < aggregates_.size(); ++i) {
                    if (aggregates_[i].type == AggregateType::Count) {
                        out_batch.get_column(1 + i).append(common::Value::make_int64(slot.counts[i]));
                    } else if (aggregates_[i].type == AggregateType::Sum ||
                               aggregates_[i].type == AggregateType::Avg) {
                        if (slot.has_float_value[i]) {
                            out_batch.get_column(1 + i).append(common::Value::make_float64(slot.sums_float64[i]));
                        } else {
                            out_batch.get_column(1 + i).append(common::Value::make_int64(slot.sums_int64[i]));
                        }
                    } else if (aggregates_[i].type == AggregateType::Min) {
                        out_batch.get_column(1 + i).append(common::Value::make_int64(slot.mins[i]));
                    } else if (aggregates_[i].type == AggregateType::Max) {
                        out_batch.get_column(1 + i).append(common::Value::make_int64(slot.maxes[i]));
                    }
                }
                slot.counts[0] = 0;  // Mark as output
                return true;
            }
        }
        return false;
    }

    bool produce_output_batch_open_addressing(VectorBatch& out_batch) {
        // Find first valid hash bucket with output pending
        for (size_t idx : hash_agg_.valid_slots()) {
            auto& bucket = hash_agg_.slot(idx);

            // Check if this bucket has been output (is_new == false means it existed before)
            // We track output by setting counts to a negative marker
            if (bucket.counts[0] >= 0) {  // Not yet output
                // Output the group key
                const auto& key_vals = hash_group_keys_[idx];
                for (size_t c = 0; c < key_vals.size(); ++c) {
                    out_batch.get_column(c).append(key_vals[c]);
                }

                // Output aggregate values
                for (size_t i = 0; i < aggregates_.size(); ++i) {
                    if (aggregates_[i].type == AggregateType::Count) {
                        out_batch.get_column(group_by_.size() + i)
                            .append(common::Value::make_int64(bucket.counts[i]));
                    } else if (aggregates_[i].type == AggregateType::Sum ||
                               aggregates_[i].type == AggregateType::Avg) {
                        if (bucket.has_float_value[i]) {
                            out_batch.get_column(group_by_.size() + i)
                                .append(common::Value::make_float64(bucket.sums_float64[i]));
                        } else {
                            out_batch.get_column(group_by_.size() + i)
                                .append(common::Value::make_int64(bucket.sums_int64[i]));
                        }
                    } else if (aggregates_[i].type == AggregateType::Min) {
                        out_batch.get_column(group_by_.size() + i)
                            .append(common::Value::make_int64(bucket.mins[i]));
                    } else if (aggregates_[i].type == AggregateType::Max) {
                        out_batch.get_column(group_by_.size() + i)
                            .append(common::Value::make_int64(bucket.maxes[i]));
                    }
                }

                // Mark as output by negating counts
                for (size_t i = 0; i < aggregates_.size(); ++i) {
                    bucket.counts[i] = -bucket.counts[i];
                }
                return true;
            }
        }
        return false;
    }
};

}  // namespace cloudsql::executor

#endif  // CLOUDSQL_EXECUTOR_VECTORIZED_OPERATOR_HPP