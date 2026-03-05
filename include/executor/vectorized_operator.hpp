/**
 * @file vectorized_operator.hpp
 * @brief Base class for vectorized query operators
 */

#ifndef CLOUDSQL_EXECUTOR_VECTORIZED_OPERATOR_HPP
#define CLOUDSQL_EXECUTOR_VECTORIZED_OPERATOR_HPP

#include <memory>
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

   public:
    VectorizedFilterOperator(std::unique_ptr<VectorizedOperator> child,
                             std::unique_ptr<parser::Expression> condition)
        : VectorizedOperator(child->output_schema()),
          child_(std::move(child)),
          condition_(std::move(condition)) {
        input_batch_ = VectorBatch::create(child_->output_schema());
    }

    bool next_batch(VectorBatch& out_batch) override {
        out_batch.clear();
        while (child_->next_batch(*input_batch_)) {
            for (size_t r = 0; r < input_batch_->row_count(); ++r) {
                // To evaluate row by row, we create a temporary Tuple for the expression
                std::vector<common::Value> row_vals;
                for (size_t c = 0; c < input_batch_->column_count(); ++c) {
                    row_vals.push_back(input_batch_->get_column(c).get(r));
                }
                Tuple t(std::move(row_vals));
                if (condition_->evaluate(&t, &child_->output_schema()).as_bool()) {
                    out_batch.append_tuple(t);
                }
            }
            if (out_batch.row_count() > 0) {
                return true;
            }
            input_batch_->clear();
        }
        return false;
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
            for (size_t r = 0; r < input_batch_->row_count(); ++r) {
                std::vector<common::Value> row_vals;
                for (size_t c = 0; c < input_batch_->column_count(); ++c) {
                    row_vals.push_back(input_batch_->get_column(c).get(r));
                }
                Tuple t(std::move(row_vals));

                std::vector<common::Value> projected_vals;
                for (const auto& expr : expressions_) {
                    projected_vals.push_back(expr->evaluate(&t, &child_->output_schema()));
                }
                out_batch.append_tuple(Tuple(std::move(projected_vals)));
            }
            return true;
        }
        return false;
    }
};

/**
 * @brief Vectorized global aggregate operator (no GROUP BY)
 */
class VectorizedAggregateOperator : public VectorizedOperator {
   private:
    std::unique_ptr<VectorizedOperator> child_;
    std::vector<AggregateType> agg_types_;
    std::vector<int64_t> results_int_;
    std::unique_ptr<VectorBatch> input_batch_;
    bool done_ = false;

   public:
    VectorizedAggregateOperator(std::unique_ptr<VectorizedOperator> child, Schema out_schema,
                                std::vector<AggregateType> types)
        : VectorizedOperator(std::move(out_schema)),
          child_(std::move(child)),
          agg_types_(std::move(types)) {
        results_int_.assign(agg_types_.size(), 0);
        input_batch_ = VectorBatch::create(child_->output_schema());
    }

    bool next_batch(VectorBatch& out_batch) override {
        if (done_) return false;

        // Process all input batches
        while (child_->next_batch(*input_batch_)) {
            for (size_t i = 0; i < agg_types_.size(); ++i) {
                if (agg_types_[i] == AggregateType::Count) {
                    results_int_[i] += input_batch_->row_count();
                } else if (agg_types_[i] == AggregateType::Sum) {
                    auto& col = input_batch_->get_column(i);
                    auto& num_col = dynamic_cast<NumericVector<int64_t>&>(col);
                    const int64_t* raw = num_col.raw_data();
                    for (size_t r = 0; r < input_batch_->row_count(); ++r) {
                        if (!num_col.is_null(r)) {
                            results_int_[i] += raw[r];
                        }
                    }
                }
            }
            input_batch_->clear();
        }

        // Produce final result batch
        out_batch.clear();
        std::vector<common::Value> row;
        for (int64_t val : results_int_) {
            row.push_back(common::Value::make_int64(val));
        }
        out_batch.append_tuple(Tuple(std::move(row)));
        done_ = true;
        return true;
    }
};

}  // namespace cloudsql::executor

#endif  // CLOUDSQL_EXECUTOR_VECTORIZED_OPERATOR_HPP
