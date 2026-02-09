/**
 * @file operator.hpp
 * @brief Operator classes for Volcano-style execution model
 */

#ifndef CLOUDSQL_EXECUTOR_OPERATOR_HPP
#define CLOUDSQL_EXECUTOR_OPERATOR_HPP

#include <memory>
#include <string>
#include <vector>
#include "executor/types.hpp"
#include "parser/expression.hpp"

namespace cloudsql {
namespace executor {

/**
 * @brief Operator types
 */
enum class OperatorType {
    SeqScan,
    IndexScan,
    Filter,
    Project,
    NestedLoopJoin,
    HashJoin,
    Sort,
    Aggregate,
    HashAggregate,
    Limit,
    Materialize,
    Result
};

/**
 * @brief Execution state
 */
enum class ExecState {
    Init,
    Open,
    Executing,
    Done,
    Error
};

/**
 * @brief Base operator class (Volcano iterator model)
 */
class Operator {
protected:
    OperatorType type_;
    ExecState state_ = ExecState::Init;
    std::string error_message_;
    
public:
    Operator(OperatorType type) : type_(type) {}
    virtual ~Operator() = default;
    
    OperatorType type() const { return type_; }
    ExecState state() const { return state_; }
    const std::string& error() const { return error_message_; }
    
    virtual bool init() { return true; }
    virtual bool open() { return true; }
    virtual bool next(Tuple& out_tuple) { 
        state_ = ExecState::Done; 
        return false; 
    }
    virtual void close() {}
    
    virtual Schema& output_schema() = 0;
    const Schema& output_schema() const { return output_schema(); }
    
    virtual void add_child(std::unique_ptr<Operator> child) {}
    virtual const std::vector<std::unique_ptr<Operator>>& children() const {
        static std::vector<std::unique_ptr<Operator>> empty;
        return empty;
    }
    
    bool is_done() const { return state_ == ExecState::Done; }
    bool has_error() const { return state_ == ExecState::Error; }
};

/**
 * @brief Sequential scan operator
 */
class SeqScanOperator : public Operator {
private:
    std::string table_name_;
    Schema schema_;
    std::vector<Tuple> tuples_;
    size_t current_index_ = 0;
    
public:
    explicit SeqScanOperator(std::string table_name)
        : Operator(OperatorType::SeqScan), table_name_(std::move(table_name)) {}
    
    bool init() override {
        // Initialize schema from table metadata would go here
        return true;
    }
    
    bool open() override {
        state_ = ExecState::Open;
        current_index_ = 0;
        return true;
    }
    
    bool next(Tuple& out_tuple) override {
        if (current_index_ >= tuples_.size()) {
            state_ = ExecState::Done;
            return false;
        }
        out_tuple = tuples_[current_index_++];
        return true;
    }
    
    void close() override {
        state_ = ExecState::Done;
    }
    
    Schema& output_schema() override { return schema_; }
    
    void set_tuples(std::vector<Tuple> tuples) { tuples_ = std::move(tuples); }
    const std::string& table_name() const { return table_name_; }
};

/**
 * @brief Filter operator (WHERE clause)
 */
class FilterOperator : public Operator {
private:
    std::unique_ptr<Operator> child_;
    std::unique_ptr<parser::Expression> condition_;
    Schema schema_;
    Tuple current_tuple_;
    
public:
    FilterOperator(std::unique_ptr<Operator> child, std::unique_ptr<parser::Expression> condition)
        : Operator(OperatorType::Filter), child_(std::move(child)), condition_(std::move(condition)) {}
    
    bool init() override {
        return child_->init();
    }
    
    bool open() override {
        if (!child_->open()) return false;
        state_ = ExecState::Open;
        return true;
    }
    
    bool next(Tuple& out_tuple) override {
        Tuple tuple;
        while (child_->next(tuple)) {
            // Evaluate condition (simplified - would need row context)
            out_tuple = tuple;
            return true;
        }
        state_ = ExecState::Done;
        return false;
    }
    
    void close() override {
        child_->close();
        state_ = ExecState::Done;
    }
    
    Schema& output_schema() override { return schema_; }
    
    void add_child(std::unique_ptr<Operator> child) override { child_ = std::move(child); }
};

/**
 * @brief Project operator (SELECT columns)
 */
class ProjectOperator : public Operator {
private:
    std::unique_ptr<Operator> child_;
    std::vector<std::unique_ptr<parser::Expression>> columns_;
    Schema schema_;
    
public:
    ProjectOperator(std::unique_ptr<Operator> child, std::vector<std::unique_ptr<parser::Expression>> columns)
        : Operator(OperatorType::Project), child_(std::move(child)), columns_(std::move(columns)) {}
    
    bool init() override {
        return child_->init();
    }
    
    bool open() override {
        if (!child_->open()) return false;
        state_ = ExecState::Open;
        return true;
    }
    
    bool next(Tuple& out_tuple) override {
        Tuple input;
        if (!child_->next(input)) {
            state_ = ExecState::Done;
            return false;
        }
        
        std::vector<common::Value> output_values;
        for (const auto& col : columns_) {
            // Simplified projection - would evaluate expressions against row
            output_values.push_back(common::Value::make_null());
        }
        out_tuple = Tuple(std::move(output_values));
        return true;
    }
    
    void close() override {
        child_->close();
        state_ = ExecState::Done;
    }
    
    Schema& output_schema() override { return schema_; }
    
    void add_child(std::unique_ptr<Operator> child) override { child_ = std::move(child); }
};

/**
 * @brief Hash join operator
 */
class HashJoinOperator : public Operator {
private:
    std::unique_ptr<Operator> left_;
    std::unique_ptr<Operator> right_;
    std::unique_ptr<parser::Expression> left_key_;
    std::unique_ptr<parser::Expression> right_key_;
    Schema schema_;
    std::vector<Tuple> results_;
    size_t current_index_ = 0;
    
public:
    HashJoinOperator(std::unique_ptr<Operator> left, std::unique_ptr<Operator> right,
                     std::unique_ptr<parser::Expression> left_key, 
                     std::unique_ptr<parser::Expression> right_key)
        : Operator(OperatorType::HashJoin), left_(std::move(left)), right_(std::move(right)),
          left_key_(std::move(left_key)), right_key_(std::move(right_key)) {}
    
    bool init() override {
        return left_->init() && right_->init();
    }
    
    bool open() override {
        // Build hash table from right side
        Tuple right_tuple;
        while (right_->next(right_tuple)) {
            // Build hash table entries
        }
        state_ = ExecState::Open;
        return true;
    }
    
    bool next(Tuple& out_tuple) override {
        if (current_index_ >= results_.size()) {
            state_ = ExecState::Done;
            return false;
        }
        // Concatenate left and right tuples
        out_tuple = results_[current_index_++];
        return true;
    }
    
    void close() override {
        left_->close();
        right_->close();
        state_ = ExecState::Done;
    }
    
    Schema& output_schema() override { return schema_; }
    
    void add_child(std::unique_ptr<Operator> child) override {
        if (!left_) {
            left_ = std::move(child);
        } else {
            right_ = std::move(child);
        }
    }
};

/**
 * @brief Limit operator
 */
class LimitOperator : public Operator {
private:
    std::unique_ptr<Operator> child_;
    uint64_t limit_;
    uint64_t offset_;
    uint64_t current_count_ = 0;
    
public:
    LimitOperator(std::unique_ptr<Operator> child, uint64_t limit, uint64_t offset = 0)
        : Operator(OperatorType::Limit), child_(std::move(child)), limit_(limit), offset_(offset) {}
    
    bool init() override {
        return child_->init();
    }
    
    bool open() override {
        if (!child_->open()) return false;
        
        // Skip offset rows
        Tuple tuple;
        while (current_count_ < offset_ && child_->next(tuple)) {
            current_count_++;
        }
        current_count_ = 0;
        state_ = ExecState::Open;
        return true;
    }
    
    bool next(Tuple& out_tuple) override {
        if (current_count_ >= limit_) {
            state_ = ExecState::Done;
            return false;
        }
        
        if (!child_->next(out_tuple)) {
            state_ = ExecState::Done;
            return false;
        }
        
        current_count_++;
        return true;
    }
    
    void close() override {
        child_->close();
        state_ = ExecState::Done;
    }
    
    Schema& output_schema() override { return child_->output_schema(); }
    
    void add_child(std::unique_ptr<Operator> child) override { child_ = std::move(child); }
};

}  // namespace executor
}  // namespace cloudsql

#endif  // CLOUDSQL_EXECUTOR_OPERATOR_HPP
