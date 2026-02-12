#include "executor/operator.hpp"

namespace cloudsql {
namespace executor {

// Operator Base
// (Virtual methods default implementation provided in header or overridden)

// SeqScanOperator
bool SeqScanOperator::init() {
    // Initialize schema from table metadata would go here
    return true;
}

bool SeqScanOperator::open() {
    state_ = ExecState::Open;
    current_index_ = 0;
    return true;
}

bool SeqScanOperator::next(Tuple& out_tuple) {
    if (current_index_ >= tuples_.size()) {
        state_ = ExecState::Done;
        return false;
    }
    out_tuple = tuples_[current_index_++];
    return true;
}

void SeqScanOperator::close() {
    state_ = ExecState::Done;
}

Schema& SeqScanOperator::output_schema() { return schema_; }

// FilterOperator
bool FilterOperator::init() {
    return child_->init();
}

bool FilterOperator::open() {
    if (!child_->open()) return false;
    state_ = ExecState::Open;
    return true;
}

bool FilterOperator::next(Tuple& out_tuple) {
    Tuple tuple;
    while (child_->next(tuple)) {
        // Evaluate condition (simplified - would need row context)
        out_tuple = tuple;
        return true;
    }
    state_ = ExecState::Done;
    return false;
}

void FilterOperator::close() {
    child_->close();
    state_ = ExecState::Done;
}

Schema& FilterOperator::output_schema() { return schema_; }

void FilterOperator::add_child(std::unique_ptr<Operator> child) { child_ = std::move(child); }

// ProjectOperator
bool ProjectOperator::init() {
    return child_->init();
}

bool ProjectOperator::open() {
    if (!child_->open()) return false;
    state_ = ExecState::Open;
    return true;
}

bool ProjectOperator::next(Tuple& out_tuple) {
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

void ProjectOperator::close() {
    child_->close();
    state_ = ExecState::Done;
}

Schema& ProjectOperator::output_schema() { return schema_; }

void ProjectOperator::add_child(std::unique_ptr<Operator> child) { child_ = std::move(child); }

// HashJoinOperator
bool HashJoinOperator::init() {
    return left_->init() && right_->init();
}

bool HashJoinOperator::open() {
    // Build hash table from right side
    Tuple right_tuple;
    while (right_->next(right_tuple)) {
        // Build hash table entries
    }
    state_ = ExecState::Open;
    return true;
}

bool HashJoinOperator::next(Tuple& out_tuple) {
    if (current_index_ >= results_.size()) {
        state_ = ExecState::Done;
        return false;
    }
    // Concatenate left and right tuples
    out_tuple = results_[current_index_++];
    return true;
}

void HashJoinOperator::close() {
    left_->close();
    right_->close();
    state_ = ExecState::Done;
}

Schema& HashJoinOperator::output_schema() { return schema_; }

void HashJoinOperator::add_child(std::unique_ptr<Operator> child) {
    if (!left_) {
        left_ = std::move(child);
    } else {
        right_ = std::move(child);
    }
}

// LimitOperator
bool LimitOperator::init() {
    return child_->init();
}

bool LimitOperator::open() {
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

bool LimitOperator::next(Tuple& out_tuple) {
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

void LimitOperator::close() {
    child_->close();
    state_ = ExecState::Done;
}

Schema& LimitOperator::output_schema() { return child_->output_schema(); }

void LimitOperator::add_child(std::unique_ptr<Operator> child) { child_ = std::move(child); }

}  // namespace executor
}  // namespace cloudsql
