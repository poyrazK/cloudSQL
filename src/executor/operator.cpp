/**
 * @file operator.cpp
 * @brief Query Executor Operators implementation
 *
 * @defgroup executor Query Executor
 * @{
 */

#include "executor/operator.hpp"

#include <algorithm>
#include <iostream>
#include <limits>
#include <map>
#include <set>

namespace cloudsql {
namespace executor {

/* --- SeqScanOperator --- */

SeqScanOperator::SeqScanOperator(std::unique_ptr<storage::HeapTable> table, Transaction* txn,
                                 LockManager* lock_manager)
    : Operator(OperatorType::SeqScan, txn, lock_manager),
      table_name_(table->table_name()),
      table_(std::move(table)) {
    /* Qualify columns in scan schema */
    auto base_schema = table_->schema();
    for (const auto& col : base_schema.columns()) {
        schema_.add_column(table_name_ + "." + col.name(), col.type(), col.nullable());
    }
}

bool SeqScanOperator::init() {
    state_ = ExecState::Init;
    return true;
}

bool SeqScanOperator::open() {
    state_ = ExecState::Open;
    iterator_ = std::make_unique<storage::HeapTable::Iterator>(table_->scan());
    return true;
}

bool SeqScanOperator::next(Tuple& out_tuple) {
    if (!iterator_ || iterator_->is_done()) {
        state_ = ExecState::Done;
        return false;
    }

    storage::HeapTable::TupleMeta meta;
    while (iterator_->next_meta(meta)) {
        /* MVCC Visibility Check */
        bool visible = true;
        if (txn_) {
            const auto& snapshot = txn_->get_snapshot();
            uint64_t my_id = txn_->get_id();

            // 1. Check xmin (creation)
            bool xmin_visible =
                (meta.xmin == my_id) || (meta.xmin == 0) || snapshot.is_visible(meta.xmin);

            // 2. Check xmax (deletion)
            bool xmax_visible =
                (meta.xmax == 0) || (meta.xmax != my_id && !snapshot.is_visible(meta.xmax));

            visible = xmin_visible && xmax_visible;
        } else {
            /* No transaction context: only show active tuples */
            visible = (meta.xmax == 0);
        }

        if (visible) {
            out_tuple = std::move(meta.tuple);
            return true;
        }
    }

    state_ = ExecState::Done;
    return false;
}

void SeqScanOperator::close() {
    iterator_.reset();
    state_ = ExecState::Done;
}

Schema& SeqScanOperator::output_schema() {
    return schema_;
}

/* --- IndexScanOperator --- */

IndexScanOperator::IndexScanOperator(std::unique_ptr<storage::HeapTable> table,
                                     std::unique_ptr<storage::BTreeIndex> index,
                                     common::Value search_key, Transaction* txn,
                                     LockManager* lock_manager)
    : Operator(OperatorType::IndexScan, txn, lock_manager),
      table_name_(table->table_name()),
      index_name_(index->index_name()),
      table_(std::move(table)),
      index_(std::move(index)),
      search_key_(std::move(search_key)) {
    /* Qualify columns in scan schema */
    auto base_schema = table_->schema();
    for (const auto& col : base_schema.columns()) {
        schema_.add_column(table_name_ + "." + col.name(), col.type(), col.nullable());
    }
}

bool IndexScanOperator::init() {
    state_ = ExecState::Init;
    return true;
}

bool IndexScanOperator::open() {
    state_ = ExecState::Open;
    matching_ids_ = index_->search(search_key_);
    current_match_index_ = 0;
    return true;
}

bool IndexScanOperator::next(Tuple& out_tuple) {
    while (current_match_index_ < matching_ids_.size()) {
        const auto& tid = matching_ids_[current_match_index_++];

        storage::HeapTable::TupleMeta meta;
        if (table_->get_meta(tid, meta)) {
            /* MVCC Visibility Check */
            bool visible = true;
            if (txn_) {
                const auto& snapshot = txn_->get_snapshot();
                uint64_t my_id = txn_->get_id();

                // 1. Check xmin (creation)
                bool xmin_visible =
                    (meta.xmin == my_id) || (meta.xmin == 0) || snapshot.is_visible(meta.xmin);

                // 2. Check xmax (deletion)
                bool xmax_visible =
                    (meta.xmax == 0) || (meta.xmax != my_id && !snapshot.is_visible(meta.xmax));

                visible = xmin_visible && xmax_visible;
            } else {
                visible = (meta.xmax == 0);
            }

            if (visible) {
                out_tuple = std::move(meta.tuple);
                return true;
            }
        }
    }

    state_ = ExecState::Done;
    return false;
}

void IndexScanOperator::close() {
    matching_ids_.clear();
    state_ = ExecState::Done;
}

Schema& IndexScanOperator::output_schema() {
    return schema_;
}

/* --- FilterOperator --- */

FilterOperator::FilterOperator(std::unique_ptr<Operator> child,
                               std::unique_ptr<parser::Expression> condition)
    : Operator(OperatorType::Filter, child->get_txn(), child->get_lock_manager()),
      child_(std::move(child)),
      condition_(std::move(condition)) {
    if (child_) schema_ = child_->output_schema();
}

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
        /* Evaluate condition against the current tuple context */
        common::Value result = condition_->evaluate(&tuple, &schema_);
        if (result.as_bool()) {
            out_tuple = std::move(tuple);
            return true;
        }
    }
    state_ = ExecState::Done;
    return false;
}

void FilterOperator::close() {
    child_->close();
    state_ = ExecState::Done;
}

Schema& FilterOperator::output_schema() {
    return schema_;
}

void FilterOperator::add_child(std::unique_ptr<Operator> child) {
    child_ = std::move(child);
}

/* --- ProjectOperator --- */

ProjectOperator::ProjectOperator(std::unique_ptr<Operator> child,
                                 std::vector<std::unique_ptr<parser::Expression>> columns)
    : Operator(OperatorType::Project, child->get_txn(), child->get_lock_manager()),
      child_(std::move(child)),
      columns_(std::move(columns)) {
    if (child_) {
        /* Result schema: use the name of the expression (e.g. column name) */
        for (size_t i = 0; i < columns_.size(); ++i) {
            schema_.add_column(columns_[i]->to_string(), common::TYPE_TEXT);
        }
    }
}

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
    auto input_schema = child_->output_schema();
    for (const auto& col : columns_) {
        /* Evaluate projection expression with input tuple context */
        common::Value v = col->evaluate(&input, &input_schema);
        output_values.push_back(v);
    }
    out_tuple = Tuple(std::move(output_values));
    return true;
}

void ProjectOperator::close() {
    child_->close();
    state_ = ExecState::Done;
}

Schema& ProjectOperator::output_schema() {
    return schema_;
}

void ProjectOperator::add_child(std::unique_ptr<Operator> child) {
    child_ = std::move(child);
}

/* --- SortOperator --- */

SortOperator::SortOperator(std::unique_ptr<Operator> child,
                           std::vector<std::unique_ptr<parser::Expression>> sort_keys,
                           std::vector<bool> ascending)
    : Operator(OperatorType::Sort, child->get_txn(), child->get_lock_manager()),
      child_(std::move(child)),
      sort_keys_(std::move(sort_keys)),
      ascending_(std::move(ascending)) {
    if (child_) schema_ = child_->output_schema();
}

bool SortOperator::init() {
    return child_->init();
}

bool SortOperator::open() {
    if (!child_->open()) return false;

    sorted_tuples_.clear();
    Tuple tuple;
    while (child_->next(tuple)) {
        sorted_tuples_.push_back(std::move(tuple));
    }

    /* Perform sort using child schema for evaluation */
    std::stable_sort(sorted_tuples_.begin(), sorted_tuples_.end(),
                     [this](const Tuple& a, const Tuple& b) {
                         for (size_t i = 0; i < sort_keys_.size(); ++i) {
                             common::Value val_a = sort_keys_[i]->evaluate(&a, &schema_);
                             common::Value val_b = sort_keys_[i]->evaluate(&b, &schema_);
                             bool asc = static_cast<bool>(ascending_[i]);
                             if (val_a < val_b) return asc;
                             if (val_b < val_a) return !asc;
                         }
                         return false;
                     });

    current_index_ = 0;
    state_ = ExecState::Open;
    return true;
}

bool SortOperator::next(Tuple& out_tuple) {
    if (current_index_ >= sorted_tuples_.size()) {
        state_ = ExecState::Done;
        return false;
    }
    out_tuple = std::move(sorted_tuples_[current_index_++]);
    return true;
}

void SortOperator::close() {
    sorted_tuples_.clear();
    child_->close();
    state_ = ExecState::Done;
}

Schema& SortOperator::output_schema() {
    return schema_;
}

/* --- AggregateOperator --- */

AggregateOperator::AggregateOperator(std::unique_ptr<Operator> child,
                                     std::vector<std::unique_ptr<parser::Expression>> group_by,
                                     std::vector<AggregateInfo> aggregates)
    : Operator(OperatorType::Aggregate, child->get_txn(), child->get_lock_manager()),
      child_(std::move(child)),
      group_by_(std::move(group_by)),
      aggregates_(std::move(aggregates)) {
    if (child_) {
        /* Use actual expression string for column name to allow lookup */
        for (const auto& gb : group_by_) {
            if (gb) schema_.add_column(gb->to_string(), common::TYPE_TEXT);
        }
        for (const auto& agg : aggregates_) {
            common::ValueType t = common::TYPE_FLOAT64;
            if (agg.type == AggregateType::Count) t = common::TYPE_INT64;
            schema_.add_column(agg.name, t);
        }
    }
}

bool AggregateOperator::init() {
    return child_->init();
}

bool AggregateOperator::open() {
    if (!child_->open()) return false;

    struct GroupState {
        std::vector<common::Value> group_values;
        std::vector<int64_t> counts;
        std::vector<double> sums;
        std::vector<common::Value> mins;
        std::vector<common::Value> maxes;
        std::vector<std::set<std::string>> distinct_seen;

        GroupState() = default;
        GroupState(size_t agg_size) {
            counts.assign(agg_size, 0);
            sums.assign(agg_size, 0.0);
            mins.assign(agg_size, common::Value::make_null());
            maxes.assign(agg_size, common::Value::make_null());
            distinct_seen.resize(agg_size);
        }
    };

    std::map<std::string, GroupState> groups_map;
    bool is_global = group_by_.empty();

    /* Pre-initialize if global aggregation */
    if (is_global) {
        groups_map["GLOBAL"] = GroupState(aggregates_.size());
    }

    Tuple tuple;
    auto child_schema = child_->output_schema();
    while (child_->next(tuple)) {
        std::string key = "GLOBAL";
        std::vector<common::Value> gb_vals;

        if (!is_global) {
            key = "";
            for (const auto& gb : group_by_) {
                auto val = gb ? gb->evaluate(&tuple, &child_schema) : common::Value::make_null();
                key += val.to_string() + "|";
                gb_vals.push_back(std::move(val));
            }
        }

        auto it = groups_map.find(key);
        if (it == groups_map.end()) {
            it = groups_map.emplace(key, GroupState(aggregates_.size())).first;
            it->second.group_values = std::move(gb_vals);
        }
        auto& state = it->second;

        for (size_t i = 0; i < aggregates_.size(); ++i) {
            common::Value val;
            if (aggregates_[i].expr) {
                val = aggregates_[i].expr->evaluate(&tuple, &child_schema);
            } else {
                val = common::Value::make_int64(1);
            }

            if (val.is_null()) continue;

            /* Handle DISTINCT */
            if (aggregates_[i].is_distinct) {
                std::string val_str = val.to_string();
                if (state.distinct_seen[i].count(val_str)) continue;
                state.distinct_seen[i].insert(val_str);
            }

            if (aggregates_[i].type == AggregateType::Count && !aggregates_[i].expr) {
                state.counts[i]++;
                continue;
            }

            state.counts[i]++;
            if (val.is_numeric()) state.sums[i] += val.to_float64();

            if (state.mins[i].is_null() || val < state.mins[i]) state.mins[i] = val;
            if (state.maxes[i].is_null() || state.maxes[i] < val) state.maxes[i] = val;
        }
    }

    groups_.clear();
    for (auto it = groups_map.begin(); it != groups_map.end(); ++it) {
        auto& state = it->second;
        std::vector<common::Value> row = std::move(state.group_values);
        for (size_t i = 0; i < aggregates_.size(); ++i) {
            switch (aggregates_[i].type) {
                case AggregateType::Count:
                    row.push_back(common::Value::make_int64(state.counts[i]));
                    break;
                case AggregateType::Sum:
                    row.push_back(common::Value::make_float64(state.sums[i]));
                    break;
                case AggregateType::Min:
                    row.push_back(std::move(state.mins[i]));
                    break;
                case AggregateType::Max:
                    row.push_back(std::move(state.maxes[i]));
                    break;
                case AggregateType::Avg:
                    if (state.counts[i] > 0)
                        row.push_back(common::Value::make_float64(state.sums[i] / state.counts[i]));
                    else
                        row.push_back(common::Value::make_null());
                    break;
            }
        }
        groups_.emplace_back(std::move(row));
    }

    current_group_ = 0;
    state_ = ExecState::Open;
    return true;
}

bool AggregateOperator::next(Tuple& out_tuple) {
    if (current_group_ >= groups_.size()) {
        state_ = ExecState::Done;
        return false;
    }
    out_tuple = std::move(groups_[current_group_++]);
    return true;
}

void AggregateOperator::close() {
    groups_.clear();
    child_->close();
    state_ = ExecState::Done;
}

Schema& AggregateOperator::output_schema() {
    return schema_;
}

/* --- HashJoinOperator --- */

HashJoinOperator::HashJoinOperator(std::unique_ptr<Operator> left, std::unique_ptr<Operator> right,
                                   std::unique_ptr<parser::Expression> left_key,
                                   std::unique_ptr<parser::Expression> right_key)
    : Operator(OperatorType::HashJoin, left->get_txn(), left->get_lock_manager()),
      left_(std::move(left)),
      right_(std::move(right)),
      left_key_(std::move(left_key)),
      right_key_(std::move(right_key)) {
    /* Build resulting schema */
    if (left_ && right_) {
        for (const auto& col : left_->output_schema().columns()) schema_.add_column(col);
        for (const auto& col : right_->output_schema().columns()) schema_.add_column(col);
    }
}

bool HashJoinOperator::init() {
    return left_->init() && right_->init();
}

bool HashJoinOperator::open() {
    if (!left_->open() || !right_->open()) return false;

    /* Build phase: scan right side into hash table */
    hash_table_.clear();
    Tuple right_tuple;
    auto right_schema = right_->output_schema();
    while (right_->next(right_tuple)) {
        common::Value key = right_key_->evaluate(&right_tuple, &right_schema);
        hash_table_.emplace(key.to_string(), std::move(right_tuple));
    }

    left_tuple_ = std::nullopt;
    match_iter_ = std::nullopt;
    state_ = ExecState::Open;
    return true;
}

bool HashJoinOperator::next(Tuple& out_tuple) {
    auto left_schema = left_->output_schema();

    while (true) {
        if (match_iter_) {
            /* We are currently iterating through matches for a left tuple */
            if (match_iter_->current != match_iter_->end) {
                const auto& right_tuple = match_iter_->current->second;

                /* Concatenate left and right tuples */
                std::vector<common::Value> joined_values = left_tuple_->values();
                joined_values.insert(joined_values.end(), right_tuple.values().begin(),
                                     right_tuple.values().end());

                out_tuple = Tuple(std::move(joined_values));
                match_iter_->current++;
                return true;
            } else {
                /* No more matches for this left tuple */
                match_iter_ = std::nullopt;
                left_tuple_ = std::nullopt;
            }
        }

        /* Pull next tuple from left side */
        Tuple next_left;
        if (!left_->next(next_left)) {
            state_ = ExecState::Done;
            return false;
        }

        left_tuple_ = std::move(next_left);
        common::Value key = left_key_->evaluate(&*left_tuple_, &left_schema);

        /* Look up in hash table */
        auto range = hash_table_.equal_range(key.to_string());
        if (range.first != range.second) {
            match_iter_ = {range.first, range.second};
            /* Continue loop to return the first match */
        } else {
            /* No match for this left tuple, pull next */
            left_tuple_ = std::nullopt;
        }
    }
}

void HashJoinOperator::close() {
    left_->close();
    right_->close();
    hash_table_.clear();
    match_iter_ = std::nullopt;
    left_tuple_ = std::nullopt;
    state_ = ExecState::Done;
}

Schema& HashJoinOperator::output_schema() {
    return schema_;
}

void HashJoinOperator::add_child(std::unique_ptr<Operator> child) {
    if (!left_) {
        left_ = std::move(child);
    } else {
        right_ = std::move(child);
    }
}

/* --- LimitOperator --- */

LimitOperator::LimitOperator(std::unique_ptr<Operator> child, uint64_t limit, uint64_t offset)
    : Operator(OperatorType::Limit, child->get_txn(), child->get_lock_manager()),
      child_(std::move(child)),
      limit_(limit),
      offset_(offset) {}

bool LimitOperator::init() {
    return child_->init();
}

bool LimitOperator::open() {
    if (!child_->open()) return false;

    /* Skip offset rows */
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

Schema& LimitOperator::output_schema() {
    return child_->output_schema();
}

void LimitOperator::add_child(std::unique_ptr<Operator> child) {
    child_ = std::move(child);
}

}  // namespace executor
}  // namespace cloudsql

/** @} */ /* executor */
