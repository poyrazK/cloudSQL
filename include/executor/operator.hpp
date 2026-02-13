/**
 * @file operator.hpp
 * @brief Operator classes for Volcano-style execution model
 */

#ifndef CLOUDSQL_EXECUTOR_OPERATOR_HPP
#define CLOUDSQL_EXECUTOR_OPERATOR_HPP

#include <memory>
#include <string>
#include <vector>
#include <optional>
#include <unordered_map>
#include "executor/types.hpp"
#include "parser/expression.hpp"
#include "storage/heap_table.hpp"
#include "storage/btree_index.hpp"

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
    std::unique_ptr<storage::HeapTable> table_;
    std::unique_ptr<storage::HeapTable::Iterator> iterator_;
    Schema schema_;
    
public:
    explicit SeqScanOperator(std::unique_ptr<storage::HeapTable> table);
    
    bool init() override;
    bool open() override;
    bool next(Tuple& out_tuple) override;
    void close() override;
    Schema& output_schema() override;
    const std::string& table_name() const { return table_name_; }
};

/**
 * @brief Index scan operator (point lookup)
 */
class IndexScanOperator : public Operator {
private:
    std::string table_name_;
    std::string index_name_;
    std::unique_ptr<storage::HeapTable> table_;
    std::unique_ptr<storage::BTreeIndex> index_;
    common::Value search_key_;
    std::vector<storage::HeapTable::TupleId> matching_ids_;
    size_t current_match_index_ = 0;
    Schema schema_;

public:
    IndexScanOperator(std::unique_ptr<storage::HeapTable> table, 
                      std::unique_ptr<storage::BTreeIndex> index,
                      common::Value search_key);

    bool init() override;
    bool open() override;
    bool next(Tuple& out_tuple) override;
    void close() override;
    Schema& output_schema() override;
};

/**
 * @brief Filter operator (WHERE clause)
 */
class FilterOperator : public Operator {
private:
    std::unique_ptr<Operator> child_;
    std::unique_ptr<parser::Expression> condition_;
    Schema schema_;
    
public:
    FilterOperator(std::unique_ptr<Operator> child, std::unique_ptr<parser::Expression> condition);
    
    bool init() override;
    bool open() override;
    bool next(Tuple& out_tuple) override;
    void close() override;
    Schema& output_schema() override;
    void add_child(std::unique_ptr<Operator> child) override;
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
    ProjectOperator(std::unique_ptr<Operator> child, std::vector<std::unique_ptr<parser::Expression>> columns);
    
    bool init() override;
    bool open() override;
    bool next(Tuple& out_tuple) override;
    void close() override;
    Schema& output_schema() override;
    void add_child(std::unique_ptr<Operator> child) override;
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
    
    /* In-memory hash table for the right side */
    std::unordered_multimap<std::string, Tuple> hash_table_;
    
    /* Probe phase state */
    std::optional<Tuple> left_tuple_;
    struct MatchIterator {
        std::unordered_multimap<std::string, Tuple>::iterator current;
        std::unordered_multimap<std::string, Tuple>::iterator end;
    };
    std::optional<MatchIterator> match_iter_;
    
public:
    HashJoinOperator(std::unique_ptr<Operator> left, std::unique_ptr<Operator> right,
                     std::unique_ptr<parser::Expression> left_key, 
                     std::unique_ptr<parser::Expression> right_key);
    
    bool init() override;
    bool open() override;
    bool next(Tuple& out_tuple) override;
    void close() override;
    Schema& output_schema() override;
    void add_child(std::unique_ptr<Operator> child) override;
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
    LimitOperator(std::unique_ptr<Operator> child, uint64_t limit, uint64_t offset = 0);
    
    bool init() override;
    bool open() override;
    bool next(Tuple& out_tuple) override;
    void close() override;
    Schema& output_schema() override;
    void add_child(std::unique_ptr<Operator> child) override;
};

}  // namespace executor
}  // namespace cloudsql

#endif  // CLOUDSQL_EXECUTOR_OPERATOR_HPP
