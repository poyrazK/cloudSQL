/**
 * @file operator.hpp
 * @brief Operator classes for Volcano-style execution model
 */

#ifndef CLOUDSQL_EXECUTOR_OPERATOR_HPP
#define CLOUDSQL_EXECUTOR_OPERATOR_HPP

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "executor/types.hpp"
#include "parser/expression.hpp"
#include "storage/btree_index.hpp"
#include "storage/heap_table.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction.hpp"

namespace cloudsql {
namespace executor {

using namespace cloudsql::transaction;

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
enum class ExecState { Init, Open, Executing, Done, Error };

/**
 * @brief Base operator class (Volcano iterator model)
 */
class Operator {
   protected:
    OperatorType type_;
    ExecState state_ = ExecState::Init;
    std::string error_message_;
    Transaction* txn_;
    LockManager* lock_manager_;

   public:
    Operator(OperatorType type, Transaction* txn = nullptr, LockManager* lock_manager = nullptr)
        : type_(type), txn_(txn), lock_manager_(lock_manager) {}
    virtual ~Operator() = default;

    OperatorType type() const { return type_; }
    ExecState state() const { return state_; }
    const std::string& error() const { return error_message_; }
    Transaction* get_txn() const { return txn_; }
    LockManager* get_lock_manager() const { return lock_manager_; }

    virtual bool init() { return true; }
    virtual bool open() { return true; }
    virtual bool next(Tuple& out_tuple) {
        (void)out_tuple;
        state_ = ExecState::Done;
        return false;
    }
    virtual void close() {}

    virtual Schema& output_schema() = 0;

    virtual void add_child(std::unique_ptr<Operator> child) { (void)child; }
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
    explicit SeqScanOperator(std::unique_ptr<storage::HeapTable> table, Transaction* txn = nullptr,
                             LockManager* lock_manager = nullptr);

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
                      std::unique_ptr<storage::BTreeIndex> index, common::Value search_key,
                      Transaction* txn = nullptr, LockManager* lock_manager = nullptr);

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
    ProjectOperator(std::unique_ptr<Operator> child,
                    std::vector<std::unique_ptr<parser::Expression>> columns);

    bool init() override;
    bool open() override;
    bool next(Tuple& out_tuple) override;
    void close() override;
    Schema& output_schema() override;
    void add_child(std::unique_ptr<Operator> child) override;
};

/**
 * @brief Sort operator (ORDER BY)
 */
class SortOperator : public Operator {
   private:
    std::unique_ptr<Operator> child_;
    std::vector<std::unique_ptr<parser::Expression>> sort_keys_;
    std::vector<bool> ascending_;
    std::vector<Tuple> sorted_tuples_;
    size_t current_index_ = 0;
    Schema schema_;

   public:
    SortOperator(std::unique_ptr<Operator> child,
                 std::vector<std::unique_ptr<parser::Expression>> sort_keys,
                 std::vector<bool> ascending);

    bool init() override;
    bool open() override;
    bool next(Tuple& out_tuple) override;
    void close() override;
    Schema& output_schema() override;
};

/**
 * @brief Aggregate types
 */
enum class AggregateType { Count, Sum, Avg, Min, Max };

/**
 * @brief Aggregate specification
 */
struct AggregateInfo {
    AggregateType type;
    std::unique_ptr<parser::Expression> expr;
    std::string name;
    bool is_distinct = false;
};

/**
 * @brief Aggregate operator (GROUP BY)
 */
class AggregateOperator : public Operator {
   private:
    std::unique_ptr<Operator> child_;
    std::vector<std::unique_ptr<parser::Expression>> group_by_;
    std::vector<AggregateInfo> aggregates_;
    std::vector<Tuple> groups_;
    size_t current_group_ = 0;
    Schema schema_;

   public:
    AggregateOperator(std::unique_ptr<Operator> child,
                      std::vector<std::unique_ptr<parser::Expression>> group_by,
                      std::vector<AggregateInfo> aggregates);

    bool init() override;
    bool open() override;
    bool next(Tuple& out_tuple) override;
    void close() override;
    Schema& output_schema() override;
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
