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

namespace cloudsql::executor {

using namespace cloudsql::transaction;

/**
 * @brief Operator types
 */
enum class OperatorType : uint8_t {
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
    Result,
    BufferScan
};

/**
 * @brief Base operator class (Volcano iterator model)
 */
class Operator {
   private:
    OperatorType type_;
    ExecState state_ = ExecState::Init;
    std::string error_message_;
    Transaction* txn_;
    LockManager* lock_manager_;

   public:
    explicit Operator(OperatorType type, Transaction* txn = nullptr,
                      LockManager* lock_manager = nullptr)
        : type_(type), txn_(txn), lock_manager_(lock_manager) {}
    virtual ~Operator() = default;

    // Disable copy/move for base operator
    Operator(const Operator&) = delete;
    Operator& operator=(const Operator&) = delete;
    Operator(Operator&&) = delete;
    Operator& operator=(Operator&&) = delete;

    [[nodiscard]] OperatorType type() const { return type_; }
    [[nodiscard]] ExecState state() const { return state_; }
    [[nodiscard]] const std::string& error() const { return error_message_; }
    [[nodiscard]] Transaction* get_txn() const { return txn_; }
    [[nodiscard]] LockManager* get_lock_manager() const { return lock_manager_; }

    virtual bool init() { return true; }
    virtual bool open() { return true; }
    virtual bool next(Tuple& out_tuple) {
        (void)out_tuple;
        state_ = ExecState::Done;
        return false;
    }
    virtual void close() {}

    [[nodiscard]] virtual Schema& output_schema() = 0;

    virtual void add_child(std::unique_ptr<Operator> child) { (void)child; }
    [[nodiscard]] virtual const std::vector<std::unique_ptr<Operator>>& children() const {
        static const std::vector<std::unique_ptr<Operator>> empty;
        return empty;
    }

    [[nodiscard]] bool is_done() const { return state_ == ExecState::Done; }
    [[nodiscard]] bool has_error() const { return state_ == ExecState::Error; }

   protected:
    void set_state(ExecState s) { state_ = s; }
    void set_error(std::string msg) {
        error_message_ = std::move(msg);
        state_ = ExecState::Error;
    }
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
    [[nodiscard]] Schema& output_schema() override;
    [[nodiscard]] const std::string& table_name() const { return table_name_; }
};

/**
 * @brief Buffer scan operator (for shuffled/broadcasted data)
 */
class BufferScanOperator : public Operator {
   private:
    std::string context_id_;
    std::string table_name_;
    std::vector<Tuple> data_;
    size_t current_index_ = 0;
    Schema schema_;

   public:
    BufferScanOperator(std::string context_id, std::string table_name, std::vector<Tuple> data,
                       Schema schema);

    bool init() override { return true; }
    bool open() override {
        current_index_ = 0;
        return true;
    }
    bool next(Tuple& out_tuple) override;
    void close() override {}
    [[nodiscard]] Schema& output_schema() override;
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
    [[nodiscard]] Schema& output_schema() override;
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
    [[nodiscard]] Schema& output_schema() override;
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
    [[nodiscard]] Schema& output_schema() override;
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
    [[nodiscard]] Schema& output_schema() override;
};

/**
 * @brief Aggregate specification
 */
struct AggregateInfo {
    AggregateType type = AggregateType::Count;
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
    [[nodiscard]] Schema& output_schema() override;
};

/**
 * @brief Hash join operator
 */
class HashJoinOperator : public Operator {
   public:
    using JoinType = cloudsql::executor::JoinType;

   private:
    struct BuildTuple {
        Tuple tuple;
        bool matched = false;
    };

    std::unique_ptr<Operator> left_;
    std::unique_ptr<Operator> right_;
    std::unique_ptr<parser::Expression> left_key_;
    std::unique_ptr<parser::Expression> right_key_;
    JoinType join_type_;
    Schema schema_;

    /* In-memory hash table for the right side */
    std::unordered_multimap<std::string, BuildTuple> hash_table_;

    /* Probe phase state */
    std::optional<Tuple> left_tuple_;
    bool left_had_match_ = false;
    struct MatchIterator {
        std::unordered_multimap<std::string, BuildTuple>::iterator current;
        std::unordered_multimap<std::string, BuildTuple>::iterator end;
    };
    std::optional<MatchIterator> match_iter_;

    /* Final phase for RIGHT/FULL joins */
    std::optional<std::unordered_multimap<std::string, BuildTuple>::iterator> right_idx_iter_;

   public:
    HashJoinOperator(std::unique_ptr<Operator> left, std::unique_ptr<Operator> right,
                     std::unique_ptr<parser::Expression> left_key,
                     std::unique_ptr<parser::Expression> right_key,
                     JoinType join_type = JoinType::Inner);

    bool init() override;
    bool open() override;
    bool next(Tuple& out_tuple) override;
    void close() override;
    [[nodiscard]] Schema& output_schema() override;
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
    [[nodiscard]] Schema& output_schema() override;
    void add_child(std::unique_ptr<Operator> child) override;
};

}  // namespace cloudsql::executor

#endif  // CLOUDSQL_EXECUTOR_OPERATOR_HPP
