/**
 * @file row_estimator.cpp
 * @brief Row count estimation utilities
 */

#include "optimizer/row_estimator.hpp"

#include <algorithm>
#include <unordered_set>

namespace cloudsql::optimizer {

uint64_t RowEstimator::estimate_scan_rows(const TableInfo& table) {
    if (table.num_rows > 0) {
        return table.num_rows;
    }
    return 0;
}

uint64_t RowEstimator::estimate_filter_rows(const TableInfo& table, const std::string& col_name,
                                            const common::Value& predicate_value) {
    if (table.num_rows == 0) {
        return 0;
    }

    // Find column stats
    auto col_opt = const_cast<TableInfo&>(table).get_column(col_name);
    if (!col_opt.has_value()) {
        return table.num_rows;  // Unknown column, assume full scan
    }

    auto* col = *col_opt;
    if (!col->has_stats) {
        return table.num_rows;  // No stats, assume full scan
    }

    // For equality predicates, use NDV to estimate selectivity
    if (col->ndv.has_value() && col->ndv.value() > 0) {
        double selectivity = 1.0 / static_cast<double>(col->ndv.value());
        return static_cast<uint64_t>(static_cast<double>(table.num_rows) * selectivity);
    }

    // Range-based selectivity for integer types
    if (predicate_value.type() == common::ValueType::TYPE_INT64) {
        if (col->min_int.has_value() && col->max_int.has_value()) {
            int64_t val = predicate_value.to_int64();
            if (val >= col->min_int.value() && val <= col->max_int.value()) {
                double range = static_cast<double>(col->max_int.value() - col->min_int.value() + 1);
                if (range > 0) {
                    double selectivity = 1.0 / range;
                    return static_cast<uint64_t>(static_cast<double>(table.num_rows) * selectivity);
                }
            }
        }
    }

    // String length-based selectivity (for LIKE prefixes, etc.)
    if (col->min_str_len.has_value() && col->max_str_len.has_value() &&
        col->max_str_len.value() > col->min_str_len.value()) {
        double range = static_cast<double>(col->max_str_len.value() - col->min_str_len.value() + 1);
        if (range > 0) {
            double selectivity = 1.0 / range;
            return static_cast<uint64_t>(static_cast<double>(table.num_rows) * selectivity);
        }
    }

    return table.num_rows;
}

uint64_t RowEstimator::estimate_join_rows(const TableInfo& left, const TableInfo& right,
                                         const std::string& key_col) {
    // Estimate join output: |A| * |B| / max(NDV(A.key), NDV(B.key))
    auto left_col_opt = const_cast<TableInfo&>(left).get_column(key_col);
    auto right_col_opt = const_cast<TableInfo&>(right).get_column(key_col);

    if (!left_col_opt.has_value() || !right_col_opt.has_value()) {
        // Fallback: cross product estimate
        return left.num_rows * right.num_rows;
    }

    auto* left_col = *left_col_opt;
    auto* right_col = *right_col_opt;

    uint64_t left_ndv = left_col->ndv.value_or(left.num_rows);
    uint64_t right_ndv = right_col->ndv.value_or(right.num_rows);

    if (left_ndv == 0 || right_ndv == 0) {
        return 0;
    }

    uint64_t max_ndv = std::max(left_ndv, right_ndv);
    if (max_ndv == 0) {
        max_ndv = 1;
    }

    uint64_t product = left.num_rows * right.num_rows;
    return product / max_ndv;
}

}  // namespace cloudsql::optimizer
