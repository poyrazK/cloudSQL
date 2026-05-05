/**
 * @file row_estimator.hpp
 * @brief Row count estimation utilities for query optimization
 */

#pragma once

#include <cstdint>
#include <string>

#include "catalog/catalog.hpp"

namespace cloudsql::optimizer {

/**
 * @brief Estimates row counts for query planning using column statistics.
 *
 * Used by the Volcano/Vectorized chooser to decide execution strategy
 * based on estimated result set size.
 */
class RowEstimator {
   public:
    /**
     * @brief Estimate rows returned by a table scan (no filter).
     */
    static uint64_t estimate_scan_rows(const TableInfo& table);

    /**
     * @brief Estimate rows returned by an equality filter: WHERE column = value.
     */
    static uint64_t estimate_filter_rows(const TableInfo& table, const std::string& col_name,
                                         const common::Value& predicate_value);

    /**
     * @brief Estimate join result size: |A join B| ≈ |A| * |B| / max(NDV(A.key), NDV(B.key))
     */
    static uint64_t estimate_join_rows(const TableInfo& left, const TableInfo& right,
                                       const std::string& key_col);
};

}  // namespace cloudsql::optimizer
