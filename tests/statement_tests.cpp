/**
 * @file statement_tests.cpp
 * @brief Unit tests for SQL Statements
 */

#include <gtest/gtest.h>
#include <string>

#include "parser/statement.hpp"
#include "test_utils.hpp"

using namespace cloudsql::parser;

namespace {

TEST(StatementTests, ToString) {
    const TransactionBeginStatement begin;
    EXPECT_STREQ(begin.to_string().c_str(), "BEGIN");

    const TransactionCommitStatement commit;
    EXPECT_STREQ(commit.to_string().c_str(), "COMMIT");

    const TransactionRollbackStatement rollback;
    EXPECT_STREQ(rollback.to_string().c_str(), "ROLLBACK");
}

}  // namespace
