/**
 * @file recovery_tests.cpp
 * @brief Unit tests for Log Manager and Recovery
 */

#include <cstdio>
#include <gtest/gtest.h>
#include <string>

#include "recovery/log_manager.hpp"
#include "recovery/log_record.hpp"

using namespace cloudsql::recovery;

namespace {

TEST(RecoveryTests, LogManagerBasic) {
    const std::string log_file = "test.log";
    static_cast<void>(std::remove(log_file.c_str()));

    {
        LogManager lm(log_file);
        lm.run_flush_thread();

        LogRecord record1(1, 0, LogRecordType::INSERT, "table1", {0, 0}, {});
        const lsn_t lsn1 = lm.append_log_record(record1);
        EXPECT_GE(lsn1, 0);

        lm.flush(true);
        lm.stop_flush_thread();
    }

    static_cast<void>(std::remove(log_file.c_str()));
}

}  // namespace
