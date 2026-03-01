/**
 * @file distributed_tests.cpp
 * @brief Unit tests for distributed execution and sharding
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "distributed/distributed_executor.hpp"
#include "distributed/shard_manager.hpp"
#include "common/cluster_manager.hpp"
#include "catalog/catalog.hpp"
#include "parser/parser.hpp"
#include "parser/lexer.hpp"

using namespace cloudsql;
using namespace cloudsql::executor;
using namespace cloudsql::cluster;
using namespace cloudsql::parser;

namespace {

TEST(ShardManagerTests, BasicHashing) {
    common::Value v1 = common::Value::make_int64(100);
    common::Value v2 = common::Value::make_int64(101);
    
    uint32_t s1 = ShardManager::compute_shard(v1, 2);
    uint32_t s2 = ShardManager::compute_shard(v2, 2);
    
    // Different values should likely land in different shards, but deterministic
    EXPECT_EQ(s1, ShardManager::compute_shard(v1, 2));
    EXPECT_EQ(s2, ShardManager::compute_shard(v2, 2));
}

TEST(DistributedExecutorTests, DDLRouting) {
    auto catalog = Catalog::create();
    config::Config config;
    ClusterManager cm(&config);
    DistributedExecutor exec(*catalog, cm);
    
    auto lexer = std::make_unique<Lexer>("CREATE TABLE test (id INT)");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    
    auto res = exec.execute(*stmt, "CREATE TABLE test (id INT)");
    EXPECT_TRUE(res.success());
}

} // namespace
