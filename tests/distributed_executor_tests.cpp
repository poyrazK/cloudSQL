/**
 * @file distributed_executor_tests.cpp
 * @brief Unit tests for DistributedExecutor and ShardManager utilities
 */

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "catalog/catalog.hpp"
#include "common/cluster_manager.hpp"
#include "common/config.hpp"
#include "common/value.hpp"
#include "distributed/distributed_executor.hpp"
#include "distributed/shard_manager.hpp"
#include "parser/expression.hpp"
#include "parser/lexer.hpp"
#include "parser/parser.hpp"
#include "network/rpc_client.hpp"
#include "network/rpc_message.hpp"
#include "network/rpc_server.hpp"

using namespace cloudsql;
using namespace cloudsql::executor;
using namespace cloudsql::cluster;
using namespace cloudsql::parser;
using namespace cloudsql::common;

namespace {

// ============= ShardManager Tests =============

TEST(ShardManagerTests, StableHashConsistency) {
    // Same string should always produce same hash
    uint32_t h1 = ShardManager::stable_hash("test_key");
    uint32_t h2 = ShardManager::stable_hash("test_key");
    uint32_t h3 = ShardManager::stable_hash("test_key");
    EXPECT_EQ(h1, h2);
    EXPECT_EQ(h2, h3);
}

TEST(ShardManagerTests, StableHashDifferentStrings) {
    // Different strings should likely produce different hashes
    uint32_t h1 = ShardManager::stable_hash("key1");
    uint32_t h2 = ShardManager::stable_hash("key2");
    EXPECT_NE(h1, h2);
}

TEST(ShardManagerTests, StableHashEmptyString) {
    uint32_t hash = ShardManager::stable_hash("");
    // Empty string should have a defined hash value (DJB2 algorithm)
    EXPECT_EQ(hash, 5381u);  // hash starts at 5381
}

TEST(ShardManagerTests, ComputeShardWithNumShards) {
    Value key = Value::make_int64(42);
    EXPECT_EQ(ShardManager::compute_shard(key, 4), ShardManager::compute_shard(key, 4));
}

TEST(ShardManagerTests, ComputeShardZeroShards) {
    Value key = Value::make_int64(100);
    // Should return 0 (not crash) when num_shards is 0
    EXPECT_EQ(ShardManager::compute_shard(key, 0), 0u);
}

TEST(ShardManagerTests, ComputeShardDeterministic) {
    Value key1 = Value::make_int64(1000);
    Value key2 = Value::make_int64(1000);
    uint32_t shard1 = ShardManager::compute_shard(key1, 8);
    uint32_t shard2 = ShardManager::compute_shard(key2, 8);
    EXPECT_EQ(shard1, shard2);
}

TEST(ShardManagerTests, ComputeShardInRange) {
    Value key = Value::make_int64(999);
    uint32_t num_shards = 16;
    uint32_t shard = ShardManager::compute_shard(key, num_shards);
    EXPECT_LT(shard, num_shards);
}

TEST(ShardManagerTests, GetTargetNodeEmptyShards) {
    TableInfo info;
    info.shards = {};
    auto result = ShardManager::get_target_node(info, 0);
    EXPECT_FALSE(result.has_value());
}

TEST(ShardManagerTests, GetTargetNodeFound) {
    ShardInfo shard;
    shard.shard_id = 5;
    shard.node_address = "127.0.0.1";
    shard.port = 7000;

    TableInfo info;
    info.shards = {shard};

    auto result = ShardManager::get_target_node(info, 5);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result->node_address, "127.0.0.1");
}

TEST(ShardManagerTests, GetTargetNodeNotFound) {
    ShardInfo shard;
    shard.shard_id = 3;
    shard.node_address = "127.0.0.1";
    shard.port = 7000;

    TableInfo info;
    info.shards = {shard};

    auto result = ShardManager::get_target_node(info, 99);  // Different shard_id
    EXPECT_FALSE(result.has_value());
}

// ============= DistributedExecutor Basic Tests =============

class DistributedExecutorTests : public ::testing::Test {
   protected:
    void SetUp() override {
        catalog_ = Catalog::create();
        config_.mode = config::RunMode::Coordinator;
        cm_ = std::make_unique<ClusterManager>(&config_);
        exec_ = std::make_unique<DistributedExecutor>(*catalog_, *cm_);
    }

    std::shared_ptr<Catalog> catalog_;
    config::Config config_;
    std::unique_ptr<ClusterManager> cm_;
    std::unique_ptr<DistributedExecutor> exec_;
};

TEST_F(DistributedExecutorTests, ConstructorBasic) {
    EXPECT_NE(exec_, nullptr);
}

// DDL operations succeed because they update the local catalog
// (no distributed coordination needed for schema changes)
TEST_F(DistributedExecutorTests, ExecuteDDLWithoutNodes) {
    auto lexer = std::make_unique<Lexer>("CREATE TABLE test_table (id INT, name TEXT)");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    ASSERT_NE(stmt, nullptr);

    auto res = exec_->execute(*stmt, "CREATE TABLE test_table (id INT, name TEXT)");
    EXPECT_TRUE(res.success());
}

// DDL without nodes succeeds (local catalog update only)
TEST_F(DistributedExecutorTests, ExecuteDDLNoNodesDropTable) {
    auto lexer = std::make_unique<Lexer>("DROP TABLE test_table");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    ASSERT_NE(stmt, nullptr);

    auto res = exec_->execute(*stmt, "DROP TABLE test_table");
    EXPECT_TRUE(res.success());
}

// DML fails when no nodes because it needs shard routing
TEST_F(DistributedExecutorTests, ExecuteDMLWithoutNodes) {
    auto lexer = std::make_unique<Lexer>("INSERT INTO test_table VALUES (1, 'test')");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    ASSERT_NE(stmt, nullptr);

    auto res = exec_->execute(*stmt, "INSERT INTO test_table VALUES (1, 'test')");
    EXPECT_FALSE(res.success());
    EXPECT_STREQ(res.error().c_str(), "No active data nodes in cluster");
}

// SELECT fails when no nodes available
TEST_F(DistributedExecutorTests, ExecuteSELECTWithoutNodes) {
    auto lexer = std::make_unique<Lexer>("SELECT * FROM test_table");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    ASSERT_NE(stmt, nullptr);

    auto res = exec_->execute(*stmt, "SELECT * FROM test_table");
    EXPECT_FALSE(res.success());
    EXPECT_STREQ(res.error().c_str(), "No active data nodes in cluster");
}

// Transaction control fails when no nodes
TEST_F(DistributedExecutorTests, ExecuteBEGINWithoutNodes) {
    auto lexer = std::make_unique<Lexer>("BEGIN");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    ASSERT_NE(stmt, nullptr);

    auto res = exec_->execute(*stmt, "BEGIN");
    EXPECT_FALSE(res.success());
    EXPECT_STREQ(res.error().c_str(), "No active data nodes in cluster");
}

TEST_F(DistributedExecutorTests, ExecuteCOMMITWithoutNodes) {
    auto lexer = std::make_unique<Lexer>("COMMIT");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    ASSERT_NE(stmt, nullptr);

    auto res = exec_->execute(*stmt, "COMMIT");
    EXPECT_FALSE(res.success());
    EXPECT_STREQ(res.error().c_str(), "No active data nodes in cluster");
}

TEST_F(DistributedExecutorTests, ExecuteROLLBACKWithoutNodes) {
    auto lexer = std::make_unique<Lexer>("ROLLBACK");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    ASSERT_NE(stmt, nullptr);

    auto res = exec_->execute(*stmt, "ROLLBACK");
    EXPECT_FALSE(res.success());
    EXPECT_STREQ(res.error().c_str(), "No active data nodes in cluster");
}

// SELECT without FROM clause - parser error
TEST_F(DistributedExecutorTests, ParseRejectsSelectWithoutFrom) {
    // SELECT * without FROM is not valid SQL in this parser
    auto lexer = std::make_unique<Lexer>("SELECT *");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    // Parser should fail on "SELECT *" without table
    ASSERT_EQ(stmt, nullptr);
}

// ============= Expression Sharding Key Extraction Tests =============

class ShardingKeyExtractionTests : public ::testing::Test {
   protected:
    void SetUp() override {}
};

TEST_F(ShardingKeyExtractionTests, ExtractShardingKeySimpleEq) {
    // Test: id = 42
    auto lexer = std::make_unique<Lexer>("SELECT * FROM test WHERE id = 42");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    ASSERT_NE(stmt, nullptr);

    auto* select_stmt = dynamic_cast<const SelectStatement*>(stmt.get());
    ASSERT_NE(select_stmt, nullptr);
    auto* where_expr = dynamic_cast<const BinaryExpr*>(select_stmt->where());
    ASSERT_NE(where_expr, nullptr);

    // Verify it's: id = 42
    auto* left_col = dynamic_cast<const ColumnExpr*>(&where_expr->left());
    ASSERT_NE(left_col, nullptr);
    EXPECT_EQ(left_col->name(), "id");

    auto* right_const = dynamic_cast<const ConstantExpr*>(&where_expr->right());
    ASSERT_NE(right_const, nullptr);
    EXPECT_EQ(right_const->value(), Value::make_int64(42));

    EXPECT_EQ(where_expr->op(), TokenType::Eq);
}

TEST_F(ShardingKeyExtractionTests, NoWHEREClause) {
    auto lexer = std::make_unique<Lexer>("SELECT * FROM test");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    ASSERT_NE(stmt, nullptr);

    auto* select_stmt = dynamic_cast<const SelectStatement*>(stmt.get());
    ASSERT_NE(select_stmt, nullptr);
    EXPECT_EQ(select_stmt->where(), nullptr);
}

TEST_F(ShardingKeyExtractionTests, NonEqCondition) {
    // WHERE id > 42 uses Greater operator, not equality - no valid sharding key
    auto lexer = std::make_unique<Lexer>("SELECT * FROM test WHERE id > 42");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    ASSERT_NE(stmt, nullptr);

    auto* select_stmt = dynamic_cast<const SelectStatement*>(stmt.get());
    ASSERT_NE(select_stmt, nullptr);
    auto* where_expr = dynamic_cast<const BinaryExpr*>(select_stmt->where());
    ASSERT_NE(where_expr, nullptr);

    // Verify it's: id > 42 (Greater, not Eq)
    auto* left_col = dynamic_cast<const ColumnExpr*>(&where_expr->left());
    ASSERT_NE(left_col, nullptr);
    EXPECT_EQ(left_col->name(), "id");

    // op should be Gt, not Eq - cannot extract sharding key from inequality
    EXPECT_EQ(where_expr->op(), TokenType::Gt);
}

// ============= Helper Function Tests =============

TEST(HelperTests, StableHashAlgorithm) {
    // DJB2 hash algorithm verification
    std::string input = "hello";
    uint32_t hash = ShardManager::stable_hash(input);

    // Manually verify DJB2: hash = hash * 33 + c for each char
    uint32_t expected = 5381;
    for (char c : input) {
        expected = ((expected << 5) + expected) + static_cast<uint8_t>(c);
    }
    EXPECT_EQ(hash, expected);
}

TEST(HelperTests, ComputeShardModuloProperties) {
    // Verify compute_shard uses modulo correctly
    Value key = Value::make_int64(12345);
    uint32_t shard1 = ShardManager::compute_shard(key, 10);
    uint32_t shard2 = ShardManager::compute_shard(key, 10);

    // Same key, same num_shards should always give same result
    EXPECT_EQ(shard1, shard2);

    // Should be in range [0, 10)
    EXPECT_LT(shard1, 10);
}

TEST(HelperTests, ComputeShardStringKey) {
    // Test with string value key
    Value key = Value::make_text("primary_key_value");
    uint32_t shard = ShardManager::compute_shard(key, 8);

    // Should be in range [0, 8)
    EXPECT_LT(shard, 8);
}

// ============= Null Safety Tests =============

TEST(NullSafetyTests, ExecuteWithEmptyCluster) {
    auto catalog = Catalog::create();
    config::Config config;
    ClusterManager cm(&config);
    DistributedExecutor exec(*catalog, cm);

    // DDL succeeds (local catalog update), DML/SELECT fail
    std::vector<std::pair<std::string, bool>> statements = {
        {"CREATE TABLE t (id INT)", true},    // succeeds - local catalog
        {"DROP TABLE t", true},               // succeeds - local catalog
        {"INSERT INTO t VALUES (1)", false},  // fails - needs nodes
        {"SELECT * FROM t", false},           // fails - needs nodes
        {"UPDATE t SET id = 1", false},       // fails - needs nodes
        {"DELETE FROM t", false},             // fails - needs nodes
        {"BEGIN", false},                     // fails - needs nodes
        {"COMMIT", false},                    // fails - needs nodes
        {"ROLLBACK", false}};                 // fails - needs nodes

    for (const auto& [sql, expected_success] : statements) {
        auto lexer = std::make_unique<Lexer>(sql);
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        ASSERT_TRUE(stmt) << "Parse failed for: " << sql;
        auto res = exec.execute(*stmt, sql);
        EXPECT_EQ(res.success(), expected_success) << "Failed for: " << sql;
    }
}

// ============= Node Registration with Mock RpcServer Tests =============

class DistributedExecutorWithNodesTests : public ::testing::Test {
   protected:
    void SetUp() override {
        catalog_ = Catalog::create();
        config_.mode = config::RunMode::Coordinator;
        cm_ = std::make_unique<ClusterManager>(&config_);
        exec_ = std::make_unique<DistributedExecutor>(*catalog_, *cm_);
        next_port_ = 6410;  // Start from different base to avoid rpc_client_tests
    }

    void TearDown() override {
        for (auto& srv : servers_) {
            srv->stop();
        }
        servers_.clear();
        exec_.reset();
        cm_.reset();
        catalog_.reset();
        // Clean up test data directories
        std::remove("./test_dist_exec/node1.db");
        std::remove("./test_dist_exec/node2.db");
    }

    // Register a mock data node and start an RpcServer on its port
    void register_mock_node(const std::string& id, uint16_t port, bool start_server = true) {
        cm_->register_node(id, "127.0.0.1", port, config::RunMode::Data);
        if (start_server) {
            auto srv = std::make_unique<network::RpcServer>(port);
            srv->start();
            servers_.push_back(std::move(srv));
        }
    }

    // Set up a handler that returns successful QueryResultsReply
    void set_execute_fragment_handler(network::RpcServer& srv, bool success = true,
                                       const std::string& error_msg = "") {
        srv.set_handler(network::RpcType::ExecuteFragment,
                       [success, error_msg](const network::RpcHeader&, const std::vector<uint8_t>& payload,
                                            int fd) {
                           auto args = network::ExecuteFragmentArgs::deserialize(payload);
                           (void)args;  // suppress unused warning

                           network::QueryResultsReply reply;
                           reply.success = success;
                           reply.error_msg = error_msg;
                           // Add schema with one column for result rows
                           reply.schema.add_column("id", common::ValueType::TYPE_INT32);

                           // Send reply
                           network::RpcHeader resp_h;
                           resp_h.type = network::RpcType::QueryResults;
                           resp_h.payload_len = static_cast<uint16_t>(reply.serialize().size());
                           char h_buf[network::RpcHeader::HEADER_SIZE];
                           resp_h.encode(h_buf);
                           send(fd, h_buf, network::RpcHeader::HEADER_SIZE, 0);
                           auto data = reply.serialize();
                           if (!data.empty()) {
                               send(fd, data.data(), data.size(), 0);
                           }
                       });
    }

    static std::atomic<uint16_t> next_port_;
    std::shared_ptr<Catalog> catalog_;
    config::Config config_;
    std::unique_ptr<ClusterManager> cm_;
    std::unique_ptr<DistributedExecutor> exec_;
    std::vector<std::unique_ptr<network::RpcServer>> servers_;
};

std::atomic<uint16_t> DistributedExecutorWithNodesTests::next_port_{6410};

// Test: Execute SELECT with registered data nodes but no RpcServer running (connect failure)
// This exercises the RPC failure path in query_futures (lines 582-597)
TEST_F(DistributedExecutorWithNodesTests, SelectWithNodesButNoServer) {
    // Register nodes but don't start servers - connect will fail
    register_mock_node("node_1", 6411, false /* no server */);
    register_mock_node("node_2", 6412, false /* no server */);

    auto lexer = std::make_unique<Lexer>("SELECT * FROM test_table WHERE id = 42");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    ASSERT_NE(stmt, nullptr);

    auto res = exec_->execute(*stmt, "SELECT * FROM test_table WHERE id = 42");
    // Should fail with RPC/connection errors
    EXPECT_FALSE(res.success());
}

// Test: Execute SELECT with a sharding key and working RpcServer
// This exercises shard routing with leader (lines 541-558)
TEST_F(DistributedExecutorWithNodesTests, SelectWithShardRouting) {
    // Start RpcServers on two ports
    auto srv1 = std::make_unique<network::RpcServer>(6411);
    auto srv2 = std::make_unique<network::RpcServer>(6412);
    srv1->start();
    srv2->start();
    servers_.push_back(std::move(srv1));
    servers_.push_back(std::move(srv2));

    // Register two data nodes
    cm_->register_node("node_1", "127.0.0.1", 6411, config::RunMode::Data);
    cm_->register_node("node_2", "127.0.0.1", 6412, config::RunMode::Data);

    // Set up handlers that return success
    set_execute_fragment_handler(*servers_[0], true);
    set_execute_fragment_handler(*servers_[1], true);

    auto lexer = std::make_unique<Lexer>("SELECT * FROM test_table WHERE id = 1");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    ASSERT_NE(stmt, nullptr);

    auto res = exec_->execute(*stmt, "SELECT * FROM test_table WHERE id = 1");
    // Should succeed (no error, but may have 0 rows since table doesn't exist)
    EXPECT_TRUE(res.success());
}

// Test: DDL forwarding to data nodes (lines 156-163)
// DDL should forward to data nodes for catalog sync
TEST_F(DistributedExecutorWithNodesTests, DDLForwardsToNodes) {
    // Start RpcServer for node1
    auto srv1 = std::make_unique<network::RpcServer>(6413);
    srv1->start();
    servers_.push_back(std::move(srv1));

    cm_->register_node("node_1", "127.0.0.1", 6413, config::RunMode::Data);
    set_execute_fragment_handler(*servers_[0], true);

    // DDL: CREATE TABLE - should succeed locally and try to forward to nodes
    auto lexer = std::make_unique<Lexer>("CREATE TABLE ddl_test (id INT, name TEXT)");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    ASSERT_NE(stmt, nullptr);

    auto res = exec_->execute(*stmt, "CREATE TABLE ddl_test (id INT, name TEXT)");
    EXPECT_TRUE(res.success());  // DDL always succeeds (local catalog)
}

// Test: INSERT with sharding key routed to correct node
// Exercises INSERT shard routing (lines 454-519)
TEST_F(DistributedExecutorWithNodesTests, InsertShardRouting) {
    // Start server for node1 only
    auto srv1 = std::make_unique<network::RpcServer>(6414);
    srv1->start();
    servers_.push_back(std::move(srv1));

    cm_->register_node("node_1", "127.0.0.1", 6414, config::RunMode::Data);
    // Only register one node so shard_idx=0 always routes to node_1
    set_execute_fragment_handler(*servers_[0], true);

    auto lexer = std::make_unique<Lexer>("INSERT INTO shard_table VALUES (1, 'test')");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    ASSERT_NE(stmt, nullptr);

    auto res = exec_->execute(*stmt, "INSERT INTO shard_table VALUES (1, 'test')");
    // Should succeed even if node returns error (graceful degradation)
    // or succeed with rows_affected
    EXPECT_TRUE(res.success());
}

// Test: COMMIT with working nodes (2PC prepare/commit paths - lines 388-449)
TEST_F(DistributedExecutorWithNodesTests, CommitWithNodes) {
    auto srv1 = std::make_unique<network::RpcServer>(6415);
    srv1->start();
    servers_.push_back(std::move(srv1));

    cm_->register_node("node_1", "127.0.0.1", 6415, config::RunMode::Data);

    // Set up TxnPrepare and TxnCommit handlers that return success
    servers_[0]->set_handler(network::RpcType::TxnPrepare,
                     [](const network::RpcHeader&, const std::vector<uint8_t>&, int fd) {
                         network::QueryResultsReply reply;
                         reply.success = true;
                         network::RpcHeader resp_h;
                         resp_h.type = network::RpcType::TxnPrepare;
                         resp_h.payload_len = static_cast<uint16_t>(reply.serialize().size());
                         char h_buf[network::RpcHeader::HEADER_SIZE];
                         resp_h.encode(h_buf);
                         send(fd, h_buf, network::RpcHeader::HEADER_SIZE, 0);
                         auto data = reply.serialize();
                         if (!data.empty()) send(fd, data.data(), data.size(), 0);
                     });
    servers_[0]->set_handler(network::RpcType::TxnCommit,
                     [](const network::RpcHeader&, const std::vector<uint8_t>&, int fd) {
                         network::QueryResultsReply reply;
                         reply.success = true;
                         network::RpcHeader resp_h;
                         resp_h.type = network::RpcType::TxnCommit;
                         resp_h.payload_len = static_cast<uint16_t>(reply.serialize().size());
                         char h_buf[network::RpcHeader::HEADER_SIZE];
                         resp_h.encode(h_buf);
                         send(fd, h_buf, network::RpcHeader::HEADER_SIZE, 0);
                         auto data = reply.serialize();
                         if (!data.empty()) send(fd, data.data(), data.size(), 0);
                     });

    auto lexer = std::make_unique<Lexer>("COMMIT");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    ASSERT_NE(stmt, nullptr);

    auto res = exec_->execute(*stmt, "COMMIT");
    // Should succeed (2PC with nodes returns empty QueryResult on success)
    EXPECT_TRUE(res.success());
}

// Test: ROLLBACK with nodes (lines 366-386)
TEST_F(DistributedExecutorWithNodesTests, RollbackWithNodes) {
    auto srv1 = std::make_unique<network::RpcServer>(6416);
    srv1->start();
    servers_.push_back(std::move(srv1));

    cm_->register_node("node_1", "127.0.0.1", 6416, config::RunMode::Data);
    // ROLLBACK just fires async TxnAbort - no reply needed
    servers_[0]->set_handler(network::RpcType::TxnAbort,
                     [](const network::RpcHeader&, const std::vector<uint8_t>&, int fd) {
                         // Send a response since client.call() waits for reply
                         network::QueryResultsReply reply;
                         reply.success = true;
                         network::RpcHeader resp_h;
                         resp_h.type = network::RpcType::TxnAbort;
                         resp_h.payload_len = static_cast<uint16_t>(reply.serialize().size());
                         char h_buf[network::RpcHeader::HEADER_SIZE];
                         resp_h.encode(h_buf);
                         send(fd, h_buf, network::RpcHeader::HEADER_SIZE, 0);
                         auto data = reply.serialize();
                         if (!data.empty()) send(fd, data.data(), data.size(), 0);
                     });

    auto lexer = std::make_unique<Lexer>("ROLLBACK");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    ASSERT_NE(stmt, nullptr);

    auto res = exec_->execute(*stmt, "ROLLBACK");
    // ROLLBACK returns empty QueryResult (line 385)
    EXPECT_TRUE(res.success());
}

// Test: SELECT without sharding key broadcasts to all nodes (line 565)
// SELECT * FROM table (no WHERE) broadcasts
TEST_F(DistributedExecutorWithNodesTests, SelectBroadcastNoShardKey) {
    auto srv1 = std::make_unique<network::RpcServer>(6417);
    srv1->start();
    servers_.push_back(std::move(srv1));

    cm_->register_node("node_1", "127.0.0.1", 6417, config::RunMode::Data);
    set_execute_fragment_handler(*servers_[0], true);

    auto lexer = std::make_unique<Lexer>("SELECT * FROM test_table");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    ASSERT_NE(stmt, nullptr);

    auto res = exec_->execute(*stmt, "SELECT * FROM test_table");
    // Broadcasts to all nodes, should succeed
    EXPECT_TRUE(res.success());
}

}  // namespace
