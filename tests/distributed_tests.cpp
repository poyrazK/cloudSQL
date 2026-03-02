/**
 * @file distributed_tests.cpp
 * @brief Unit tests for distributed execution and sharding
 */

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <array>
#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

#include "catalog/catalog.hpp"
#include "common/cluster_manager.hpp"
#include "distributed/distributed_executor.hpp"
#include "distributed/shard_manager.hpp"
#include "network/rpc_client.hpp"
#include "network/rpc_message.hpp"
#include "network/rpc_server.hpp"
#include "parser/lexer.hpp"
#include "parser/parser.hpp"

using namespace cloudsql;
using namespace cloudsql::executor;
using namespace cloudsql::cluster;
using namespace cloudsql::parser;
using namespace cloudsql::network;

namespace {

TEST(ShardManagerTests, BasicHashing) {
    const common::Value v1 = common::Value::make_int64(100);
    const common::Value v2 = common::Value::make_int64(101);

    const uint32_t s1 = ShardManager::compute_shard(v1, 2);
    const uint32_t s2 = ShardManager::compute_shard(v2, 2);

    // Different values should likely land in different shards, but deterministic
    EXPECT_EQ(s1, ShardManager::compute_shard(v1, 2));
    EXPECT_EQ(s2, ShardManager::compute_shard(v2, 2));
}

TEST(DistributedExecutorTests, DDLRouting) {
    auto catalog = Catalog::create();
    const config::Config config;
    ClusterManager cm(&config);
    DistributedExecutor exec(*catalog, cm);

    auto lexer = std::make_unique<Lexer>("CREATE TABLE test (id INT)");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();

    auto res = exec.execute(*stmt, "CREATE TABLE test (id INT)");
    EXPECT_TRUE(res.success());
}

TEST(DistributedExecutorTests, AggregationMerge) {
    // 1. Setup mock shards
    RpcServer node1(7300);
    RpcServer node2(7301);
    
    auto agg_handler = [](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
        (void)h; (void)p;
        QueryResultsReply reply;
        reply.success = true;
        
        std::vector<common::Value> vals;
        vals.push_back(common::Value::make_int64(10)); // Each node returns 10
        executor::Tuple t(std::move(vals));
        reply.rows.push_back(std::move(t));
        
        auto resp_p = reply.serialize();
        RpcHeader resp_h;
        resp_h.type = RpcType::QueryResults;
        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
        std::array<char, 8> h_buf{};
        resp_h.encode(h_buf.data());
        static_cast<void>(send(fd, h_buf.data(), 8, 0));
        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
    };

    node1.set_handler(RpcType::ExecuteFragment, agg_handler);
    node2.set_handler(RpcType::ExecuteFragment, agg_handler);
    ASSERT_TRUE(node1.start());
    ASSERT_TRUE(node2.start());

    // 2. Setup Coordinator
    auto catalog = Catalog::create();
    const config::Config config;
    ClusterManager cm(&config);
    cm.register_node("n1", "127.0.0.1", 7300, config::RunMode::Data);
    cm.register_node("n2", "127.0.0.1", 7301, config::RunMode::Data);
    DistributedExecutor exec(*catalog, cm);

    // 3. Execute COUNT(*)
    auto lexer = std::make_unique<Lexer>("SELECT COUNT(*) FROM test");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    auto res = exec.execute(*stmt, "SELECT COUNT(*) FROM test");

    // 4. Verify result is merged (10 + 10 = 20)
    EXPECT_TRUE(res.success());
    EXPECT_EQ(res.rows().size(), 1U);
    EXPECT_EQ(res.rows()[0].get(0).as_int64(), 20);

    node1.stop();
    node2.stop();
}

TEST(DistributedExecutorTests, ShardPruningSelect) {
    RpcServer node1(7400);
    RpcServer node2(7401);
    
    std::atomic<int> n1_calls{0};
    std::atomic<int> n2_calls{0};

    auto h1 = [&](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
        (void)h; (void)p; n1_calls++;
        QueryResultsReply reply; reply.success = true;
        auto resp_p = reply.serialize();
        RpcHeader resp_h; resp_h.type = RpcType::QueryResults;
        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
        std::array<char, 8> h_buf{}; resp_h.encode(h_buf.data());
        static_cast<void>(send(fd, h_buf.data(), 8, 0));
        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
    };
    auto h2 = [&](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
        (void)h; (void)p; n2_calls++;
        QueryResultsReply reply; reply.success = true;
        auto resp_p = reply.serialize();
        RpcHeader resp_h; resp_h.type = RpcType::QueryResults;
        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
        std::array<char, 8> h_buf{}; resp_h.encode(h_buf.data());
        static_cast<void>(send(fd, h_buf.data(), 8, 0));
        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
    };

    node1.set_handler(RpcType::ExecuteFragment, h1);
    node2.set_handler(RpcType::ExecuteFragment, h2);
    ASSERT_TRUE(node1.start());
    ASSERT_TRUE(node2.start());

    auto catalog = Catalog::create();
    const config::Config config;
    ClusterManager cm(&config);
    cm.register_node("n1", "127.0.0.1", 7400, config::RunMode::Data);
    cm.register_node("n2", "127.0.0.1", 7401, config::RunMode::Data);
    DistributedExecutor exec(*catalog, cm);

    // Execute point query. We don't care which node it hits, as long as it hits EXACTLY ONE.
    auto lexer = std::make_unique<Lexer>("SELECT * FROM test WHERE id = 100");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    auto res = exec.execute(*stmt, "SELECT * FROM test WHERE id = 100");

    EXPECT_TRUE(res.success());
    EXPECT_EQ(n1_calls.load() + n2_calls.load(), 1);

    node1.stop();
    node2.stop();
}

TEST(DistributedExecutorTests, DataRedistributionShuffle) {
    // 1. Setup target mock node
    RpcServer target_node(7500);
    std::atomic<int> received_rows{0};
    std::string received_table;

    target_node.set_handler(RpcType::PushData, [&](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
        (void)h;
        auto args = PushDataArgs::deserialize(p);
        received_rows += static_cast<int>(args.rows.size());
        received_table = args.table_name;

        // Send response back to unblock the client
        QueryResultsReply reply;
        reply.success = true;
        auto resp_p = reply.serialize();
        RpcHeader resp_h;
        resp_h.type = RpcType::QueryResults;
        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
        std::array<char, 8> h_buf{};
        resp_h.encode(h_buf.data());
        static_cast<void>(send(fd, h_buf.data(), 8, 0));
        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
    });
    ASSERT_TRUE(target_node.start());

    // 2. Node A pushes data
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        RpcClient client("127.0.0.1", 7500);
        ASSERT_TRUE(client.connect());

        PushDataArgs args;
        args.table_name = "users";
        std::vector<common::Value> vals1; vals1.push_back(common::Value::make_int64(1));
        std::vector<common::Value> vals2; vals2.push_back(common::Value::make_int64(2));
        args.rows.emplace_back(std::move(vals1));
        args.rows.emplace_back(std::move(vals2));

        std::vector<uint8_t> resp;
        ASSERT_TRUE(client.call(RpcType::PushData, args.serialize(), resp));

        // Verify while client is connected
        EXPECT_EQ(received_rows.load(), 2);
        EXPECT_EQ(received_table, "users");
        
        client.disconnect();
    }

    // 3. Stop server
    target_node.stop();
}

TEST(DistributedExecutorTests, BroadcastJoinOrchestration) {
    // 1. Setup mock shards
    RpcServer node1(7600);
    RpcServer node2(7601);
    
    std::atomic<int> push_calls{0};
    
    auto handler = [&](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
        QueryResultsReply reply;
        reply.success = true;
        
        if (h.type == RpcType::ExecuteFragment) {
            auto args = ExecuteFragmentArgs::deserialize(p);
            // If it's the fetch part of broadcast: "SELECT * FROM small_table"
            if (args.sql.find("small_table") != std::string::npos) {
                std::vector<common::Value> vals;
                vals.push_back(common::Value::make_int64(1));
                reply.rows.emplace_back(std::move(vals));
            }
        } else if (h.type == RpcType::PushData) {
            push_calls++;
        }
        
        auto resp_p = reply.serialize();
        RpcHeader resp_h;
        resp_h.type = RpcType::QueryResults;
        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
        std::array<char, 8> h_buf{};
        resp_h.encode(h_buf.data());
        static_cast<void>(send(fd, h_buf.data(), 8, 0));
        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
    };

    node1.set_handler(RpcType::ExecuteFragment, handler);
    node1.set_handler(RpcType::PushData, handler);
    node2.set_handler(RpcType::ExecuteFragment, handler);
    node2.set_handler(RpcType::PushData, handler);
    
    ASSERT_TRUE(node1.start());
    ASSERT_TRUE(node2.start());

    // 2. Setup Coordinator
    auto catalog = Catalog::create();
    const config::Config config;
    ClusterManager cm(&config);
    cm.register_node("n1", "127.0.0.1", 7600, config::RunMode::Data);
    cm.register_node("n2", "127.0.0.1", 7601, config::RunMode::Data);
    DistributedExecutor exec(*catalog, cm);

    // 3. Execute JOIN
    // Use a format that build_plan understands
    auto lexer = std::make_unique<Lexer>("SELECT * FROM big_table JOIN small_table ON big_table.id = small_table.id");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    
    // This should trigger broadcast_table("small_table")
    auto res = exec.execute(*stmt, "SELECT * FROM big_table JOIN small_table ON big_table.id = small_table.id");

    // 4. Verify orchestration
    // Each node should have been asked to fetch (2 calls) AND each node should have received push (2 calls)
    // Wait for async operations if any (though currently broadcast_table is synchronous loop)
    EXPECT_GE(push_calls.load(), 2);
    EXPECT_TRUE(res.success());

    node1.stop();
    node2.stop();
}

} // namespace
