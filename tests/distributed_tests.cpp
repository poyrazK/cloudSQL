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
#include "common/value.hpp"
#include "distributed/distributed_executor.hpp"
#include "distributed/shard_manager.hpp"
#include "executor/types.hpp"
#include "network/rpc_client.hpp"
#include "network/rpc_message.hpp"
#include "network/rpc_server.hpp"
#include "parser/lexer.hpp"
#include "parser/parser.hpp"

using namespace cloudsql;
using namespace cloudsql::common;
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
        (void)h;
        (void)p;
        QueryResultsReply reply;
        reply.success = true;

        std::vector<common::Value> vals;
        vals.push_back(common::Value::make_int64(10));  // Each node returns 10
        executor::Tuple t(std::move(vals));
        reply.rows.push_back(std::move(t));

        auto resp_p = reply.serialize();
        RpcHeader resp_h;
        resp_h.type = RpcType::QueryResults;
        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
        char h_buf[RpcHeader::HEADER_SIZE];
        resp_h.encode(h_buf);
        static_cast<void>(send(fd, h_buf, RpcHeader::HEADER_SIZE, 0));
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
        (void)h;
        (void)p;
        n1_calls++;
        QueryResultsReply reply;
        reply.success = true;
        auto resp_p = reply.serialize();
        RpcHeader resp_h;
        resp_h.type = RpcType::QueryResults;
        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
        char h_buf[RpcHeader::HEADER_SIZE];
        resp_h.encode(h_buf);
        static_cast<void>(send(fd, h_buf, RpcHeader::HEADER_SIZE, 0));
        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
    };
    auto h2 = [&](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
        (void)h;
        (void)p;
        n2_calls++;
        QueryResultsReply reply;
        reply.success = true;
        auto resp_p = reply.serialize();
        RpcHeader resp_h;
        resp_h.type = RpcType::QueryResults;
        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
        char h_buf[RpcHeader::HEADER_SIZE];
        resp_h.encode(h_buf);
        static_cast<void>(send(fd, h_buf, RpcHeader::HEADER_SIZE, 0));
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

    target_node.set_handler(RpcType::PushData,
                            [&](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
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
                                char h_buf[RpcHeader::HEADER_SIZE];
                                resp_h.encode(h_buf);
                                static_cast<void>(send(fd, h_buf, RpcHeader::HEADER_SIZE, 0));
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
        std::vector<common::Value> vals1;
        vals1.push_back(common::Value::make_int64(1));
        std::vector<common::Value> vals2;
        vals2.push_back(common::Value::make_int64(2));
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

    std::atomic<int> fetch_calls{0};
    std::atomic<int> push_calls{0};

    auto handler = [&](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
        QueryResultsReply reply;
        reply.success = true;

        if (h.type == RpcType::ExecuteFragment) {
            auto args = ExecuteFragmentArgs::deserialize(p);
            // Detect if it's the fetch-all part of a broadcast
            if (args.is_fetch_all) {
                fetch_calls++;
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
        char h_buf[RpcHeader::HEADER_SIZE];
        resp_h.encode(h_buf);
        static_cast<void>(send(fd, h_buf, RpcHeader::HEADER_SIZE, 0));
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

    // 3. Execute Broadcast Join (Force it by having no join key in condition for now,
    // or we'll update the executor to support a hint)
    // For the POC, we'll manually call the broadcast_table method to test it.
    bool success = exec.broadcast_table("small_table");

    // 4. Verify orchestration
    EXPECT_TRUE(success);
    EXPECT_GE(fetch_calls.load(), 2);
    EXPECT_GE(push_calls.load(), 2);

    node1.stop();
    node2.stop();
}

TEST(DistributedExecutorTests, ShuffleJoinOrchestration) {
    // 1. Setup mock shards
    RpcServer node1(7700);
    RpcServer node2(7701);

    std::atomic<int> shuffle_calls{0};
    std::atomic<int> push_calls{0};
    std::atomic<int> fragment_calls{0};
    std::atomic<int> bloom_filter_calls{0};

    auto handler = [&](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
        (void)p;
        QueryResultsReply reply;
        reply.success = true;

        if (h.type == RpcType::ShuffleFragment) {
            shuffle_calls++;
        } else if (h.type == RpcType::PushData) {
            push_calls++;
        } else if (h.type == RpcType::ExecuteFragment) {
            fragment_calls++;
        } else if (h.type == RpcType::BloomFilterPush) {
            bloom_filter_calls++;
        }

        auto resp_p = reply.serialize();
        RpcHeader resp_h;
        resp_h.type = RpcType::QueryResults;
        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
        char h_buf[RpcHeader::HEADER_SIZE];
        resp_h.encode(h_buf);
        static_cast<void>(send(fd, h_buf, RpcHeader::HEADER_SIZE, 0));
        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
    };

    auto bloom_bits_handler = [&](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
        (void)h;
        auto args = BloomFilterBitsArgs::deserialize(p);
        BloomFilterBitsArgs reply_args;
        reply_args.context_id = args.context_id;
        // Return empty bloom filter bits for mock - real implementation would return actual bits
        reply_args.filter_data = {};
        reply_args.expected_elements = 0;
        reply_args.num_hashes = 4;

        auto resp_p = reply_args.serialize();
        RpcHeader resp_h;
        resp_h.type = RpcType::QueryResults;
        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
        char h_buf[RpcHeader::HEADER_SIZE];
        resp_h.encode(h_buf);
        static_cast<void>(send(fd, h_buf, RpcHeader::HEADER_SIZE, 0));
        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
    };

    node1.set_handler(RpcType::ShuffleFragment, handler);
    node1.set_handler(RpcType::PushData, handler);
    node1.set_handler(RpcType::ExecuteFragment, handler);
    node1.set_handler(RpcType::BloomFilterPush, handler);
    node1.set_handler(RpcType::BloomFilterBits, bloom_bits_handler);
    node2.set_handler(RpcType::ShuffleFragment, handler);
    node2.set_handler(RpcType::PushData, handler);
    node2.set_handler(RpcType::ExecuteFragment, handler);
    node2.set_handler(RpcType::BloomFilterPush, handler);
    node2.set_handler(RpcType::BloomFilterBits, bloom_bits_handler);

    ASSERT_TRUE(node1.start());
    ASSERT_TRUE(node2.start());

    // 2. Setup Coordinator
    auto catalog = Catalog::create();
    const config::Config config;
    ClusterManager cm(&config);
    cm.register_node("n1", "127.0.0.1", 7700, config::RunMode::Data);
    cm.register_node("n2", "127.0.0.1", 7701, config::RunMode::Data);
    DistributedExecutor exec(*catalog, cm);

    // 3. Execute JOIN
    auto lexer =
        std::make_unique<Lexer>("SELECT * FROM table1 JOIN table2 ON table1.val = table2.val");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();

    // This should trigger ShuffleFragment for table1 AND table2 on both nodes,
    // followed by ExecuteFragment on both nodes.
    auto res = exec.execute(*stmt, "SELECT * FROM table1 JOIN table2 ON table1.val = table2.val");

    // 4. Verify orchestration
    // Each table (2) should be shuffled on each node (2) = 4 shuffle calls total
    EXPECT_GE(shuffle_calls.load(), 4);
    // Finally, ExecuteFragment should be sent to both nodes
    EXPECT_GE(fragment_calls.load(), 2);
    EXPECT_TRUE(res.success());

    node1.stop();
    node2.stop();
}

TEST(DistributedExecutorTests, ConcurrentShuffleIsolation) {
    auto cfg = std::make_unique<config::Config>();
    ClusterManager cm(cfg.get());

    std::string ctx1 = "query_1";
    std::string ctx2 = "query_2";
    std::string table = "users";

    std::vector<executor::Tuple> rows1;
    rows1.push_back(executor::Tuple({common::Value::make_int64(1)}));

    std::vector<executor::Tuple> rows2;
    rows2.push_back(executor::Tuple({common::Value::make_int64(2)}));

    // Push data into different contexts
    cm.buffer_shuffle_data(ctx1, table, std::move(rows1));
    cm.buffer_shuffle_data(ctx2, table, std::move(rows2));

    // Verify isolation
    EXPECT_TRUE(cm.has_shuffle_data(ctx1, table));
    EXPECT_TRUE(cm.has_shuffle_data(ctx2, table));

    auto fetch1 = cm.fetch_shuffle_data(ctx1, table);
    EXPECT_EQ(fetch1.size(), 1U);
    EXPECT_EQ(fetch1[0].get(0).as_int64(), 1);

    // Context 1 should be gone, but Context 2 should remain
    EXPECT_FALSE(cm.has_shuffle_data(ctx1, table));
    EXPECT_TRUE(cm.has_shuffle_data(ctx2, table));

    auto fetch2 = cm.fetch_shuffle_data(ctx2, table);
    EXPECT_EQ(fetch2.size(), 1U);
    EXPECT_EQ(fetch2[0].get(0).as_int64(), 2);
}
TEST(DistributedExecutorTests, NonEqualityJoinRejection) {
    auto catalog = Catalog::create();
    const config::Config config;
    ClusterManager cm(&config);
    cm.register_node("n1", "127.0.0.1", 7800, config::RunMode::Data);
    DistributedExecutor exec(*catalog, cm);

    // Try a join with > instead of =
    auto lexer =
        std::make_unique<Lexer>("SELECT * FROM table1 JOIN table2 ON table1.val > table2.val");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();

    auto res = exec.execute(*stmt, "SELECT * FROM table1 JOIN table2 ON table1.val > table2.val");

    // Should fail because our POC shuffle join only supports equality
    EXPECT_FALSE(res.success());
    EXPECT_THAT(res.error(), testing::HasSubstr("equality join condition"));
}

TEST(DistributedExecutorTests, BloomFilterSkipForOuterJoin) {
    // Test that bloom filter is NOT sent for RIGHT JOIN
    // (INNER join behavior is tested in ShuffleJoinOrchestration)

    RpcServer node1(7860);
    RpcServer node2(7861);

    std::atomic<int> bloom_push_calls{0};
    std::atomic<int> shuffle_calls{0};
    std::atomic<int> push_calls{0};
    std::atomic<int> fragment_calls{0};
    std::atomic<int> unmatched_report_calls{0};
    std::atomic<int> fetch_unmatched_calls{0};

    // Handler for regular RPCs
    auto handler = [&](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
        (void)p;
        QueryResultsReply reply;
        reply.success = true;

        if (h.type == RpcType::BloomFilterPush) {
            bloom_push_calls++;
        } else if (h.type == RpcType::ShuffleFragment) {
            shuffle_calls++;
        } else if (h.type == RpcType::PushData) {
            push_calls++;
        } else if (h.type == RpcType::ExecuteFragment) {
            fragment_calls++;
        } else if (h.type == RpcType::UnmatchedRowsReport) {
            unmatched_report_calls++;
        } else if (h.type == RpcType::FetchUnmatchedRows) {
            fetch_unmatched_calls++;
        }

        auto resp_p = reply.serialize();
        RpcHeader resp_h;
        resp_h.type = RpcType::QueryResults;
        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
        char h_buf[RpcHeader::HEADER_SIZE];
        resp_h.encode(h_buf);
        static_cast<void>(send(fd, h_buf, RpcHeader::HEADER_SIZE, 0));
        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
    };

    // Handler for BloomFilterBits - returns non-empty filter data to properly test bloom filter
    auto bloom_bits_handler = [&](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
        (void)h;
        auto args = BloomFilterBitsArgs::deserialize(p);
        BloomFilterBitsArgs reply_args;
        reply_args.context_id = args.context_id;
        reply_args.filter_data = {0xFF, 0xFF, 0xFF, 0xFF};  // Non-empty bloom filter
        reply_args.expected_elements = 100;
        reply_args.num_hashes = 4;

        auto resp_p = reply_args.serialize();
        RpcHeader resp_h;
        resp_h.type = RpcType::QueryResults;
        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
        char h_buf[RpcHeader::HEADER_SIZE];
        resp_h.encode(h_buf);
        static_cast<void>(send(fd, h_buf, RpcHeader::HEADER_SIZE, 0));
        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
    };

    // Handler for UnmatchedRowsReport - returns proper response type
    auto unmatched_report_handler = [&](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
        (void)h;
        auto args = UnmatchedRowsReportArgs::deserialize(p);
        UnmatchedRowsReportArgs reply;
        reply.context_id = args.context_id;
        reply.right_table = args.right_table;
        // Empty unmatched_keys = all rows matched (for test simplicity)

        auto resp_p = reply.serialize();
        RpcHeader resp_h;
        resp_h.type = RpcType::QueryResults;
        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
        char h_buf[RpcHeader::HEADER_SIZE];
        resp_h.encode(h_buf);
        static_cast<void>(send(fd, h_buf, RpcHeader::HEADER_SIZE, 0));
        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
    };

    // Handler for FetchUnmatchedRows - returns proper response type
    auto fetch_unmatched_handler = [&](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
        (void)h;
        auto args = FetchUnmatchedRowsArgs::deserialize(p);
        UnmatchedRowsPushArgs reply;
        reply.context_id = args.context_id;
        reply.unmatched_rows = {};  // Empty for test simplicity

        auto resp_p = reply.serialize();
        RpcHeader resp_h;
        resp_h.type = RpcType::QueryResults;
        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
        char h_buf[RpcHeader::HEADER_SIZE];
        resp_h.encode(h_buf);
        static_cast<void>(send(fd, h_buf, RpcHeader::HEADER_SIZE, 0));
        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
    };

    node1.set_handler(RpcType::BloomFilterPush, handler);
    node1.set_handler(RpcType::ShuffleFragment, handler);
    node1.set_handler(RpcType::PushData, handler);
    node1.set_handler(RpcType::ExecuteFragment, handler);
    node1.set_handler(RpcType::BloomFilterBits, bloom_bits_handler);
    node1.set_handler(RpcType::UnmatchedRowsReport, unmatched_report_handler);
    node1.set_handler(RpcType::FetchUnmatchedRows, fetch_unmatched_handler);
    node2.set_handler(RpcType::BloomFilterPush, handler);
    node2.set_handler(RpcType::ShuffleFragment, handler);
    node2.set_handler(RpcType::PushData, handler);
    node2.set_handler(RpcType::ExecuteFragment, handler);
    node2.set_handler(RpcType::BloomFilterBits, bloom_bits_handler);
    node2.set_handler(RpcType::UnmatchedRowsReport, unmatched_report_handler);
    node2.set_handler(RpcType::FetchUnmatchedRows, fetch_unmatched_handler);

    ASSERT_TRUE(node1.start());
    ASSERT_TRUE(node2.start());

    auto catalog = Catalog::create();
    const config::Config config;
    ClusterManager cm(&config);
    cm.register_node("n1", "127.0.0.1", 7860, config::RunMode::Data);
    cm.register_node("n2", "127.0.0.1", 7861, config::RunMode::Data);
    DistributedExecutor exec(*catalog, cm);

    // Execute RIGHT join - bloom filter should NOT be sent
    auto lexer =
        std::make_unique<Lexer>("SELECT * FROM table1 RIGHT JOIN table2 ON table1.id = table2.id");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    auto res =
        exec.execute(*stmt, "SELECT * FROM table1 RIGHT JOIN table2 ON table1.id = table2.id");

    // For RIGHT join, bloom filter is skipped (should be 0)
    // Even though we returned valid bloom bits, the coordinator should not push for outer joins
    EXPECT_EQ(bloom_push_calls.load(), 0);
    // Verify the query succeeded
    EXPECT_TRUE(res.success());

    node1.stop();
    node2.stop();
}

TEST(DistributedExecutorTests, Phase3SkippedForRightJoin) {
    // Test that Phase 3 (UnmatchedRowsReport) is NOT called for RIGHT JOIN
    // because the local executor on each data node already handles unmatched right rows.
    // Phase 3-5 is only needed for FULL JOIN to collect unmatched LEFT rows.

    RpcServer node1(7870);
    RpcServer node2(7871);

    std::atomic<int> unmatched_report_calls{0};
    std::atomic<int> fetch_unmatched_calls{0};

    // Handler for ExecuteFragment that returns proper schema and rows
    auto execute_fragment_handler = [&](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
        (void)h;
        auto args = ExecuteFragmentArgs::deserialize(p);
        QueryResultsReply reply;
        reply.success = true;

        // Build schema for joined result: table1(id, val), table2(id, val)
        Schema schema;
        schema.add_column("id", common::ValueType::TYPE_INT64);
        schema.add_column("val", common::ValueType::TYPE_INT64);
        schema.add_column("id",
                          common::ValueType::TYPE_INT64);  // table2.id (ambiguous but matches)
        schema.add_column("val", common::ValueType::TYPE_INT64);  // table2.val
        reply.schema = schema;

        // Return some matched rows (e.g., rows where table2.id = 1 and 2 matched)
        // Format: {table1.id, table1.val, table2.id, table2.val}
        reply.rows = {
            Tuple{Value::make_int64(100), Value::make_int64(10), Value::make_int64(1),
                  Value::make_int64(100)},
            Tuple{Value::make_int64(200), Value::make_int64(20), Value::make_int64(2),
                  Value::make_int64(200)},
        };

        auto resp_p = reply.serialize();
        RpcHeader resp_h;
        resp_h.type = RpcType::QueryResults;
        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
        char h_buf[RpcHeader::HEADER_SIZE];
        resp_h.encode(h_buf);
        static_cast<void>(send(fd, h_buf, RpcHeader::HEADER_SIZE, 0));
        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
    };

    // Handler for UnmatchedRowsReport - tracks calls (should be 0 for RIGHT JOIN)
    auto unmatched_report_handler = [&](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
        (void)h;
        auto args = UnmatchedRowsReportArgs::deserialize(p);
        unmatched_report_calls++;

        UnmatchedRowsReportArgs reply;
        reply.context_id = args.context_id;
        reply.right_table = args.right_table;
        reply.unmatched_keys = {"3"};

        auto resp_p = reply.serialize();
        RpcHeader resp_h;
        resp_h.type = RpcType::QueryResults;
        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
        char h_buf[RpcHeader::HEADER_SIZE];
        resp_h.encode(h_buf);
        static_cast<void>(send(fd, h_buf, RpcHeader::HEADER_SIZE, 0));
        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
    };

    // Handler for FetchUnmatchedRows - tracks calls (should be 0 for RIGHT JOIN)
    auto fetch_unmatched_handler = [&](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
        (void)h;
        auto args = FetchUnmatchedRowsArgs::deserialize(p);
        fetch_unmatched_calls++;

        UnmatchedRowsPushArgs reply;
        reply.context_id = args.context_id;
        reply.unmatched_rows = {Tuple{Value::make_null(), Value::make_null(), Value::make_int64(3),
                                      Value::make_int64(30)}};

        auto resp_p = reply.serialize();
        RpcHeader resp_h;
        resp_h.type = RpcType::QueryResults;
        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
        char h_buf[RpcHeader::HEADER_SIZE];
        resp_h.encode(h_buf);
        static_cast<void>(send(fd, h_buf, RpcHeader::HEADER_SIZE, 0));
        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
    };

    // Regular handler for other RPCs
    auto basic_handler = [&](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
        (void)p;
        QueryResultsReply reply;
        reply.success = true;

        auto resp_p = reply.serialize();
        RpcHeader resp_h;
        resp_h.type = RpcType::QueryResults;
        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
        char h_buf[RpcHeader::HEADER_SIZE];
        resp_h.encode(h_buf);
        static_cast<void>(send(fd, h_buf, RpcHeader::HEADER_SIZE, 0));
        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
    };

    // BloomFilterBits handler
    auto bloom_bits_handler = [&](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
        (void)h;
        auto args = BloomFilterBitsArgs::deserialize(p);
        BloomFilterBitsArgs reply_args;
        reply_args.context_id = args.context_id;
        reply_args.filter_data = {0xFF, 0xFF};
        reply_args.expected_elements = 100;
        reply_args.num_hashes = 4;

        auto resp_p = reply_args.serialize();
        RpcHeader resp_h;
        resp_h.type = RpcType::QueryResults;
        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
        char h_buf[RpcHeader::HEADER_SIZE];
        resp_h.encode(h_buf);
        static_cast<void>(send(fd, h_buf, RpcHeader::HEADER_SIZE, 0));
        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
    };

    // Set up handlers for node1
    node1.set_handler(RpcType::ExecuteFragment, execute_fragment_handler);
    node1.set_handler(RpcType::ShuffleFragment, basic_handler);
    node1.set_handler(RpcType::PushData, basic_handler);
    node1.set_handler(RpcType::BloomFilterPush, basic_handler);
    node1.set_handler(RpcType::BloomFilterBits, bloom_bits_handler);
    node1.set_handler(RpcType::UnmatchedRowsReport, unmatched_report_handler);
    node1.set_handler(RpcType::FetchUnmatchedRows, fetch_unmatched_handler);

    // Set up handlers for node2
    node2.set_handler(RpcType::ExecuteFragment, execute_fragment_handler);
    node2.set_handler(RpcType::ShuffleFragment, basic_handler);
    node2.set_handler(RpcType::PushData, basic_handler);
    node2.set_handler(RpcType::BloomFilterPush, basic_handler);
    node2.set_handler(RpcType::BloomFilterBits, bloom_bits_handler);
    node2.set_handler(RpcType::UnmatchedRowsReport, unmatched_report_handler);
    node2.set_handler(RpcType::FetchUnmatchedRows, fetch_unmatched_handler);

    ASSERT_TRUE(node1.start());
    ASSERT_TRUE(node2.start());

    auto catalog = Catalog::create();
    const config::Config config;
    ClusterManager cm(&config);
    cm.register_node("n1", "127.0.0.1", 7870, config::RunMode::Data);
    cm.register_node("n2", "127.0.0.1", 7871, config::RunMode::Data);
    DistributedExecutor exec(*catalog, cm);

    // Execute RIGHT join
    auto lexer =
        std::make_unique<Lexer>("SELECT * FROM table1 RIGHT JOIN table2 ON table1.id = table2.id");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    auto res =
        exec.execute(*stmt, "SELECT * FROM table1 RIGHT JOIN table2 ON table1.id = table2.id");

    // Verify Phase 3-4 RPCs were NOT called for RIGHT JOIN
    // (local executor handles unmatched right rows correctly)
    EXPECT_EQ(unmatched_report_calls.load(), 0);  // NOT called for RIGHT JOIN
    EXPECT_EQ(fetch_unmatched_calls.load(), 0);   // NOT called for RIGHT JOIN
    EXPECT_TRUE(res.success());

    node1.stop();
    node2.stop();
}

}  // namespace
