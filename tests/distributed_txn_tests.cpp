/**
 * @file distributed_txn_tests.cpp
 * @brief Unit tests for 2PC distributed transactions
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include <thread>
#include <chrono>
#include <atomic>

#include "distributed/distributed_executor.hpp"
#include "common/cluster_manager.hpp"
#include "catalog/catalog.hpp"
#include "parser/parser.hpp"
#include "parser/lexer.hpp"
#include "network/rpc_server.hpp"

using namespace cloudsql;
using namespace cloudsql::executor;
using namespace cloudsql::cluster;
using namespace cloudsql::parser;
using namespace cloudsql::network;

namespace {

TEST(DistributedTxnTests, CommitSuccessNoNodes) {
    auto catalog = Catalog::create();
    config::Config config;
    ClusterManager cm(&config);
    DistributedExecutor exec(*catalog, cm);
    
    auto lexer = std::make_unique<Lexer>("COMMIT");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    
    auto res = exec.execute(*stmt, "COMMIT");
    EXPECT_FALSE(res.success()); 
    EXPECT_STREQ(res.error().c_str(), "No active data nodes in cluster");
}

TEST(DistributedTxnTests, TwoPhaseCommitSuccess) {
    RpcServer data_node1(7100);
    RpcServer data_node2(7101);
    
    std::atomic<int> prepare_count{0};
    std::atomic<int> commit_count{0};
    
    auto prepare_handler = [&](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
        (void)h; (void)p;
        prepare_count++;
        QueryResultsReply reply;
        reply.success = true;
        auto resp_p = reply.serialize();
        RpcHeader resp_h;
        resp_h.type = RpcType::QueryResults;
        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
        char h_buf[8];
        resp_h.encode(h_buf);
        static_cast<void>(send(fd, h_buf, 8, 0));
        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
    };

    auto commit_handler = [&](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
        (void)h; (void)p;
        commit_count++;
        QueryResultsReply reply;
        reply.success = true;
        auto resp_p = reply.serialize();
        RpcHeader resp_h;
        resp_h.type = RpcType::QueryResults;
        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
        char h_buf[8];
        resp_h.encode(h_buf);
        static_cast<void>(send(fd, h_buf, 8, 0));
        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
    };

    data_node1.set_handler(RpcType::TxnPrepare, prepare_handler);
    data_node1.set_handler(RpcType::TxnCommit, commit_handler);
    data_node2.set_handler(RpcType::TxnPrepare, prepare_handler);
    data_node2.set_handler(RpcType::TxnCommit, commit_handler);
    
    ASSERT_TRUE(data_node1.start());
    ASSERT_TRUE(data_node2.start());

    auto catalog = Catalog::create();
    config::Config config;
    ClusterManager cm(&config);
    
    cm.register_node("dn1", "127.0.0.1", 7100, config::RunMode::Data);
    cm.register_node("dn2", "127.0.0.1", 7101, config::RunMode::Data);
    
    DistributedExecutor exec(*catalog, cm);

    auto lexer = std::make_unique<Lexer>("COMMIT");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    
    auto res = exec.execute(*stmt, "COMMIT");
    
    EXPECT_TRUE(res.success());
    EXPECT_EQ(prepare_count.load(), 2);
    std::this_thread::sleep_for(std::chrono::milliseconds(50)); 
    EXPECT_EQ(commit_count.load(), 2);
    
    data_node1.stop();
    data_node2.stop();
}

TEST(DistributedTxnTests, TwoPhaseCommitAbortOnFailure) {
    RpcServer data_node1(7200);
    RpcServer data_node2(7201);
    
    std::atomic<int> prepare_count{0};
    std::atomic<int> abort_count{0};
    std::atomic<int> commit_count{0};
    
    auto prepare_handler_success = [&](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
        (void)h; (void)p;
        prepare_count++;
        QueryResultsReply reply;
        reply.success = true;
        auto resp_p = reply.serialize();
        RpcHeader resp_h;
        resp_h.type = RpcType::QueryResults;
        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
        char h_buf[8];
        resp_h.encode(h_buf);
        static_cast<void>(send(fd, h_buf, 8, 0));
        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
    };

    auto prepare_handler_fail = [&](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
        (void)h; (void)p;
        prepare_count++;
        QueryResultsReply reply;
        reply.success = false;
        reply.error_msg = "Failed to lock resources";
        auto resp_p = reply.serialize();
        RpcHeader resp_h;
        resp_h.type = RpcType::QueryResults;
        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
        char h_buf[8];
        resp_h.encode(h_buf);
        static_cast<void>(send(fd, h_buf, 8, 0));
        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
    };

    auto abort_handler = [&](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
        (void)h; (void)p;
        abort_count++;
        QueryResultsReply reply;
        reply.success = true;
        auto resp_p = reply.serialize();
        RpcHeader resp_h;
        resp_h.type = RpcType::QueryResults;
        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
        char h_buf[8];
        resp_h.encode(h_buf);
        static_cast<void>(send(fd, h_buf, 8, 0));
        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
    };

    auto commit_handler = [&](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
        (void)h; (void)p;
        commit_count++;
        QueryResultsReply reply;
        reply.success = true;
        auto resp_p = reply.serialize();
        RpcHeader resp_h;
        resp_h.type = RpcType::QueryResults;
        resp_h.payload_len = static_cast<uint16_t>(resp_p.size());
        char h_buf[8];
        resp_h.encode(h_buf);
        static_cast<void>(send(fd, h_buf, 8, 0));
        static_cast<void>(send(fd, resp_p.data(), resp_p.size(), 0));
    };

    data_node1.set_handler(RpcType::TxnPrepare, prepare_handler_success);
    data_node1.set_handler(RpcType::TxnCommit, commit_handler);
    data_node1.set_handler(RpcType::TxnAbort, abort_handler);
    
    // Node 2 fails prepare
    data_node2.set_handler(RpcType::TxnPrepare, prepare_handler_fail);
    data_node2.set_handler(RpcType::TxnCommit, commit_handler);
    data_node2.set_handler(RpcType::TxnAbort, abort_handler);
    
    ASSERT_TRUE(data_node1.start());
    ASSERT_TRUE(data_node2.start());

    auto catalog = Catalog::create();
    config::Config config;
    ClusterManager cm(&config);
    cm.register_node("dn1", "127.0.0.1", 7200, config::RunMode::Data);
    cm.register_node("dn2", "127.0.0.1", 7201, config::RunMode::Data);
    
    DistributedExecutor exec(*catalog, cm);

    auto lexer = std::make_unique<Lexer>("COMMIT");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    
    auto res = exec.execute(*stmt, "COMMIT");
    
    EXPECT_FALSE(res.success());
    EXPECT_TRUE(res.error().find("Prepare failed: Failed to lock resources") != std::string::npos);
    
    EXPECT_EQ(prepare_count.load(), 2);
    
    std::this_thread::sleep_for(std::chrono::milliseconds(50)); 
    EXPECT_EQ(abort_count.load(), 2);
    EXPECT_EQ(commit_count.load(), 0);
    
    data_node1.stop();
    data_node2.stop();
}

} // namespace
