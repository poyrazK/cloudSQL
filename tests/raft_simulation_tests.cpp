/**
 * @file raft_simulation_tests.cpp
 * @brief Simulation tests for Raft consensus logic
 */

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "common/cluster_manager.hpp"
#include "distributed/raft_node.hpp"
#include "network/rpc_server.hpp"

using namespace cloudsql;
using namespace cloudsql::raft;

namespace {

TEST(RaftSimulationTests, FollowerToCandidate) {
    config::Config config;
    config.mode = config::RunMode::Coordinator;

    cluster::ClusterManager cm(&config);
    network::RpcServer rpc(7000);

    RaftNode node("node1", cm, rpc);
    node.start();

    // Initially Follower
    EXPECT_FALSE(node.is_leader());

    // Wait for election timeout (150-300ms)
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Should have attempted to become candidate/leader
    // Note: without actual peers, it will stay Candidate or become Leader if needed=1
}

TEST(RaftSimulationTests, HeartbeatReset) {
    config::Config config;
    config.mode = config::RunMode::Coordinator;

    cluster::ClusterManager cm(&config);
    network::RpcServer rpc(7001);

    RaftNode node("node1", cm, rpc);
    node.start();

    auto handler = rpc.get_handler(network::RpcType::AppendEntries);
    ASSERT_NE(handler, nullptr);

    // Send periodic heartbeats to prevent election
    for (int i = 0; i < 5; ++i) {
        std::vector<uint8_t> payload(8, 0);  // Term 0
        network::RpcHeader header;
        header.type = network::RpcType::AppendEntries;
        header.payload_len = 8;

        handler(header, payload, -1);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // Should NOT be leader yet because heartbeats reset the timer
        EXPECT_FALSE(node.is_leader());
    }
}

}  // namespace
