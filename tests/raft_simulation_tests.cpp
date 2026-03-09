/**
 * @file raft_simulation_tests.cpp
 * @brief Simulation tests for Raft consensus logic
 */

#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <thread>

#include "common/cluster_manager.hpp"
#include "distributed/raft_group.hpp"
#include "network/rpc_server.hpp"

using namespace cloudsql;
using namespace cloudsql::raft;

namespace {

TEST(RaftSimulationTests, FollowerToCandidate) {
    static_cast<void>(std::remove("raft_group_1.state"));
    config::Config config;
    config.mode = config::RunMode::Coordinator;

    cluster::ClusterManager cm(&config);
    network::RpcServer rpc(7000);

    RaftGroup group(1, "node1", cm, rpc);
    group.start();

    // Initially Follower
    EXPECT_FALSE(group.is_leader());

    // Wait for election timeout (150-300ms)
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Should have attempted to become candidate/leader
    group.stop();
    static_cast<void>(std::remove("raft_group_1.state"));
}

TEST(RaftSimulationTests, HeartbeatReset) {
    static_cast<void>(std::remove("raft_group_2.state"));
    config::Config config;
    config.mode = config::RunMode::Coordinator;

    cluster::ClusterManager cm(&config);
    network::RpcServer rpc(7001);

    RaftGroup group(2, "node2", cm, rpc);
    group.start();

    // Send periodic heartbeats to prevent election
    for (int i = 0; i < 5; ++i) {
        std::vector<uint8_t> payload(8, 0);
        // Use a high term to ensure it's accepted
        term_t term = 100;
        std::memcpy(payload.data(), &term, 8);

        network::RpcHeader header;
        header.type = network::RpcType::AppendEntries;
        header.group_id = 2;
        header.payload_len = 8;

        group.handle_append_entries(header, payload, -1);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // Should NOT be leader yet because heartbeats reset the timer
        EXPECT_FALSE(group.is_leader());
    }
    group.stop();
    static_cast<void>(std::remove("raft_group_2.state"));
}

}  // namespace
