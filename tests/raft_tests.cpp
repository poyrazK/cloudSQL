/**
 * @file raft_tests.cpp
 * @brief Unit tests for Raft consensus implementation
 */

#include <gtest/gtest.h>

#include "common/cluster_manager.hpp"
#include "common/config.hpp"
#include "distributed/raft_group.hpp"
#include "distributed/raft_manager.hpp"
#include "network/rpc_server.hpp"

using namespace cloudsql;
using namespace cloudsql::raft;

namespace {

TEST(RaftTests, StateTransitions) {
    config::Config config;
    config.mode = config::RunMode::Coordinator;
    constexpr uint16_t TEST_PORT = 6000;
    config.cluster_port = TEST_PORT;

    cluster::ClusterManager cm(&config);
    network::RpcServer rpc(TEST_PORT);

    RaftGroup group(1, "node1", cm, rpc);
    EXPECT_FALSE(group.is_leader());
}

}  // namespace
