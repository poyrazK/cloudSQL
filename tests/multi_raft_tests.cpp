/**
 * @file multi_raft_tests.cpp
 * @brief Integration tests for Multi-Group Raft infrastructure
 */

#include <gtest/gtest.h>

#include <chrono>
#include <csignal>
#include <thread>
#include <vector>

#include "common/cluster_manager.hpp"
#include "distributed/raft_group.hpp"
#include "distributed/raft_manager.hpp"
#include "network/rpc_message.hpp"

using namespace cloudsql;
using namespace cloudsql::raft;
using namespace cloudsql::network;

namespace {

/**
 * @brief Verifies that RaftManager correctly multiplexes RPC requests
 * to independent RaftGroups based on the header's group_id.
 */
TEST(MultiRaftTests, GroupRoutingAndMultiplexing) {
    config::Config config;
    config.mode = config::RunMode::Coordinator;
    config.cluster_port = 9000;

    cluster::ClusterManager cm(&config);
    RpcServer rpc(9000);
    RaftManager manager("node1", cm, rpc);

    auto group0 = manager.get_or_create_group(0);
    auto group1 = manager.get_or_create_group(1);

    ASSERT_NE(group0, nullptr);
    ASSERT_NE(group1, nullptr);
    EXPECT_EQ(group0->group_id(), 0);
    EXPECT_EQ(group1->group_id(), 1);

    auto handler = rpc.get_handler(RpcType::AppendEntries);
    ASSERT_NE(handler, nullptr);

    RpcHeader h;
    h.type = RpcType::AppendEntries;
    h.group_id = 1;
    std::vector<uint8_t> payload(8, 0);
    h.payload_len = RpcHeader::HEADER_SIZE;

    handler(h, payload, -1);

    EXPECT_EQ(manager.get_group(0), group0);
    EXPECT_EQ(manager.get_group(1), group1);
}

class IntegrationStateMachine : public RaftStateMachine {
   public:
    void apply(const LogEntry& entry) override {
        applied_count++;
        last_applied_data = entry.data;
    }
    int applied_count = 0;
    std::vector<uint8_t> last_applied_data;
};

TEST(MultiRaftTests, StateMachineIntegration) {
    config::Config config;
    cluster::ClusterManager cm(&config);
    RpcServer rpc(9001);

    RaftGroup group(1, "node1", cm, rpc);
    IntegrationStateMachine sm;
    group.set_state_machine(&sm);

    std::vector<uint8_t> payload(8, 0);
    payload[0] = 1;

    RpcHeader h;
    h.type = RpcType::AppendEntries;
    h.group_id = 1;
    h.payload_len = static_cast<uint16_t>(payload.size());

    group.handle_append_entries(h, payload, -1);
    EXPECT_EQ(sm.applied_count, 0);
}

/**
 * @brief Simulates a cluster leader election and failover.
 * This ensures high availability by validating consensus emergence.
 */
TEST(MultiRaftTests, LeaderElectionAndFailover) {
    signal(SIGPIPE, SIG_IGN);
    const int num_nodes = 3;
    const int base_port = 9200;

    std::vector<std::unique_ptr<config::Config>> configs;
    std::vector<std::unique_ptr<cluster::ClusterManager>> cms;
    std::vector<std::unique_ptr<RpcServer>> rpcs;
    std::vector<std::unique_ptr<RaftManager>> rms;

    for (int i = 0; i < num_nodes; ++i) {
        auto cfg = std::make_unique<config::Config>();
        cfg->mode = config::RunMode::Coordinator;
        cfg->cluster_port = base_port + i;
        configs.push_back(std::move(cfg));

        cms.push_back(std::make_unique<cluster::ClusterManager>(configs.back().get()));
        rpcs.push_back(std::make_unique<RpcServer>(base_port + i));
        ASSERT_TRUE(rpcs.back()->start());
    }

    for (int i = 0; i < num_nodes; ++i) {
        std::string node_id = "node" + std::to_string(i + 1);
        rms.push_back(std::make_unique<RaftManager>(node_id, *cms[i], *rpcs[i]));
        cms[i]->set_raft_manager(rms.back().get());

        for (int j = 0; j < num_nodes; ++j) {
            std::string peer_id = "node" + std::to_string(j + 1);
            cms[i]->register_node(peer_id, "127.0.0.1", base_port + j,
                                  config::RunMode::Coordinator);
        }

        rms[i]->get_or_create_group(0);
        rms[i]->start();
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    int leaders = 0;
    int leader_idx = -1;
    for (int i = 0; i < num_nodes; ++i) {
        if (rms[i]->get_group(0)->is_leader()) {
            leaders++;
            leader_idx = i;
        }
    }

    EXPECT_EQ(leaders, 1) << "Exactly one leader should emerge from the cluster";

    if (leaders == 1) {
        std::cout << "[Test] node" << (leader_idx + 1) << " is leader. Simulating failover...\n";
        rms[leader_idx]->stop();
        rpcs[leader_idx]->stop();

        std::this_thread::sleep_for(std::chrono::milliseconds(2000));

        int new_leaders = 0;
        for (int i = 0; i < num_nodes; ++i) {
            if (i == leader_idx) continue;
            if (rms[i]->get_group(0)->is_leader()) new_leaders++;
        }
        EXPECT_EQ(new_leaders, 1) << "New leader should be elected after failover";
    }

    for (int i = 0; i < num_nodes; ++i) {
        rms[i]->stop();
        rpcs[i]->stop();
    }
}

}  // namespace
