/**
 * @file raft_group_tests.cpp
 * @brief Unit tests for RaftGroup consensus implementation
 */

#include <gtest/gtest.h>

#include <atomic>
#include <cstdio>
#include <memory>
#include <vector>

#include "common/cluster_manager.hpp"
#include "common/config.hpp"
#include "distributed/raft_group.hpp"
#include "distributed/raft_manager.hpp"
#include "network/rpc_server.hpp"

using namespace cloudsql;
using namespace cloudsql::raft;
using namespace cloudsql::cluster;
using namespace cloudsql::network;

namespace {

class RaftGroupTests : public ::testing::Test {
   protected:
    void SetUp() override {
        config_.mode = config::RunMode::Coordinator;
        constexpr uint16_t TEST_PORT = 6200;
        config_.cluster_port = TEST_PORT;
        cm_ = std::make_unique<ClusterManager>(&config_);
        rpc_ = std::make_unique<RpcServer>(TEST_PORT);
        ASSERT_TRUE(rpc_->start()) << "RpcServer failed to start - port may be in use";
        manager_ = std::make_unique<RaftManager>("node1", *cm_, *rpc_);
        group_ = manager_->get_or_create_group(1);
    }

    void TearDown() override {
        if (group_) {
            group_->stop();
        }
        if (manager_) {
            manager_->stop();
        }
        if (rpc_) {
            rpc_->stop();
        }
        // Cleanup state files for all possible group IDs
        std::remove("raft_group_1.state");
        std::remove("raft_group_2.state");
        std::remove("raft_group_3.state");
    }

    config::Config config_;
    std::unique_ptr<ClusterManager> cm_;
    std::unique_ptr<RpcServer> rpc_;
    std::unique_ptr<RaftManager> manager_;
    std::shared_ptr<RaftGroup> group_;
};

// ============= Constructor and Basic Tests =============

TEST_F(RaftGroupTests, ConstructorInitialState) {
    EXPECT_NE(group_, nullptr);
    EXPECT_FALSE(group_->is_leader());
    EXPECT_EQ(group_->group_id(), 1);
}

TEST_F(RaftGroupTests, StartStopLifecycle) {
    group_->start();
    group_->stop();
    group_->start();
    group_->stop();
    SUCCEED();  // No crash
}

// ============= State Machine Tests =============

TEST_F(RaftGroupTests, SetStateMachine) {
    // Should not crash when setting state machine
    group_->set_state_machine(nullptr);
    SUCCEED();
}

// ============= Log Replication Tests =============

TEST_F(RaftGroupTests, ReplicateNotLeader) {
    // Before becoming leader, replicate should fail
    std::vector<uint8_t> data = {1, 2, 3};
    EXPECT_FALSE(group_->replicate(data));
}

TEST_F(RaftGroupTests, ReplicateDataSize) {
    // Test with various data sizes
    std::vector<uint8_t> small_data = {1};
    std::vector<uint8_t> large_data(1024, 42);

    // Both should fail when not leader
    EXPECT_FALSE(group_->replicate(small_data));
    EXPECT_FALSE(group_->replicate(large_data));
}

// ============= Timeout Tests =============

// Note: get_random_timeout() is private and tested indirectly through
// election timing behavior in integration tests

// ============= RPC Handler Serialization Tests =============

TEST_F(RaftGroupTests, RequestVoteArgsSerialization) {
    RequestVoteArgs args;
    args.term = 5;
    args.candidate_id = "node2";
    args.last_log_index = 10;
    args.last_log_term = 3;

    auto serialized = args.serialize();

    // Should have: 8 (term) + 8 (id_len) + id + 8 (last_log_index) + 8 (last_log_term)
    EXPECT_EQ(serialized.size(), 8 + 8 + 5 + 8 + 8);
}

TEST_F(RaftGroupTests, AppendEntriesArgsStructure) {
    AppendEntriesArgs args;
    args.term = 1;
    args.leader_id = "leader1";
    args.prev_log_index = 5;
    args.prev_log_term = 1;
    args.leader_commit = 3;

    // Verify all fields are set correctly
    EXPECT_EQ(args.term, 1);
    EXPECT_EQ(args.leader_id, "leader1");
    EXPECT_EQ(args.prev_log_index, 5);
    EXPECT_EQ(args.prev_log_term, 1);
    EXPECT_EQ(args.leader_commit, 3);
    EXPECT_TRUE(args.entries.empty());
}

// ============= Log Entry Tests =============

TEST_F(RaftGroupTests, LogEntryDefaultValues) {
    LogEntry entry;
    EXPECT_EQ(entry.term, 0);
    EXPECT_EQ(entry.index, 0);
    EXPECT_TRUE(entry.data.empty());
}

TEST_F(RaftGroupTests, LogEntryWithData) {
    LogEntry entry;
    entry.term = 1;
    entry.index = 5;
    entry.data = {1, 2, 3, 4, 5};

    EXPECT_EQ(entry.term, 1);
    EXPECT_EQ(entry.index, 5);
    EXPECT_EQ(entry.data.size(), 5);
}

// ============= Persistent State Tests =============

TEST_F(RaftGroupTests, PersistentStateDefaultValues) {
    RaftPersistentState state;
    EXPECT_EQ(state.current_term, 0);
    EXPECT_TRUE(state.voted_for.empty());
    EXPECT_TRUE(state.log.empty());
}

TEST_F(RaftGroupTests, VolatileStateDefaultValues) {
    RaftVolatileState state;
    EXPECT_EQ(state.commit_index, 0);
    EXPECT_EQ(state.last_applied, 0);
}

// ============= Leader State Tests =============

TEST_F(RaftGroupTests, LeaderStateEmpty) {
    LeaderState state;
    EXPECT_TRUE(state.next_index.empty());
    EXPECT_TRUE(state.match_index.empty());
}

TEST_F(RaftGroupTests, LeaderStateWithPeers) {
    LeaderState state;
    state.next_index["node2"] = 1;
    state.next_index["node3"] = 1;
    state.match_index["node2"] = 0;
    state.match_index["node3"] = 0;

    EXPECT_EQ(state.next_index.size(), 2);
    EXPECT_EQ(state.match_index.size(), 2);
    EXPECT_EQ(state.next_index["node2"], 1);
    EXPECT_EQ(state.match_index["node3"], 0);
}

// ============= Vote Reply Tests =============

TEST_F(RaftGroupTests, RequestVoteReplyStructure) {
    RequestVoteReply reply;
    reply.term = 5;
    reply.vote_granted = true;

    EXPECT_EQ(reply.term, 5);
    EXPECT_TRUE(reply.vote_granted);
}

// ============= AppendEntries Reply Tests =============

TEST_F(RaftGroupTests, AppendEntriesReplyStructure) {
    AppendEntriesReply reply;
    reply.term = 3;
    reply.success = true;

    EXPECT_EQ(reply.term, 3);
    EXPECT_TRUE(reply.success);
}

// ============= NodeState Tests =============

TEST_F(RaftGroupTests, NodeStateEnum) {
    EXPECT_EQ(static_cast<uint8_t>(NodeState::Follower), 0);
    EXPECT_EQ(static_cast<uint8_t>(NodeState::Candidate), 1);
    EXPECT_EQ(static_cast<uint8_t>(NodeState::Leader), 2);
    EXPECT_EQ(static_cast<uint8_t>(NodeState::Shutdown), 3);
}

// ============= Group ID Tests =============

TEST_F(RaftGroupTests, DifferentGroupIds) {
    auto group1 = manager_->get_or_create_group(1);
    auto group2 = manager_->get_or_create_group(2);

    EXPECT_NE(group1, group2);
    EXPECT_EQ(group1->group_id(), 1);
    EXPECT_EQ(group2->group_id(), 2);
}

}  // namespace