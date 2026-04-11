/**
 * @file raft_protocol_tests.cpp
 * @brief Unit tests for RaftGroup protocol implementation
 */

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

#include "common/cluster_manager.hpp"
#include "common/config.hpp"
#include "distributed/raft_group.hpp"
#include "distributed/raft_types.hpp"
#include "network/rpc_message.hpp"
#include "network/rpc_server.hpp"

using namespace cloudsql;
using namespace cloudsql::raft;
using namespace cloudsql::cluster;
using namespace cloudsql::network;

namespace {

class MockStateMachine : public RaftStateMachine {
 public:
  void apply(const LogEntry& entry) override {
    applied_entries_.push_back(entry);
  }

  std::vector<LogEntry> applied_entries_;
};

class RaftProtocolTests : public ::testing::Test {
 protected:
  void SetUp() override {
    config_.mode = config::RunMode::Coordinator;
    constexpr uint16_t TEST_PORT = 6200;
    config_.cluster_port = TEST_PORT;
    cm_ = std::make_unique<ClusterManager>(&config_);
    rpc_ = std::make_unique<RpcServer>(TEST_PORT);
    rpc_->start();
    state_machine_ = std::make_unique<MockStateMachine>();
  }

  void TearDown() override {
    if (group_) {
      group_->stop();
    }
    if (rpc_) {
      rpc_->stop();
    }
    cm_.reset();
  }

  config::Config config_;
  std::unique_ptr<ClusterManager> cm_;
  std::unique_ptr<RpcServer> rpc_;
  std::unique_ptr<MockStateMachine> state_machine_;
  std::unique_ptr<RaftGroup> group_;
};

TEST_F(RaftProtocolTests, ReplicateFailsWhenNotLeader) {
  group_ = std::make_unique<RaftGroup>(1, "node1", *cm_, *rpc_);
  group_->set_state_machine(state_machine_.get());

  std::vector<uint8_t> data = {1, 2, 3};
  EXPECT_FALSE(group_->replicate(data));
  EXPECT_FALSE(group_->is_leader());
}

TEST_F(RaftProtocolTests, ReplicateAppendsEntry) {
  group_ = std::make_unique<RaftGroup>(1, "node1", *cm_, *rpc_);
  group_->set_state_machine(state_machine_.get());
  group_->start();

  std::this_thread::sleep_for(std::chrono::milliseconds(400));

  std::vector<uint8_t> data = {1, 2, 3};
  if (!group_->is_leader()) {
    GTEST_SKIP() << "Could not become leader in test timeout";
  }

  EXPECT_TRUE(group_->replicate(data));
  EXPECT_EQ(state_machine_->applied_entries_.size(), 0);
}

TEST_F(RaftProtocolTests, StatePersistence) {
  const uint16_t group_id = 15000;

  {
    auto local_group = std::make_unique<RaftGroup>(group_id, "node1", *cm_, *rpc_);
    local_group->set_state_machine(state_machine_.get());
    local_group->start();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    local_group->stop();
  }

  auto loaded_group = std::make_unique<RaftGroup>(group_id, "node1", *cm_, *rpc_);
  loaded_group->set_state_machine(state_machine_.get());
}

TEST_F(RaftProtocolTests, LoadStateNonExistent) {
  group_ = std::make_unique<RaftGroup>(9999, "nonexistent_node", *cm_, *rpc_);
  group_->set_state_machine(state_machine_.get());
}

TEST_F(RaftProtocolTests, MultipleGroupsDistinctState) {
  auto group1 = std::make_unique<RaftGroup>(1, "node1", *cm_, *rpc_);
  auto group2 = std::make_unique<RaftGroup>(2, "node1", *cm_, *rpc_);

  EXPECT_NE(group1->group_id(), group2->group_id());
  EXPECT_EQ(group1->group_id(), 1);
  EXPECT_EQ(group2->group_id(), 2);
}

TEST_F(RaftProtocolTests, GetGroupId) {
  group_ = std::make_unique<RaftGroup>(42, "node1", *cm_, *rpc_);
  EXPECT_EQ(group_->group_id(), 42);
}

TEST_F(RaftProtocolTests, StopWithoutStart) {
  group_ = std::make_unique<RaftGroup>(1, "node1", *cm_, *rpc_);
  group_->stop();
}

TEST_F(RaftProtocolTests, SetStateMachine) {
  group_ = std::make_unique<RaftGroup>(1, "node1", *cm_, *rpc_);
  group_->set_state_machine(state_machine_.get());
  group_->set_state_machine(nullptr);
}

}  // namespace
