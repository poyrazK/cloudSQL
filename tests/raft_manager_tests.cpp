/**
 * @file raft_manager_tests.cpp
 * @brief Unit tests for RaftManager - Multi-Raft group management
 */

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <thread>
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

class RaftManagerTests : public ::testing::Test {
   protected:
    void SetUp() override {
        config_.mode = config::RunMode::Coordinator;
        constexpr uint16_t TEST_PORT = 6100;
        config_.cluster_port = TEST_PORT;
        cm_ = std::make_unique<ClusterManager>(&config_);
        rpc_ = std::make_unique<RpcServer>(TEST_PORT);
        ASSERT_TRUE(rpc_->start()) << "RpcServer failed to start - port may be in use";
        manager_ = std::make_unique<RaftManager>("node1", *cm_, *rpc_);
    }

    void TearDown() override {
        if (manager_) {
            manager_->stop();
        }
        if (rpc_) {
            rpc_->stop();
        }
    }

    config::Config config_;
    std::unique_ptr<ClusterManager> cm_;
    std::unique_ptr<RpcServer> rpc_;
    std::unique_ptr<RaftManager> manager_;
};

TEST_F(RaftManagerTests, GroupCreation) {
    auto group = manager_->get_or_create_group(1);
    ASSERT_NE(group, nullptr);
    EXPECT_EQ(group->group_id(), 1);
}

TEST_F(RaftManagerTests, GroupCreationReturnsExisting) {
    auto group1 = manager_->get_or_create_group(1);
    auto group2 = manager_->get_or_create_group(1);
    EXPECT_EQ(group1, group2);
}

TEST_F(RaftManagerTests, GetGroupExisting) {
    auto group = manager_->get_or_create_group(42);
    ASSERT_NE(group, nullptr);
    EXPECT_EQ(group->group_id(), 42);

    auto retrieved = manager_->get_group(42);
    ASSERT_NE(retrieved, nullptr);
    EXPECT_EQ(retrieved, group);
}

TEST_F(RaftManagerTests, GetGroupNonExistent) {
    auto result = manager_->get_group(999);
    EXPECT_EQ(result, nullptr);
}

TEST_F(RaftManagerTests, GetGroupAfterGetOrCreate) {
    auto created = manager_->get_or_create_group(77);
    ASSERT_NE(created, nullptr);

    auto retrieved = manager_->get_group(77);
    ASSERT_NE(retrieved, nullptr);
    EXPECT_EQ(created, retrieved);
}

TEST_F(RaftManagerTests, MultipleGroups) {
    auto group1 = manager_->get_or_create_group(1);
    auto group2 = manager_->get_or_create_group(2);
    auto group3 = manager_->get_or_create_group(3);

    ASSERT_NE(group1, nullptr);
    ASSERT_NE(group2, nullptr);
    ASSERT_NE(group3, nullptr);

    EXPECT_NE(group1, group2);
    EXPECT_NE(group2, group3);
    EXPECT_NE(group1, group3);

    EXPECT_EQ(group1->group_id(), 1);
    EXPECT_EQ(group2->group_id(), 2);
    EXPECT_EQ(group3->group_id(), 3);
}

TEST_F(RaftManagerTests, LifecycleStartStop) {
    manager_->start();
    manager_->stop();
    manager_->start();
    manager_->stop();
}

TEST_F(RaftManagerTests, GetOrCreateGroupAfterStartStop) {
    auto group1 = manager_->get_or_create_group(1);
    manager_->start();
    manager_->stop();

    auto group2 = manager_->get_or_create_group(1);
    EXPECT_EQ(group1, group2);
}

}  // namespace
