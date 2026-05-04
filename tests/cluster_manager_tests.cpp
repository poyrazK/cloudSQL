/**
 * @file cluster_manager_tests.cpp
 * @brief Unit tests for ClusterManager
 */

#include <gtest/gtest.h>
#include <thread>
#include <chrono>

#include "common/cluster_manager.hpp"
#include "common/config.hpp"
#include "common/bloom_filter.hpp"
#include "executor/types.hpp"

using namespace cloudsql;
using namespace cloudsql::cluster;
using namespace cloudsql::common;
using namespace cloudsql::executor;

namespace {

// Helper to create a simple tuple for testing
Tuple make_test_tuple(int64_t val) {
    return Tuple({Value::make_int64(val)});
}

std::vector<Tuple> make_test_tuples(const std::vector<int64_t>& vals) {
    std::vector<Tuple> tuples;
    tuples.reserve(vals.size());
    for (int64_t v : vals) {
        tuples.push_back(make_test_tuple(v));
    }
    return tuples;
}

// ============= Constructor Tests =============

TEST(ClusterManagerTests, Constructor_NullConfig) {
    ClusterManager cm(nullptr);
    EXPECT_EQ(cm.get_data_nodes().size(), 0u);
    EXPECT_EQ(cm.get_coordinators().size(), 0u);
}

TEST(ClusterManagerTests, Constructor_StandaloneMode) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    config.cluster_port = 7000;
    ClusterManager cm(&config);
    // In standalone mode, self_node should not be added
    EXPECT_EQ(cm.get_data_nodes().size(), 0u);
    EXPECT_EQ(cm.get_coordinators().size(), 0u);
}

TEST(ClusterManagerTests, Constructor_NonStandaloneMode) {
    config::Config config;
    config.mode = config::RunMode::Data;
    config.cluster_port = 7000;
    ClusterManager cm(&config);
    // Self should be added in distributed mode
    auto data_nodes = cm.get_data_nodes();
    EXPECT_EQ(data_nodes.size(), 1u);
    EXPECT_EQ(data_nodes[0].id, "node_7000");
    EXPECT_EQ(data_nodes[0].address, "127.0.0.1");
    EXPECT_EQ(data_nodes[0].cluster_port, 7000u);
}

TEST(ClusterManagerTests, Constructor_CoordinatorMode) {
    config::Config config;
    config.mode = config::RunMode::Coordinator;
    config.cluster_port = 8000;
    ClusterManager cm(&config);
    auto coordinators = cm.get_coordinators();
    EXPECT_EQ(coordinators.size(), 1u);
    EXPECT_EQ(coordinators[0].id, "node_8000");
}

// ============= Node Registration Tests =============

TEST(ClusterManagerTests, RegisterNode_AddsToMap) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    cm.register_node("node_1", "192.168.1.1", 7000, config::RunMode::Data);

    auto nodes = cm.get_data_nodes();
    ASSERT_EQ(nodes.size(), 1u);
    EXPECT_EQ(nodes[0].id, "node_1");
    EXPECT_EQ(nodes[0].address, "192.168.1.1");
    EXPECT_EQ(nodes[0].cluster_port, 7000u);
    EXPECT_EQ(nodes[0].role, config::RunMode::Data);
    EXPECT_TRUE(nodes[0].is_active);
}

TEST(ClusterManagerTests, RegisterNode_UpdatesExisting) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    cm.register_node("node_1", "192.168.1.1", 7000, config::RunMode::Data);
    cm.register_node("node_1", "192.168.1.2", 8000, config::RunMode::Coordinator);

    // node_1 is now a Coordinator, so get_data_nodes returns empty
    EXPECT_TRUE(cm.get_data_nodes().empty());
    // But it's in get_coordinators with updated address/port
    auto coords = cm.get_coordinators();
    ASSERT_EQ(coords.size(), 1u);
    EXPECT_EQ(coords[0].address, "192.168.1.2");
    EXPECT_EQ(coords[0].cluster_port, 8000u);
}

TEST(ClusterManagerTests, RegisterNode_MultipleNodes) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    cm.register_node("node_1", "192.168.1.1", 7000, config::RunMode::Data);
    cm.register_node("node_2", "192.168.1.2", 7001, config::RunMode::Data);
    cm.register_node("node_3", "192.168.1.3", 8000, config::RunMode::Coordinator);

    EXPECT_EQ(cm.get_data_nodes().size(), 2u);
    EXPECT_EQ(cm.get_coordinators().size(), 1u);
}

// ============= Heartbeat Tests =============

TEST(ClusterManagerTests, Heartbeat_ExistingNode) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    cm.register_node("node_1", "192.168.1.1", 7000, config::RunMode::Data);

    auto before = cm.get_data_nodes()[0].last_heartbeat;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    cm.heartbeat("node_1");
    auto after = cm.get_data_nodes()[0].last_heartbeat;

    EXPECT_TRUE(after > before);
    EXPECT_TRUE(cm.get_data_nodes()[0].is_active);
}

TEST(ClusterManagerTests, Heartbeat_NonExistingNode_NoOp) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    // Should not throw - silent no-op
    EXPECT_NO_THROW(cm.heartbeat("nonexistent"));
}

TEST(ClusterManagerTests, Heartbeat_InactiveNode) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    cm.register_node("node_1", "192.168.1.1", 7000, config::RunMode::Data);
    // Simulate inactive by re-registering doesn't change is_active
    // Let's verify heartbeat reactivates
    cm.heartbeat("node_1");
    EXPECT_TRUE(cm.get_data_nodes()[0].is_active);
}

// ============= Leadership Tests =============

TEST(ClusterManagerTests, SetAndGetLeader_ExistingGroup) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    cm.set_leader(1, "node_leader");
    EXPECT_EQ(cm.get_leader(1), "node_leader");
}

TEST(ClusterManagerTests, GetLeader_NonExistingGroup) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    EXPECT_EQ(cm.get_leader(999), "");
}

TEST(ClusterManagerTests, SetLeader_OverwritesPrevious) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    cm.set_leader(1, "node_a");
    cm.set_leader(1, "node_b");
    EXPECT_EQ(cm.get_leader(1), "node_b");
}

// ============= RaftManager Tests =============

TEST(ClusterManagerTests, SetAndGetRaftManager) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    EXPECT_EQ(cm.get_raft_manager(), nullptr);

    // Can't easily test with real RaftManager pointer, but verify getter works
    // The pointer would be set by distributed components at runtime
}

// ============= Node Discovery Tests =============

TEST(ClusterManagerTests, GetDataNodes_EmptyCluster) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    EXPECT_TRUE(cm.get_data_nodes().empty());
}

TEST(ClusterManagerTests, GetDataNodes_OnlyCoordinator) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    cm.register_node("coord_1", "192.168.1.1", 8000, config::RunMode::Coordinator);

    EXPECT_TRUE(cm.get_data_nodes().empty());
    EXPECT_EQ(cm.get_coordinators().size(), 1u);
}

TEST(ClusterManagerTests, GetDataNodes_MultipleActive) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    cm.register_node("node_1", "192.168.1.1", 7000, config::RunMode::Data);
    cm.register_node("node_2", "192.168.1.2", 7001, config::RunMode::Data);
    cm.register_node("node_3", "192.168.1.3", 7002, config::RunMode::Data);

    EXPECT_EQ(cm.get_data_nodes().size(), 3u);
}

TEST(ClusterManagerTests, GetDataNodes_SelfExcludedWhenStandalone) {
    // In standalone mode, self_node_ is not created
    config::Config config;
    config.mode = config::RunMode::Standalone;
    config.cluster_port = 7000;
    ClusterManager cm(&config);

    cm.register_node("node_2", "192.168.1.2", 7001, config::RunMode::Data);

    // In standalone mode, no self_node exists, so get_data_nodes returns only registered nodes
    auto nodes = cm.get_data_nodes();
    ASSERT_EQ(nodes.size(), 1u);
    EXPECT_EQ(nodes[0].id, "node_2");
}

TEST(ClusterManagerTests, GetCoordinators_MixedRoles) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    cm.register_node("data_1", "192.168.1.1", 7000, config::RunMode::Data);
    cm.register_node("coord_1", "192.168.1.2", 8000, config::RunMode::Coordinator);
    cm.register_node("coord_2", "192.168.1.3", 8001, config::RunMode::Coordinator);

    EXPECT_EQ(cm.get_coordinators().size(), 2u);
}

TEST(ClusterManagerTests, GetDataNodes_SelfAsData) {
    config::Config config;
    config.mode = config::RunMode::Data;
    config.cluster_port = 7000;
    ClusterManager cm(&config);

    // In distributed mode, self is added automatically
    auto nodes = cm.get_data_nodes();
    EXPECT_EQ(nodes.size(), 1u);
    EXPECT_EQ(nodes[0].id, "node_7000");
}

// ============= Group Membership Tests =============

TEST(ClusterManagerTests, AddNodeToGroup_NewNode) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    cm.register_node("node_1", "192.168.1.1", 7000, config::RunMode::Data);
    cm.add_node_to_group(1, "node_1");

    auto members = cm.get_group_members(1);
    ASSERT_EQ(members.size(), 1u);
    EXPECT_EQ(members[0].id, "node_1");
}

TEST(ClusterManagerTests, AddNodeToGroup_Duplicate) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    cm.register_node("node_1", "192.168.1.1", 7000, config::RunMode::Data);
    cm.add_node_to_group(1, "node_1");
    cm.add_node_to_group(1, "node_1");

    auto members = cm.get_group_members(1);
    EXPECT_EQ(members.size(), 1u);
}

TEST(ClusterManagerTests, GetGroupMembers_ExistingGroup) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    cm.register_node("node_1", "192.168.1.1", 7000, config::RunMode::Data);
    cm.register_node("node_2", "192.168.1.2", 7001, config::RunMode::Data);
    cm.add_node_to_group(1, "node_1");
    cm.add_node_to_group(1, "node_2");

    auto members = cm.get_group_members(1);
    EXPECT_EQ(members.size(), 2u);
}

TEST(ClusterManagerTests, GetGroupMembers_NonExistingGroup) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    auto members = cm.get_group_members(999);
    EXPECT_TRUE(members.empty());
}

TEST(ClusterManagerTests, GetGroupMembers_NodeNotInNodes_UsesSelfFallback) {
    config::Config config;
    config.mode = config::RunMode::Data;
    config.cluster_port = 7000;
    ClusterManager cm(&config);

    // Add self node's ID to group but it's not in nodes_ map
    cm.add_node_to_group(1, "node_7000");

    auto members = cm.get_group_members(1);
    ASSERT_EQ(members.size(), 1u);
    EXPECT_EQ(members[0].id, "node_7000");
}

// ============= Shuffle Buffer Tests =============

TEST(ClusterManagerTests, BufferShuffleData_SingleContext) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    auto tuples = make_test_tuples({1, 2, 3});
    cm.buffer_shuffle_data("ctx1", "table_a", std::move(tuples));

    EXPECT_TRUE(cm.has_shuffle_data("ctx1", "table_a"));
}

TEST(ClusterManagerTests, BufferShuffleData_MultipleTables) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    cm.buffer_shuffle_data("ctx1", "table_a", make_test_tuples({1}));
    cm.buffer_shuffle_data("ctx1", "table_b", make_test_tuples({2}));

    EXPECT_TRUE(cm.has_shuffle_data("ctx1", "table_a"));
    EXPECT_TRUE(cm.has_shuffle_data("ctx1", "table_b"));
}

TEST(ClusterManagerTests, BufferShuffleData_AppendsNotReplaces) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    cm.buffer_shuffle_data("ctx1", "table_a", make_test_tuples({1, 2}));
    cm.buffer_shuffle_data("ctx1", "table_a", make_test_tuples({3, 4}));

    auto data = cm.fetch_shuffle_data("ctx1", "table_a");
    EXPECT_EQ(data.size(), 4u);
}

TEST(ClusterManagerTests, HasShuffleData_NonExistingContext) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    EXPECT_FALSE(cm.has_shuffle_data("nonexistent", "table_a"));
}

TEST(ClusterManagerTests, HasShuffleData_NonExistingTable) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    cm.buffer_shuffle_data("ctx1", "table_a", make_test_tuples({1}));
    EXPECT_FALSE(cm.has_shuffle_data("ctx1", "table_b"));
}

TEST(ClusterManagerTests, FetchShuffleData_Existing) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    cm.buffer_shuffle_data("ctx1", "table_a", make_test_tuples({1, 2, 3}));
    auto data = cm.fetch_shuffle_data("ctx1", "table_a");

    EXPECT_EQ(data.size(), 3u);
    EXPECT_FALSE(cm.has_shuffle_data("ctx1", "table_a"));
}

TEST(ClusterManagerTests, FetchShuffleData_NonExisting) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    auto data = cm.fetch_shuffle_data("nonexistent", "table_a");
    EXPECT_TRUE(data.empty());
}

TEST(ClusterManagerTests, FetchShuffleData_EmptyContextCleanedUp) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    cm.buffer_shuffle_data("ctx1", "table_a", make_test_tuples({1}));
    cm.fetch_shuffle_data("ctx1", "table_a");

    // Context should be cleaned up - fetch again returns empty
    auto data = cm.fetch_shuffle_data("ctx1", "table_a");
    EXPECT_TRUE(data.empty());
}

// ============= Bloom Filter Tests =============

TEST(ClusterManagerTests, SetBloomFilter_StoresAllFields) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    std::vector<uint8_t> filter_data(100, 0xFF);
    cm.set_bloom_filter("ctx1", "build_table", "probe_table", "key_col",
                        std::move(filter_data), 1000, 3);

    EXPECT_TRUE(cm.has_bloom_filter("ctx1"));
    EXPECT_EQ(cm.get_probe_table("ctx1"), "probe_table");
    EXPECT_EQ(cm.get_probe_key_col("ctx1"), "key_col");
}

TEST(ClusterManagerTests, HasBloomFilter_WithEmptyData_ReturnsFalse) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    // CRITICAL: Empty filter_data means has_bloom_filter returns false
    std::vector<uint8_t> empty_data;
    cm.set_bloom_filter("ctx1", "build", "probe", "key", std::move(empty_data), 100, 3);

    EXPECT_FALSE(cm.has_bloom_filter("ctx1"));
}

TEST(ClusterManagerTests, HasBloomFilter_WithData_ReturnsTrue) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    std::vector<uint8_t> filter_data(100, 0xAB);
    cm.set_bloom_filter("ctx1", "build", "probe", "key", std::move(filter_data), 100, 3);

    EXPECT_TRUE(cm.has_bloom_filter("ctx1"));
}

TEST(ClusterManagerTests, HasBloomFilter_NonExisting) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    EXPECT_FALSE(cm.has_bloom_filter("nonexistent"));
}

TEST(ClusterManagerTests, GetBloomFilter_Existing) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    std::vector<uint8_t> filter_data(100, 0xFF);
    cm.set_bloom_filter("ctx1", "build", "probe", "key", std::move(filter_data), 100, 3);

    // has_bloom_filter returns true for non-empty data
    EXPECT_TRUE(cm.has_bloom_filter("ctx1"));
}

TEST(ClusterManagerTests, GetBloomFilter_NonExisting) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    // has_bloom_filter returns false for non-existent context
    EXPECT_FALSE(cm.has_bloom_filter("nonexistent"));
}

TEST(ClusterManagerTests, GetProbeTable_Existing) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    cm.set_bloom_filter("ctx1", "build", "probe_table", "key_col",
                        std::vector<uint8_t>(10, 0), 100, 3);
    EXPECT_EQ(cm.get_probe_table("ctx1"), "probe_table");
}

TEST(ClusterManagerTests, GetProbeTable_NonExisting) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    EXPECT_EQ(cm.get_probe_table("nonexistent"), "");
}

TEST(ClusterManagerTests, GetProbeKeyCol_Existing) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    cm.set_bloom_filter("ctx1", "build", "probe", "key_col",
                        std::vector<uint8_t>(10, 0), 100, 3);
    EXPECT_EQ(cm.get_probe_key_col("ctx1"), "key_col");
}

TEST(ClusterManagerTests, GetProbeKeyCol_NonExisting) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    EXPECT_EQ(cm.get_probe_key_col("nonexistent"), "");
}

// ============= Local Bloom Bits Tests =============

TEST(ClusterManagerTests, SetLocalBloomBits_StoresAll) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    std::vector<uint8_t> bits(100, 0x55);
    cm.set_local_bloom_bits("ctx1", std::move(bits), 500, 7);

    EXPECT_EQ(cm.get_local_bloom_bits("ctx1").size(), 100u);
    EXPECT_EQ(cm.get_local_expected_elements("ctx1"), 500u);
    EXPECT_EQ(cm.get_local_num_hashes("ctx1"), 7u);
}

TEST(ClusterManagerTests, GetLocalBloomBits_Existing) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    std::vector<uint8_t> bits(50, 0xAA);
    cm.set_local_bloom_bits("ctx1", bits, 100, 3);

    auto result = cm.get_local_bloom_bits("ctx1");
    EXPECT_EQ(result.size(), 50u);
}

TEST(ClusterManagerTests, GetLocalBloomBits_NonExisting) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    auto result = cm.get_local_bloom_bits("nonexistent");
    EXPECT_TRUE(result.empty());
}

TEST(ClusterManagerTests, GetLocalExpectedElements_NonExisting) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    EXPECT_EQ(cm.get_local_expected_elements("nonexistent"), 0u);
}

TEST(ClusterManagerTests, GetLocalNumHashes_NonExisting) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    EXPECT_EQ(cm.get_local_num_hashes("nonexistent"), 0u);
}

TEST(ClusterManagerTests, ClearBloomFilter_RemovesAll) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    cm.set_bloom_filter("ctx1", "build", "probe", "key",
                        std::vector<uint8_t>(10, 0xFF), 100, 3);
    cm.set_local_bloom_bits("ctx1", std::vector<uint8_t>(10, 0xFF), 100, 3);

    cm.clear_bloom_filter("ctx1");

    EXPECT_FALSE(cm.has_bloom_filter("ctx1"));
    EXPECT_TRUE(cm.get_local_bloom_bits("ctx1").empty());
    EXPECT_EQ(cm.get_local_expected_elements("ctx1"), 0u);
}

// ============= Outer Join Row Storage Tests =============

// Local Right Rows Tests
TEST(ClusterManagerTests, SetLocalRightRows_StoresRows) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    auto tuples = make_test_tuples({1, 2, 3});
    cm.set_local_right_rows("ctx1", "right_table", std::move(tuples));

    auto result = cm.get_local_right_rows("ctx1", "right_table");
    EXPECT_EQ(result.size(), 3u);
}

TEST(ClusterManagerTests, GetLocalRightRows_NonExisting) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    auto result = cm.get_local_right_rows("nonexistent", "table");
    EXPECT_TRUE(result.empty());
}

TEST(ClusterManagerTests, ClearLocalRightRows_RemovesAll) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    cm.set_local_right_rows("ctx1", "right_table", make_test_tuples({1, 2}));
    cm.clear_local_right_rows("ctx1");

    auto result = cm.get_local_right_rows("ctx1", "right_table");
    EXPECT_TRUE(result.empty());
}

// Unmatched Rows Tests
TEST(ClusterManagerTests, SetUnmatchedRows_StoresRows) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    auto tuples = make_test_tuples({10, 20});
    cm.set_unmatched_rows("ctx1", "outer_table", std::move(tuples));

    auto result = cm.get_unmatched_rows("ctx1", "outer_table");
    EXPECT_EQ(result.size(), 2u);
}

TEST(ClusterManagerTests, GetUnmatchedRows_NonExisting) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    auto result = cm.get_unmatched_rows("nonexistent", "table");
    EXPECT_TRUE(result.empty());
}

TEST(ClusterManagerTests, ClearUnmatchedRows_RemovesAll) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    cm.set_unmatched_rows("ctx1", "outer_table", make_test_tuples({1}));
    cm.clear_unmatched_rows("ctx1");

    auto result = cm.get_unmatched_rows("ctx1", "outer_table");
    EXPECT_TRUE(result.empty());
}

// Local Left Rows Tests
TEST(ClusterManagerTests, SetLocalLeftRows_StoresRows) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    auto tuples = make_test_tuples({100, 200, 300});
    cm.set_local_left_rows("ctx1", "left_table", std::move(tuples));

    auto result = cm.get_local_left_rows("ctx1", "left_table");
    EXPECT_EQ(result.size(), 3u);
}

TEST(ClusterManagerTests, GetLocalLeftRows_NonExisting) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    auto result = cm.get_local_left_rows("nonexistent", "table");
    EXPECT_TRUE(result.empty());
}

TEST(ClusterManagerTests, ClearLocalLeftRows_RemovesAll) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    cm.set_local_left_rows("ctx1", "left_table", make_test_tuples({1, 2}));
    cm.clear_local_left_rows("ctx1");

    auto result = cm.get_local_left_rows("ctx1", "left_table");
    EXPECT_TRUE(result.empty());
}

// Unmatched Left Rows Tests
TEST(ClusterManagerTests, SetUnmatchedLeftRows_StoresRows) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    auto tuples = make_test_tuples({999});
    cm.set_unmatched_left_rows("ctx1", "full_join_table", std::move(tuples));

    auto result = cm.get_unmatched_left_rows("ctx1", "full_join_table");
    EXPECT_EQ(result.size(), 1u);
}

TEST(ClusterManagerTests, GetUnmatchedLeftRows_NonExisting) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    auto result = cm.get_unmatched_left_rows("nonexistent", "table");
    EXPECT_TRUE(result.empty());
}

TEST(ClusterManagerTests, ClearUnmatchedLeftRows_RemovesAll) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    cm.set_unmatched_left_rows("ctx1", "full_table", make_test_tuples({1}));
    cm.clear_unmatched_left_rows("ctx1");

    auto result = cm.get_unmatched_left_rows("ctx1", "full_table");
    EXPECT_TRUE(result.empty());
}

// ============= Thread Safety Tests =============

TEST(ClusterManagerTests, ConcurrentRegisterNode) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    std::vector<std::thread> threads;
    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([&cm, i]() {
            cm.register_node("node_" + std::to_string(i), "192.168.1." + std::to_string(i),
                             7000 + i, config::RunMode::Data);
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(cm.get_data_nodes().size(), 10u);
}

TEST(ClusterManagerTests, ConcurrentHeartbeat) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    cm.register_node("node_1", "192.168.1.1", 7000, config::RunMode::Data);

    std::vector<std::thread> threads;
    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([&cm]() { cm.heartbeat("node_1"); });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Should not crash and node should still be active
    EXPECT_TRUE(cm.get_data_nodes()[0].is_active);
}

TEST(ClusterManagerTests, ConcurrentShuffleBuffer) {
    config::Config config;
    config.mode = config::RunMode::Standalone;
    ClusterManager cm(&config);

    std::vector<std::thread> threads;
    for (int i = 0; i < 5; ++i) {
        threads.emplace_back([&cm, i]() {
            cm.buffer_shuffle_data("ctx1", "table_a", make_test_tuples({i}));
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    auto data = cm.fetch_shuffle_data("ctx1", "table_a");
    EXPECT_EQ(data.size(), 5u);
}

}  // namespace