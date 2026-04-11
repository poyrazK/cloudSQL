/**
 * @file cluster_manager.hpp
 * @brief Manager for cluster topology and node health
 */

#ifndef SQL_ENGINE_COMMON_CLUSTER_MANAGER_HPP
#define SQL_ENGINE_COMMON_CLUSTER_MANAGER_HPP

#include <algorithm>
#include <chrono>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/config.hpp"
#include "common/bloom_filter.hpp"
#include "executor/types.hpp"

namespace cloudsql::raft {
class RaftManager;
}

namespace cloudsql::cluster {

/**
 * @brief Represents a node in the cluster
 */
struct NodeInfo {
    std::string id;
    std::string address;
    uint16_t cluster_port = 0;
    config::RunMode role = config::RunMode::Standalone;
    std::chrono::system_clock::time_point last_heartbeat;
    bool is_active = true;
};

/**
 * @brief Manages the cluster topology and node discovery
 */
class ClusterManager {
   public:
    explicit ClusterManager(const config::Config* config)
        : config_(config), raft_manager_(nullptr) {
        // Add self to node map if in distributed mode
        if (config_ != nullptr && config_->mode != config::RunMode::Standalone) {
            self_node_.id = "node_" + std::to_string(config_->cluster_port);
            self_node_.address = "127.0.0.1";
            self_node_.cluster_port = config_->cluster_port;
            self_node_.role = config_->mode;
            self_node_.last_heartbeat = std::chrono::system_clock::now();
            self_node_.is_active = true;
        }
    }

    /**
     * @brief Register a new node in the cluster
     */
    void register_node(const std::string& id, const std::string& address, uint16_t port,
                       config::RunMode role) {
        const std::scoped_lock<std::mutex> lock(mutex_);
        nodes_[id] = {id, address, port, role, std::chrono::system_clock::now(), true};
    }

    /**
     * @brief Set Raft manager for this node
     */
    void set_raft_manager(raft::RaftManager* rm) { raft_manager_ = rm; }

    /**
     * @brief Get Raft manager for this node
     */
    [[nodiscard]] raft::RaftManager* get_raft_manager() const { return raft_manager_; }

    /**
     * @brief Update heartbeat for a node
     */
    void heartbeat(const std::string& id) {
        const std::scoped_lock<std::mutex> lock(mutex_);
        if (nodes_.count(id) != 0U) {
            nodes_[id].last_heartbeat = std::chrono::system_clock::now();
            nodes_[id].is_active = true;
        }
    }

    /**
     * @brief Update leader ID for a specific Raft group
     */
    void set_leader(uint16_t group_id, const std::string& leader_id) {
        const std::scoped_lock<std::mutex> lock(mutex_);
        group_leaders_[group_id] = leader_id;
    }

    /**
     * @brief Get current leader for a Raft group
     */
    [[nodiscard]] std::string get_leader(uint16_t group_id) const {
        const std::scoped_lock<std::mutex> lock(mutex_);
        auto it = group_leaders_.find(group_id);
        if (it != group_leaders_.end()) {
            return it->second;
        }
        return "";
    }

    /**
     * @brief Get list of active data nodes
     */
    [[nodiscard]] std::vector<NodeInfo> get_data_nodes() const {
        const std::scoped_lock<std::mutex> lock(mutex_);
        std::vector<NodeInfo> data_nodes;
        if (self_node_.role == config::RunMode::Data && self_node_.is_active) {
            data_nodes.push_back(self_node_);
        }
        for (const auto& [id, info] : nodes_) {
            if (info.role == config::RunMode::Data && info.is_active && id != self_node_.id) {
                data_nodes.push_back(info);
            }
        }
        return data_nodes;
    }

    /**
     * @brief Get list of active coordinator nodes
     */
    [[nodiscard]] std::vector<NodeInfo> get_coordinators() const {
        const std::scoped_lock<std::mutex> lock(mutex_);
        std::vector<NodeInfo> coordinators;
        if (self_node_.role == config::RunMode::Coordinator && self_node_.is_active) {
            coordinators.push_back(self_node_);
        }
        for (const auto& [id, info] : nodes_) {
            if (info.role == config::RunMode::Coordinator && info.is_active &&
                id != self_node_.id) {
                coordinators.push_back(info);
            }
        }
        return coordinators;
    }

    /**
     * @brief Add a node to a specific Raft group
     */
    void add_node_to_group(uint16_t group_id, const std::string& node_id) {
        const std::scoped_lock<std::mutex> lock(mutex_);
        auto& members = group_membership_[group_id];
        if (std::find(members.begin(), members.end(), node_id) == members.end()) {
            members.push_back(node_id);
        }
    }

    /**
     * @brief Get all members of a specific Raft group
     */
    [[nodiscard]] std::vector<NodeInfo> get_group_members(uint16_t group_id) const {
        const std::scoped_lock<std::mutex> lock(mutex_);
        std::vector<NodeInfo> members;
        auto it = group_membership_.find(group_id);
        if (it != group_membership_.end()) {
            for (const auto& node_id : it->second) {
                if (nodes_.count(node_id) != 0U) {
                    members.push_back(nodes_.at(node_id));
                } else if (node_id == self_node_.id) {
                    members.push_back(self_node_);
                }
            }
        }
        return members;
    }

    /**
     * @brief Buffer received shuffle data
     */
    void buffer_shuffle_data(const std::string& context_id, const std::string& table,
                             std::vector<executor::Tuple> rows) {
        const std::scoped_lock<std::mutex> lock(mutex_);
        auto& target = shuffle_buffers_[context_id][table];
        target.insert(target.end(), std::make_move_iterator(rows.begin()),
                      std::make_move_iterator(rows.end()));
    }

    /**
     * @brief Check if shuffle data exists for a table in a context
     */
    [[nodiscard]] bool has_shuffle_data(const std::string& context_id,
                                        const std::string& table) const {
        const std::scoped_lock<std::mutex> lock(mutex_);
        if (shuffle_buffers_.count(context_id) == 0U) {
            return false;
        }
        return shuffle_buffers_.at(context_id).count(table) != 0U;
    }

    /**
     * @brief Retrieve and clear buffered shuffle data for a context
     */
    std::vector<executor::Tuple> fetch_shuffle_data(const std::string& context_id,
                                                    const std::string& table) {
        const std::scoped_lock<std::mutex> lock(mutex_);
        std::vector<executor::Tuple> data;
        if (shuffle_buffers_.count(context_id) != 0U) {
            auto& context_buffers = shuffle_buffers_[context_id];
            if (context_buffers.count(table) != 0U) {
                data = std::move(context_buffers[table]);
                context_buffers.erase(table);
            }
            if (context_buffers.empty()) {
                shuffle_buffers_.erase(context_id);
            }
        }
        return data;
    }

    /**
     * @brief Store a bloom filter for a shuffle context
     */
    void set_bloom_filter(const std::string& context_id, const std::string& build_table,
                          const std::string& probe_table, const std::string& probe_key_col,
                          std::vector<uint8_t> filter_data, size_t expected_elements,
                          size_t num_hashes) {
        const std::scoped_lock<std::mutex> lock(mutex_);
        auto& entry = bloom_filters_[context_id];
        entry.build_table = build_table;
        entry.probe_table = probe_table;
        entry.probe_key_col = probe_key_col;
        entry.filter_data = std::move(filter_data);
        entry.expected_elements = expected_elements;
        entry.num_hashes = num_hashes;
    }

    /**
     * @brief Check if a bloom filter exists for a context
     */
    [[nodiscard]] bool has_bloom_filter(const std::string& context_id) const {
        const std::scoped_lock<std::mutex> lock(mutex_);
        return bloom_filters_.count(context_id) != 0U;
    }

    /**
     * @brief Get bloom filter for a context (reconstructs BloomFilter object)
     */
    [[nodiscard]] common::BloomFilter get_bloom_filter(const std::string& context_id) const {
        const std::scoped_lock<std::mutex> lock(mutex_);
        auto it = bloom_filters_.find(context_id);
        if (it != bloom_filters_.end() && !it->second.filter_data.empty()) {
            return common::BloomFilter(it->second.filter_data.data(), it->second.filter_data.size());
        }
        return common::BloomFilter(1);  // Empty filter
    }

    /**
     * @brief Get probe table name for a context
     */
    [[nodiscard]] std::string get_probe_table(const std::string& context_id) const {
        const std::scoped_lock<std::mutex> lock(mutex_);
        auto it = bloom_filters_.find(context_id);
        if (it != bloom_filters_.end()) {
            return it->second.probe_table;
        }
        return "";
    }

    /**
     * @brief Get probe key column for a context
     */
    [[nodiscard]] std::string get_probe_key_col(const std::string& context_id) const {
        const std::scoped_lock<std::mutex> lock(mutex_);
        auto it = bloom_filters_.find(context_id);
        if (it != bloom_filters_.end()) {
            return it->second.probe_key_col;
        }
        return "";
    }

    /**
     * @brief Clear bloom filter for a context
     */
    void clear_bloom_filter(const std::string& context_id) {
        const std::scoped_lock<std::mutex> lock(mutex_);
        bloom_filters_.erase(context_id);
    }

   private:
    /**
     * @brief Stored bloom filter data for a context
     */
    struct BloomFilterEntry {
        std::string build_table;
        std::string probe_table;
        std::string probe_key_col;  // Join key column on probe side
        std::vector<uint8_t> filter_data;
        size_t expected_elements = 0;
        size_t num_hashes = 0;
    };

    const config::Config* config_;
    raft::RaftManager* raft_manager_;
    NodeInfo self_node_;
    std::unordered_map<std::string, NodeInfo> nodes_;
    std::unordered_map<uint16_t, std::string> group_leaders_;
    std::unordered_map<uint16_t, std::vector<std::string>> group_membership_;
    /* context_id -> table_name -> rows */
    std::unordered_map<std::string, std::unordered_map<std::string, std::vector<executor::Tuple>>>
        shuffle_buffers_;
    /* context_id -> bloom filter data */
    std::unordered_map<std::string, BloomFilterEntry> bloom_filters_;
    mutable std::mutex mutex_;
};

}  // namespace cloudsql::cluster

#endif  // SQL_ENGINE_COMMON_CLUSTER_MANAGER_HPP
