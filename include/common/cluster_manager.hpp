/**
 * @file cluster_manager.hpp
 * @brief Manager for cluster topology and node health
 */

#ifndef SQL_ENGINE_COMMON_CLUSTER_MANAGER_HPP
#define SQL_ENGINE_COMMON_CLUSTER_MANAGER_HPP

#include <chrono>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/config.hpp"

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
    explicit ClusterManager(const config::Config* config) : config_(config) {
        // Add self to node map if in distributed mode
        if (config_ != nullptr && config_->mode != config::RunMode::Standalone) {
            self_node_.id = "local_node";  // Will be replaced by unique ID later
            self_node_.address = "127.0.0.1";
            self_node_.cluster_port = config_->cluster_port;
            self_node_.role = config_->mode;
            self_node_.last_heartbeat = std::chrono::system_clock::now();
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
     * @brief Get list of active data nodes
     */
    [[nodiscard]] std::vector<NodeInfo> get_data_nodes() const {
        const std::scoped_lock<std::mutex> lock(mutex_);
        std::vector<NodeInfo> data_nodes;
        for (const auto& [id, info] : nodes_) {
            if (info.role == config::RunMode::Data && info.is_active) {
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
        for (const auto& [id, info] : nodes_) {
            if (info.role == config::RunMode::Coordinator && info.is_active) {
                coordinators.push_back(info);
            }
        }
        return coordinators;
    }

   private:
    const config::Config* config_;
    NodeInfo self_node_;
    std::unordered_map<std::string, NodeInfo> nodes_;
    mutable std::mutex mutex_;
};

}  // namespace cloudsql::cluster

#endif  // SQL_ENGINE_COMMON_CLUSTER_MANAGER_HPP
