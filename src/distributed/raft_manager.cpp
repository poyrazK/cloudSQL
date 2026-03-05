/**
 * @file raft_manager.cpp
 * @brief Manages multiple Raft consensus groups on a single node
 */

#include "distributed/raft_manager.hpp"

namespace cloudsql::raft {

RaftManager::RaftManager(std::string node_id, cluster::ClusterManager& cluster_manager,
                         network::RpcServer& rpc_server)
    : node_id_(std::move(node_id)), cluster_manager_(cluster_manager), rpc_server_(rpc_server) {
    // Register routing handlers
    rpc_server_.set_handler(network::RpcType::RequestVote,
                            [this](const network::RpcHeader& h, const std::vector<uint8_t>& p,
                                   int fd) { handle_raft_rpc(h, p, fd); });
    rpc_server_.set_handler(network::RpcType::AppendEntries,
                            [this](const network::RpcHeader& h, const std::vector<uint8_t>& p,
                                   int fd) { handle_raft_rpc(h, p, fd); });
}

void RaftManager::start() {
    const std::scoped_lock<std::mutex> lock(mutex_);
    for (auto& [id, group] : groups_) {
        group->start();
    }
}

void RaftManager::stop() {
    const std::scoped_lock<std::mutex> lock(mutex_);
    for (auto& [id, group] : groups_) {
        group->stop();
    }
}

std::shared_ptr<RaftGroup> RaftManager::get_or_create_group(uint16_t group_id) {
    const std::scoped_lock<std::mutex> lock(mutex_);
    auto it = groups_.find(group_id);
    if (it != groups_.end()) {
        return it->second;
    }

    auto group = std::make_shared<RaftGroup>(group_id, node_id_, cluster_manager_, rpc_server_);
    groups_[group_id] = group;
    return group;
}

std::shared_ptr<RaftGroup> RaftManager::get_group(uint16_t group_id) {
    const std::scoped_lock<std::mutex> lock(mutex_);
    auto it = groups_.find(group_id);
    if (it != groups_.end()) {
        return it->second;
    }
    return nullptr;
}

void RaftManager::handle_raft_rpc(const network::RpcHeader& header,
                                  const std::vector<uint8_t>& payload, int client_fd) {
    std::shared_ptr<RaftGroup> group;
    {
        const std::scoped_lock<std::mutex> lock(mutex_);
        auto it = groups_.find(header.group_id);
        if (it != groups_.end()) {
            group = it->second;
        }
    }

    if (!group) {
        return;
    }

    if (header.type == network::RpcType::RequestVote) {
        group->handle_request_vote(header, payload, client_fd);
    } else if (header.type == network::RpcType::AppendEntries) {
        group->handle_append_entries(header, payload, client_fd);
    }
}

}  // namespace cloudsql::raft
