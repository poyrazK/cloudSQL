/**
 * @file raft_node.hpp
 * @brief Raft consensus node implementation
 */

#ifndef SQL_ENGINE_DISTRIBUTED_RAFT_NODE_HPP
#define SQL_ENGINE_DISTRIBUTED_RAFT_NODE_HPP

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <random>
#include <thread>

#include "common/cluster_manager.hpp"
#include "distributed/raft_types.hpp"
#include "network/rpc_client.hpp"
#include "network/rpc_server.hpp"

namespace cloudsql::raft {

/**
 * @brief Implementation of a Raft consensus node
 */
class RaftNode {
   public:
    RaftNode(std::string node_id, cluster::ClusterManager& cluster_manager,
             network::RpcServer& rpc_server);
    ~RaftNode();

    // Prevent copying and moving
    RaftNode(const RaftNode&) = delete;
    RaftNode& operator=(const RaftNode&) = delete;
    RaftNode(RaftNode&&) = delete;
    RaftNode& operator=(RaftNode&&) = delete;

    void start();
    void stop();

    // Raft RPC Handlers
    void handle_request_vote(const network::RpcHeader& header, const std::vector<uint8_t>& payload,
                             int client_fd);
    void handle_append_entries(const network::RpcHeader& header,
                               const std::vector<uint8_t>& payload, int client_fd);

    // Client interface
    bool replicate(const std::string& command);
    [[nodiscard]] bool is_leader() const { return state_.load() == NodeState::Leader; }

   private:
    void run_loop();
    void do_follower();
    void do_candidate();
    void do_leader();

    void step_down(term_t new_term);
    void persist_state();
    void load_state();

    // Helpers
    [[nodiscard]] std::chrono::milliseconds get_random_timeout() const;

    std::string node_id_;
    cluster::ClusterManager& cluster_manager_;
    network::RpcServer& rpc_server_;

    // State
    std::atomic<NodeState> state_{NodeState::Follower};
    RaftPersistentState persistent_state_;
    RaftVolatileState volatile_state_;
    LeaderState leader_state_;

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<bool> running_{false};
    std::thread raft_thread_;

    std::chrono::system_clock::time_point last_heartbeat_;
    std::mt19937 rng_;
};

}  // namespace cloudsql::raft

#endif  // SQL_ENGINE_DISTRIBUTED_RAFT_NODE_HPP
