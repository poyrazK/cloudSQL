/**
 * @file raft_node.cpp
 * @brief Raft consensus node implementation
 */

#include "distributed/raft_node.hpp"

#include <sys/socket.h>

#include <chrono>
#include <cstring>
#include <iostream>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

namespace cloudsql::raft {

namespace {
constexpr int TIMEOUT_MIN_MS = 150;
constexpr int TIMEOUT_MAX_MS = 300;
constexpr int HEARTBEAT_INTERVAL_MS = 50;
constexpr int ELECTION_RETRY_MS = 100;
constexpr size_t VOTE_REPLY_SIZE = 9;
constexpr size_t APPEND_REPLY_SIZE = 9;
}  // namespace

RaftNode::RaftNode(std::string node_id, cluster::ClusterManager& cluster_manager,
                   network::RpcServer& rpc_server)
    : node_id_(std::move(node_id)),
      cluster_manager_(cluster_manager),
      rpc_server_(rpc_server),
      rng_(std::random_device{}()) {
    last_heartbeat_ = std::chrono::system_clock::now();
}

RaftNode::~RaftNode() {
    stop();
}

void RaftNode::start() {
    running_ = true;
    raft_thread_ = std::thread(&RaftNode::run_loop, this);

    // Register handlers
    rpc_server_.set_handler(network::RpcType::RequestVote,
                            [this](const network::RpcHeader& h, const std::vector<uint8_t>& p,
                                   int fd) { handle_request_vote(h, p, fd); });
    rpc_server_.set_handler(network::RpcType::AppendEntries,
                            [this](const network::RpcHeader& h, const std::vector<uint8_t>& p,
                                   int fd) { handle_append_entries(h, p, fd); });
}

void RaftNode::stop() {
    running_ = false;
    cv_.notify_all();
    if (raft_thread_.joinable()) {
        raft_thread_.join();
    }
}

void RaftNode::run_loop() {
    while (running_) {
        switch (state_.load()) {
            case NodeState::Follower:
                do_follower();
                break;
            case NodeState::Candidate:
                do_candidate();
                break;
            case NodeState::Leader:
                do_leader();
                break;
            case NodeState::Shutdown:
                return;
        }
    }
}

void RaftNode::do_follower() {
    const auto timeout = get_random_timeout();
    std::unique_lock<std::mutex> lock(mutex_);
    if (cv_.wait_for(lock, timeout, [this] {
            return !running_ ||
                   (std::chrono::system_clock::now() - last_heartbeat_ > get_random_timeout());
        })) {
        if (!running_) {
            return;
        }
        // Election timeout reached, become candidate
        state_ = NodeState::Candidate;
    }
}

void RaftNode::do_candidate() {
    {
        const std::scoped_lock<std::mutex> lock(mutex_);
        persistent_state_.current_term++;
        persistent_state_.voted_for = node_id_;
        persist_state();
        last_heartbeat_ = std::chrono::system_clock::now();
    }

    auto peers = cluster_manager_.get_coordinators();
    size_t votes = 1;  // Vote for self
    const size_t needed = (peers.size() / 2) + 1;

    RequestVoteArgs args{};
    {
        const std::scoped_lock<std::mutex> lock(mutex_);
        args.term = persistent_state_.current_term;
        args.candidate_id = node_id_;
        args.last_log_index =
            persistent_state_.log.empty() ? 0 : persistent_state_.log.back().index;
        args.last_log_term = persistent_state_.log.empty() ? 0 : persistent_state_.log.back().term;
    }

    // Send RequestVote to peers
    for (const auto& peer : peers) {
        if (peer.id == node_id_) {
            continue;
        }

        // Simplified synchronous call for now
        network::RpcClient client(peer.address, peer.cluster_port);
        if (client.connect()) {
            std::vector<uint8_t> reply_payload;
            if (client.call(network::RpcType::RequestVote, args.serialize(), reply_payload)) {
                if (reply_payload.size() >= VOTE_REPLY_SIZE) {
                    term_t resp_term = 0;
                    std::memcpy(&resp_term, reply_payload.data(), 8);
                    const bool granted = reply_payload[8] != 0;

                    if (resp_term > args.term) {
                        step_down(resp_term);
                        return;
                    }
                    if (granted) {
                        votes++;
                    }
                }
            }
        }
    }

    if (votes >= needed) {
        state_ = NodeState::Leader;
        // Initialize leader state
        const std::scoped_lock<std::mutex> lock(mutex_);
        for (const auto& peer : peers) {
            leader_state_.next_index[peer.id] = persistent_state_.log.size() + 1;
            leader_state_.match_index[peer.id] = 0;
        }
    } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(ELECTION_RETRY_MS));
    }
}

void RaftNode::do_leader() {
    auto peers = cluster_manager_.get_coordinators();
    for (const auto& peer : peers) {
        if (peer.id == node_id_) {
            continue;
        }
        // Send Heartbeat (AppendEntries with no entries)
        std::vector<uint8_t> args_payload(24, 0);  // Minimal heartbeat
        {
            const std::scoped_lock<std::mutex> lock(mutex_);
            const term_t t = persistent_state_.current_term;
            std::memcpy(args_payload.data(), &t, 8);
            // More fields would go here in full implementation
        }

        network::RpcClient client(peer.address, peer.cluster_port);
        if (client.connect()) {
            static_cast<void>(client.send_only(network::RpcType::AppendEntries, args_payload));
        }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(HEARTBEAT_INTERVAL_MS));
}

void RaftNode::handle_request_vote(const network::RpcHeader& header,
                                   const std::vector<uint8_t>& payload, int client_fd) {
    (void)header;
    if (payload.size() < 24) {
        return;
    }

    term_t term = 0;
    uint64_t id_len = 0;
    std::memcpy(&term, payload.data(), 8);
    std::memcpy(&id_len, payload.data() + 8, 8);
    const std::string candidate_id(reinterpret_cast<const char*>(payload.data() + 16), id_len);

    std::scoped_lock<std::mutex> lock(mutex_);
    RequestVoteReply reply{};
    reply.term = persistent_state_.current_term;
    reply.vote_granted = false;

    if (term > persistent_state_.current_term) {
        step_down(term);
    }

    if (term == persistent_state_.current_term &&
        (persistent_state_.voted_for.empty() || persistent_state_.voted_for == candidate_id)) {
        persistent_state_.voted_for = candidate_id;
        persist_state();
        reply.vote_granted = true;
        last_heartbeat_ = std::chrono::system_clock::now();
    }

    std::vector<uint8_t> out(VOTE_REPLY_SIZE);
    std::memcpy(out.data(), &reply.term, 8);
    out[8] = reply.vote_granted ? 1 : 0;

    // Send response back
    network::RpcHeader resp_h;
    resp_h.type = network::RpcType::RequestVote;
    resp_h.payload_len = static_cast<uint16_t>(VOTE_REPLY_SIZE);
    char h_buf[8];
    resp_h.encode(h_buf);
    static_cast<void>(send(client_fd, h_buf, 8, 0));
    static_cast<void>(send(client_fd, out.data(), out.size(), 0));
}

void RaftNode::handle_append_entries(const network::RpcHeader& header,
                                     const std::vector<uint8_t>& payload, int client_fd) {
    (void)header;
    if (payload.size() < 8) {
        return;
    }

    term_t term = 0;
    std::memcpy(&term, payload.data(), 8);

    std::scoped_lock<std::mutex> lock(mutex_);
    AppendEntriesReply reply{};
    reply.term = persistent_state_.current_term;
    reply.success = false;

    if (term >= persistent_state_.current_term) {
        if (term > persistent_state_.current_term) {
            step_down(term);
        }
        state_ = NodeState::Follower;
        last_heartbeat_ = std::chrono::system_clock::now();
        reply.success = true;
    }

    std::vector<uint8_t> out(APPEND_REPLY_SIZE);
    std::memcpy(out.data(), &reply.term, 8);
    out[8] = reply.success ? 1 : 0;

    network::RpcHeader resp_h;
    resp_h.type = network::RpcType::AppendEntries;
    resp_h.payload_len = static_cast<uint16_t>(APPEND_REPLY_SIZE);
    char h_buf[8];
    resp_h.encode(h_buf);
    static_cast<void>(send(client_fd, h_buf, 8, 0));
    static_cast<void>(send(client_fd, out.data(), out.size(), 0));
}

void RaftNode::step_down(term_t new_term) {
    persistent_state_.current_term = new_term;
    persistent_state_.voted_for = "";
    state_ = NodeState::Follower;
    persist_state();
}

std::chrono::milliseconds RaftNode::get_random_timeout() const {
    std::uniform_int_distribution<int> dist(TIMEOUT_MIN_MS, TIMEOUT_MAX_MS);
    auto& mutable_rng = const_cast<std::mt19937&>(rng_);
    return std::chrono::milliseconds(dist(mutable_rng));
}

void RaftNode::persist_state() { /* TODO */ }
void RaftNode::load_state() { /* TODO */ }

bool RaftNode::replicate(const std::string& command) {
    if (state_.load() != NodeState::Leader) {
        return false;
    }
    (void)command;
    return true;
}

}  // namespace cloudsql::raft
