/**
 * @file raft_group.cpp
 * @brief Raft consensus group implementation
 */

#include "distributed/raft_group.hpp"

#include <sys/socket.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <fstream>
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

/**
 * @brief Simple helper to serialize a LogEntry
 */
void serialize_entry(const LogEntry& entry, std::vector<uint8_t>& out) {
    size_t offset = out.size();
    out.resize(offset + 24 + entry.data.size());
    std::memcpy(out.data() + offset, &entry.term, 8);
    std::memcpy(out.data() + offset + 8, &entry.index, 8);
    uint64_t data_len = entry.data.size();
    std::memcpy(out.data() + offset + 16, &data_len, 8);
    std::memcpy(out.data() + offset + 24, entry.data.data(), data_len);
}

/**
 * @brief Simple helper to deserialize a LogEntry
 */
LogEntry deserialize_entry(const uint8_t* data, size_t& offset, size_t size) {
    LogEntry entry;
    if (offset + 24 > size) return entry;
    std::memcpy(&entry.term, data + offset, 8);
    std::memcpy(&entry.index, data + offset + 8, 8);
    uint64_t data_len = 0;
    std::memcpy(&data_len, data + offset + 16, 8);
    offset += 24;
    if (offset + data_len <= size) {
        entry.data.assign(data + offset, data + offset + data_len);
        offset += data_len;
    }
    return entry;
}

}  // namespace

RaftGroup::RaftGroup(uint16_t group_id, std::string node_id,
                     cluster::ClusterManager& cluster_manager, network::RpcServer& rpc_server)
    : group_id_(group_id),
      node_id_(std::move(node_id)),
      cluster_manager_(cluster_manager),
      rpc_server_(rpc_server),
      rng_(std::random_device{}()) {
    last_heartbeat_ = std::chrono::system_clock::now();
    load_state();
}

RaftGroup::~RaftGroup() {
    stop();
}

void RaftGroup::start() {
    running_ = true;
    raft_thread_ = std::thread(&RaftGroup::run_loop, this);
}

void RaftGroup::stop() {
    running_ = false;
    cv_.notify_all();
    if (raft_thread_.joinable()) {
        raft_thread_.join();
    }
}

void RaftGroup::run_loop() {
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

void RaftGroup::do_follower() {
    const auto timeout = get_random_timeout();
    std::unique_lock<std::mutex> lock(mutex_);
    if (!cv_.wait_for(lock, timeout, [this] { return !running_; })) {
        auto now = std::chrono::system_clock::now();
        if (now - last_heartbeat_ >= timeout) {
            state_ = NodeState::Candidate;
        }
    }
}

void RaftGroup::do_candidate() {
    {
        const std::scoped_lock<std::mutex> lock(mutex_);
        persistent_state_.current_term++;
        persistent_state_.voted_for = node_id_;
        persist_state();
        last_heartbeat_ = std::chrono::system_clock::now();
    }

    auto peers = cluster_manager_.get_coordinators();
    size_t votes = 1;
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

    for (const auto& peer : peers) {
        if (peer.id == node_id_) continue;

        network::RpcClient client(peer.address, peer.cluster_port);
        if (client.connect()) {
            std::vector<uint8_t> reply_payload;
            if (client.call(network::RpcType::RequestVote, args.serialize(), reply_payload,
                            group_id_)) {
                if (reply_payload.size() >= VOTE_REPLY_SIZE) {
                    term_t resp_term = 0;
                    std::memcpy(&resp_term, reply_payload.data(), 8);
                    const bool granted = reply_payload[8] != 0;

                    if (resp_term > args.term) {
                        step_down(resp_term);
                        return;
                    }
                    if (granted) votes++;
                }
            }
        }
    }

    if (votes >= needed) {
        state_ = NodeState::Leader;
        cluster_manager_.set_leader(group_id_, node_id_);
        const std::scoped_lock<std::mutex> lock(mutex_);
        for (const auto& peer : peers) {
            leader_state_.next_index[peer.id] =
                persistent_state_.log.empty() ? 1 : persistent_state_.log.back().index + 1;
            leader_state_.match_index[peer.id] = 0;
        }
    } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(ELECTION_RETRY_MS));
    }
}

void RaftGroup::do_leader() {
    auto peers = cluster_manager_.get_coordinators();
    for (const auto& peer : peers) {
        if (peer.id == node_id_) continue;

        std::vector<uint8_t> payload(32, 0);
        {
            const std::scoped_lock<std::mutex> lock(mutex_);
            std::memcpy(payload.data(), &persistent_state_.current_term, 8);
            uint64_t id_len = node_id_.size();
            std::memcpy(payload.data() + 8, &id_len, 8);
        }

        network::RpcClient client(peer.address, peer.cluster_port);
        if (client.connect()) {
            static_cast<void>(
                client.send_only(network::RpcType::AppendEntries, payload, group_id_));
        }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(HEARTBEAT_INTERVAL_MS));
}

void RaftGroup::handle_request_vote(const network::RpcHeader& header,
                                    const std::vector<uint8_t>& payload, int client_fd) {
    (void)header;
    if (payload.size() < 24) return;

    term_t term = 0;
    uint64_t id_len = 0;
    std::memcpy(&term, payload.data(), 8);
    std::memcpy(&id_len, payload.data() + 8, 8);
    const std::string candidate_id(reinterpret_cast<const char*>(payload.data() + 16), id_len);

    std::scoped_lock<std::mutex> lock(mutex_);
    RequestVoteReply reply{};
    reply.term = persistent_state_.current_term;
    reply.vote_granted = false;

    if (term > persistent_state_.current_term) step_down(term);

    if (term == persistent_state_.current_term &&
        (persistent_state_.voted_for.empty() || persistent_state_.voted_for == candidate_id)) {
        persistent_state_.voted_for = candidate_id;
        persist_state();
        reply.vote_granted = true;
        last_heartbeat_ = std::chrono::system_clock::now();
        cv_.notify_all();
    }

    if (client_fd >= 0) {
        std::vector<uint8_t> out(VOTE_REPLY_SIZE);
        std::memcpy(out.data(), &reply.term, 8);
        out[8] = reply.vote_granted ? 1 : 0;

        network::RpcHeader resp_h;
        resp_h.type = network::RpcType::RequestVote;
        resp_h.group_id = group_id_;
        resp_h.payload_len = static_cast<uint16_t>(VOTE_REPLY_SIZE);
        char h_buf[network::RpcHeader::HEADER_SIZE];
        resp_h.encode(h_buf);
        static_cast<void>(send(client_fd, h_buf, network::RpcHeader::HEADER_SIZE, 0));
        static_cast<void>(send(client_fd, out.data(), out.size(), 0));
    }
}

void RaftGroup::handle_append_entries(const network::RpcHeader& header,
                                      const std::vector<uint8_t>& payload, int client_fd) {
    (void)header;
    if (payload.size() < 8) return;

    term_t term = 0;
    std::memcpy(&term, payload.data(), 8);

    std::scoped_lock<std::mutex> lock(mutex_);
    AppendEntriesReply reply{};
    reply.term = persistent_state_.current_term;
    reply.success = false;

    if (term >= persistent_state_.current_term) {
        if (term > persistent_state_.current_term) step_down(term);
        state_ = NodeState::Follower;
        last_heartbeat_ = std::chrono::system_clock::now();
        cv_.notify_all();
        reply.success = true;

        if (state_machine_) {
            while (volatile_state_.last_applied < volatile_state_.commit_index) {
                volatile_state_.last_applied++;
                for (const auto& entry : persistent_state_.log) {
                    if (entry.index == volatile_state_.last_applied) {
                        state_machine_->apply(entry);
                        break;
                    }
                }
            }
        }
    }

    if (client_fd >= 0) {
        std::vector<uint8_t> out(APPEND_REPLY_SIZE);
        std::memcpy(out.data(), &reply.term, 8);
        out[8] = reply.success ? 1 : 0;

        network::RpcHeader resp_h;
        resp_h.type = network::RpcType::AppendEntries;
        resp_h.group_id = group_id_;
        resp_h.payload_len = static_cast<uint16_t>(APPEND_REPLY_SIZE);
        char h_buf[network::RpcHeader::HEADER_SIZE];
        resp_h.encode(h_buf);
        static_cast<void>(send(client_fd, h_buf, network::RpcHeader::HEADER_SIZE, 0));
        static_cast<void>(send(client_fd, out.data(), out.size(), 0));
    }
}

void RaftGroup::step_down(term_t new_term) {
    persistent_state_.current_term = new_term;
    persistent_state_.voted_for = "";
    state_ = NodeState::Follower;
    persist_state();
}

std::chrono::milliseconds RaftGroup::get_random_timeout() const {
    std::uniform_int_distribution<int> dist(TIMEOUT_MIN_MS, TIMEOUT_MAX_MS);
    auto& mutable_rng = const_cast<std::mt19937&>(rng_);
    return std::chrono::milliseconds(dist(mutable_rng));
}

void RaftGroup::persist_state() {
    std::string filename = "raft_group_" + std::to_string(group_id_) + ".state";
    std::ofstream out(filename, std::ios::binary);
    if (out.is_open()) {
        out.write(reinterpret_cast<const char*>(&persistent_state_.current_term), 8);
        uint64_t v_len = persistent_state_.voted_for.size();
        out.write(reinterpret_cast<const char*>(&v_len), 8);
        out.write(persistent_state_.voted_for.data(), v_len);

        uint64_t log_size = persistent_state_.log.size();
        out.write(reinterpret_cast<const char*>(&log_size), 8);
        for (const auto& entry : persistent_state_.log) {
            std::vector<uint8_t> serialized;
            serialize_entry(entry, serialized);
            uint64_t entry_len = serialized.size();
            out.write(reinterpret_cast<const char*>(&entry_len), 8);
            out.write(reinterpret_cast<const char*>(serialized.data()), entry_len);
        }
    }
}

void RaftGroup::load_state() {
    std::string filename = "raft_group_" + std::to_string(group_id_) + ".state";
    std::ifstream in(filename, std::ios::binary);
    if (in.is_open()) {
        in.read(reinterpret_cast<char*>(&persistent_state_.current_term), 8);
        uint64_t v_len = 0;
        in.read(reinterpret_cast<char*>(&v_len), 8);
        persistent_state_.voted_for.resize(v_len);
        in.read(&persistent_state_.voted_for[0], v_len);

        uint64_t log_size = 0;
        in.read(reinterpret_cast<char*>(&log_size), 8);
        for (uint64_t i = 0; i < log_size; ++i) {
            uint64_t entry_len = 0;
            in.read(reinterpret_cast<char*>(&entry_len), 8);
            std::vector<uint8_t> buf(entry_len);
            in.read(reinterpret_cast<char*>(buf.data()), entry_len);
            size_t offset = 0;
            persistent_state_.log.push_back(deserialize_entry(buf.data(), offset, entry_len));
        }
    }
}

bool RaftGroup::replicate(const std::vector<uint8_t>& data) {
    if (state_.load() != NodeState::Leader) return false;

    std::scoped_lock<std::mutex> lock(mutex_);
    LogEntry entry;
    entry.term = persistent_state_.current_term;
    entry.index = persistent_state_.log.empty() ? 1 : persistent_state_.log.back().index + 1;
    entry.data = data;
    persistent_state_.log.push_back(std::move(entry));
    persist_state();
    return true;
}

}  // namespace cloudsql::raft
