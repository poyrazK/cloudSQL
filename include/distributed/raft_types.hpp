/**
 * @file raft_types.hpp
 * @brief Core types and structures for the Raft consensus algorithm
 */

#ifndef SQL_ENGINE_DISTRIBUTED_RAFT_TYPES_HPP
#define SQL_ENGINE_DISTRIBUTED_RAFT_TYPES_HPP

#include <cstdint>
#include <cstring>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/value.hpp"

namespace cloudsql::raft {

using term_t = uint64_t;
using index_t = uint64_t;

/**
 * @brief Raft Node States
 */
enum class NodeState : uint8_t { Follower, Candidate, Leader, Shutdown };

/**
 * @brief A single entry in the Raft log
 */
struct LogEntry {
    term_t term = 0;
    index_t index = 0;
    std::string data;  // Serialized command (e.g., DDL SQL)
};

/**
 * @brief RequestVote RPC arguments
 */
struct RequestVoteArgs {
    term_t term = 0;
    std::string candidate_id;
    index_t last_log_index = 0;
    term_t last_log_term = 0;

    [[nodiscard]] std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> out;
        constexpr size_t BASE_SIZE = 24;
        out.resize(BASE_SIZE + candidate_id.size());
        std::memcpy(out.data(), &term, sizeof(term_t));
        const uint64_t id_len = candidate_id.size();
        std::memcpy(out.data() + 8, &id_len, 8);
        std::memcpy(out.data() + 16, candidate_id.data(), id_len);
        std::memcpy(out.data() + 16 + id_len, &last_log_index, sizeof(index_t));
        std::memcpy(out.data() + 24 + id_len, &last_log_term, sizeof(term_t));
        return out;
    }
};

/**
 * @brief RequestVote RPC response
 */
struct RequestVoteReply {
    term_t term = 0;
    bool vote_granted = false;
};

/**
 * @brief AppendEntries RPC arguments
 */
struct AppendEntriesArgs {
    term_t term = 0;
    std::string leader_id;
    index_t prev_log_index = 0;
    term_t prev_log_term = 0;
    std::vector<LogEntry> entries;
    index_t leader_commit = 0;
};

/**
 * @brief AppendEntries RPC response
 */
struct AppendEntriesReply {
    term_t term = 0;
    bool success = false;
};

/**
 * @brief Persistent state that must be saved to stable storage before responding to RPCs
 */
struct RaftPersistentState {
    term_t current_term = 0;
    std::string voted_for;  // Node ID of the candidate that received vote in current term
    std::vector<LogEntry> log;
};

/**
 * @brief Volatile state on all servers
 */
struct RaftVolatileState {
    index_t commit_index = 0;
    index_t last_applied = 0;
};

/**
 * @brief Volatile state on leaders (reinitialized after election)
 */
struct LeaderState {
    // For each server, index of the next log entry to send to that server
    std::unordered_map<std::string, index_t> next_index;
    // For each server, index of highest log entry known to be replicated on server
    std::unordered_map<std::string, index_t> match_index;
};

}  // namespace cloudsql::raft

#endif  // SQL_ENGINE_DISTRIBUTED_RAFT_TYPES_HPP
