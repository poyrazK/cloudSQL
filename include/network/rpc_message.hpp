/**
 * @file rpc_message.hpp
 * @brief Binary message format for internal cluster communication
 */

#ifndef SQL_ENGINE_NETWORK_RPC_MESSAGE_HPP
#define SQL_ENGINE_NETWORK_RPC_MESSAGE_HPP

#include <arpa/inet.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "executor/types.hpp"

namespace cloudsql::network {

/**
 * @brief Internal RPC Message Types
 */
enum class RpcType : uint8_t {
    Heartbeat = 0,
    RegisterNode = 1,
    RequestVote = 2,
    AppendEntries = 3,
    ExecuteFragment = 4,
    QueryResults = 5,
    TxnPrepare = 6,
    TxnCommit = 7,
    TxnAbort = 8,
    Error = 255
};

/**
 * @brief Header for all internal RPC messages (fixed 8 bytes)
 */
struct RpcHeader {
    static constexpr uint32_t MAGIC = 0x4353514C;  // 'CSQL'
    static constexpr size_t HEADER_SIZE = 8;

    uint32_t magic = MAGIC;
    RpcType type = RpcType::Error;
    uint8_t flags = 0;
    uint16_t payload_len = 0;

    void encode(char* out) const {
        uint32_t n_magic = htonl(magic);
        uint16_t n_len = htons(payload_len);
        std::memcpy(out, &n_magic, 4);
        out[4] = static_cast<char>(type);
        out[5] = static_cast<char>(flags);
        std::memcpy(out + 6, &n_len, 2);
    }

    static RpcHeader decode(const char* in) {
        RpcHeader h;
        uint32_t n_magic = 0;
        uint16_t n_len = 0;
        std::memcpy(&n_magic, in, 4);
        h.magic = ntohl(n_magic);
        h.type = static_cast<RpcType>(static_cast<uint8_t>(in[4]));
        h.flags = static_cast<uint8_t>(in[5]);
        std::memcpy(&n_len, in + 6, 2);
        h.payload_len = ntohs(n_len);
        return h;
    }
};

/**
 * @brief Payload for executing a SQL fragment on a data node
 */
struct ExecuteFragmentArgs {
    std::string sql;

    [[nodiscard]] std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> out(sql.size());
        std::memcpy(out.data(), sql.data(), sql.size());
        return out;
    }

    static ExecuteFragmentArgs deserialize(const std::vector<uint8_t>& in) {
        ExecuteFragmentArgs args;
        args.sql = std::string(reinterpret_cast<const char*>(in.data()), in.size());
        return args;
    }
};

/**
 * @brief Simple payload for returning query success/failure and data
 */
struct QueryResultsReply {
    bool success = false;
    std::string error_msg;
    std::vector<executor::Tuple> rows;

    [[nodiscard]] std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> out;
        out.push_back(success ? 1 : 0);

        uint32_t err_len = static_cast<uint32_t>(error_msg.size());
        size_t offset = out.size();
        out.resize(offset + 4 + err_len);
        std::memcpy(out.data() + offset, &err_len, 4);
        std::memcpy(out.data() + offset + 4, error_msg.data(), err_len);

        // Simplified row count serialization
        uint32_t row_count = static_cast<uint32_t>(rows.size());
        offset = out.size();
        out.resize(offset + 4);
        std::memcpy(out.data() + offset, &row_count, 4);

        // In a real implementation, we'd serialize each tuple's values here.
        // For Phase 4 POC, we'll return row counts.

        return out;
    }

    static QueryResultsReply deserialize(const std::vector<uint8_t>& in) {
        QueryResultsReply reply;
        if (in.empty()) {
            return reply;
        }

        reply.success = in[0] != 0;

        uint32_t err_len = 0;
        std::memcpy(&err_len, in.data() + 1, 4);
        if (in.size() >= 5 + err_len) {
            reply.error_msg = std::string(reinterpret_cast<const char*>(in.data() + 5), err_len);
        }

        uint32_t row_count = 0;
        if (in.size() >= 9 + err_len) {
            std::memcpy(&row_count, in.data() + 5 + err_len, 4);
            reply.rows.resize(row_count);  // Placeholders
        }

        return reply;
    }
};

/**
 * @brief Payload for 2PC Operations (Prepare, Commit, Abort)
 */
struct TxnOperationArgs {
    uint64_t txn_id = 0;

    [[nodiscard]] std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> out(8);
        std::memcpy(out.data(), &txn_id, 8);
        return out;
    }

    static TxnOperationArgs deserialize(const std::vector<uint8_t>& in) {
        TxnOperationArgs args;
        if (in.size() >= 8) {
            std::memcpy(&args.txn_id, in.data(), 8);
        }
        return args;
    }
};

/**
 * @brief Base class for RPC Payloads
 */
class RpcMessage {
   public:
    virtual ~RpcMessage() = default;
    [[nodiscard]] virtual RpcType type() const = 0;
    [[nodiscard]] virtual std::vector<uint8_t> serialize() const = 0;

    RpcMessage() = default;
    RpcMessage(const RpcMessage&) = default;
    RpcMessage& operator=(const RpcMessage&) = default;
    RpcMessage(RpcMessage&&) = default;
    RpcMessage& operator=(RpcMessage&&) = default;
};

}  // namespace cloudsql::network

#endif  // SQL_ENGINE_NETWORK_RPC_MESSAGE_HPP
