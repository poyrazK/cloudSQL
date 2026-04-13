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

#include "common/value.hpp"
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
    PushData = 9,
    ShuffleFragment = 10,
    BloomFilterPush = 11,
    BloomFilterBits = 12,
    Error = 255
};

/**
 * @brief Serialization utilities for Common types
 */
class Serializer {
   public:
    static constexpr size_t VAL_SIZE_64 = 8;
    static constexpr size_t VAL_SIZE_32 = 4;

    static void serialize_value(const common::Value& val, std::vector<uint8_t>& out) {
        auto type = static_cast<uint8_t>(val.type());
        out.push_back(type);
        if (val.is_null()) {
            return;
        }

        if (val.is_numeric()) {
            // POC: unify all numerics to 64-bit float/int in the stream for simplicity
            if (val.type() == common::ValueType::TYPE_FLOAT32 ||
                val.type() == common::ValueType::TYPE_FLOAT64) {
                double v = val.to_float64();
                const size_t offset = out.size();
                out.resize(offset + VAL_SIZE_64);
                std::memcpy(out.data() + offset, &v, VAL_SIZE_64);
            } else {
                int64_t v = val.to_int64();
                const size_t offset = out.size();
                out.resize(offset + VAL_SIZE_64);
                std::memcpy(out.data() + offset, &v, VAL_SIZE_64);
            }
            return;
        }

        switch (val.type()) {
            case common::ValueType::TYPE_TEXT:
            case common::ValueType::TYPE_VARCHAR:
            case common::ValueType::TYPE_CHAR: {
                const std::string& s = val.to_string();  // fallback to string for anything else
                const auto len = static_cast<uint32_t>(s.size());
                const size_t offset = out.size();
                out.resize(offset + VAL_SIZE_32 + len);
                std::memcpy(out.data() + offset, &len, VAL_SIZE_32);
                std::memcpy(out.data() + offset + VAL_SIZE_32, s.data(), len);
                break;
            }
            default:
                break;
        }
    }

    static common::Value deserialize_value(const uint8_t* data, size_t& offset, size_t size) {
        if (offset >= size) {
            return common::Value::make_null();
        }
        auto type = static_cast<common::ValueType>(data[offset++]);
        if (type == common::ValueType::TYPE_NULL) {
            return common::Value::make_null();
        }

        if (type == common::ValueType::TYPE_FLOAT32 || type == common::ValueType::TYPE_FLOAT64) {
            double v = 0;
            if (offset + VAL_SIZE_64 <= size) {
                std::memcpy(&v, data + offset, VAL_SIZE_64);
                offset += VAL_SIZE_64;
            }
            return common::Value::make_float64(v);
        }

        if (type == common::ValueType::TYPE_INT8 || type == common::ValueType::TYPE_INT16 ||
            type == common::ValueType::TYPE_INT32 || type == common::ValueType::TYPE_INT64) {
            int64_t v = 0;
            if (offset + VAL_SIZE_64 <= size) {
                std::memcpy(&v, data + offset, VAL_SIZE_64);
                offset += VAL_SIZE_64;
            }
            return common::Value::make_int64(v);
        }

        switch (type) {
            case common::ValueType::TYPE_TEXT:
            case common::ValueType::TYPE_VARCHAR:
            case common::ValueType::TYPE_CHAR: {
                uint32_t len = 0;
                if (offset + VAL_SIZE_32 <= size) {
                    std::memcpy(&len, data + offset, VAL_SIZE_32);
                    offset += VAL_SIZE_32;
                }
                std::string s;
                if (offset + len <= size) {
                    s = std::string(reinterpret_cast<const char*>(data + offset), len);
                    offset += len;
                }
                return common::Value::make_text(s);
            }
            default:
                return common::Value::make_null();
        }
    }

    static void serialize_tuple(const executor::Tuple& tuple, std::vector<uint8_t>& out) {
        const auto count = static_cast<uint32_t>(tuple.size());
        const size_t offset = out.size();
        out.resize(offset + VAL_SIZE_32);
        std::memcpy(out.data() + offset, &count, VAL_SIZE_32);
        for (size_t i = 0; i < count; ++i) {
            serialize_value(tuple.get(i), out);
        }
    }

    static executor::Tuple deserialize_tuple(const uint8_t* data, size_t& offset, size_t size) {
        uint32_t count = 0;
        if (offset + VAL_SIZE_32 <= size) {
            std::memcpy(&count, data + offset, VAL_SIZE_32);
            offset += VAL_SIZE_32;
        }
        std::vector<common::Value> values;
        values.reserve(count);
        for (uint32_t i = 0; i < count; ++i) {
            values.push_back(deserialize_value(data, offset, size));
        }
        return executor::Tuple(std::move(values));
    }

    static void serialize_string(const std::string& s, std::vector<uint8_t>& out) {
        const auto len = static_cast<uint32_t>(s.size());
        const size_t offset = out.size();
        out.resize(offset + VAL_SIZE_32 + len);
        std::memcpy(out.data() + offset, &len, VAL_SIZE_32);
        std::memcpy(out.data() + offset + VAL_SIZE_32, s.data(), len);
    }

    static std::string deserialize_string(const uint8_t* data, size_t& offset, size_t size) {
        uint32_t len = 0;
        if (offset + VAL_SIZE_32 <= size) {
            std::memcpy(&len, data + offset, VAL_SIZE_32);
            offset += VAL_SIZE_32;
        }
        std::string s;
        if (offset + len <= size) {
            s.assign(reinterpret_cast<const char*>(data + offset), len);
            offset += len;
        }
        return s;
    }

    static void serialize_schema(const executor::Schema& schema, std::vector<uint8_t>& out) {
        const auto count = static_cast<uint32_t>(schema.column_count());
        const size_t offset = out.size();
        out.resize(offset + VAL_SIZE_32);
        std::memcpy(out.data() + offset, &count, VAL_SIZE_32);
        for (size_t i = 0; i < count; ++i) {
            const auto& col = schema.get_column(i);
            serialize_string(col.name(), out);
            out.push_back(static_cast<uint8_t>(col.type()));
        }
    }

    static executor::Schema deserialize_schema(const uint8_t* data, size_t& offset, size_t size) {
        uint32_t count = 0;
        if (offset + VAL_SIZE_32 <= size) {
            std::memcpy(&count, data + offset, VAL_SIZE_32);
            offset += VAL_SIZE_32;
        }
        executor::Schema schema;
        for (uint32_t i = 0; i < count; ++i) {
            std::string name = deserialize_string(data, offset, size);
            auto type = static_cast<common::ValueType>(data[offset++]);
            schema.add_column(name, type);
        }
        return schema;
    }
};

/**
 * @brief Header for all internal RPC messages (fixed 12 bytes)
 */
struct RpcHeader {
    static constexpr uint32_t MAGIC = 0x4353514C;  // 'CSQL'
    static constexpr size_t HEADER_SIZE = 12;

    uint32_t magic = MAGIC;
    RpcType type = RpcType::Error;
    uint8_t flags = 0;
    uint16_t group_id = 0;  // For Multi-Group Raft
    uint16_t reserved = 0;
    uint16_t payload_len = 0;

    void encode(char* out) const {
        uint32_t n_magic = htonl(magic);
        uint16_t n_group = htons(group_id);
        uint16_t n_reserved = htons(reserved);
        uint16_t n_len = htons(payload_len);
        std::memcpy(out, &n_magic, 4);
        out[4] = static_cast<char>(type);
        out[5] = static_cast<char>(flags);
        std::memcpy(out + 6, &n_group, 2);
        std::memcpy(out + 8, &n_reserved, 2);
        std::memcpy(out + 10, &n_len, 2);
    }

    static RpcHeader decode(const char* in) {
        RpcHeader h;
        uint32_t n_magic = 0;
        uint16_t n_group = 0;
        uint16_t n_reserved = 0;
        uint16_t n_len = 0;
        std::memcpy(&n_magic, in, 4);
        h.magic = ntohl(n_magic);
        h.type = static_cast<RpcType>(static_cast<uint8_t>(in[4]));
        h.flags = static_cast<uint8_t>(in[5]);
        std::memcpy(&n_group, in + 6, 2);
        h.group_id = ntohs(n_group);
        std::memcpy(&n_reserved, in + 8, 2);
        h.reserved = ntohs(n_reserved);
        std::memcpy(&n_len, in + 10, 2);
        h.payload_len = ntohs(n_len);
        return h;
    }
};

/**
 * @brief Arguments for RegisterNode RPC
 */
struct RegisterNodeArgs {
    std::string id;
    std::string address;
    uint16_t port;
    uint8_t mode;  // 0: Standalone, 1: Coordinator, 2: Data

    [[nodiscard]] std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> out;
        Serializer::serialize_string(id, out);
        Serializer::serialize_string(address, out);
        const size_t offset = out.size();
        out.resize(offset + 2);
        uint16_t n_port = htons(port);
        std::memcpy(out.data() + offset, &n_port, 2);
        out.push_back(mode);
        return out;
    }

    static RegisterNodeArgs deserialize(const std::vector<uint8_t>& in) {
        RegisterNodeArgs args;
        size_t offset = 0;
        args.id = Serializer::deserialize_string(in.data(), offset, in.size());
        args.address = Serializer::deserialize_string(in.data(), offset, in.size());
        if (offset + 2 <= in.size()) {
            uint16_t n_port = 0;
            std::memcpy(&n_port, in.data() + offset, 2);
            args.port = ntohs(n_port);
            offset += 2;
        }
        if (offset < in.size()) {
            args.mode = in[offset];
        }
        return args;
    }
};

/**
 * @brief Arguments for ExecuteFragment RPC
 */
struct ExecuteFragmentArgs {
    std::string sql;
    std::string context_id;
    bool is_fetch_all = false;

    [[nodiscard]] std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> out;
        Serializer::serialize_string(sql, out);
        Serializer::serialize_string(context_id, out);
        out.push_back(is_fetch_all ? 1 : 0);
        return out;
    }

    static ExecuteFragmentArgs deserialize(const std::vector<uint8_t>& in) {
        ExecuteFragmentArgs args;
        size_t offset = 0;
        args.sql = Serializer::deserialize_string(in.data(), offset, in.size());
        args.context_id = Serializer::deserialize_string(in.data(), offset, in.size());
        if (offset < in.size()) {
            args.is_fetch_all = in[offset] != 0;
        }
        return args;
    }
};

/**
 * @brief Simple payload for returning query success/failure and data
 */
struct QueryResultsReply {
    bool success = false;
    std::string error_msg;
    executor::Schema schema;
    std::vector<executor::Tuple> rows;

    [[nodiscard]] std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> out;
        out.push_back(success ? 1 : 0);
        Serializer::serialize_string(error_msg, out);
        Serializer::serialize_schema(schema, out);

        const auto row_count = static_cast<uint32_t>(rows.size());
        const size_t offset = out.size();
        out.resize(offset + Serializer::VAL_SIZE_32);
        std::memcpy(out.data() + offset, &row_count, Serializer::VAL_SIZE_32);

        for (const auto& row : rows) {
            Serializer::serialize_tuple(row, out);
        }

        return out;
    }

    static QueryResultsReply deserialize(const std::vector<uint8_t>& in) {
        QueryResultsReply reply;
        if (in.empty()) {
            return reply;
        }

        reply.success = in[0] != 0;
        size_t offset = 1;
        reply.error_msg = Serializer::deserialize_string(in.data(), offset, in.size());
        reply.schema = Serializer::deserialize_schema(in.data(), offset, in.size());

        uint32_t row_count = 0;
        if (offset + Serializer::VAL_SIZE_32 <= in.size()) {
            std::memcpy(&row_count, in.data() + offset, Serializer::VAL_SIZE_32);
            offset += Serializer::VAL_SIZE_32;
        }
        for (uint32_t i = 0; i < row_count; ++i) {
            reply.rows.push_back(Serializer::deserialize_tuple(in.data(), offset, in.size()));
        }

        return reply;
    }
};

/**
 * @brief Payload for pushing data between nodes (Shuffle)
 */
struct PushDataArgs {
    std::string context_id;
    std::string table_name;
    std::vector<executor::Tuple> rows;

    [[nodiscard]] std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> out;
        Serializer::serialize_string(context_id, out);
        Serializer::serialize_string(table_name, out);

        const auto row_count = static_cast<uint32_t>(rows.size());
        const size_t offset = out.size();
        out.resize(offset + Serializer::VAL_SIZE_32);
        std::memcpy(out.data() + offset, &row_count, Serializer::VAL_SIZE_32);
        for (const auto& row : rows) {
            Serializer::serialize_tuple(row, out);
        }
        return out;
    }

    static PushDataArgs deserialize(const std::vector<uint8_t>& in) {
        PushDataArgs args;
        size_t offset = 0;
        args.context_id = Serializer::deserialize_string(in.data(), offset, in.size());
        args.table_name = Serializer::deserialize_string(in.data(), offset, in.size());
        uint32_t row_count = 0;
        if (offset + Serializer::VAL_SIZE_32 <= in.size()) {
            std::memcpy(&row_count, in.data() + offset, Serializer::VAL_SIZE_32);
            offset += Serializer::VAL_SIZE_32;
        }
        for (uint32_t i = 0; i < row_count; ++i) {
            args.rows.push_back(Serializer::deserialize_tuple(in.data(), offset, in.size()));
        }
        return args;
    }
};

/**
 * @brief Arguments for ShuffleFragment RPC
 */
struct ShuffleFragmentArgs {
    std::string context_id;
    std::string table_name;
    std::string join_key_col;

    [[nodiscard]] std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> out;
        Serializer::serialize_string(context_id, out);
        Serializer::serialize_string(table_name, out);
        Serializer::serialize_string(join_key_col, out);
        return out;
    }

    static ShuffleFragmentArgs deserialize(const std::vector<uint8_t>& in) {
        ShuffleFragmentArgs args;
        size_t offset = 0;
        args.context_id = Serializer::deserialize_string(in.data(), offset, in.size());
        args.table_name = Serializer::deserialize_string(in.data(), offset, in.size());
        args.join_key_col = Serializer::deserialize_string(in.data(), offset, in.size());
        return args;
    }
};

/**
 * @brief Arguments for BloomFilterPush RPC
 */
struct BloomFilterArgs {
    std::string context_id;
    std::string build_table;
    std::string probe_table;
    std::string probe_key_col;  // Join key column on probe side for filtering
    std::vector<uint8_t> filter_data;
    size_t expected_elements = 0;
    size_t num_hashes = 0;

    [[nodiscard]] std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> out;
        Serializer::serialize_string(context_id, out);
        Serializer::serialize_string(build_table, out);
        Serializer::serialize_string(probe_table, out);
        Serializer::serialize_string(probe_key_col, out);

        // Serialize filter data (blob)
        const auto filter_len = static_cast<uint32_t>(filter_data.size());
        const size_t off = out.size();
        out.resize(off + Serializer::VAL_SIZE_32);
        std::memcpy(out.data() + off, &filter_len, Serializer::VAL_SIZE_32);
        out.insert(out.end(), filter_data.begin(), filter_data.end());

        // Serialize metadata using fixed-width temporaries
        uint64_t tmp_expected = static_cast<uint64_t>(expected_elements);
        uint8_t tmp_hashes = static_cast<uint8_t>(num_hashes);
        const size_t off2 = out.size();
        out.resize(off2 + 9);  // 8 bytes for expected_elements + 1 for num_hashes
        std::memcpy(out.data() + off2, &tmp_expected, 8);
        out[off2 + 8] = tmp_hashes;
        return out;
    }

    static BloomFilterArgs deserialize(const std::vector<uint8_t>& in) {
        BloomFilterArgs args;
        size_t offset = 0;
        args.context_id = Serializer::deserialize_string(in.data(), offset, in.size());
        args.build_table = Serializer::deserialize_string(in.data(), offset, in.size());
        args.probe_table = Serializer::deserialize_string(in.data(), offset, in.size());
        args.probe_key_col = Serializer::deserialize_string(in.data(), offset, in.size());

        uint32_t filter_len = 0;
        if (offset + Serializer::VAL_SIZE_32 <= in.size()) {
            std::memcpy(&filter_len, in.data() + offset, Serializer::VAL_SIZE_32);
            offset += Serializer::VAL_SIZE_32;
        }
        if (offset + filter_len <= in.size()) {
            args.filter_data.resize(filter_len);
            std::memcpy(args.filter_data.data(), in.data() + offset, filter_len);
            offset += filter_len;
        }

        // Deserialize metadata using fixed-width temporaries
        if (offset + 9 <= in.size()) {
            uint64_t tmp_expected = 0;
            std::memcpy(&tmp_expected, in.data() + offset, 8);
            args.expected_elements = static_cast<size_t>(tmp_expected);
            offset += 8;
            args.num_hashes = static_cast<size_t>(in[offset]);
        }
        return args;
    }
};

/**
 * @brief Arguments for sending local bloom filter bits from data node to coordinator
 *         Used during Phase 1 to collect and aggregate bloom filters from all nodes
 */
struct BloomFilterBitsArgs {
    std::string context_id;
    std::vector<uint8_t> filter_data;
    size_t expected_elements = 0;
    size_t num_hashes = 0;

    [[nodiscard]] std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> out;
        Serializer::serialize_string(context_id, out);

        // Serialize filter data (blob)
        const uint32_t filter_len = static_cast<uint32_t>(filter_data.size());
        const size_t off = out.size();
        out.resize(off + Serializer::VAL_SIZE_32);
        std::memcpy(out.data() + off, &filter_len, Serializer::VAL_SIZE_32);
        out.insert(out.end(), filter_data.begin(), filter_data.end());

        // Serialize metadata
        uint64_t tmp_expected = static_cast<uint64_t>(expected_elements);
        uint8_t tmp_hashes = static_cast<uint8_t>(num_hashes);
        const size_t off2 = out.size();
        out.resize(off2 + 9);  // 8 bytes for expected_elements + 1 for num_hashes
        std::memcpy(out.data() + off2, &tmp_expected, 8);
        out[off2 + 8] = tmp_hashes;
        return out;
    }

    static BloomFilterBitsArgs deserialize(const std::vector<uint8_t>& in) {
        BloomFilterBitsArgs args;
        size_t offset = 0;
        args.context_id = Serializer::deserialize_string(in.data(), offset, in.size());

        uint32_t filter_len = 0;
        if (offset + Serializer::VAL_SIZE_32 <= in.size()) {
            std::memcpy(&filter_len, in.data() + offset, Serializer::VAL_SIZE_32);
            offset += Serializer::VAL_SIZE_32;
        }
        if (offset + filter_len <= in.size()) {
            args.filter_data.resize(filter_len);
            std::memcpy(args.filter_data.data(), in.data() + offset, filter_len);
            offset += filter_len;
        }

        if (offset + 9 <= in.size()) {
            uint64_t tmp_expected = 0;
            std::memcpy(&tmp_expected, in.data() + offset, 8);
            args.expected_elements = static_cast<size_t>(tmp_expected);
            offset += 8;
            args.num_hashes = static_cast<size_t>(in[offset]);
        }
        return args;
    }
};

/**
 * @brief Arguments for TxnPrepare/Commit/Abort RPC
 */
struct TxnOperationArgs {
    uint64_t txn_id;

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
 * @brief Represents a single RPC call
 */
struct RpcMessage {
    RpcHeader header;
    std::vector<uint8_t> payload;

    RpcMessage() = default;
    ~RpcMessage() = default;

    // Disable copy/move for message
    RpcMessage(const RpcMessage&) = default;
    RpcMessage& operator=(const RpcMessage&) = default;
    RpcMessage(RpcMessage&&) = default;
    RpcMessage& operator=(RpcMessage&&) = default;
};

}  // namespace cloudsql::network

#endif  // SQL_ENGINE_NETWORK_RPC_MESSAGE_HPP
