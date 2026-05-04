/**
 * @file rpc_message_tests.cpp
 * @brief Unit tests for RPC message serialization/deserialization
 */

#include <gtest/gtest.h>
#include <cstring>

#include "common/value.hpp"
#include "executor/types.hpp"
#include "network/rpc_message.hpp"

using namespace cloudsql;
using namespace cloudsql::network;
using namespace cloudsql::executor;
using namespace cloudsql::common;

namespace {

// Helper to create a test tuple
Tuple make_tuple(const std::vector<int64_t>& vals) {
    std::vector<Value> values;
    for (auto v : vals) {
        values.push_back(Value::make_int64(v));
    }
    return Tuple(std::move(values));
}

// Helper to create a text tuple
Tuple make_text_tuple(const std::vector<std::string>& vals) {
    std::vector<Value> values;
    for (const auto& v : vals) {
        values.push_back(Value::make_text(v));
    }
    return Tuple(std::move(values));
}

// ============= Serializer Value Tests =============

TEST(RpcMessageTests, SerializeValue_Int64) {
    std::vector<uint8_t> out;
    Value val = Value::make_int64(42);
    Serializer::serialize_value(val, out);

    EXPECT_GT(out.size(), 0u);
    EXPECT_EQ(out[0], static_cast<uint8_t>(ValueType::TYPE_INT64));
}

TEST(RpcMessageTests, SerializeValue_Float64) {
    std::vector<uint8_t> out;
    Value val = Value::make_float64(3.14);
    Serializer::serialize_value(val, out);

    EXPECT_GT(out.size(), 0u);
    EXPECT_EQ(out[0], static_cast<uint8_t>(ValueType::TYPE_FLOAT64));
}

TEST(RpcMessageTests, SerializeValue_Text) {
    std::vector<uint8_t> out;
    Value val = Value::make_text("hello");
    Serializer::serialize_value(val, out);

    EXPECT_GT(out.size(), 0u);
    EXPECT_EQ(out[0], static_cast<uint8_t>(ValueType::TYPE_TEXT));
}

TEST(RpcMessageTests, SerializeValue_Null) {
    std::vector<uint8_t> out;
    Value val = Value::make_null();
    Serializer::serialize_value(val, out);

    EXPECT_EQ(out.size(), 1u);
    EXPECT_EQ(out[0], static_cast<uint8_t>(ValueType::TYPE_NULL));
}

TEST(RpcMessageTests, DeserializeValue_Int64) {
    std::vector<uint8_t> data;
    data.push_back(static_cast<uint8_t>(ValueType::TYPE_INT64));
    int64_t v = 12345;
    data.insert(data.end(), reinterpret_cast<uint8_t*>(&v),
               reinterpret_cast<uint8_t*>(&v) + sizeof(v));

    size_t offset = 0;
    auto result = Serializer::deserialize_value(data.data(), offset, data.size());

    EXPECT_EQ(result.type(), ValueType::TYPE_INT64);
    EXPECT_EQ(result.to_int64(), 12345);
}

TEST(RpcMessageTests, DeserializeValue_Float64) {
    std::vector<uint8_t> data;
    data.push_back(static_cast<uint8_t>(ValueType::TYPE_FLOAT64));
    double v = 2.718;
    data.insert(data.end(), reinterpret_cast<uint8_t*>(&v),
               reinterpret_cast<uint8_t*>(&v) + sizeof(v));

    size_t offset = 0;
    auto result = Serializer::deserialize_value(data.data(), offset, data.size());

    EXPECT_EQ(result.type(), ValueType::TYPE_FLOAT64);
    EXPECT_DOUBLE_EQ(result.to_float64(), 2.718);
}

TEST(RpcMessageTests, DeserializeValue_Text) {
    std::vector<uint8_t> data;
    data.push_back(static_cast<uint8_t>(ValueType::TYPE_TEXT));
    std::string s = "test string";
    uint32_t len = static_cast<uint32_t>(s.size());
    data.insert(data.end(), reinterpret_cast<uint8_t*>(&len),
               reinterpret_cast<uint8_t*>(&len) + sizeof(len));
    data.insert(data.end(), s.begin(), s.end());

    size_t offset = 0;
    auto result = Serializer::deserialize_value(data.data(), offset, data.size());

    EXPECT_EQ(result.type(), ValueType::TYPE_TEXT);
    EXPECT_EQ(result.to_string(), "test string");
}

TEST(RpcMessageTests, DeserializeValue_Null) {
    std::vector<uint8_t> data;
    data.push_back(static_cast<uint8_t>(ValueType::TYPE_NULL));

    size_t offset = 0;
    auto result = Serializer::deserialize_value(data.data(), offset, data.size());

    EXPECT_TRUE(result.is_null());
}

TEST(RpcMessageTests, DeserializeValue_TruncatedData) {
    std::vector<uint8_t> data;
    data.push_back(static_cast<uint8_t>(ValueType::TYPE_INT64));
    // Only 4 bytes instead of 8
    int64_t v = 42;
    data.insert(data.end(), reinterpret_cast<uint8_t*>(&v),
               reinterpret_cast<uint8_t*>(&v) + 4);

    size_t offset = 0;
    auto result = Serializer::deserialize_value(data.data(), offset, data.size());

    // Returns default value (0) when insufficient data, not null
    EXPECT_EQ(result.type(), ValueType::TYPE_INT64);
    EXPECT_EQ(result.to_int64(), 0);
}

TEST(RpcMessageTests, DeserializeValue_UnsupportedType) {
    std::vector<uint8_t> data;
    data.push_back(255);  // Invalid type

    size_t offset = 0;
    auto result = Serializer::deserialize_value(data.data(), offset, data.size());

    // Should return null for unsupported types
    EXPECT_TRUE(result.is_null());
}

// ============= Serializer Tuple Tests =============

TEST(RpcMessageTests, RoundTrip_Int64Tuple) {
    auto original = make_tuple({1, 2, 3, 4, 5});

    std::vector<uint8_t> serialized;
    Serializer::serialize_tuple(original, serialized);

    size_t offset = 0;
    auto deserialized = Serializer::deserialize_tuple(serialized.data(), offset, serialized.size());

    EXPECT_EQ(deserialized.size(), original.size());
    for (size_t i = 0; i < original.size(); ++i) {
        EXPECT_EQ(deserialized.get(i).to_int64(), original.get(i).to_int64());
    }
}

TEST(RpcMessageTests, RoundTrip_TextTuple) {
    auto original = make_text_tuple({"apple", "banana", "cherry"});

    std::vector<uint8_t> serialized;
    Serializer::serialize_tuple(original, serialized);

    size_t offset = 0;
    auto deserialized = Serializer::deserialize_tuple(serialized.data(), offset, serialized.size());

    EXPECT_EQ(deserialized.size(), original.size());
    for (size_t i = 0; i < original.size(); ++i) {
        EXPECT_EQ(deserialized.get(i).to_string(), original.get(i).to_string());
    }
}

TEST(RpcMessageTests, RoundTrip_MixedTuple) {
    std::vector<Value> values;
    values.push_back(Value::make_int64(42));
    values.push_back(Value::make_text("hello"));
    values.push_back(Value::make_float64(3.14));
    auto original = Tuple(std::move(values));

    std::vector<uint8_t> serialized;
    Serializer::serialize_tuple(original, serialized);

    size_t offset = 0;
    auto deserialized = Serializer::deserialize_tuple(serialized.data(), offset, serialized.size());

    EXPECT_EQ(deserialized.size(), original.size());
    EXPECT_EQ(deserialized.get(0).to_int64(), 42);
    EXPECT_EQ(deserialized.get(1).to_string(), "hello");
}

TEST(RpcMessageTests, RoundTrip_EmptyTuple) {
    auto original = make_tuple({});

    std::vector<uint8_t> serialized;
    Serializer::serialize_tuple(original, serialized);

    size_t offset = 0;
    auto deserialized = Serializer::deserialize_tuple(serialized.data(), offset, serialized.size());

    EXPECT_EQ(deserialized.size(), 0u);
}

// ============= Serializer String Tests =============

TEST(RpcMessageTests, RoundTrip_String) {
    std::string original = "Hello, World! This is a test string.";

    std::vector<uint8_t> serialized;
    Serializer::serialize_string(original, serialized);

    size_t offset = 0;
    auto deserialized = Serializer::deserialize_string(serialized.data(), offset, serialized.size());

    EXPECT_EQ(deserialized, original);
}

TEST(RpcMessageTests, RoundTrip_EmptyString) {
    std::string original = "";

    std::vector<uint8_t> serialized;
    Serializer::serialize_string(original, serialized);

    size_t offset = 0;
    auto deserialized = Serializer::deserialize_string(serialized.data(), offset, serialized.size());

    EXPECT_EQ(deserialized, original);
}

TEST(RpcMessageTests, RoundTrip_UnicodeString) {
    std::string original = "Japanese test";

    std::vector<uint8_t> serialized;
    Serializer::serialize_string(original, serialized);

    size_t offset = 0;
    auto deserialized = Serializer::deserialize_string(serialized.data(), offset, serialized.size());

    EXPECT_EQ(deserialized, original);
}

// ============= Serializer Schema Tests =============

TEST(RpcMessageTests, RoundTrip_Schema) {
    Schema original;
    original.add_column("id", ValueType::TYPE_INT64);
    original.add_column("name", ValueType::TYPE_TEXT);
    original.add_column("price", ValueType::TYPE_FLOAT64);

    std::vector<uint8_t> serialized;
    Serializer::serialize_schema(original, serialized);

    size_t offset = 0;
    auto deserialized = Serializer::deserialize_schema(serialized.data(), offset, serialized.size());

    EXPECT_EQ(deserialized.column_count(), original.column_count());
    EXPECT_EQ(deserialized.get_column(0).name(), "id");
    EXPECT_EQ(deserialized.get_column(0).type(), ValueType::TYPE_INT64);
    EXPECT_EQ(deserialized.get_column(1).name(), "name");
    EXPECT_EQ(deserialized.get_column(1).type(), ValueType::TYPE_TEXT);
}

TEST(RpcMessageTests, RoundTrip_EmptySchema) {
    Schema original;

    std::vector<uint8_t> serialized;
    Serializer::serialize_schema(original, serialized);

    size_t offset = 0;
    auto deserialized = Serializer::deserialize_schema(serialized.data(), offset, serialized.size());

    EXPECT_EQ(deserialized.column_count(), 0u);
}

// ============= RpcHeader Tests =============

TEST(RpcMessageTests, RoundTrip_RpcHeader) {
    RpcHeader original;
    original.magic = RpcHeader::MAGIC;
    original.type = RpcType::PushData;
    original.flags = 0x01;
    original.group_id = 42;
    original.reserved = 0;
    original.payload_len = 1000;

    char encoded[RpcHeader::HEADER_SIZE];
    original.encode(encoded);

    auto decoded = RpcHeader::decode(encoded);

    EXPECT_EQ(decoded.magic, original.magic);
    EXPECT_EQ(decoded.type, original.type);
    EXPECT_EQ(decoded.flags, original.flags);
    EXPECT_EQ(decoded.group_id, original.group_id);
    EXPECT_EQ(decoded.payload_len, original.payload_len);
}

TEST(RpcMessageTests, RpcHeader_AllRpcTypes) {
    for (uint8_t i = 0; i <= 17; ++i) {
        RpcHeader h;
        h.type = static_cast<RpcType>(i);
        h.payload_len = i * 10;

        char encoded[RpcHeader::HEADER_SIZE];
        h.encode(encoded);

        auto decoded = RpcHeader::decode(encoded);
        EXPECT_EQ(decoded.type, h.type) << "Failed for RpcType " << static_cast<int>(i);
    }
}

// ============= RegisterNodeArgs Tests =============

TEST(RpcMessageTests, RoundTrip_RegisterNodeArgs) {
    RegisterNodeArgs original;
    original.id = "node_1";
    original.address = "192.168.1.100";
    original.port = 7000;
    original.mode = 2;  // Data mode

    auto serialized = original.serialize();
    auto deserialized = RegisterNodeArgs::deserialize(serialized);

    EXPECT_EQ(deserialized.id, original.id);
    EXPECT_EQ(deserialized.address, original.address);
    EXPECT_EQ(deserialized.port, original.port);
    EXPECT_EQ(deserialized.mode, original.mode);
}

TEST(RpcMessageTests, RoundTrip_RegisterNodeArgs_Coordinator) {
    RegisterNodeArgs original;
    original.id = "coordinator_1";
    original.address = "10.0.0.1";
    original.port = 8000;
    original.mode = 1;  // Coordinator mode

    auto serialized = original.serialize();
    auto deserialized = RegisterNodeArgs::deserialize(serialized);

    EXPECT_EQ(deserialized.id, original.id);
    EXPECT_EQ(deserialized.port, original.port);
    EXPECT_EQ(deserialized.mode, 1);
}

// ============= ExecuteFragmentArgs Tests =============

TEST(RpcMessageTests, RoundTrip_ExecuteFragmentArgs) {
    ExecuteFragmentArgs original;
    original.sql = "SELECT * FROM users WHERE id > 100";
    original.context_id = "ctx_123";
    original.is_fetch_all = true;

    auto serialized = original.serialize();
    auto deserialized = ExecuteFragmentArgs::deserialize(serialized);

    EXPECT_EQ(deserialized.sql, original.sql);
    EXPECT_EQ(deserialized.context_id, original.context_id);
    EXPECT_EQ(deserialized.is_fetch_all, original.is_fetch_all);
}

TEST(RpcMessageTests, RoundTrip_ExecuteFragmentArgs_NotFetchAll) {
    ExecuteFragmentArgs original;
    original.sql = "SELECT * FROM orders LIMIT 10";
    original.context_id = "ctx_456";
    original.is_fetch_all = false;

    auto serialized = original.serialize();
    auto deserialized = ExecuteFragmentArgs::deserialize(serialized);

    EXPECT_EQ(deserialized.is_fetch_all, false);
}

TEST(RpcMessageTests, RoundTrip_ExecuteFragmentArgs_EmptySQL) {
    ExecuteFragmentArgs original;
    original.sql = "";
    original.context_id = "ctx_empty";
    original.is_fetch_all = false;

    auto serialized = original.serialize();
    auto deserialized = ExecuteFragmentArgs::deserialize(serialized);

    EXPECT_EQ(deserialized.sql, "");
}

// ============= QueryResultsReply Tests =============

TEST(RpcMessageTests, RoundTrip_QueryResultsReply_Success) {
    QueryResultsReply original;
    original.success = true;
    original.error_msg = "";

    Schema schema;
    schema.add_column("id", ValueType::TYPE_INT64);
    schema.add_column("name", ValueType::TYPE_TEXT);
    original.schema = schema;

    original.rows.push_back(make_tuple({1, 100}));
    original.rows.push_back(make_tuple({2, 200}));

    auto serialized = original.serialize();
    auto deserialized = QueryResultsReply::deserialize(serialized);

    EXPECT_TRUE(deserialized.success);
    EXPECT_EQ(deserialized.error_msg, "");
    EXPECT_EQ(deserialized.schema.column_count(), 2u);
    EXPECT_EQ(deserialized.rows.size(), 2u);
    EXPECT_EQ(deserialized.rows[0].get(0).to_int64(), 1);
    EXPECT_EQ(deserialized.rows[1].get(0).to_int64(), 2);
}

TEST(RpcMessageTests, RoundTrip_QueryResultsReply_Error) {
    QueryResultsReply original;
    original.success = false;
    original.error_msg = "Table 'users' not found";

    auto serialized = original.serialize();
    auto deserialized = QueryResultsReply::deserialize(serialized);

    EXPECT_FALSE(deserialized.success);
    EXPECT_EQ(deserialized.error_msg, "Table 'users' not found");
}

TEST(RpcMessageTests, RoundTrip_QueryResultsReply_EmptyRows) {
    QueryResultsReply original;
    original.success = true;
    original.rows = {};

    auto serialized = original.serialize();
    auto deserialized = QueryResultsReply::deserialize(serialized);

    EXPECT_TRUE(deserialized.success);
    EXPECT_EQ(deserialized.rows.size(), 0u);
}

TEST(RpcMessageTests, RoundTrip_QueryResultsReply_EmptyData) {
    std::vector<uint8_t> empty_data;
    auto deserialized = QueryResultsReply::deserialize(empty_data);

    EXPECT_FALSE(deserialized.success);
    EXPECT_EQ(deserialized.rows.size(), 0u);
}

// ============= PushDataArgs Tests =============

TEST(RpcMessageTests, RoundTrip_PushDataArgs) {
    PushDataArgs original;
    original.context_id = "ctx_shuffle";
    original.table_name = "orders";
    original.rows.push_back(make_tuple({1, 100}));
    original.rows.push_back(make_tuple({2, 200}));
    original.rows.push_back(make_tuple({3, 300}));

    auto serialized = original.serialize();
    auto deserialized = PushDataArgs::deserialize(serialized);

    EXPECT_EQ(deserialized.context_id, original.context_id);
    EXPECT_EQ(deserialized.table_name, original.table_name);
    EXPECT_EQ(deserialized.rows.size(), 3u);
    EXPECT_EQ(deserialized.rows[0].get(0).to_int64(), 1);
    EXPECT_EQ(deserialized.rows[1].get(1).to_int64(), 200);
}

TEST(RpcMessageTests, RoundTrip_PushDataArgs_EmptyRows) {
    PushDataArgs original;
    original.context_id = "ctx_empty";
    original.table_name = "empty_table";
    original.rows = {};

    auto serialized = original.serialize();
    auto deserialized = PushDataArgs::deserialize(serialized);

    EXPECT_EQ(deserialized.context_id, original.context_id);
    EXPECT_EQ(deserialized.rows.size(), 0u);
}

TEST(RpcMessageTests, PushDataArgs_TruncatedData) {
    std::vector<uint8_t> truncated;
    truncated.push_back(2);  // length prefix of 2
    truncated.insert(truncated.end(), {'a'});  // only 1 byte of string data

    auto deserialized = PushDataArgs::deserialize(truncated);
    // With truncated data, context_id may be partial, table_name empty, and row_count 0
    EXPECT_TRUE(deserialized.table_name.empty());
    EXPECT_EQ(deserialized.rows.size(), 0u);
}

// ============= ShuffleFragmentArgs Tests =============

TEST(RpcMessageTests, RoundTrip_ShuffleFragmentArgs) {
    ShuffleFragmentArgs original;
    original.context_id = "ctx_join";
    original.table_name = "lineitem";
    original.join_key_col = "order_id";

    auto serialized = original.serialize();
    auto deserialized = ShuffleFragmentArgs::deserialize(serialized);

    EXPECT_EQ(deserialized.context_id, original.context_id);
    EXPECT_EQ(deserialized.table_name, original.table_name);
    EXPECT_EQ(deserialized.join_key_col, original.join_key_col);
}

TEST(RpcMessageTests, RoundTrip_ShuffleFragmentArgs_EmptyKeys) {
    ShuffleFragmentArgs original;
    original.context_id = "";
    original.table_name = "";
    original.join_key_col = "";

    auto serialized = original.serialize();
    auto deserialized = ShuffleFragmentArgs::deserialize(serialized);

    EXPECT_EQ(deserialized.context_id, "");
    EXPECT_EQ(deserialized.join_key_col, "");
}

// ============= BloomFilterArgs Tests =============

TEST(RpcMessageTests, RoundTrip_BloomFilterArgs) {
    BloomFilterArgs original;
    original.context_id = "ctx_bloom";
    original.build_table = "orders";
    original.probe_table = "lineitem";
    original.probe_key_col = "order_id";
    original.filter_data = {0xDE, 0xAD, 0xBE, 0xEF};
    original.expected_elements = 10000;
    original.num_hashes = 7;

    auto serialized = original.serialize();
    auto deserialized = BloomFilterArgs::deserialize(serialized);

    EXPECT_EQ(deserialized.context_id, original.context_id);
    EXPECT_EQ(deserialized.build_table, original.build_table);
    EXPECT_EQ(deserialized.probe_table, original.probe_table);
    EXPECT_EQ(deserialized.probe_key_col, original.probe_key_col);
    EXPECT_EQ(deserialized.filter_data.size(), original.filter_data.size());
    EXPECT_EQ(deserialized.expected_elements, original.expected_elements);
    EXPECT_EQ(deserialized.num_hashes, original.num_hashes);
}

TEST(RpcMessageTests, RoundTrip_BloomFilterArgs_EmptyFilter) {
    BloomFilterArgs original;
    original.context_id = "ctx_bloom_empty";
    original.filter_data = {};
    original.expected_elements = 0;
    original.num_hashes = 0;

    auto serialized = original.serialize();
    auto deserialized = BloomFilterArgs::deserialize(serialized);

    EXPECT_EQ(deserialized.filter_data.size(), 0u);
    EXPECT_EQ(deserialized.expected_elements, 0u);
}

TEST(RpcMessageTests, RoundTrip_BloomFilterArgs_LargeFilter) {
    BloomFilterArgs original;
    original.context_id = "ctx_bloom_large";
    original.filter_data.resize(1000, 0xFF);
    original.expected_elements = 1000000;
    original.num_hashes = 11;

    auto serialized = original.serialize();
    auto deserialized = BloomFilterArgs::deserialize(serialized);

    EXPECT_EQ(deserialized.filter_data.size(), 1000u);
    EXPECT_EQ(deserialized.expected_elements, 1000000u);
}

// ============= Integration Tests =============

TEST(RpcMessageTests, RoundTrip_QueryResultsReply_FullSchema) {
    QueryResultsReply original;
    original.success = true;

    Schema schema;
    schema.add_column("order_id", ValueType::TYPE_INT64);
    schema.add_column("customer_name", ValueType::TYPE_TEXT);
    schema.add_column("total_price", ValueType::TYPE_FLOAT64);
    schema.add_column("is_active", ValueType::TYPE_INT64);
    original.schema = schema;

    original.rows.push_back(make_text_tuple({"1", "Alice", "150.50", "1"}));
    original.rows.push_back(make_text_tuple({"2", "Bob", "200.00", "0"}));

    auto serialized = original.serialize();
    auto deserialized = QueryResultsReply::deserialize(serialized);

    EXPECT_TRUE(deserialized.success);
    EXPECT_EQ(deserialized.schema.column_count(), 4u);
    EXPECT_EQ(deserialized.rows.size(), 2u);
}

TEST(RpcMessageTests, SerializeDeserialize_MultipleValues) {
    std::vector<uint8_t> data;

    Value v1 = Value::make_int64(42);
    Value v2 = Value::make_float64(3.14);
    Value v3 = Value::make_text("test");

    Serializer::serialize_value(v1, data);
    Serializer::serialize_value(v2, data);
    Serializer::serialize_value(v3, data);

    size_t offset = 0;
    auto d1 = Serializer::deserialize_value(data.data(), offset, data.size());
    auto d2 = Serializer::deserialize_value(data.data(), offset, data.size());
    auto d3 = Serializer::deserialize_value(data.data(), offset, data.size());

    EXPECT_EQ(d1.to_int64(), 42);
    EXPECT_DOUBLE_EQ(d2.to_float64(), 3.14);
    EXPECT_EQ(d3.to_string(), "test");
}

}  // namespace
