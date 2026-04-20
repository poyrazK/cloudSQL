/**
 * @file string_vector_tests.cpp
 * @brief Unit tests for StringVector - variable-length string column storage
 */

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "common/value.hpp"
#include "executor/types.hpp"

using namespace cloudsql;
using namespace cloudsql::common;
using namespace cloudsql::executor;

namespace {

class StringVectorTests : public ::testing::Test {};

// Test basic append and get
TEST_F(StringVectorTests, BasicAppendAndGet) {
    StringVector vec(ValueType::TYPE_TEXT);

    vec.append(Value::make_text("hello"));
    vec.append(Value::make_text("world"));
    vec.append(Value::make_text(""));

    EXPECT_EQ(vec.size(), 3U);
    EXPECT_EQ(vec.get(0).as_text(), "hello");
    EXPECT_EQ(vec.get(1).as_text(), "world");
    EXPECT_EQ(vec.get(2).as_text(), "");
    EXPECT_EQ(vec.type(), ValueType::TYPE_TEXT);
}

// Test null handling
TEST_F(StringVectorTests, NullHandling) {
    StringVector vec(ValueType::TYPE_TEXT);

    vec.append(Value::make_text("hello"));
    vec.append(common::Value::make_null());
    vec.append(Value::make_text("world"));

    EXPECT_EQ(vec.size(), 3U);
    EXPECT_FALSE(vec.is_null(0));
    EXPECT_TRUE(vec.is_null(1));
    EXPECT_FALSE(vec.is_null(2));

    EXPECT_EQ(vec.get(0).as_text(), "hello");
    EXPECT_TRUE(vec.get(1).is_null());
    EXPECT_EQ(vec.get(2).as_text(), "world");
}

// Test is_null edge cases
TEST_F(StringVectorTests, IsNullEdgeCases) {
    StringVector vec(ValueType::TYPE_TEXT);

    EXPECT_TRUE(vec.is_null(0));  // Empty vector, out of bounds returns true
    EXPECT_TRUE(vec.is_null(5));  // Out of bounds returns true

    vec.append(Value::make_text("test"));
    EXPECT_FALSE(vec.is_null(0));
    EXPECT_TRUE(vec.is_null(1));  // Out of bounds
}

// Test clear
TEST_F(StringVectorTests, Clear) {
    StringVector vec(ValueType::TYPE_TEXT);

    vec.append(Value::make_text("hello"));
    vec.append(Value::make_text("world"));
    EXPECT_EQ(vec.size(), 2U);

    vec.clear();
    EXPECT_EQ(vec.size(), 0U);
    EXPECT_TRUE(vec.is_null(0));  // After clear, is_null returns true for index 0
}

// Test resize
TEST_F(StringVectorTests, Resize) {
    StringVector vec(ValueType::TYPE_TEXT);

    vec.append(Value::make_text("hello"));
    vec.resize(5);

    EXPECT_EQ(vec.size(), 5U);
    // After resize, entries are NOT null (is_null returns false)
    // They are empty strings by default
    EXPECT_FALSE(vec.is_null(1));
    EXPECT_FALSE(vec.is_null(4));
    EXPECT_EQ(vec.get(1).as_text(), "");
    EXPECT_EQ(vec.get(4).as_text(), "");

    // Can set values after resize
    vec.set(3, "world");
    EXPECT_EQ(vec.get(3).as_text(), "world");
    EXPECT_FALSE(vec.is_null(3));
}

// Test set
TEST_F(StringVectorTests, Set) {
    StringVector vec(ValueType::TYPE_TEXT);

    vec.append(Value::make_text("hello"));
    vec.set(0, "world");

    EXPECT_EQ(vec.get(0).as_text(), "world");
    EXPECT_FALSE(vec.is_null(0));
}

// Test set auto-resizes
TEST_F(StringVectorTests, SetAutoResize) {
    StringVector vec(ValueType::TYPE_TEXT);

    vec.set(2, "auto resize");

    EXPECT_EQ(vec.size(), 3U);
    // After auto-resize via set(), entries 0 and 1 are empty strings (not null)
    EXPECT_FALSE(vec.is_null(0));
    EXPECT_FALSE(vec.is_null(1));
    EXPECT_EQ(vec.get(0).as_text(), "");
    EXPECT_EQ(vec.get(1).as_text(), "");
    EXPECT_EQ(vec.get(2).as_text(), "auto resize");
}

// Test raw_data
TEST_F(StringVectorTests, RawData) {
    StringVector vec(ValueType::TYPE_TEXT);

    vec.append(Value::make_text("hello"));
    vec.append(Value::make_text("world"));

    const auto& data = vec.raw_data();
    EXPECT_EQ(data.size(), 2U);
    EXPECT_EQ(data[0], "hello");
    EXPECT_EQ(data[1], "world");
}

// Test VARCHAR type
TEST_F(StringVectorTests, VarcharType) {
    StringVector vec(common::ValueType::TYPE_VARCHAR);

    vec.append(Value::make_text("test varchar"));

    EXPECT_EQ(vec.size(), 1U);
    EXPECT_EQ(vec.get(0).as_text(), "test varchar");
    EXPECT_EQ(vec.type(), common::ValueType::TYPE_VARCHAR);
}

// Test CHAR type
TEST_F(StringVectorTests, CharType) {
    StringVector vec(common::ValueType::TYPE_CHAR);

    vec.append(Value::make_text("test char"));

    EXPECT_EQ(vec.size(), 1U);
    EXPECT_EQ(vec.get(0).as_text(), "test char");
    EXPECT_EQ(vec.type(), common::ValueType::TYPE_CHAR);
}

// Test long strings
TEST_F(StringVectorTests, LongStrings) {
    StringVector vec(ValueType::TYPE_TEXT);

    std::string long_str(1000, 'x');
    vec.append(Value::make_text(long_str));

    EXPECT_EQ(vec.size(), 1U);
    EXPECT_EQ(vec.get(0).as_text(), long_str);
}

// Test special characters
TEST_F(StringVectorTests, SpecialCharacters) {
    StringVector vec(ValueType::TYPE_TEXT);

    vec.append(Value::make_text("hello\nworld\ttab"));
    vec.append(Value::make_text("emoji: 🎉 NULL: \0 embedded"));

    EXPECT_EQ(vec.get(0).as_text(), "hello\nworld\ttab");
    // Note: strings with embedded nulls may be truncated due to C++ string behavior
}

// Test empty string
TEST_F(StringVectorTests, EmptyString) {
    StringVector vec(ValueType::TYPE_TEXT);

    vec.append(Value::make_text(""));
    vec.append(Value::make_text("non-empty"));

    EXPECT_EQ(vec.size(), 2U);
    EXPECT_EQ(vec.get(0).as_text(), "");
    EXPECT_FALSE(vec.is_null(0));
    EXPECT_EQ(vec.get(1).as_text(), "non-empty");
}

// Test mixed null and non-null append
TEST_F(StringVectorTests, MixedAppend) {
    StringVector vec(ValueType::TYPE_TEXT);

    for (int i = 0; i < 5; ++i) {
        if (i % 2 == 0) {
            vec.append(Value::make_text(std::to_string(i)));
        } else {
            vec.append(common::Value::make_null());
        }
    }

    EXPECT_EQ(vec.size(), 5U);
    for (size_t i = 0; i < 5; ++i) {
        if (i % 2 == 0) {
            EXPECT_FALSE(vec.is_null(i));
            EXPECT_EQ(vec.get(i).as_text(), std::to_string(i));
        } else {
            EXPECT_TRUE(vec.is_null(i));
            EXPECT_TRUE(vec.get(i).is_null());
        }
    }
}

}  // namespace