/**
 * @file log_record.cpp
 * @brief LogRecord implementation
 */

#include "recovery/log_record.hpp"
#include "common/value.hpp"
#include <cstring>

namespace cloudsql {
namespace recovery {

namespace {

// Helper to serialize a value
void serialize_value(const common::Value& val, char*& ptr) {
    // Write type
    common::ValueType type = val.type();
    std::memcpy(ptr, &type, sizeof(common::ValueType));
    ptr += sizeof(common::ValueType);

    // Write data
    if (val.is_null()) {
        return;
    }
    
    switch (type) {
        case common::TYPE_BOOL: {
            bool v = val.as_bool();
            std::memcpy(ptr, &v, sizeof(bool));
            ptr += sizeof(bool);
            break;
        }
        case common::TYPE_INT8: {
            int8_t v = val.as_int8();
            std::memcpy(ptr, &v, sizeof(int8_t));
            ptr += sizeof(int8_t);
            break;
        }
        case common::TYPE_INT16: {
            int16_t v = val.as_int16();
            std::memcpy(ptr, &v, sizeof(int16_t));
            ptr += sizeof(int16_t);
            break;
        }
        case common::TYPE_INT32: {
            int32_t v = val.as_int32();
            std::memcpy(ptr, &v, sizeof(int32_t));
            ptr += sizeof(int32_t);
            break;
        }
        case common::TYPE_INT64: {
            int64_t v = val.as_int64();
            std::memcpy(ptr, &v, sizeof(int64_t));
            ptr += sizeof(int64_t);
            break;
        }
        case common::TYPE_FLOAT32: {
            float v = val.as_float32();
            std::memcpy(ptr, &v, sizeof(float));
            ptr += sizeof(float);
            break;
        }
        case common::TYPE_FLOAT64: {
            double v = val.as_float64();
            std::memcpy(ptr, &v, sizeof(double));
            ptr += sizeof(double);
            break;
        }
        case common::TYPE_TEXT:
        case common::TYPE_VARCHAR:
        case common::TYPE_CHAR: {
            const std::string& s = val.as_text();
            uint32_t len = s.length();
            std::memcpy(ptr, &len, sizeof(uint32_t));
            ptr += sizeof(uint32_t);
            std::memcpy(ptr, s.c_str(), len);
            ptr += len;
            break;
        }
        default:
            // Fallback for other numeric-like types or unsupported
            if (val.is_numeric()) {
                 int64_t v = val.to_int64();
                 std::memcpy(ptr, &v, sizeof(int64_t));
                 ptr += sizeof(int64_t);
            }
            break;
    }
}

// Helper to deserialize a value
common::Value deserialize_value(const char*& ptr) {
    common::ValueType type;
    std::memcpy(&type, ptr, sizeof(common::ValueType));
    ptr += sizeof(common::ValueType);

    if (type == common::TYPE_NULL) {
        return common::Value::make_null();
    }

    switch (type) {
        case common::TYPE_BOOL: {
            bool v;
            std::memcpy(&v, ptr, sizeof(bool));
            ptr += sizeof(bool);
            return common::Value::make_bool(v);
        }
        case common::TYPE_INT8: {
            int8_t v;
            std::memcpy(&v, ptr, sizeof(int8_t));
            ptr += sizeof(int8_t);
            return common::Value(v);
        }
        case common::TYPE_INT16: {
            int16_t v;
            std::memcpy(&v, ptr, sizeof(int16_t));
            ptr += sizeof(int16_t);
            return common::Value(v);
        }
        case common::TYPE_INT32: {
            int32_t v;
            std::memcpy(&v, ptr, sizeof(int32_t));
            ptr += sizeof(int32_t);
            return common::Value(v);
        }
        case common::TYPE_INT64: {
            int64_t v;
            std::memcpy(&v, ptr, sizeof(int64_t));
            ptr += sizeof(int64_t);
            return common::Value::make_int64(v);
        }
        case common::TYPE_FLOAT32: {
            float v;
            std::memcpy(&v, ptr, sizeof(float));
            ptr += sizeof(float);
            return common::Value(v);
        }
        case common::TYPE_FLOAT64: {
            double v;
            std::memcpy(&v, ptr, sizeof(double));
            ptr += sizeof(double);
            return common::Value::make_float64(v);
        }
        case common::TYPE_TEXT:
        case common::TYPE_VARCHAR:
        case common::TYPE_CHAR: {
            uint32_t len;
            std::memcpy(&len, ptr, sizeof(uint32_t));
            ptr += sizeof(uint32_t);
            std::string s(ptr, len);
            ptr += len;
            return common::Value::make_text(s);
        }
        default:
            return common::Value::make_null();
    }
}

// Helper to calculate value size
uint32_t get_value_size(const common::Value& val) {
    uint32_t size = sizeof(common::ValueType);
    if (val.is_null()) return size;
    
    common::ValueType type = val.type();
    switch (type) {
        case common::TYPE_BOOL: return size + sizeof(bool);
        case common::TYPE_INT8: return size + sizeof(int8_t);
        case common::TYPE_INT16: return size + sizeof(int16_t);
        case common::TYPE_INT32: return size + sizeof(int32_t);
        case common::TYPE_INT64: return size + sizeof(int64_t);
        case common::TYPE_FLOAT32: return size + sizeof(float);
        case common::TYPE_FLOAT64: return size + sizeof(double);
        case common::TYPE_TEXT: 
        case common::TYPE_VARCHAR: 
        case common::TYPE_CHAR:
            return size + sizeof(uint32_t) + val.as_text().length();
        default: return size;
    }
}

} // anonymous namespace

uint32_t LogRecord::serialize(char* buffer) const {
    char* start = buffer;
    
    // Header serialization field by field to avoid padding issues
    std::memcpy(buffer, &size_, sizeof(uint32_t)); buffer += sizeof(uint32_t);
    std::memcpy(buffer, &lsn_, sizeof(lsn_t)); buffer += sizeof(lsn_t);
    std::memcpy(buffer, &prev_lsn_, sizeof(lsn_t)); buffer += sizeof(lsn_t);
    std::memcpy(buffer, &txn_id_, sizeof(txn_id_t)); buffer += sizeof(txn_id_t);
    std::memcpy(buffer, &type_, sizeof(LogRecordType)); buffer += sizeof(LogRecordType);
    
    // Body
    if (type_ == LogRecordType::INSERT || type_ == LogRecordType::MARK_DELETE || 
        type_ == LogRecordType::APPLY_DELETE || type_ == LogRecordType::ROLLBACK_DELETE ||
        type_ == LogRecordType::UPDATE) {
        
        // Table Name
        uint32_t name_len = table_name_.length();
        std::memcpy(buffer, &name_len, sizeof(uint32_t));
        buffer += sizeof(uint32_t);
        std::memcpy(buffer, table_name_.c_str(), name_len);
        buffer += name_len;
        
        // RID
        std::memcpy(buffer, &rid_, sizeof(storage::HeapTable::TupleId));
        buffer += sizeof(storage::HeapTable::TupleId);
        
        // Tuple(s)
        if (type_ == LogRecordType::INSERT) {
            uint32_t count = tuple_.size();
            std::memcpy(buffer, &count, sizeof(uint32_t));
            buffer += sizeof(uint32_t);
            for (size_t i = 0; i < count; ++i) {
                serialize_value(tuple_.get(i), buffer);
            }
        } else if (type_ == LogRecordType::UPDATE) {
            // Old tuple
            uint32_t old_count = old_tuple_.size();
            std::memcpy(buffer, &old_count, sizeof(uint32_t));
            buffer += sizeof(uint32_t);
            for (size_t i = 0; i < old_count; ++i) {
                serialize_value(old_tuple_.get(i), buffer);
            }
            // New tuple
            uint32_t new_count = tuple_.size();
            std::memcpy(buffer, &new_count, sizeof(uint32_t));
            buffer += sizeof(uint32_t);
            for (size_t i = 0; i < new_count; ++i) {
                serialize_value(tuple_.get(i), buffer);
            }
        } else { // DELETE types
             uint32_t count = old_tuple_.size();
             std::memcpy(buffer, &count, sizeof(uint32_t));
             buffer += sizeof(uint32_t);
             for (size_t i = 0; i < count; ++i) {
                 serialize_value(old_tuple_.get(i), buffer);
             }
        }
    } else if (type_ == LogRecordType::NEW_PAGE) {
        std::memcpy(buffer, &page_id_, sizeof(uint32_t));
        buffer += sizeof(uint32_t);
    }
    
    return buffer - start;
}

LogRecord LogRecord::deserialize(const char* buffer) {
    LogRecord record;
    const char* ptr = buffer;
    
    // Header
    std::memcpy(&record.size_, ptr, sizeof(uint32_t)); ptr += sizeof(uint32_t);
    std::memcpy(&record.lsn_, ptr, sizeof(lsn_t)); ptr += sizeof(lsn_t);
    std::memcpy(&record.prev_lsn_, ptr, sizeof(lsn_t)); ptr += sizeof(lsn_t);
    std::memcpy(&record.txn_id_, ptr, sizeof(txn_id_t)); ptr += sizeof(txn_id_t);
    std::memcpy(&record.type_, ptr, sizeof(LogRecordType)); ptr += sizeof(LogRecordType);
    
    // Body
    if (record.type_ == LogRecordType::INSERT || record.type_ == LogRecordType::MARK_DELETE || 
        record.type_ == LogRecordType::APPLY_DELETE || record.type_ == LogRecordType::ROLLBACK_DELETE ||
        record.type_ == LogRecordType::UPDATE) {
        
        // Table Name
        uint32_t name_len;
        std::memcpy(&name_len, ptr, sizeof(uint32_t));
        ptr += sizeof(uint32_t);
        record.table_name_ = std::string(ptr, name_len);
        ptr += name_len;
        
        // RID
        std::memcpy(&record.rid_, ptr, sizeof(storage::HeapTable::TupleId));
        ptr += sizeof(storage::HeapTable::TupleId);
        
        if (record.type_ == LogRecordType::INSERT) {
            uint32_t count;
            std::memcpy(&count, ptr, sizeof(uint32_t));
            ptr += sizeof(uint32_t);
            std::vector<common::Value> values;
            for (uint32_t i = 0; i < count; ++i) {
                values.push_back(deserialize_value(ptr));
            }
            record.tuple_ = executor::Tuple(std::move(values));
        } else if (record.type_ == LogRecordType::UPDATE) {
            // Old
            uint32_t old_count;
            std::memcpy(&old_count, ptr, sizeof(uint32_t));
            ptr += sizeof(uint32_t);
            std::vector<common::Value> old_values;
            for (uint32_t i = 0; i < old_count; ++i) {
                old_values.push_back(deserialize_value(ptr));
            }
            record.old_tuple_ = executor::Tuple(std::move(old_values));
            
            // New
            uint32_t new_count;
            std::memcpy(&new_count, ptr, sizeof(uint32_t));
            ptr += sizeof(uint32_t);
            std::vector<common::Value> new_values;
            for (uint32_t i = 0; i < new_count; ++i) {
                new_values.push_back(deserialize_value(ptr));
            }
            record.tuple_ = executor::Tuple(std::move(new_values));
        } else {
            uint32_t count;
            std::memcpy(&count, ptr, sizeof(uint32_t));
            ptr += sizeof(uint32_t);
            std::vector<common::Value> values;
            for (uint32_t i = 0; i < count; ++i) {
                values.push_back(deserialize_value(ptr));
            }
            record.old_tuple_ = executor::Tuple(std::move(values));
        }
    } else if (record.type_ == LogRecordType::NEW_PAGE) {
        std::memcpy(&record.page_id_, ptr, sizeof(uint32_t));
        ptr += sizeof(uint32_t);
    }
    
    return record;
}

uint32_t LogRecord::get_size() const {
    if (size_ > 0) return size_;
    
    // Header size (sum of fields)
    uint32_t s = sizeof(uint32_t) + sizeof(lsn_t) * 2 + sizeof(txn_id_t) + sizeof(LogRecordType);
    
    if (type_ == LogRecordType::INSERT || type_ == LogRecordType::MARK_DELETE || 
        type_ == LogRecordType::APPLY_DELETE || type_ == LogRecordType::ROLLBACK_DELETE ||
        type_ == LogRecordType::UPDATE) {
        
        s += sizeof(uint32_t) + table_name_.length();
        s += sizeof(storage::HeapTable::TupleId);
        
        if (type_ == LogRecordType::INSERT) {
            s += sizeof(uint32_t);
            for (size_t i = 0; i < tuple_.size(); ++i) {
                s += get_value_size(tuple_.get(i));
            }
        } else if (type_ == LogRecordType::UPDATE) {
            s += sizeof(uint32_t);
            for (size_t i = 0; i < old_tuple_.size(); ++i) {
                s += get_value_size(old_tuple_.get(i));
            }
            s += sizeof(uint32_t);
            for (size_t i = 0; i < tuple_.size(); ++i) {
                s += get_value_size(tuple_.get(i));
            }
        } else {
            s += sizeof(uint32_t);
            for (size_t i = 0; i < old_tuple_.size(); ++i) {
                s += get_value_size(old_tuple_.get(i));
            }
        }
    } else if (type_ == LogRecordType::NEW_PAGE) {
        s += sizeof(uint32_t);
    }
    
    return s;
}

} // namespace recovery
} // namespace cloudsql
