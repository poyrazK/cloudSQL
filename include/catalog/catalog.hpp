/**
 * @file catalog.hpp
 * @brief C++ System Catalog for database metadata
 */

#ifndef SQL_ENGINE_CATALOG_CATALOG_HPP
#define SQL_ENGINE_CATALOG_CATALOG_HPP

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <optional>
#include <variant>
#include <fstream>
#include <iostream>

namespace cloudsql {

// Type aliases
using oid_t = uint32_t;

/**
 * @brief Value types supported by the database (C++ enum)
 */
enum class ValueType : uint8_t {
    Null = 0,
    Bool = 1,
    Int8 = 2,
    Int16 = 3,
    Int32 = 4,
    Int64 = 5,
    Float32 = 6,
    Float64 = 7,
    Decimal = 8,
    Char = 9,
    Varchar = 10,
    Text = 11,
    Date = 12,
    Time = 13,
    Timestamp = 14,
    Json = 15,
    Blob = 16
};

/**
 * @brief Column information structure (C++ class)
 */
class ColumnInfo {
public:
    std::string name;
    ValueType type;
    uint16_t position;
    uint32_t max_length;
    bool nullable;
    bool is_primary_key;
    std::optional<std::string> default_value;
    uint32_t flags;

    ColumnInfo() 
        : type(ValueType::Null)
        , position(0)
        , max_length(0)
        , nullable(true)
        , is_primary_key(false)
        , flags(0)
    {}

    ColumnInfo(std::string name, ValueType type, uint16_t pos)
        : name(std::move(name))
        , type(type)
        , position(pos)
        , max_length(0)
        , nullable(true)
        , is_primary_key(false)
        , flags(0)
    {}
};

/**
 * @brief Index type enumeration
 */
enum class IndexType : uint8_t {
    BTree = 0,
    Hash = 1,
    GiST = 2,
    SPGiST = 3,
    GIN = 3,
    BRIN = 4
};

/**
 * @brief Index information structure (C++ class)
 */
class IndexInfo {
public:
    oid_t index_id;
    std::string name;
    oid_t table_id;
    std::vector<uint16_t> column_positions;
    IndexType index_type;
    std::string filename;
    bool is_unique;
    bool is_primary;
    uint32_t flags;

    IndexInfo()
        : index_id(0)
        , table_id(0)
        , index_type(IndexType::BTree)
        , is_unique(false)
        , is_primary(false)
        , flags(0)
    {}
};

/**
 * @brief Table information structure (C++ class)
 */
class TableInfo {
public:
    oid_t table_id;
    std::string name;
    std::vector<ColumnInfo> columns;
    std::vector<IndexInfo> indexes;
    uint64_t num_rows;
    std::string filename;
    uint32_t flags;
    uint64_t created_at;
    uint64_t modified_at;

    TableInfo()
        : table_id(0)
        , num_rows(0)
        , flags(0)
        , created_at(0)
        , modified_at(0)
    {}

    /**
     * @brief Get column by name
     */
    std::optional<ColumnInfo*> get_column(const std::string& col_name) {
        for (auto& col : columns) {
            if (col.name == col_name) {
                return &col;
            }
        }
        return std::nullopt;
    }

    /**
     * @brief Get column by position (0-based)
     */
    std::optional<ColumnInfo*> get_column_by_position(uint16_t pos) {
        if (pos < columns.size()) {
            return &columns[pos];
        }
        return std::nullopt;
    }

    /**
     * @brief Get number of columns
     */
    uint16_t num_columns() const {
        return static_cast<uint16_t>(columns.size());
    }

    /**
     * @brief Get number of indexes
     */
    uint16_t num_indexes() const {
        return static_cast<uint16_t>(indexes.size());
    }
};

/**
 * @brief Database information structure (C++ class)
 */
class DatabaseInfo {
public:
    oid_t database_id;
    std::string name;
    uint32_t encoding;
    std::string collation;
    std::vector<oid_t> table_ids;
    uint64_t created_at;

    DatabaseInfo()
        : database_id(0)
        , encoding(0)
        , created_at(0)
    {}
};

/**
 * @brief System Catalog class
 */
class Catalog {
public:
    /**
     * @brief Default constructor
     */
    Catalog() : next_oid_(1) {}
    
    /**
     * @brief Create a new catalog
     */
    static std::unique_ptr<Catalog> create();

    /**
     * @brief Load catalog from file
     */
    bool load(const std::string& filename);

    /**
     * @brief Save catalog to file
     */
    bool save(const std::string& filename) const;

    /**
     * @brief Create a new table
     * @return Table OID or 0 on error
     */
    oid_t create_table(const std::string& table_name, 
                       std::vector<ColumnInfo> columns);

    /**
     * @brief Drop a table
     */
    bool drop_table(oid_t table_id);

    /**
     * @brief Get table by ID
     */
    std::optional<TableInfo*> get_table(oid_t table_id);

    /**
     * @brief Get table by name
     */
    std::optional<TableInfo*> get_table_by_name(const std::string& table_name);

    /**
     * @brief Get all tables
     */
    std::vector<TableInfo*> get_all_tables();

    /**
     * @brief Create an index
     * @return Index OID or 0 on error
     */
    oid_t create_index(const std::string& index_name, oid_t table_id,
                       std::vector<uint16_t> column_positions,
                       IndexType index_type, bool is_unique);

    /**
     * @brief Drop an index
     */
    bool drop_index(oid_t index_id);

    /**
     * @brief Get index by ID
     */
    std::optional<std::pair<TableInfo*, IndexInfo*>> get_index(oid_t index_id);

    /**
     * @brief Get indexes for a table
     */
    std::vector<IndexInfo*> get_table_indexes(oid_t table_id);

    /**
     * @brief Update table statistics
     */
    bool update_table_stats(oid_t table_id, uint64_t num_rows);

    /**
     * @brief Check if table exists
     */
    bool table_exists(oid_t table_id) const;

    /**
     * @brief Check if table exists by name
     */
    bool table_exists_by_name(const std::string& table_name) const;

    /**
     * @brief Get database info
     */
    const DatabaseInfo& get_database() const {
        return database_;
    }

    /**
     * @brief Set database info
     */
    void set_database(const DatabaseInfo& db) {
        database_ = db;
    }

    /**
     * @brief Print catalog contents
     */
    void print() const;

    /**
     * @brief Get catalog version
     */
    uint64_t get_version() const { return version_; }

private:
    std::unordered_map<oid_t, std::unique_ptr<TableInfo>> tables_;
    DatabaseInfo database_;
    oid_t next_oid_;
    uint64_t version_ = 1;

    static uint64_t get_current_time();
};

} // namespace cloudsql

#endif // SQL_ENGINE_CATALOG_CATALOG_HPP
