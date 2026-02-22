/**
 * @file catalog.hpp
 * @brief C++ System Catalog for database metadata
 */

#ifndef SQL_ENGINE_CATALOG_CATALOG_HPP
#define SQL_ENGINE_CATALOG_CATALOG_HPP

#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

#include "common/value.hpp"

namespace cloudsql {

// Type aliases
using oid_t = uint32_t;

/**
 * @brief Column information structure
 */
struct ColumnInfo {
    std::string name;
    common::ValueType type = common::ValueType::TYPE_NULL;
    uint16_t position = 0;
    uint32_t max_length = 0;
    bool nullable = true;
    bool is_primary_key = false;
    std::optional<std::string> default_value;
    uint32_t flags = 0;

    ColumnInfo() = default;

    ColumnInfo(std::string name, common::ValueType type, uint16_t pos)
        : name(std::move(name)), type(type), position(pos) {}
};

/**
 * @brief Index type enumeration
 */
enum class IndexType : uint8_t { BTree = 0, Hash = 1, GiST = 2, SPGiST = 3, GIN = 4, BRIN = 5 };

/**
 * @brief Index information structure
 */
struct IndexInfo {
    oid_t index_id = 0;
    std::string name;
    oid_t table_id = 0;
    std::vector<uint16_t> column_positions;
    IndexType index_type = IndexType::BTree;
    std::string filename;
    bool is_unique = false;
    bool is_primary = false;
    uint32_t flags = 0;

    IndexInfo() = default;
};

/**
 * @brief Table information structure
 */
struct TableInfo {
    oid_t table_id = 0;
    std::string name;
    std::vector<ColumnInfo> columns;
    std::vector<IndexInfo> indexes;
    uint64_t num_rows = 0;
    std::string filename;
    uint32_t flags = 0;
    uint64_t created_at = 0;
    uint64_t modified_at = 0;

    TableInfo() = default;

    /**
     * @brief Get column by name
     */
    [[nodiscard]] std::optional<ColumnInfo*> get_column(const std::string& col_name) {
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
    [[nodiscard]] std::optional<ColumnInfo*> get_column_by_position(uint16_t pos) {
        if (pos < columns.size()) {
            return &columns[pos];
        }
        return std::nullopt;
    }

    /**
     * @brief Get number of columns
     */
    [[nodiscard]] uint16_t num_columns() const { return static_cast<uint16_t>(columns.size()); }

    /**
     * @brief Get number of indexes
     */
    [[nodiscard]] uint16_t num_indexes() const { return static_cast<uint16_t>(indexes.size()); }
};

/**
 * @brief Database information structure
 */
struct DatabaseInfo {
    oid_t database_id = 0;
    std::string name;
    uint32_t encoding = 0;
    std::string collation;
    std::vector<oid_t> table_ids;
    uint64_t created_at = 0;

    DatabaseInfo() = default;
};

/**
 * @brief System Catalog class
 */
class Catalog {
   public:
    /**
     * @brief Default constructor
     */
    Catalog() = default;

    /**
     * @brief Create a new catalog
     */
    [[nodiscard]] static std::unique_ptr<Catalog> create();

    /**
     * @brief Load catalog from file
     */
    bool load(const std::string& filename);

    /**
     * @brief Save catalog to file
     */
    [[nodiscard]] bool save(const std::string& filename) const;

    /**
     * @brief Create a new table
     * @return Table OID or 0 on error
     */
    oid_t create_table(const std::string& table_name, std::vector<ColumnInfo> columns);

    /**
     * @brief Drop a table
     */
    bool drop_table(oid_t table_id);

    /**
     * @brief Get table by ID
     */
    [[nodiscard]] std::optional<TableInfo*> get_table(oid_t table_id);

    /**
     * @brief Get table by name
     */
    [[nodiscard]] std::optional<TableInfo*> get_table_by_name(const std::string& table_name);

    /**
     * @brief Get all tables
     */
    [[nodiscard]] std::vector<TableInfo*> get_all_tables();

    /**
     * @brief Create an index
     * @return Index OID or 0 on error
     */
    oid_t create_index(const std::string& index_name, oid_t table_id,
                       std::vector<uint16_t> column_positions, IndexType index_type,
                       bool is_unique);

    /**
     * @brief Drop an index
     */
    bool drop_index(oid_t index_id);

    /**
     * @brief Get index by ID
     */
    [[nodiscard]] std::optional<std::pair<TableInfo*, IndexInfo*>> get_index(oid_t index_id);

    /**
     * @brief Get indexes for a table
     */
    [[nodiscard]] std::vector<IndexInfo*> get_table_indexes(oid_t table_id);

    /**
     * @brief Update table statistics
     */
    bool update_table_stats(oid_t table_id, uint64_t num_rows);

    /**
     * @brief Check if table exists
     */
    [[nodiscard]] bool table_exists(oid_t table_id) const;

    /**
     * @brief Check if table exists by name
     */
    [[nodiscard]] bool table_exists_by_name(const std::string& table_name) const;

    /**
     * @brief Get database info
     */
    [[nodiscard]] const DatabaseInfo& get_database() const { return database_; }

    /**
     * @brief Set database info
     */
    void set_database(const DatabaseInfo& db) { database_ = db; }

    /**
     * @brief Print catalog contents
     */
    void print() const;

    /**
     * @brief Get catalog version
     */
    [[nodiscard]] uint64_t get_version() const { return version_; }

   private:
    std::unordered_map<oid_t, std::unique_ptr<TableInfo>> tables_;
    DatabaseInfo database_;
    oid_t next_oid_ = 1;
    uint64_t version_ = 1;

    [[nodiscard]] static uint64_t get_current_time();
};



}  // namespace cloudsql

#endif  // SQL_ENGINE_CATALOG_CATALOG_HPP
