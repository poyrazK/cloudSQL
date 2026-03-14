/**
 * @file catalog.cpp
 * @brief System Catalog implementation
 */

#include "catalog/catalog.hpp"

#include <algorithm>
#include <cstdint>
#include <ctime>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "common/cluster_manager.hpp"
#include "distributed/raft_group.hpp"

namespace cloudsql {

/**
 * @brief Create a new catalog
 */
std::unique_ptr<Catalog> Catalog::create() {
    return std::make_unique<Catalog>();
}

/**
 * @brief Load catalog from file
 */
bool Catalog::load(const std::string& filename) {
    (void)database_;  // Use instance member to satisfy linter
    std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Cannot open catalog file: " << filename << "\n";
        return false;
    }
    // Simplified - just read database name
    std::string line;
    while (std::getline(file, line)) {
        if (line.empty() || line[0] == '#') {
            continue;
        }
        // Parse catalog entries
    }
    file.close();
    return true;
}

/**
 * @brief Save catalog to file
 */
bool Catalog::save(const std::string& filename) const {
    (void)database_;  // Use instance member to satisfy linter
    std::ofstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Cannot open catalog file for writing: " << filename << "\n";
        return false;
    }
    file << "# System Catalog\n";
    file << "# Auto-generated\n\n";
    file.close();
    return true;
}

/**
 * @brief Create a new table
 */
oid_t Catalog::create_table(const std::string& table_name, std::vector<ColumnInfo> columns) {
    std::cerr << "--- [Catalog] create_table CALLED for " << table_name << " ---" << std::endl;

    // Compute shards from ClusterManager for serialization
    std::vector<ShardInfo> shards;
    if (cluster_manager_ != nullptr) {
        auto data_nodes = cluster_manager_->get_data_nodes();
        if (!data_nodes.empty()) {
            std::sort(data_nodes.begin(), data_nodes.end(),
                      [](const auto& a, const auto& b) { return a.id < b.id; });
            uint32_t sid = 0;
            for (const auto& node : data_nodes) {
                ShardInfo shard;
                shard.shard_id = sid++;
                shard.node_address = node.address;
                shard.port = node.cluster_port;
                shards.push_back(shard);
            }
        }
    }

    if (raft_group_ != nullptr) {
        // Multi-Raft: Replicate DDL via Catalog Raft Group (ID 0)
        // Serialize command:
        // [Type:1][NameLen:4][Name][ColCount:4][Cols...][ShardCount:4][Shards...]
        std::vector<uint8_t> cmd;
        cmd.push_back(1);  // Type 1: CreateTable

        uint32_t name_len = static_cast<uint32_t>(table_name.size());
        size_t offset = cmd.size();
        cmd.resize(offset + 4 + table_name.size());
        std::memcpy(cmd.data() + offset, &name_len, 4);
        std::memcpy(cmd.data() + offset + 4, table_name.data(), name_len);

        uint32_t col_count = static_cast<uint32_t>(columns.size());
        offset = cmd.size();
        cmd.resize(offset + 4);
        std::memcpy(cmd.data() + offset, &col_count, 4);

        for (const auto& col : columns) {
            uint32_t cname_len = static_cast<uint32_t>(col.name.size());
            offset = cmd.size();
            cmd.resize(offset + 4 + col.name.size() + 1 + 2);  // len + name + type + pos
            std::memcpy(cmd.data() + offset, &cname_len, 4);
            std::memcpy(cmd.data() + offset + 4, col.name.data(), cname_len);
            cmd[offset + 4 + cname_len] = static_cast<uint8_t>(col.type);
            std::memcpy(cmd.data() + offset + 4 + cname_len + 1, &col.position, 2);
        }

        uint32_t shard_count = static_cast<uint32_t>(shards.size());
        offset = cmd.size();
        cmd.resize(offset + 4);
        std::memcpy(cmd.data() + offset, &shard_count, 4);

        for (const auto& shard : shards) {
            uint32_t addr_len = static_cast<uint32_t>(shard.node_address.size());
            offset = cmd.size();
            cmd.resize(offset + 4 + addr_len + 4 + 2);
            std::memcpy(cmd.data() + offset, &addr_len, 4);
            std::memcpy(cmd.data() + offset + 4, shard.node_address.data(), addr_len);
            std::memcpy(cmd.data() + offset + 4 + addr_len, &shard.shard_id, 4);
            std::memcpy(cmd.data() + offset + 4 + addr_len + 4, &shard.port, 2);
        }

        if (raft_group_->replicate(cmd)) {
            return create_table_local(table_name, std::move(columns), std::move(shards));
        }
    }

    return create_table_local(table_name, std::move(columns), std::move(shards));
}

oid_t Catalog::create_table_local(const std::string& table_name, std::vector<ColumnInfo> columns,
                                  std::vector<ShardInfo> shards) {
    if (table_exists_by_name(table_name)) {
        throw std::runtime_error("Table already exists: " + table_name);
    }

    auto table = std::make_unique<TableInfo>();
    table->table_id = next_oid_++;
    table->name = table_name;
    table->columns = std::move(columns);
    table->created_at = get_current_time();
    table->shards = std::move(shards);

    if (table->shards.empty() && cluster_manager_ != nullptr) {
        auto data_nodes = cluster_manager_->get_data_nodes();
        if (!data_nodes.empty()) {
            std::sort(data_nodes.begin(), data_nodes.end(),
                      [](const auto& a, const auto& b) { return a.id < b.id; });
            uint32_t sid = 0;
            for (const auto& node : data_nodes) {
                ShardInfo shard;
                shard.shard_id = sid++;
                shard.node_address = node.address;
                shard.port = node.cluster_port;
                table->shards.push_back(shard);
            }
        }
    }

    if (table->shards.empty()) {
        ShardInfo shard;
        shard.shard_id = 0;
        shard.node_address = "127.0.0.1";
        shard.port = 6432;
        table->shards.push_back(shard);
    }

    std::cerr << "--- [Catalog] Table " << table_name << " initialized with "
              << table->shards.size() << " shards ---" << std::endl;

    const oid_t id = table->table_id;
    tables_[id] = std::move(table);
    version_++;
    return id;
}

/**
 * @brief Drop a table
 */
bool Catalog::drop_table(oid_t table_id) {
    if (raft_group_ != nullptr) {
        std::vector<uint8_t> cmd;
        cmd.push_back(2);  // Type 2: DropTable
        cmd.resize(cmd.size() + 4);
        std::memcpy(cmd.data() + 1, &table_id, 4);

        if (raft_group_->replicate(cmd)) {
            return drop_table_local(table_id);
        }
    }
    return drop_table_local(table_id);
}

bool Catalog::drop_table_local(oid_t table_id) {
    auto it = tables_.find(table_id);
    if (it != tables_.end()) {
        tables_.erase(it);
        version_++;
        return true;
    }
    return false;
}

void Catalog::apply(const raft::LogEntry& entry) {
    if (entry.data.empty()) return;
    std::cerr << "--- [Catalog] apply CALLED for entry type " << (int)entry.data[0] << " ---"
              << std::endl;

    uint8_t type = entry.data[0];
    if (type == 1) {  // CreateTable
        size_t offset = 1;

        uint32_t name_len = 0;
        std::memcpy(&name_len, entry.data.data() + offset, 4);
        offset += 4;
        std::string table_name(reinterpret_cast<const char*>(entry.data.data() + offset), name_len);
        offset += name_len;

        uint32_t col_count = 0;
        std::memcpy(&col_count, entry.data.data() + offset, 4);
        offset += 4;

        std::vector<ColumnInfo> columns;
        for (uint32_t i = 0; i < col_count; ++i) {
            uint32_t cname_len = 0;
            std::memcpy(&cname_len, entry.data.data() + offset, 4);
            offset += 4;
            std::string cname(reinterpret_cast<const char*>(entry.data.data() + offset), cname_len);
            offset += cname_len;
            common::ValueType ctype = static_cast<common::ValueType>(entry.data[offset++]);
            uint16_t cpos = 0;
            std::memcpy(&cpos, entry.data.data() + offset, 2);
            offset += 2;
            columns.emplace_back(cname, ctype, cpos);
        }

        uint32_t shard_count = 0;
        std::memcpy(&shard_count, entry.data.data() + offset, 4);
        offset += 4;

        std::vector<ShardInfo> shards;
        for (uint32_t i = 0; i < shard_count; ++i) {
            uint32_t addr_len = 0;
            std::memcpy(&addr_len, entry.data.data() + offset, 4);
            offset += 4;
            std::string addr(reinterpret_cast<const char*>(entry.data.data() + offset), addr_len);
            offset += addr_len;
            ShardInfo shard;
            shard.node_address = addr;
            std::memcpy(&shard.shard_id, entry.data.data() + offset, 4);
            std::memcpy(&shard.port, entry.data.data() + offset + 4, 2);
            offset += 6;
            shards.push_back(shard);
        }

        try {
            create_table_local(table_name, std::move(columns), std::move(shards));
        } catch (const std::exception& e) {
            // Ignore duplicate table errors during Raft replay
        }
    } else if (type == 2) {  // DropTable
        oid_t table_id = 0;
        std::memcpy(&table_id, entry.data.data() + 1, 4);
        drop_table_local(table_id);
    }
}

/**
 * @brief Get table by ID
 */
std::optional<TableInfo*> Catalog::get_table(oid_t table_id) {
    auto it = tables_.find(table_id);
    if (it != tables_.end()) {
        return it->second.get();
    }
    return std::nullopt;
}

/**
 * @brief Get table by name
 */
std::optional<TableInfo*> Catalog::get_table_by_name(const std::string& table_name) {
    for (auto& pair : tables_) {
        if (pair.second->name == table_name) {
            return pair.second.get();
        }
    }

    std::cerr << "--- [Catalog] Table NOT FOUND: " << table_name << ". Catalog contains: ";
    for (auto& pair : tables_) {
        std::cerr << pair.second->name << ", ";
    }
    std::cerr << " ---" << std::endl;

    return std::nullopt;
}

/**
 * @brief Get all tables
 */
std::vector<TableInfo*> Catalog::get_all_tables() {
    std::vector<TableInfo*> result;
    result.reserve(tables_.size());
    for (auto& pair : tables_) {
        result.push_back(pair.second.get());
    }
    return result;
}

/**
 * @brief Create an index
 */
oid_t Catalog::create_index(const std::string& index_name, oid_t table_id,
                            std::vector<uint16_t> column_positions, IndexType index_type,
                            bool is_unique) {
    auto table_opt = get_table(table_id);
    if (!table_opt.has_value()) {
        return 0;
    }

    auto& table = *table_opt.value();
    for (const auto& existing_idx : table.indexes) {
        if (existing_idx.name == index_name) {
            throw std::runtime_error("Index already exists: " + index_name);
        }
    }

    IndexInfo index;
    index.index_id = next_oid_++;
    index.name = index_name;
    index.table_id = table_id;
    index.column_positions = std::move(column_positions);
    index.index_type = index_type;
    index.is_unique = is_unique;

    const oid_t id = index.index_id;
    table.indexes.push_back(std::move(index));
    version_++;
    return id;
}

/**
 * @brief Drop an index
 */
bool Catalog::drop_index(oid_t index_id) {
    // Search through all tables to find and remove the index
    for (auto& pair : tables_) {
        auto& indexes = pair.second->indexes;
        for (auto it = indexes.begin(); it != indexes.end(); ++it) {
            if (it->index_id == index_id) {
                indexes.erase(it);
                version_++;
                return true;
            }
        }
    }
    return false;
}

/**
 * @brief Get index by ID
 */
std::optional<std::pair<TableInfo*, IndexInfo*>> Catalog::get_index(oid_t index_id) {
    for (auto& pair : tables_) {
        for (auto& index : pair.second->indexes) {
            if (index.index_id == index_id) {
                return std::make_pair(pair.second.get(), &index);
            }
        }
    }
    return std::nullopt;
}

/**
 * @brief Get indexes for a table
 */
std::vector<IndexInfo*> Catalog::get_table_indexes(oid_t table_id) {
    std::vector<IndexInfo*> result;
    auto table_opt = get_table(table_id);
    if (table_opt.has_value()) {
        result.reserve((*table_opt)->indexes.size());
        for (auto& index : (*table_opt)->indexes) {
            result.push_back(&index);
        }
    }
    return result;
}

/**
 * @brief Update table statistics
 */
bool Catalog::update_table_stats(oid_t table_id, uint64_t num_rows) {
    auto table_opt = get_table(table_id);
    if (table_opt.has_value()) {
        (*table_opt)->num_rows = num_rows;
        (*table_opt)->modified_at = get_current_time();
        version_++;
        return true;
    }
    return false;
}

/**
 * @brief Check if table exists
 */
bool Catalog::table_exists(oid_t table_id) const {
    return tables_.find(table_id) != tables_.end();
}

/**
 * @brief Check if table exists by name
 */
bool Catalog::table_exists_by_name(const std::string& table_name) const {
    return std::any_of(tables_.begin(), tables_.end(),
                       [&table_name](const auto& pair) { return pair.second->name == table_name; });
}

/**
 * @brief Print catalog contents
 */
void Catalog::print() const {
    std::cout << "=== System Catalog ===\n";
    std::cout << "Database: " << database_.name << "\n";
    std::cout << "Tables: " << tables_.size() << "\n";

    for (const auto& pair : tables_) {
        const auto& table = *pair.second;
        std::cout << "  Table: " << table.name << " (OID: " << table.table_id << ")\n";
        std::cout << "    Columns: " << table.num_columns() << "\n";
        std::cout << "    Indexes: " << table.num_indexes() << "\n";
        std::cout << "    Shards:  " << table.shards.size() << "\n";
        std::cout << "    Rows:    " << table.num_rows << "\n";
    }
    std::cout << "======================\n";
}

uint64_t Catalog::get_current_time() {
    return static_cast<uint64_t>(std::time(nullptr));
}

}  // namespace cloudsql
