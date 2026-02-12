#include "catalog/catalog.hpp"
#include <ctime>

namespace cloudsql {

std::unique_ptr<Catalog> Catalog::create() {
    return std::make_unique<Catalog>();
}

bool Catalog::load(const std::string& filename) {
    std::ifstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Cannot open catalog file: " << filename << std::endl;
        return false;
    }
    // Simplified - just read database name
    std::string line;
    while (std::getline(file, line)) {
        if (line.empty() || line[0] == '#') continue;
        // Parse catalog entries
    }
    file.close();
    return true;
}

bool Catalog::save(const std::string& filename) const {
    std::ofstream file(filename);
    if (!file.is_open()) {
        std::cerr << "Cannot open catalog file for writing: " << filename << std::endl;
        return false;
    }
    file << "# System Catalog
";
    file << "# Auto-generated

";
    file.close();
    return true;
}

oid_t Catalog::create_table(const std::string& table_name, 
                   std::vector<ColumnInfo> columns) {
    TableInfo table;
    table.table_id = next_oid_++;
    table.name = table_name;
    table.columns = std::move(columns);
    table.created_at = get_current_time();
    
    tables_[table.table_id] = std::make_unique<TableInfo>(std::move(table));
    return table.table_id;
}

bool Catalog::drop_table(oid_t table_id) {
    auto it = tables_.find(table_id);
    if (it != tables_.end()) {
        tables_.erase(it);
        return true;
    }
    return false;
}

std::optional<TableInfo*> Catalog::get_table(oid_t table_id) {
    auto it = tables_.find(table_id);
    if (it != tables_.end()) {
        return it->second.get();
    }
    return std::nullopt;
}

std::optional<TableInfo*> Catalog::get_table_by_name(const std::string& table_name) {
    for (auto& pair : tables_) {
        if (pair.second->name == table_name) {
            return pair.second.get();
        }
    }
    return std::nullopt;
}

std::vector<TableInfo*> Catalog::get_all_tables() {
    std::vector<TableInfo*> result;
    for (auto& pair : tables_) {
        result.push_back(pair.second.get());
    }
    return result;
}

oid_t Catalog::create_index(const std::string& index_name, oid_t table_id,
                   std::vector<uint16_t> column_positions,
                   IndexType index_type, bool is_unique) {
    auto table = get_table(table_id);
    if (!table.has_value()) {
        return 0;
    }

    IndexInfo index;
    index.index_id = next_oid_++;
    index.name = index_name;
    index.table_id = table_id;
    index.column_positions = std::move(column_positions);
    index.index_type = index_type;
    index.is_unique = is_unique;

    (*table)->indexes.push_back(std::move(index));
    return index.index_id;
}

bool Catalog::drop_index(oid_t index_id) {
    // Search through all tables to find and remove the index
    for (auto& pair : tables_) {
        auto& indexes = pair.second->indexes;
        for (auto it = indexes.begin(); it != indexes.end(); ++it) {
            if (it->index_id == index_id) {
                indexes.erase(it);
                return true;
            }
        }
    }
    return false;
}

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

std::vector<IndexInfo*> Catalog::get_table_indexes(oid_t table_id) {
    std::vector<IndexInfo*> result;
    auto table = get_table(table_id);
    if (table.has_value()) {
        for (auto& index : (*table)->indexes) {
            result.push_back(&index);
        }
    }
    return result;
}

bool Catalog::update_table_stats(oid_t table_id, uint64_t num_rows) {
    auto table = get_table(table_id);
    if (table.has_value()) {
        (*table)->num_rows = num_rows;
        (*table)->modified_at = get_current_time();
        return true;
    }
    return false;
}

bool Catalog::table_exists(oid_t table_id) const {
    return tables_.find(table_id) != tables_.end();
}

bool Catalog::table_exists_by_name(const std::string& table_name) const {
    for (const auto& pair : tables_) {
        if (pair.second->name == table_name) {
            return true;
        }
    }
    return false;
}

void Catalog::print() const {
    std::cout << "=== System Catalog ===" << std::endl;
    std::cout << "Database: " << database_.name << std::endl;
    std::cout << "Tables: " << tables_.size() << std::endl;
    
    for (const auto& pair : tables_) {
        const auto& table = *pair.second;
        std::cout << "  Table: " << table.name << " (OID: " << table.table_id << ")" << std::endl;
        std::cout << "    Columns: " << table.num_columns() << std::endl;
        std::cout << "    Indexes: " << table.num_indexes() << std::endl;
        std::cout << "    Rows: " << table.num_rows << std::endl;
    }
    std::cout << "======================" << std::endl;
}

uint64_t Catalog::get_current_time() {
    return static_cast<uint64_t>(std::time(nullptr));
}

} // namespace cloudsql
