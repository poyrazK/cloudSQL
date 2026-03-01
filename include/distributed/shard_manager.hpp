/**
 * @file shard_manager.hpp
 * @brief Utility for hash-based sharding and routing
 */

#ifndef SQL_ENGINE_DISTRIBUTED_SHARD_MANAGER_HPP
#define SQL_ENGINE_DISTRIBUTED_SHARD_MANAGER_HPP

#include <cstdint>
#include <string>
#include <vector>

#include "catalog/catalog.hpp"
#include "common/value.hpp"

namespace cloudsql::cluster {

/**
 * @brief Manages data sharding logic
 */
class ShardManager {
   public:
    /**
     * @brief Compute target shard index based on primary key value
     */
    static uint32_t compute_shard(const common::Value& pk_value, uint32_t num_shards) {
        if (num_shards == 0) return 0;

        // Simple hash for demo purposes
        std::string s = pk_value.to_string();
        size_t hash = std::hash<std::string>{}(s);
        return static_cast<uint32_t>(hash % num_shards);
    }

    /**
     * @brief Find the node info for a specific shard of a table
     */
    static std::optional<ShardInfo> get_target_node(const TableInfo& table, uint32_t shard_id) {
        for (const auto& shard : table.shards) {
            if (shard.shard_id == shard_id) {
                return shard;
            }
        }
        return std::nullopt;
    }
};

}  // namespace cloudsql::cluster

#endif  // SQL_ENGINE_DISTRIBUTED_SHARD_MANAGER_HPP
