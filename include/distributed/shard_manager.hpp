/**
 * @file shard_manager.hpp
 * @brief Utility for hash-based sharding and routing
 */

#ifndef SQL_ENGINE_DISTRIBUTED_SHARD_MANAGER_HPP
#define SQL_ENGINE_DISTRIBUTED_SHARD_MANAGER_HPP

#include <cstdint>
#include <string>
#include <vector>
#include <optional>

#include "catalog/catalog.hpp"
#include "common/value.hpp"

namespace cloudsql::cluster {

/**
 * @brief Manages data sharding and node mapping
 */
class ShardManager {
   public:
    /**
     * @brief Stable hash function (DJB2) to ensure consistent sharding across processes
     */
    static uint32_t stable_hash(const std::string& s) {
        uint32_t hash = 5381;
        for (char c : s) {
            hash = ((hash << 5) + hash) + static_cast<uint8_t>(c); /* hash * 33 + c */
        }
        return hash;
    }

    /**
     * @brief Compute destination shard index for a given key
     */
    static uint32_t compute_shard(const common::Value& pk_value, uint32_t num_shards) {
        if (num_shards == 0) {
            return 0;
        }

        // Use stable hash instead of std::hash which can vary per process/implementation
        const std::string s = pk_value.to_string();
        const uint32_t hash = stable_hash(s);
        return hash % num_shards;
    }

    /**
     * @brief Find which data node is responsible for a given shard ID
     */
    static std::optional<cloudsql::ShardInfo> get_target_node(const cloudsql::TableInfo& table, uint32_t shard_id) {
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
