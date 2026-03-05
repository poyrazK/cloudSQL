/**
 * @file rpc_client.hpp
 * @brief Internal RPC client for node-to-node communication
 */

#ifndef SQL_ENGINE_NETWORK_RPC_CLIENT_HPP
#define SQL_ENGINE_NETWORK_RPC_CLIENT_HPP

#include <mutex>
#include <string>
#include <vector>

#include "network/rpc_message.hpp"

namespace cloudsql::network {

/**
 * @brief Client for sending internal cluster RPCs
 */
class RpcClient {
   public:
    RpcClient(const std::string& address, uint16_t port);
    ~RpcClient();

    bool connect();
    void disconnect();
    bool is_connected() const { return fd_ >= 0; }

    /**
     * @brief Send a request and wait for a response
     */
    bool call(RpcType type, const std::vector<uint8_t>& payload, std::vector<uint8_t>& response_out,
              uint16_t group_id = 0);

    /**
     * @brief Send a request without waiting for a response
     */
    bool send_only(RpcType type, const std::vector<uint8_t>& payload, uint16_t group_id = 0);

   private:
    std::string address_;
    uint16_t port_;
    int fd_ = -1;
    mutable std::mutex mutex_;
};

}  // namespace cloudsql::network

#endif  // SQL_ENGINE_NETWORK_RPC_CLIENT_HPP
