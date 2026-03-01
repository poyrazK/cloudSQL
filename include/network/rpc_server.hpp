/**
 * @file rpc_server.hpp
 * @brief Internal RPC server for node-to-node communication
 */

#ifndef SQL_ENGINE_NETWORK_RPC_SERVER_HPP
#define SQL_ENGINE_NETWORK_RPC_SERVER_HPP

#include <atomic>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>
#include <netinet/in.h>
#include <unordered_map>

#include "network/rpc_message.hpp"

namespace cloudsql::network {

/**
 * @brief Callback type for handling incoming RPCs
 */
using RpcHandler = std::function<void(const RpcHeader&, const std::vector<uint8_t>&, int)>;

/**
 * @brief Server for handling internal cluster RPCs
 */
class RpcServer {
   public:
    explicit RpcServer(uint16_t port) : port_(port) {}
    ~RpcServer() { stop(); }

    // Prevent copying
    RpcServer(const RpcServer&) = delete;
    RpcServer& operator=(const RpcServer&) = delete;

    bool start();
    void stop();
    void set_handler(RpcType type, RpcHandler handler);

    /**
     * @brief Get a handler for a specific type (for testing)
     */
    RpcHandler get_handler(RpcType type) {
        std::scoped_lock<std::mutex> lock(handlers_mutex_);
        if (handlers_.count(type) != 0U) {
            return handlers_[type];
        }
        return nullptr;
    }

   private:
    void accept_loop();
    void handle_client(int client_fd);

    uint16_t port_;
    int listen_fd_ = -1;
    std::atomic<bool> running_{false};
    std::thread accept_thread_;
    std::vector<std::thread> worker_threads_;
    std::unordered_map<RpcType, RpcHandler> handlers_;
    std::mutex handlers_mutex_;
};

} // namespace cloudsql::network

#endif // SQL_ENGINE_NETWORK_RPC_SERVER_HPP
