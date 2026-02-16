/**
 * @file server.hpp
 * @brief C++ Network Server for PostgreSQL wire protocol
 */

#ifndef SQL_ENGINE_NETWORK_SERVER_HPP
#define SQL_ENGINE_NETWORK_SERVER_HPP

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "catalog/catalog.hpp"
#include "executor/query_executor.hpp"
#include "storage/storage_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction_manager.hpp"

namespace cloudsql {
namespace network {

/**
 * @brief Server statistics (C++ class)
 */
class ServerStats {
   public:
    std::atomic<uint64_t> connections_accepted{0};
    std::atomic<uint64_t> connections_active{0};
    std::atomic<uint64_t> queries_executed{0};
    std::atomic<uint64_t> bytes_received{0};
    std::atomic<uint64_t> bytes_sent{0};
    std::atomic<uint64_t> uptime_seconds{0};
};

/**
 * @brief Server status enumeration
 */
enum class ServerStatus { Stopped, Starting, Running, Stopping, Error };

/**
 * @brief Network Server class
 */
class Server {
   public:
    /**
     * @brief Constructor
     */
    Server(uint16_t port, Catalog& catalog, storage::StorageManager& storage_manager);

    /**
     * @brief Destructor
     */
    ~Server() {
        stop();
        if (listen_fd_ >= 0) {
            close(listen_fd_);
        }
    }

    /**
     * @brief Create a new server instance
     */
    static std::unique_ptr<Server> create(uint16_t port, Catalog& catalog,
                                          storage::StorageManager& storage_manager);

    /**
     * @brief Start the server
     */
    bool start();

    /**
     * @brief Stop the server
     */
    bool stop();

    /**
     * @brief Wait for server to stop
     */
    void wait();

    const ServerStats& get_stats() const { return stats_; }
    ServerStatus get_status() const { return status_; }
    uint16_t get_port() const { return port_; }
    bool is_running() const { return running_.load(); }
    std::string get_status_string() const;

   private:
    void accept_connections();
    void handle_connection(int client_fd);

    uint16_t port_;
    int listen_fd_;
    std::atomic<bool> running_{false};
    std::atomic<ServerStatus> status_;

    Catalog& catalog_;
    storage::StorageManager& storage_manager_;
    transaction::LockManager lock_manager_;
    transaction::TransactionManager transaction_manager_;

    ServerStats stats_;
    std::thread accept_thread_;
    std::vector<std::thread> worker_threads_;
    std::mutex thread_mutex_;
};

}  // namespace network
}  // namespace cloudsql

#endif  // SQL_ENGINE_NETWORK_SERVER_HPP
