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
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "catalog/catalog.hpp"
#include "common/cluster_manager.hpp"
#include "common/config.hpp"
#include "executor/query_executor.hpp"
#include "storage/buffer_pool_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction_manager.hpp"

namespace cloudsql::network {

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
enum class ServerStatus : uint8_t { Stopped, Starting, Running, Stopping, Error };

/**
 * @brief Network Server class
 */
class Server {
   public:
    static constexpr int BACKLOG = 10;

    /**
     * @brief Constructor
     */
    Server(uint16_t port, Catalog& catalog, storage::BufferPoolManager& bpm,
           const config::Config& config, cluster::ClusterManager* cm);

    /**
     * @brief Destructor
     */
    ~Server() noexcept {
        try {
            static_cast<void>(stop());
        } catch (...) {
            static_cast<void>(0);  // Destructors should not throw
        }
        if (listen_fd_ >= 0) {
            static_cast<void>(close(listen_fd_));
        }
    }

    // Disable copy/move for server
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;
    Server(Server&&) = delete;
    Server& operator=(Server&&) = delete;

    /**
     * @brief Create a new server instance
     */
    [[nodiscard]] static std::unique_ptr<Server> create(uint16_t port, Catalog& catalog,
                                                        storage::BufferPoolManager& bpm,
                                                        const config::Config& config,
                                                        cluster::ClusterManager* cm);

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

    [[nodiscard]] const ServerStats& get_stats() const { return stats_; }
    [[nodiscard]] ServerStatus get_status() const;
    [[nodiscard]] uint16_t get_port() const { return port_; }
    [[nodiscard]] bool is_running() const;
    [[nodiscard]] int get_listen_fd() const;
    [[nodiscard]] std::string get_status_string() const;

   private:
    void accept_connections();
    void handle_connection(int client_fd);

    uint16_t port_;
    int listen_fd_ = -1;
    bool running_{false};
    ServerStatus status_ = ServerStatus::Stopped;

    Catalog& catalog_;
    storage::BufferPoolManager& bpm_;
    const config::Config& config_;
    cluster::ClusterManager* cluster_manager_;

    transaction::LockManager lock_manager_;
    transaction::TransactionManager transaction_manager_;

    ServerStats stats_;
    std::thread accept_thread_;
    std::vector<std::thread> worker_threads_;
    std::vector<int> client_fds_;
    std::mutex thread_mutex_;
    mutable std::mutex state_mutex_;
};

}  // namespace cloudsql::network

#endif  // SQL_ENGINE_NETWORK_SERVER_HPP
