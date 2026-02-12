/**
 * @file server.hpp
 * @brief C++ Network Server for PostgreSQL wire protocol
 */

#ifndef SQL_ENGINE_NETWORK_SERVER_HPP
#define SQL_ENGINE_NETWORK_SERVER_HPP

#include <cstdint>
#include <cstring>
#include <string>
#include <functional>
#include <atomic>
#include <thread>
#include <iostream>
#include <memory>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

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
 * @brief Server callbacks using std::function
 */
struct ServerCallbacks {
    std::function<int(class Server* server, int client_fd)> on_connect;
    std::function<int(class Server* server, int client_fd, const std::string& sql)> on_query;
    std::function<int(class Server* server, int client_fd)> on_disconnect;
};

/**
 * @brief Server status enumeration
 */
enum class ServerStatus {
    Stopped,
    Starting,
    Running,
    Stopping,
    Error
};

/**
 * @brief Network Server class
 */
class Server {
public:
    /**
     * @brief Constructor
     */
    explicit Server(uint16_t port) 
        : port_(port), listen_fd_(-1), status_(ServerStatus::Stopped) {}

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
    static std::unique_ptr<Server> create(uint16_t port);

    /**
     * @brief Set server callbacks
     */
    void set_callbacks(const ServerCallbacks& callbacks) {
        callbacks_ = callbacks;
    }

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

    /**
     * @brief Get server statistics
     */
    const ServerStats& get_stats() const {
        return stats_;
    }

    /**
     * @brief Get server status
     */
    ServerStatus get_status() const {
        return status_;
    }

    /**
     * @brief Get server port
     */
    uint16_t get_port() const {
        return port_;
    }

    /**
     * @brief Check if server is running
     */
    bool is_running() const {
        return running_.load();
    }

    /**
     * @brief Get status string
     */
    std::string get_status_string() const;

private:
    void accept_connections();

    void handle_connection(int client_fd);

    uint16_t port_;
    int listen_fd_;
    std::atomic<bool> running_{false};
    std::atomic<ServerStatus> status_;
    ServerCallbacks callbacks_;
    ServerStats stats_;
    std::thread accept_thread_;
};

} // namespace network
} // namespace cloudsql

#endif // SQL_ENGINE_NETWORK_SERVER_HPP
