#include "network/server.hpp"

namespace cloudsql {
namespace network {

std::unique_ptr<Server> Server::create(uint16_t port) {
    return std::make_unique<Server>(port);
}

bool Server::start() {
    if (running_.load()) {
        return false;
    }

    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        return false;
    }

    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);

    if (bind(listen_fd_, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(listen_fd_);
        return false;
    }

    if (listen(listen_fd_, 10) < 0) {
        close(listen_fd_);
        return false;
    }

    status_ = ServerStatus::Running;
    running_ = true;
    accept_thread_ = std::thread(&Server::accept_connections, this);
    return true;
}

bool Server::stop() {
    if (!running_.load()) {
        return true;
    }

    status_ = ServerStatus::Stopping;
    running_ = false;

    // Connect to self to unblock accept() if needed, or close socket
    // Simple close(listen_fd_) might not wake up accept() on all platforms immediately
    // or might cause undefined behavior if another thread is using it.
    // For this simple implementation, closing and joining is the standard approach
    // although slightly race-prone without proper shutdown signaling.
    
    // Using shutdown to signal
    shutdown(listen_fd_, SHUT_RDWR);
    close(listen_fd_);
    listen_fd_ = -1;

    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }

    status_ = ServerStatus::Stopped;
    return true;
}

void Server::wait() {
    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }
}

std::string Server::get_status_string() const {
    switch (status_) {
        case ServerStatus::Stopped: return "Stopped";
        case ServerStatus::Starting: return "Starting";
        case ServerStatus::Running: return "Running";
        case ServerStatus::Stopping: return "Stopping";
        case ServerStatus::Error: return "Error";
        default: return "Unknown";
    }
}

void Server::accept_connections() {
    while (running_.load()) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);

        int client_fd = accept(listen_fd_, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) {
            // Check if we are still running, if not, it was likely closed by stop()
            if (!running_.load()) break;
            continue;
        }

        stats_.connections_accepted.fetch_add(1);
        stats_.connections_active.fetch_add(1);

        std::thread([this, client_fd]() {
            handle_connection(client_fd);
            stats_.connections_active.fetch_sub(1);
        }).detach();
    }
}

void Server::handle_connection(int client_fd) {
    char buffer[4096];
    std::string query;

    while (running_.load()) {
        ssize_t bytes_read = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
        if (bytes_read <= 0) {
            break;
        }

        buffer[bytes_read] = '\0';
        stats_.bytes_received.fetch_add(bytes_read);
        query += buffer;

        if (query.find(';') != std::string::npos) {
            stats_.queries_executed.fetch_add(1);
            query.clear();
        }
    }

    close(client_fd);
}

} // namespace network
} // namespace cloudsql
