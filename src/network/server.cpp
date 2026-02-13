/**
 * @file server.cpp
 * @brief PostgreSQL wire protocol implementation
 *
 * @defgroup network Network Server
 * @{
 */

#include "network/server.hpp"
#include <arpa/inet.h>
#include <vector>

namespace cloudsql {
namespace network {

/**
 * @brief Helper for parsing PostgreSQL binary protocol
 */
class ProtocolReader {
public:
    static uint32_t read_int32(const char* buffer) {
        uint32_t val;
        std::memcpy(&val, buffer, 4);
        return ntohl(val);
    }

    static std::string read_string(const char* buffer, size_t& offset, size_t limit) {
        std::string s;
        while (offset < limit && buffer[offset] != '\0') {
            s += buffer[offset++];
        }
        if (offset < limit) offset++; /* Skip null terminator */
        return s;
    }
};

/**
 * @brief Helper for building PostgreSQL binary responses
 */
class ProtocolWriter {
public:
    static void append_int32(std::vector<char>& buf, uint32_t val) {
        uint32_t nval = htonl(val);
        const char* p = reinterpret_cast<const char*>(&nval);
        buf.insert(buf.end(), p, p + 4);
    }

    static void append_string(std::vector<char>& buf, const std::string& s) {
        buf.insert(buf.end(), s.begin(), s.end());
        buf.push_back('\0');
    }
};

/**
 * @brief Create a new server instance
 */
std::unique_ptr<Server> Server::create(uint16_t port) {
    return std::make_unique<Server>(port);
}

/**
 * @brief Start the server
 */
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

/**
 * @brief Stop the server
 */
bool Server::stop() {
    if (!running_.load()) {
        return true;
    }

    status_ = ServerStatus::Stopping;
    running_ = false;

    /* Signal shutdown */
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

/**
 * @brief Accept incoming connections
 */
void Server::accept_connections() {
    while (running_.load()) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);

        int client_fd = accept(listen_fd_, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) {
            if (!running_.load()) break;
            continue;
        }

        stats_.connections_accepted.fetch_add(1);
        stats_.connections_active.fetch_add(1);

        /* Handle connection in a new thread */
        std::thread([this, client_fd]() {
            handle_connection(client_fd);
            stats_.connections_active.fetch_sub(1);
        }).detach();
    }
}

/**
 * @brief Handle a client connection using PostgreSQL protocol
 */
void Server::handle_connection(int client_fd) {
    char buffer[8192];
    
    /* 1. Read Length (Initial Startup/SSL) */
    ssize_t n = recv(client_fd, buffer, 4, 0);
    if (n < 4) { close(client_fd); return; }
    
    uint32_t len = ProtocolReader::read_int32(buffer);
    if (len > 8192) { close(client_fd); return; }

    /* 2. Read Rest of Startup/SSL Packet */
    n = recv(client_fd, buffer + 4, len - 4, 0);
    if (n < (ssize_t)(len - 4)) { close(client_fd); return; }

    uint32_t protocol = ProtocolReader::read_int32(buffer + 4);
    
    /* Check for SSL Request (80877103) */
    if (protocol == 80877103) {
        /* We don't support SSL, send 'N' */
        char ssl_deny = 'N';
        send(client_fd, &ssl_deny, 1, 0);
        
        /* Read actual StartupMessage */
        n = recv(client_fd, buffer, 4, 0);
        if (n < 4) { close(client_fd); return; }
        len = ProtocolReader::read_int32(buffer);
        n = recv(client_fd, buffer + 4, len - 4, 0);
        if (n < (ssize_t)(len - 4)) { close(client_fd); return; }
        protocol = ProtocolReader::read_int32(buffer + 4);
    }

    /* Verify Protocol Version (3.0 is 196608) */
    if (protocol != 196608) {
        close(client_fd);
        return;
    }

    /* 3. Send AuthenticationOK ('R') */
    std::vector<char> auth_ok = {'R'};
    ProtocolWriter::append_int32(auth_ok, 8); // Length
    ProtocolWriter::append_int32(auth_ok, 0); // Success
    send(client_fd, auth_ok.data(), auth_ok.size(), 0);

    /* 4. Send ReadyForQuery ('Z') */
    std::vector<char> ready = {'Z'};
    ProtocolWriter::append_int32(ready, 5);
    ready.push_back('I'); // Idle
    send(client_fd, ready.data(), ready.size(), 0);

    /* 5. Main Message Loop */
    while (running_.load()) {
        char type;
        n = recv(client_fd, &type, 1, 0);
        if (n <= 0) break;

        n = recv(client_fd, buffer, 4, 0);
        if (n < 4) break;
        len = ProtocolReader::read_int32(buffer);
        
        std::vector<char> body(len - 4);
        if (len > 4) {
            n = recv(client_fd, body.data(), len - 4, 0);
            if (n < (ssize_t)(len - 4)) break;
        }

        if (type == 'Q') { /* Simple Query */
            std::string sql(body.data());
            stats_.queries_executed.fetch_add(1);
            
            /* TODO: Invoke QueryExecutor and send RowDescription/DataRow/CommandComplete */
            
            /* For now, send empty response or Error */
            std::vector<char> complete = {'C'};
            std::string msg = "SELECT 0";
            ProtocolWriter::append_int32(complete, 4 + msg.size() + 1);
            ProtocolWriter::append_string(complete, msg);
            send(client_fd, complete.data(), complete.size(), 0);
        } else if (type == 'X') { /* Terminate */
            break;
        }

        /* Ready for Query */
        send(client_fd, ready.data(), ready.size(), 0);
    }

    close(client_fd);
}

} // namespace network
} // namespace cloudsql

/** @} */ /* network */
