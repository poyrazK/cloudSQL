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

#include "parser/lexer.hpp"
#include "parser/parser.hpp"

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
    static void append_int16(std::vector<char>& buf, uint16_t val) {
        uint16_t nval = htons(val);
        const char* p = reinterpret_cast<const char*>(&nval);
        buf.insert(buf.end(), p, p + 2);
    }

    static void append_int32(std::vector<char>& buf, uint32_t val) {
        uint32_t nval = htonl(val);
        const char* p = reinterpret_cast<const char*>(&nval);
        buf.insert(buf.end(), p, p + 4);
    }

    static void append_string(std::vector<char>& buf, const std::string& s) {
        buf.insert(buf.end(), s.begin(), s.end());
        buf.push_back('\0');
    }

    static void finish_message(std::vector<char>& buf) {
        uint32_t len = htonl(static_cast<uint32_t>(buf.size() - 1));
        std::memcpy(&buf[1], &len, 4);
    }
};

Server::Server(uint16_t port, Catalog& catalog, storage::StorageManager& storage_manager)
    : port_(port),
      listen_fd_(-1),
      status_(ServerStatus::Stopped),
      catalog_(catalog),
      storage_manager_(storage_manager),
      lock_manager_(),
      transaction_manager_(lock_manager_, catalog, storage_manager) {}

std::unique_ptr<Server> Server::create(uint16_t port, Catalog& catalog,
                                       storage::StorageManager& storage_manager) {
    return std::make_unique<Server>(port, catalog, storage_manager);
}

/**
 * @brief Start the server
 */
bool Server::start() {
    if (running_.load()) return false;

    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) return false;

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
    if (!running_.load()) return true;

    status_ = ServerStatus::Stopping;
    running_ = false;

    shutdown(listen_fd_, SHUT_RDWR);
    close(listen_fd_);
    listen_fd_ = -1;

    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }

    /* Join all connection worker threads */
    std::lock_guard<std::mutex> lock(thread_mutex_);
    for (auto& t : worker_threads_) {
        if (t.joinable()) {
            t.join();
        }
    }
    worker_threads_.clear();

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
        case ServerStatus::Stopped:
            return "Stopped";
        case ServerStatus::Starting:
            return "Starting";
        case ServerStatus::Running:
            return "Running";
        case ServerStatus::Stopping:
            return "Stopping";
        case ServerStatus::Error:
            return "Error";
        default:
            return "Unknown";
    }
}

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

        std::lock_guard<std::mutex> lock(thread_mutex_);
        worker_threads_.emplace_back([this, client_fd]() {
            handle_connection(client_fd);
            stats_.connections_active.fetch_sub(1);
        });
    }
}

/**
 * @brief Handle a client connection using PostgreSQL protocol
 */
void Server::handle_connection(int client_fd) {
    char buffer[8192];
    executor::QueryExecutor client_executor(catalog_, storage_manager_, lock_manager_,
                                            transaction_manager_);

    /* 1. Read Length (Initial Startup/SSL) */
    ssize_t n = recv(client_fd, buffer, 4, 0);
    if (n < 4) {
        close(client_fd);
        return;
    }

    uint32_t len = ProtocolReader::read_int32(buffer);
    if (len > 8192) {
        close(client_fd);
        return;
    }

    /* 2. Read Rest of Startup/SSL Packet */
    n = recv(client_fd, buffer + 4, len - 4, 0);
    if (n < (ssize_t)(len - 4)) {
        close(client_fd);
        return;
    }

    uint32_t protocol = ProtocolReader::read_int32(buffer + 4);

    /* Check for SSL Request */
    if (protocol == 80877103) {
        char ssl_deny = 'N';
        send(client_fd, &ssl_deny, 1, 0);

        n = recv(client_fd, buffer, 4, 0);
        if (n < 4) {
            close(client_fd);
            return;
        }
        len = ProtocolReader::read_int32(buffer);
        n = recv(client_fd, buffer + 4, len - 4, 0);
        if (n < (ssize_t)(len - 4)) {
            close(client_fd);
            return;
        }
        protocol = ProtocolReader::read_int32(buffer + 4);
    }

    if (protocol != 196608) {
        close(client_fd);
        return;
    }

    /* Send AuthenticationOK ('R') */
    std::vector<char> auth_ok = {'R', 0, 0, 0, 8, 0, 0, 0, 0};
    send(client_fd, auth_ok.data(), auth_ok.size(), 0);

    /* Send ReadyForQuery ('Z') */
    std::vector<char> ready = {'Z', 0, 0, 0, 5, 'I'};
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

            try {
                auto lexer = std::make_unique<parser::Lexer>(sql);
                parser::Parser parser(std::move(lexer));
                auto stmt = parser.parse_statement();

                if (stmt) {
                    auto result = client_executor.execute(*stmt);

                    if (result.success()) {
                        /* 1. Send RowDescription ('T') for SELECT */
                        if (stmt->type() == parser::StmtType::Select) {
                            std::vector<char> desc = {'T'};
                            ProtocolWriter::append_int32(desc, 0);  // Length placeholder
                            ProtocolWriter::append_int16(desc, result.schema().column_count());

                            for (const auto& col : result.schema().columns()) {
                                ProtocolWriter::append_string(desc, col.name());
                                ProtocolWriter::append_int32(desc, 0);   // Table OID
                                ProtocolWriter::append_int16(desc, 0);   // Attr index
                                ProtocolWriter::append_int32(desc, 25);  // Type OID (TEXT=25)
                                ProtocolWriter::append_int16(desc, -1);  // Type size
                                ProtocolWriter::append_int32(desc, -1);  // Type modifier
                                ProtocolWriter::append_int16(desc, 0);   // Format (Text)
                            }
                            ProtocolWriter::finish_message(desc);
                            send(client_fd, desc.data(), desc.size(), 0);

                            /* 2. Send DataRows ('D') */
                            for (const auto& row : result.rows()) {
                                std::vector<char> data = {'D'};
                                ProtocolWriter::append_int32(data, 0);  // Length
                                ProtocolWriter::append_int16(data, row.size());

                                for (const auto& val : row.values()) {
                                    std::string s = val.to_string();
                                    ProtocolWriter::append_int32(data, s.size());
                                    data.insert(data.end(), s.begin(), s.end());
                                }
                                ProtocolWriter::finish_message(data);
                                send(client_fd, data.data(), data.size(), 0);
                            }
                        }

                        /* 3. Send CommandComplete ('C') */
                        std::vector<char> complete = {'C'};
                        std::string msg = (stmt->type() == parser::StmtType::Select)
                                              ? "SELECT " + std::to_string(result.row_count())
                                              : "OK";
                        ProtocolWriter::append_int32(complete, 4 + msg.size() + 1);
                        ProtocolWriter::append_string(complete, msg);
                        send(client_fd, complete.data(), complete.size(), 0);
                    } else {
                        /* TODO: Send ErrorResponse ('E') */
                    }
                }
            } catch (...) { /* Handle parsing/exec errors */
            }
        } else if (type == 'X') {
            break;
        }

        /* Ready for Query */
        send(client_fd, ready.data(), ready.size(), 0);
    }

    close(client_fd);
}

}  // namespace network
}  // namespace cloudsql

/** @} */ /* network */
