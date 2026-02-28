/**
 * @file server.cpp
 * @brief PostgreSQL wire protocol implementation
 *
 * @defgroup network Network Server
 * @{
 */

#include "network/server.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "catalog/catalog.hpp"
#include "executor/query_executor.hpp"
#include "executor/types.hpp"
#include "parser/lexer.hpp"
#include "parser/parser.hpp"
#include "storage/buffer_pool_manager.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/transaction_manager.hpp"

namespace cloudsql::network {

namespace {

constexpr size_t HEADER_SIZE = 4;
constexpr int SELECT_TIMEOUT_SEC = 1;
constexpr uint32_t PG_SSL_CODE = 80877103;
constexpr uint32_t PG_STARTUP_CODE = 196608;

/**
 * @brief Simple utility to receive exactly N bytes
 */
ssize_t recv_all(int fd, char* buf, size_t count) {
    size_t total = 0;
    while (total < count) {
        const ssize_t n = recv(fd, buf + total, static_cast<size_t>(count - total), 0);
        if (n <= 0) {
            return n;
        }
        total += static_cast<size_t>(n);
    }
    return static_cast<ssize_t>(total);
}

/**
 * @brief Reader for PostgreSQL protocol types
 */
class ProtocolReader {
   public:
    static uint32_t read_int32(const char* data) {
        uint32_t val = 0;
        std::memcpy(&val, data, 4);
        return ntohl(val);
    }
};

/**
 * @brief Writer for PostgreSQL protocol types
 */
class ProtocolWriter {
   public:
    static void write_int32(char* data, uint32_t val) {
        const uint32_t nval = htonl(val);
        std::memcpy(data, &nval, 4);
    }

    static void write_int16(char* data, uint16_t val) {
        const uint16_t nval = htons(val);
        std::memcpy(data, &nval, 2);
    }
};

}  // namespace

Server::Server(uint16_t port, Catalog& catalog, storage::BufferPoolManager& bpm)
    : port_(port),
      catalog_(catalog),
      bpm_(bpm),
      transaction_manager_(lock_manager_, catalog, bpm, bpm.get_log_manager()) {}

std::unique_ptr<Server> Server::create(uint16_t port, Catalog& catalog,
                                       storage::BufferPoolManager& bpm) {
    return std::make_unique<Server>(port, catalog, bpm);
}

bool Server::start() {
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return false;
    }

    int opt = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        static_cast<void>(close(fd));
        return false;
    }

    struct sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);

    if (bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        static_cast<void>(close(fd));
        return false;
    }

    if (listen(fd, BACKLOG) < 0) {
        static_cast<void>(close(fd));
        return false;
    }

    {
        const std::scoped_lock<std::mutex> lock(state_mutex_);
        listen_fd_ = fd;
        status_ = ServerStatus::Running;
        running_ = true;
    }

    const std::scoped_lock<std::mutex> lock(thread_mutex_);
    accept_thread_ = std::thread(&Server::accept_connections, this);
    return true;
}

bool Server::stop() {
    int fd_to_close = -1;
    {
        const std::scoped_lock<std::mutex> lock(state_mutex_);
        if (!running_) {
            return true;
        }
        running_ = false;
        fd_to_close = listen_fd_;
        listen_fd_ = -1;
    }

    std::thread t;
    {
        const std::scoped_lock<std::mutex> lock(thread_mutex_);
        if (accept_thread_.joinable()) {
            t = std::move(accept_thread_);
        }
    }
    if (t.joinable()) {
        t.join();
    }

    std::vector<std::thread> workers;
    std::vector<int> fds;
    {
        const std::scoped_lock<std::mutex> lock(thread_mutex_);
        fds.swap(client_fds_);
        workers.swap(worker_threads_);
    }
    for (auto& worker : workers) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    // Now close the fds
    for (const int fd : fds) {
        static_cast<void>(close(fd));
    }

    if (fd_to_close >= 0) {
        static_cast<void>(close(fd_to_close));
    }

    {
        const std::scoped_lock<std::mutex> lock(state_mutex_);
        status_ = ServerStatus::Stopped;
    }

    return true;
}

void Server::wait() {
    std::thread t;
    {
        const std::scoped_lock<std::mutex> lock(thread_mutex_);
        if (accept_thread_.joinable()) {
            t = std::move(accept_thread_);
        }
    }
    if (t.joinable()) {
        t.join();
    }
}

bool Server::is_running() const {
    const std::scoped_lock<std::mutex> lock(state_mutex_);
    return running_;
}

ServerStatus Server::get_status() const {
    const std::scoped_lock<std::mutex> lock(state_mutex_);
    return status_;
}

int Server::get_listen_fd() const {
    const std::scoped_lock<std::mutex> lock(state_mutex_);
    return listen_fd_;
}

std::string Server::get_status_string() const {
    const std::scoped_lock<std::mutex> lock(state_mutex_);
    switch (status_) {
        case ServerStatus::Starting:
            return "Starting";
        case ServerStatus::Running:
            return "Running";
        case ServerStatus::Stopping:
            return "Stopping";
        case ServerStatus::Stopped:
            return "Stopped";
        case ServerStatus::Error:
            return "Error";
    }
    return "Unknown";
}

void Server::accept_connections() {
    while (true) {
        int fd = -1;
        {
            const std::scoped_lock<std::mutex> lock(state_mutex_);
            if (!running_) {
                break;
            }
            fd = listen_fd_;
        }

        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(fd, &read_fds);
        struct timeval timeout {
            SELECT_TIMEOUT_SEC, 0
        };

        const int res = select(fd + 1, &read_fds, nullptr, nullptr, &timeout);
        if (res <= 0) {
            continue;
        }

        struct sockaddr_in client_addr {};
        socklen_t client_len = sizeof(client_addr);
        const int client_fd =
            accept(fd, reinterpret_cast<struct sockaddr*>(&client_addr), &client_len);

        if (client_fd >= 0) {
            const std::scoped_lock<std::mutex> lock(thread_mutex_);
            client_fds_.push_back(client_fd);
            worker_threads_.emplace_back(&Server::handle_connection, this, client_fd);
        }
    }
}

void Server::handle_connection(int client_fd) {
    constexpr size_t PKT_BUF_SIZE = 8192;
    std::array<char, PKT_BUF_SIZE> buffer{};

    // 1. Handshake
    ssize_t n = recv_all(client_fd, buffer.data(), HEADER_SIZE);
    if (n < static_cast<ssize_t>(HEADER_SIZE)) {
        static_cast<void>(close(client_fd));
        return;
    }

    uint32_t len = ProtocolReader::read_int32(buffer.data());
    if (len > buffer.size() || len < HEADER_SIZE) {
        static_cast<void>(close(client_fd));
        return;
    }

    n = recv_all(client_fd, buffer.data() + HEADER_SIZE, len - HEADER_SIZE);
    if (n < static_cast<ssize_t>(len - HEADER_SIZE)) {
        static_cast<void>(close(client_fd));
        return;
    }

    uint32_t code = ProtocolReader::read_int32(buffer.data() + HEADER_SIZE);
    if (code == PG_SSL_CODE) {  // SSL Request
        const char n_response = 'N';
        static_cast<void>(send(client_fd, &n_response, 1, 0));

        // Expect startup packet next
        n = recv_all(client_fd, buffer.data(), HEADER_SIZE);
        if (n < static_cast<ssize_t>(HEADER_SIZE)) {
            static_cast<void>(close(client_fd));
            return;
        }
        len = ProtocolReader::read_int32(buffer.data());
        static_cast<void>(recv_all(client_fd, buffer.data() + HEADER_SIZE, len - HEADER_SIZE));
        code = ProtocolReader::read_int32(buffer.data() + HEADER_SIZE);
    }

    if (code != PG_STARTUP_CODE) {
        static_cast<void>(close(client_fd));
        return;
    }

    // Auth OK
    const std::array<char, 9> auth_ok = {'R', 0, 0, 0, 8, 0, 0, 0, 0};
    static_cast<void>(send(client_fd, auth_ok.data(), auth_ok.size(), 0));

    // Ready for Query
    const std::array<char, 6> ready = {'Z', 0, 0, 0, 5, 'I'};
    static_cast<void>(send(client_fd, ready.data(), ready.size(), 0));

    // 2. Query Loop
    while (true) {
        char type = 0;
        n = recv(client_fd, &type, 1, 0);
        if (n <= 0) {
            break;
        }

        n = recv_all(client_fd, buffer.data(), 4);
        if (n < 4) {
            break;
        }
        len = ProtocolReader::read_int32(buffer.data());

        if (type == 'Q') {
            std::vector<char> sql_buf(len - 4);
            static_cast<void>(recv_all(client_fd, sql_buf.data(), len - 4));
            const std::string sql(sql_buf.data());

            try {
                auto lexer = std::make_unique<parser::Lexer>(sql);
                parser::Parser parser(std::move(lexer));
                auto stmt = parser.parse_statement();

                if (stmt) {
                    executor::QueryExecutor exec(catalog_, bpm_, lock_manager_,
                                                 transaction_manager_);
                    const auto res = exec.execute(*stmt);

                    if (res.success()) {
                        // Row Description (T)
                        if (!res.rows().empty() && res.schema().column_count() > 0) {
                            const auto& schema = res.schema();
                            const auto num_cols = static_cast<uint32_t>(schema.column_count());

                            // Calculate T packet length
                            uint32_t t_len = 4 + 2;  // len + num_cols
                            for (uint32_t i = 0; i < num_cols; ++i) {
                                t_len += static_cast<uint32_t>(schema.get_column(i).name().size()) +
                                         1 + 4 + 2 + 4 + 2 + 4 + 2;
                            }

                            const char t_type = 'T';
                            const uint32_t net_t_len = htonl(t_len);
                            const uint16_t net_num_cols = htons(static_cast<uint16_t>(num_cols));

                            static_cast<void>(send(client_fd, &t_type, 1, 0));
                            static_cast<void>(send(client_fd, &net_t_len, 4, 0));
                            static_cast<void>(send(client_fd, &net_num_cols, 2, 0));

                            for (uint32_t i = 0; i < num_cols; ++i) {
                                const auto& col = schema.get_column(i);
                                static_cast<void>(
                                    send(client_fd, col.name().c_str(), col.name().size() + 1, 0));
                                const uint32_t table_oid = 0;
                                const uint16_t col_attr = 0;
                                const uint32_t type_oid = htonl(23);  // 23 is int4, simplified
                                const uint16_t type_len = htons(4);
                                const uint32_t type_mod = htonl(0xFFFFFFFF);
                                const uint16_t format = 0;  // Text format

                                static_cast<void>(send(client_fd, &table_oid, 4, 0));
                                static_cast<void>(send(client_fd, &col_attr, 2, 0));
                                static_cast<void>(send(client_fd, &type_oid, 4, 0));
                                static_cast<void>(send(client_fd, &type_len, 2, 0));
                                static_cast<void>(send(client_fd, &type_mod, 4, 0));
                                static_cast<void>(send(client_fd, &format, 2, 0));
                            }

                            // Data Rows (D)
                            for (const auto& row : res.rows()) {
                                const char d_type = 'D';
                                uint32_t d_len = 4 + 2;  // len + num_cols
                                std::vector<std::string> str_vals;
                                for (uint32_t i = 0; i < num_cols; ++i) {
                                    const std::string s_val = row.get(i).to_string();
                                    str_vals.push_back(s_val);
                                    d_len +=
                                        4 + static_cast<uint32_t>(s_val.size());  // len + value
                                }

                                const uint32_t net_d_len = htonl(d_len);
                                static_cast<void>(send(client_fd, &d_type, 1, 0));
                                static_cast<void>(send(client_fd, &net_d_len, 4, 0));
                                static_cast<void>(send(client_fd, &net_num_cols, 2, 0));

                                for (const auto& s_val : str_vals) {
                                    const uint32_t val_len =
                                        htonl(static_cast<uint32_t>(s_val.size()));
                                    static_cast<void>(send(client_fd, &val_len, 4, 0));
                                    static_cast<void>(
                                        send(client_fd, s_val.c_str(), s_val.size(), 0));
                                }
                            }
                        }

                        // Command Complete (C)
                        const std::string tag = "SELECT " + std::to_string(res.row_count());
                        const uint32_t tag_len = htonl(static_cast<uint32_t>(tag.size() + 4 + 1));
                        const char c_type = 'C';
                        static_cast<void>(send(client_fd, &c_type, 1, 0));
                        static_cast<void>(send(client_fd, &tag_len, 4, 0));
                        static_cast<void>(send(client_fd, tag.c_str(), tag.size() + 1, 0));
                    } else {
                        // Error Response (E)
                        const std::string& err = res.error();
                        const uint32_t e_len = htonl(static_cast<uint32_t>(err.size() + 4 + 1));
                        const char e_type = 'E';
                        static_cast<void>(send(client_fd, &e_type, 1, 0));
                        static_cast<void>(send(client_fd, &e_len, 4, 0));
                        static_cast<void>(send(client_fd, err.c_str(), err.size() + 1, 0));
                    }
                }
            } catch (const std::exception& e) {
                const std::string err = e.what();
                const uint32_t e_len = htonl(static_cast<uint32_t>(err.size() + 4 + 1));
                const char e_type = 'E';
                static_cast<void>(send(client_fd, &e_type, 1, 0));
                static_cast<void>(send(client_fd, &e_len, 4, 0));
                static_cast<void>(send(client_fd, err.c_str(), err.size() + 1, 0));
            }
        } else if (type == 'X') {
            break;
        }

        /* Ready for Query */
        static_cast<void>(send(client_fd, ready.data(), ready.size(), 0));
    }

    {
        const std::scoped_lock<std::mutex> lock(thread_mutex_);
        auto it = std::find(client_fds_.begin(), client_fds_.end(), client_fd);
        if (it != client_fds_.end()) {
            static_cast<void>(client_fds_.erase(it));
        }
    }
    static_cast<void>(close(client_fd));
}

}  // namespace cloudsql::network

/** @} */
