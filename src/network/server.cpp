/**
 * @file server.cpp
 * @brief PostgreSQL wire protocol implementation
 *
 * @defgroup network Network Server
 * @{
 */

#include "network/server.hpp"

#include <arpa/inet.h>   // IWYU pragma: keep
#include <netinet/in.h>  // IWYU pragma: keep
#include <sys/socket.h>  // IWYU pragma: keep
#include <sys/types.h>   // IWYU pragma: keep
#include <sys/select.h>  // IWYU pragma: keep
#include <sys/time.h>    // IWYU pragma: keep
#include <unistd.h>      // IWYU pragma: keep

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "catalog/catalog.hpp"
#include "executor/query_executor.hpp"
#include "parser/lexer.hpp"
#include "parser/parser.hpp"
#include "parser/statement.hpp"
#include "storage/storage_manager.hpp"

namespace cloudsql::network {

namespace {
constexpr int PROTOCOL_VERSION_3 = 196608;
constexpr int SSL_REQUEST_CODE = 80877103;
constexpr int AUTH_OK_MSG_SIZE = 8;
constexpr int READY_FOR_QUERY_MSG_SIZE = 5;
constexpr int TEXT_FORMAT_CODE = 0;
constexpr int TEXT_TYPE_OID = 25;
constexpr size_t MAX_PACKET_SIZE = 8192;
constexpr int SELECT_TIMEOUT_USEC = 100000;
constexpr int ERROR_MSG_LEN = 9;
constexpr size_t MIN_MSG_SIZE = 5;
constexpr size_t HEADER_SIZE = 4;
}  // anonymous namespace

/**
 * @brief Helper for parsing PostgreSQL binary protocol
 */
class ProtocolReader {
   public:
    [[nodiscard]] static uint32_t read_int32(const char* buffer) {
        uint32_t val = 0;
        std::memcpy(&val, buffer, sizeof(uint32_t));
        return ntohl(val);
    }

    [[nodiscard]] static std::string read_string(const char* buffer, size_t& offset, size_t limit) {
        std::string s;
        const std::string_view view(buffer, limit);
        while (offset < limit) {
            const char c = view[offset];
            if (c == '\0') {
                break;
            }
            s += c;
            offset++;
        }
        if (offset < limit) {
            offset++; /* Skip null terminator */
        }
        return s;
    }
};

/**
 * @brief Helper for building PostgreSQL binary responses
 */
class ProtocolWriter {
   public:
    static void append_int16(std::vector<char>& buf, uint16_t val) {
        const uint16_t nval = htons(val);
        std::array<char, sizeof(uint16_t)> p{};
        std::memcpy(p.data(), &nval, sizeof(uint16_t));
        buf.insert(buf.end(), p.begin(), p.end());
    }

    static void append_int32(std::vector<char>& buf, uint32_t val) {
        const uint32_t nval = htonl(val);
        std::array<char, sizeof(uint32_t)> p{};
        std::memcpy(p.data(), &nval, sizeof(uint32_t));
        buf.insert(buf.end(), p.begin(), p.end());
    }

    static void append_string(std::vector<char>& buf, const std::string& s) {
        buf.insert(buf.end(), s.begin(), s.end());
        buf.push_back('\0');
    }

    static void finish_message(std::vector<char>& buf) {
        if (buf.size() < MIN_MSG_SIZE) {
            return;
        }
        const uint32_t len = htonl(static_cast<uint32_t>(buf.size() - 1));
        std::memcpy(&buf[1], &len, sizeof(uint32_t));
    }
};

Server::Server(uint16_t port, Catalog& catalog, storage::StorageManager& storage_manager)
    : port_(port),
      catalog_(catalog),
      storage_manager_(storage_manager),
      transaction_manager_(lock_manager_, catalog, storage_manager) {}

std::unique_ptr<Server> Server::create(uint16_t port, Catalog& catalog,
                                       storage::StorageManager& storage_manager) {
    return std::make_unique<Server>(port, catalog, storage_manager);
}

/**
 * @brief Start the server
 */
bool Server::start() {
    {
        const std::scoped_lock<std::mutex> lock(state_mutex_);
        if (running_) {
            return false;
        }
    }

    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return false;
    }

    const int opt = 1;
    static_cast<void>(setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)));

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

/**
 * @brief Stop the server
 */
bool Server::stop() {
    int fd_to_close = -1;
    {
        const std::scoped_lock<std::mutex> lock(state_mutex_);
        if (!running_) {
            return true;
        }
        status_ = ServerStatus::Stopping;
        running_ = false;
        if (listen_fd_ >= 0) {
            fd_to_close = listen_fd_;
            listen_fd_ = -1;
        }
    }

    // 1. Signal all active client connections to shut down
    std::vector<int> fds;
    {
        const std::scoped_lock<std::mutex> lock(thread_mutex_);
        fds = client_fds_;
    }
    
    for (const int fd : fds) {
        static_cast<void>(shutdown(fd, SHUT_RDWR));
    }

    // 2. Join the accept thread
    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }

    // 3. Join all connection worker threads
    std::vector<std::thread> workers;
    {
        const std::scoped_lock<std::mutex> lock(thread_mutex_);
        workers.swap(worker_threads_);
    }
    for (auto& t : workers) {
        if (t.joinable()) {
            t.join();
        }
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
    const std::scoped_lock<std::mutex> lock(thread_mutex_);
    if (accept_thread_.joinable()) {
        accept_thread_.join();
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
    switch (get_status()) {
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
    while (is_running()) {
        const int fd = get_listen_fd();
        if (fd < 0) {
            break;
        }

        /* Use select with timeout to allow periodic is_running() check */
        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(fd, &read_fds);
        struct timeval timeout {0, SELECT_TIMEOUT_USEC};

        const int res = select(fd + 1, &read_fds, nullptr, nullptr, &timeout);
        if (res <= 0) {
            continue; /* Timeout or error */
        }

        struct sockaddr_in client_addr {};
        socklen_t client_len = sizeof(client_addr);

        const int client_fd = accept(fd, reinterpret_cast<struct sockaddr*>(&client_addr), &client_len);
        if (client_fd < 0) {
            continue;
        }

        static_cast<void>(stats_.connections_accepted.fetch_add(1));
        static_cast<void>(stats_.connections_active.fetch_add(1));

        const std::scoped_lock<std::mutex> lock(thread_mutex_);
        client_fds_.push_back(client_fd);
        worker_threads_.emplace_back([this, client_fd]() {
            handle_connection(client_fd);
            static_cast<void>(stats_.connections_active.fetch_sub(1));
            
            const std::scoped_lock<std::mutex> lock(thread_mutex_);
            auto it = std::remove(client_fds_.begin(), client_fds_.end(), client_fd);
            client_fds_.erase(it, client_fds_.end());
        });
    }
}

/**
 * @brief Handle a client connection using PostgreSQL protocol
 */
void Server::handle_connection(int client_fd) {
    std::array<char, MAX_PACKET_SIZE> buffer{};
    executor::QueryExecutor client_executor(catalog_, storage_manager_, lock_manager_,
                                            transaction_manager_);

    /* 1. Read Length (Initial Startup/SSL) */
    ssize_t n = recv(client_fd, buffer.data(), HEADER_SIZE, 0);
    if (n < static_cast<ssize_t>(HEADER_SIZE)) {
        static_cast<void>(close(client_fd));
        return;
    }

    uint32_t len = ProtocolReader::read_int32(buffer.data());
    if (len > buffer.size() || len < HEADER_SIZE) {
        static_cast<void>(close(client_fd));
        return;
    }

    /* 2. Read Rest of Startup/SSL Packet */
    n = recv(client_fd, &buffer[HEADER_SIZE], len - HEADER_SIZE, 0);
    if (n < static_cast<ssize_t>(len - HEADER_SIZE)) {
        static_cast<void>(close(client_fd));
        return;
    }

    uint32_t protocol = ProtocolReader::read_int32(&buffer[HEADER_SIZE]);

    /* Check for SSL Request */
    if (protocol == static_cast<uint32_t>(SSL_REQUEST_CODE)) {
        const char ssl_deny = 'N';
        static_cast<void>(send(client_fd, &ssl_deny, 1, 0));

        n = recv(client_fd, buffer.data(), HEADER_SIZE, 0);
        if (n < static_cast<ssize_t>(HEADER_SIZE)) {
            static_cast<void>(close(client_fd));
            return;
        }
        len = ProtocolReader::read_int32(buffer.data());
        if (len < HEADER_SIZE || len > buffer.size()) {
            static_cast<void>(close(client_fd));
            return;
        }
        n = recv(client_fd, &buffer[HEADER_SIZE], len - HEADER_SIZE, 0);
        if (n < static_cast<ssize_t>(len - HEADER_SIZE)) {
            static_cast<void>(close(client_fd));
            return;
        }
        protocol = ProtocolReader::read_int32(&buffer[HEADER_SIZE]);
    }

    if (protocol != static_cast<uint32_t>(PROTOCOL_VERSION_3)) {
        static_cast<void>(close(client_fd));
        return;
    }

    /* Send AuthenticationOK ('R') */
    const std::vector<char> auth_ok = {'R', 0, 0, 0, static_cast<char>(AUTH_OK_MSG_SIZE), 0, 0, 0, 0};
    static_cast<void>(send(client_fd, auth_ok.data(), auth_ok.size(), 0));

    /* Send ReadyForQuery ('Z') */
    const std::vector<char> ready = {'Z', 0, 0, 0, static_cast<char>(READY_FOR_QUERY_MSG_SIZE), 'I'};
    static_cast<void>(send(client_fd, ready.data(), ready.size(), 0));

    /* 5. Main Message Loop */
    while (is_running()) {
        char type = '\0';
        n = recv(client_fd, &type, 1, 0);
        if (n <= 0) {
            break;
        }

        n = recv(client_fd, buffer.data(), HEADER_SIZE, 0);
        if (n < static_cast<ssize_t>(HEADER_SIZE)) {
            break;
        }
        len = ProtocolReader::read_int32(buffer.data());
        if (len < HEADER_SIZE) {
            break;
        }

        std::vector<char> body(len - HEADER_SIZE);
        if (len > HEADER_SIZE) {
            n = recv(client_fd, body.data(), len - HEADER_SIZE, 0);
            if (n < static_cast<ssize_t>(len - HEADER_SIZE)) {
                break;
            }
        }

        if (type == 'Q') { /* Simple Query */
            const std::string sql(body.data());
            static_cast<void>(stats_.queries_executed.fetch_add(1));

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
                            ProtocolWriter::append_int16(desc, 
                                static_cast<uint16_t>(result.schema().column_count()));

                            for (const auto& col : result.schema().columns()) {
                                ProtocolWriter::append_string(desc, col.name());
                                ProtocolWriter::append_int32(desc, 0);   // Table OID
                                ProtocolWriter::append_int16(desc, 0);   // Attr index
                                ProtocolWriter::append_int32(desc, static_cast<uint32_t>(TEXT_TYPE_OID));
                                ProtocolWriter::append_int16(desc, static_cast<uint16_t>(-1));  // Type size
                                ProtocolWriter::append_int32(desc, static_cast<uint32_t>(-1));  // Type modifier
                                ProtocolWriter::append_int16(desc, static_cast<uint16_t>(TEXT_FORMAT_CODE));
                            }
                            ProtocolWriter::finish_message(desc);
                            static_cast<void>(send(client_fd, desc.data(), desc.size(), 0));

                            /* 2. Send DataRows ('D') */
                            for (const auto& row : result.rows()) {
                                std::vector<char> data = {'D'};
                                ProtocolWriter::append_int32(data, 0);  // Length
                                ProtocolWriter::append_int16(data, static_cast<uint16_t>(row.size()));

                                for (const auto& val : row.values()) {
                                    const std::string s = val.to_string();
                                    ProtocolWriter::append_int32(data, static_cast<uint32_t>(s.size()));
                                    data.insert(data.end(), s.begin(), s.end());
                                }
                                ProtocolWriter::finish_message(data);
                                static_cast<void>(send(client_fd, data.data(), data.size(), 0));
                            }
                        }

                        /* 3. Send CommandComplete ('C') */
                        std::vector<char> complete = {'C'};
                        const std::string msg = (stmt->type() == parser::StmtType::Select)
                                              ? "SELECT " + std::to_string(result.row_count())
                                              : "OK";
                        ProtocolWriter::append_int32(complete, 
                            static_cast<uint32_t>(4 + msg.size() + 1));
                        ProtocolWriter::append_string(complete, msg);
                        static_cast<void>(send(client_fd, complete.data(), complete.size(), 0));
                    } else {
                        /* Send ErrorResponse ('E') or CommandComplete with error */
                        std::vector<char> complete = {'C'};
                        ProtocolWriter::append_int32(complete, ERROR_MSG_LEN);
                        ProtocolWriter::append_string(complete, "ERROR");
                        static_cast<void>(send(client_fd, complete.data(), complete.size(), 0));
                    }
                }
            } catch (...) { /* Handle parsing/exec errors */
                std::vector<char> complete = {'C'};
                ProtocolWriter::append_int32(complete, ERROR_MSG_LEN);
                ProtocolWriter::append_string(complete, "ERROR");
                static_cast<void>(send(client_fd, complete.data(), complete.size(), 0));
            }
        } else if (type == 'X') {
            break;
        }

        /* Ready for Query */
        static_cast<void>(send(client_fd, ready.data(), ready.size(), 0));
    }

    static_cast<void>(close(client_fd));
}

}  // namespace cloudsql::network
