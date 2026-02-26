/**
 * @file server_tests.cpp
 * @brief Unit tests for Network Server and Protocol
 */

#include <arpa/inet.h>   // IWYU pragma: keep
#include <netinet/in.h>  // IWYU pragma: keep
#include <sys/socket.h>  // IWYU pragma: keep
#include <sys/time.h>    // IWYU pragma: keep
#include <sys/types.h>   // IWYU pragma: keep
#include <unistd.h>      // IWYU pragma: keep

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "catalog/catalog.hpp"
#include "common/config.hpp"
#include "common/value.hpp"
#include "executor/types.hpp"
#include "network/server.hpp"
#include "storage/buffer_pool_manager.hpp"
#include "storage/heap_table.hpp"
#include "storage/storage_manager.hpp"
#include "test_utils.hpp"

using namespace cloudsql;
using namespace cloudsql::network;
using namespace cloudsql::common;

namespace {

using cloudsql::tests::tests_failed;
using cloudsql::tests::tests_passed;

constexpr uint16_t PORT_STATUS = 54321;
constexpr uint16_t PORT_SIMPLE = 54322;
constexpr uint16_t PORT_INVALID = 54323;
constexpr uint16_t PORT_TERM = 54324;
constexpr uint16_t PORT_HANDSHAKE = 54325;
constexpr uint16_t PORT_MULTI = 54326;

constexpr int CONN_RETRIES = 10;
constexpr int RETRY_DELAY_MS = 100;
constexpr int STARTUP_PKT_LEN = 8;
constexpr int PG_STARTUP_CODE = 196608;
constexpr int PG_SSL_CODE = 80877103;
constexpr int BUF_SIZE = 1024;
constexpr int AUTH_OK_LEN = 9;
constexpr int READY_LEN = 6;
constexpr int TEST_TIMEOUT_SEC = 2;

/**
 * @brief Set a receive timeout on a socket to prevent hangs in tests
 */
void set_sock_timeout(int sock) {
    struct timeval tv {};
    tv.tv_sec = TEST_TIMEOUT_SEC;
    tv.tv_usec = 0;
    static_cast<void>(setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)));
}

TEST(Server_StatusStrings) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
    Server s(PORT_STATUS, *catalog, sm);

    EXPECT_EQ(s.get_status_string(), std::string("Stopped"));
    static_cast<void>(s.start());
    EXPECT_EQ(s.get_status_string(), std::string("Running"));
    static_cast<void>(s.stop());
    EXPECT_EQ(s.get_status_string(), std::string("Stopped"));
}

TEST(Server_SimpleQuery) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
    const uint16_t port = PORT_SIMPLE;

    /* Register table in catalog */
    std::vector<ColumnInfo> cols;
    cols.emplace_back("id", common::ValueType::TYPE_INT32, 0);
    static_cast<void>(catalog->create_table("dual", std::move(cols)));

    auto server = Server::create(port, *catalog, sm);

    static_cast<void>(std::remove("./test_data/dual.heap"));
    storage::HeapTable table(
        "dual", sm,
        executor::Schema({executor::ColumnMeta("id", common::ValueType::TYPE_INT32, true)}));
    static_cast<void>(table.create());
    static_cast<void>(table.insert(executor::Tuple({common::Value(1)}), 0));

    static_cast<void>(server->start());

    struct sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    struct sockaddr sa {};
    std::memcpy(&sa, &addr, sizeof(addr));

    int sock = -1;
    for (int i = 0; i < CONN_RETRIES; ++i) {
        sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock >= 0) {
            set_sock_timeout(sock);
            if (connect(sock, &sa, sizeof(addr)) == 0) {
                break;
            }
            static_cast<void>(close(sock));
            sock = -1;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(RETRY_DELAY_MS));
    }

    if (sock >= 0) {
        const std::array<uint32_t, 2> startup = {htonl(static_cast<uint32_t>(STARTUP_PKT_LEN)),
                                                 htonl(static_cast<uint32_t>(PG_STARTUP_CODE))};
        static_cast<void>(send(sock, startup.data(), STARTUP_PKT_LEN, 0));

        std::array<char, BUF_SIZE> buffer{};
        static_cast<void>(recv(sock, buffer.data(), AUTH_OK_LEN, 0));  // AuthOK
        static_cast<void>(recv(sock, buffer.data(), READY_LEN, 0));    // ReadyForQuery

        const std::string sql = "SELECT id FROM dual";
        const char q_type = 'Q';
        const uint32_t q_len = htonl(static_cast<uint32_t>(sql.size() + 4 + 1));
        static_cast<void>(send(sock, &q_type, 1, 0));
        static_cast<void>(send(sock, &q_len, 4, 0));
        static_cast<void>(send(sock, sql.c_str(), sql.size() + 1, 0));

        const ssize_t n_t = recv(sock, buffer.data(), 1, 0);
        EXPECT_GT(n_t, 0);
        EXPECT_EQ(buffer[0], 'T');

        uint32_t res_len = 0;
        static_cast<void>(recv(sock, &res_len, 4, 0));
        res_len = ntohl(res_len);
        std::vector<char> body(res_len - 4);
        static_cast<void>(recv(sock, body.data(), res_len - 4, 0));

        const ssize_t n_d = recv(sock, buffer.data(), 1, 0);
        EXPECT_GT(n_d, 0);
        EXPECT_EQ(buffer[0], 'D');

        static_cast<void>(recv(sock, &res_len, 4, 0));
        res_len = ntohl(res_len);
        body.resize(res_len - 4);
        static_cast<void>(recv(sock, body.data(), res_len - 4, 0));

        const ssize_t n_c = recv(sock, buffer.data(), 1, 0);
        EXPECT_GT(n_c, 0);
        EXPECT_EQ(buffer[0], 'C');

        static_cast<void>(recv(sock, &res_len, 4, 0));
        res_len = ntohl(res_len);
        body.resize(res_len - 4);
        static_cast<void>(recv(sock, body.data(), res_len - 4, 0));

        const ssize_t n_z = recv(sock, buffer.data(), 1, 0);
        EXPECT_GT(n_z, 0);
        EXPECT_EQ(buffer[0], 'Z');

        static_cast<void>(close(sock));
    } else {
        throw std::runtime_error("Failed to connect to server");
    }

    static_cast<void>(server->stop());
}

TEST(Server_InvalidProtocol) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
    const uint16_t port = PORT_INVALID;
    auto server = Server::create(port, *catalog, sm);
    static_cast<void>(server->start());

    struct sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    struct sockaddr sa {};
    std::memcpy(&sa, &addr, sizeof(addr));

    const int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock >= 0) {
        set_sock_timeout(sock);
        if (connect(sock, &sa, sizeof(addr)) == 0) {
            const std::array<uint32_t, 2> startup = {htonl(static_cast<uint32_t>(STARTUP_PKT_LEN)),
                                                     htonl(12345)};
            static_cast<void>(send(sock, startup.data(), STARTUP_PKT_LEN, 0));

            std::array<char, 1> buffer{};
            const ssize_t n = recv(sock, buffer.data(), 1, 0);
            EXPECT_EQ(n, 0);
        }
        static_cast<void>(close(sock));
    }

    static_cast<void>(server->stop());
}

TEST(Server_Terminate) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
    const uint16_t port = PORT_TERM;
    auto server = Server::create(port, *catalog, sm);
    static_cast<void>(server->start());

    struct sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    struct sockaddr sa {};
    std::memcpy(&sa, &addr, sizeof(addr));

    const int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock >= 0) {
        set_sock_timeout(sock);
        if (connect(sock, &sa, sizeof(addr)) == 0) {
            const std::array<uint32_t, 2> startup = {htonl(static_cast<uint32_t>(STARTUP_PKT_LEN)),
                                                     htonl(static_cast<uint32_t>(PG_STARTUP_CODE))};
            static_cast<void>(send(sock, startup.data(), STARTUP_PKT_LEN, 0));

            std::array<char, BUF_SIZE> buffer{};
            static_cast<void>(recv(sock, buffer.data(), AUTH_OK_LEN, 0));  // AuthOK
            static_cast<void>(recv(sock, buffer.data(), READY_LEN, 0));    // ReadyForQuery

            const char terminate = 'X';
            const uint32_t len = htonl(4);
            static_cast<void>(send(sock, &terminate, 1, 0));
            static_cast<void>(send(sock, &len, 4, 0));

            const ssize_t n = recv(sock, buffer.data(), 1, 0);
            EXPECT_EQ(n, 0);
        }
        static_cast<void>(close(sock));
    }

    static_cast<void>(server->stop());
}

TEST(Server_Handshake) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
    const uint16_t port = PORT_HANDSHAKE;
    auto server = Server::create(port, *catalog, sm);
    static_cast<void>(server->start());

    struct sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    struct sockaddr sa {};
    std::memcpy(&sa, &addr, sizeof(addr));

    const int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock >= 0) {
        set_sock_timeout(sock);
        if (connect(sock, &sa, sizeof(addr)) == 0) {
            // 1. SSL Request
            const std::array<uint32_t, 2> ssl_req = {htonl(static_cast<uint32_t>(STARTUP_PKT_LEN)),
                                                     htonl(static_cast<uint32_t>(PG_SSL_CODE))};
            static_cast<void>(send(sock, ssl_req.data(), STARTUP_PKT_LEN, 0));
            char response{};
            static_cast<void>(recv(sock, &response, 1, 0));
            EXPECT_EQ(static_cast<int>(response), static_cast<int>('N'));

            // 2. Startup
            const std::array<uint32_t, 2> startup = {htonl(static_cast<uint32_t>(STARTUP_PKT_LEN)),
                                                     htonl(static_cast<uint32_t>(PG_STARTUP_CODE))};
            static_cast<void>(send(sock, startup.data(), STARTUP_PKT_LEN, 0));
            char type{};
            static_cast<void>(recv(sock, &type, 1, 0));
            EXPECT_EQ(type, 'R');
        }
        static_cast<void>(close(sock));
    }

    static_cast<void>(server->stop());
}

TEST(Server_MultiClient) {
    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
    auto server = Server::create(PORT_MULTI, *catalog, sm);
    static_cast<void>(server->start());

    constexpr int NUM_CLIENTS = 5;
    std::vector<std::thread> clients;
    clients.reserve(NUM_CLIENTS);
    std::atomic<int> success_count{0};

    for (int i = 0; i < NUM_CLIENTS; ++i) {
        clients.emplace_back([&success_count]() {
            struct sockaddr_in client_addr {};
            client_addr.sin_family = AF_INET;
            client_addr.sin_port = htons(PORT_MULTI);
            inet_pton(AF_INET, "127.0.0.1", &client_addr.sin_addr);

            struct sockaddr sa {};
            std::memcpy(&sa, &client_addr, sizeof(client_addr));

            const int sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock >= 0) {
                set_sock_timeout(sock);
                if (connect(sock, &sa, sizeof(client_addr)) == 0) {
                    const std::array<uint32_t, 2> startup = {
                        htonl(static_cast<uint32_t>(STARTUP_PKT_LEN)),
                        htonl(static_cast<uint32_t>(PG_STARTUP_CODE))};
                    static_cast<void>(send(sock, startup.data(), STARTUP_PKT_LEN, 0));
                    char type{};
                    if (recv(sock, &type, 1, 0) > 0 && type == 'R') {
                        success_count++;
                    }
                }
                static_cast<void>(close(sock));
            }
        });
    }

    for (auto& t : clients) {
        t.join();
    }
    EXPECT_EQ(success_count.load(), NUM_CLIENTS);

    static_cast<void>(server->stop());
}

}  // namespace

int main() {
    std::cout << "Server Unit Tests\n";
    std::cout << "=================\n";

    RUN_TEST(Server_StatusStrings);
    RUN_TEST(Server_SimpleQuery);
    RUN_TEST(Server_InvalidProtocol);
    RUN_TEST(Server_Terminate);
    RUN_TEST(Server_Handshake);
    RUN_TEST(Server_MultiClient);

    std::cout << "\nResults: \n" << tests_passed << " passed, \n" << tests_failed << " failed\n";
    return (tests_failed > 0);
}
