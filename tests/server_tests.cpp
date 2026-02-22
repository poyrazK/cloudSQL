#include "network/server.hpp"
#include "catalog/catalog.hpp"
#include "storage/storage_manager.hpp"
#include "storage/heap_table.hpp"
#include "executor/types.hpp"
#include "common/value.hpp"
#include "test_utils.hpp"
#include <iostream>
#include <vector>
#include <array>
#include <string>
#include <thread>
#include <chrono>
#include <cstdint>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>   // IWYU pragma: keep
#include <unistd.h>      // IWYU pragma: keep
#include <stdexcept>
#include <memory>
#include <atomic>
#include <utility>
#include <cstdio>

using namespace cloudsql;
using namespace cloudsql::network;
using namespace cloudsql::tests;

namespace {

constexpr uint16_t PORT_STATUS = 5440;
constexpr uint16_t PORT_SIMPLE = 5441;
constexpr uint16_t PORT_INVALID = 5442;
constexpr uint16_t PORT_TERM = 5443;
constexpr uint16_t PORT_HANDSHAKE = 5444;
constexpr uint16_t PORT_MULTI = 5445;

constexpr int CONN_RETRIES = 5;
constexpr int RETRY_MS = 200;
constexpr int NUM_CLIENTS = 5;

constexpr int PG_SSL_CODE = 80877103;
constexpr int PG_STARTUP_CODE = 196608;

constexpr size_t BUF_SIZE = 1024;
constexpr size_t STARTUP_PKT_LEN = 8;
constexpr size_t AUTH_OK_LEN = 9;
constexpr size_t READY_LEN = 6;
constexpr uint32_t Q_LEN_BASE = 5;

void test_Server_StatusStrings() {
    auto catalog = std::make_unique<Catalog>();
    storage::StorageManager sm("./test_data");
    Server s(PORT_STATUS, *catalog, sm);
    
    EXPECT_EQ(s.get_status_string(), std::string("Stopped"));
    static_cast<void>(s.start());
    EXPECT_EQ(s.get_status_string(), std::string("Running"));
    static_cast<void>(s.stop());
    EXPECT_EQ(s.get_status_string(), std::string("Stopped"));
}

void test_Server_SimpleQuery() {
    auto catalog = std::make_unique<Catalog>();
    storage::StorageManager sm("./test_data");
    const uint16_t port = PORT_SIMPLE;
    
    /* Register table in catalog */
    std::vector<ColumnInfo> cols;
    cols.emplace_back("id", common::ValueType::TYPE_INT32, 0);
    static_cast<void>(catalog->create_table("dual", std::move(cols)));

    auto server = Server::create(port, *catalog, sm);
    
    static_cast<void>(std::remove("./test_data/dual.heap"));
    storage::HeapTable table("dual", sm, executor::Schema({executor::ColumnMeta("id", common::ValueType::TYPE_INT32, true)}));
    static_cast<void>(table.create());
    static_cast<void>(table.insert(executor::Tuple({common::Value(1)}), 0));

    static_cast<void>(server->start());

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    int sock = -1;
    for (int i = 0; i < CONN_RETRIES; ++i) {
        sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock >= 0) {
            if (connect(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0) { // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                break;
            }
            static_cast<void>(close(sock));
            sock = -1;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(RETRY_MS));
    }

    if (sock >= 0) {
        const std::array<uint32_t, 2> startup = {htonl(static_cast<uint32_t>(STARTUP_PKT_LEN)), htonl(static_cast<uint32_t>(PG_STARTUP_CODE))};
        static_cast<void>(send(sock, startup.data(), STARTUP_PKT_LEN, 0));
        
        std::array<char, BUF_SIZE> buffer{};
        static_cast<void>(recv(sock, buffer.data(), AUTH_OK_LEN, 0));  // AuthOK
        static_cast<void>(recv(sock, buffer.data(), READY_LEN, 0));  // ReadyForQuery

        const std::string sql = "SELECT id FROM dual";
        const char q_type = 'Q';
        const uint32_t q_len = htonl(static_cast<uint32_t>(Q_LEN_BASE - 1 + sql.size() + 1));
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

void test_Server_InvalidProtocol() {
    auto catalog = std::make_unique<Catalog>();
    storage::StorageManager sm("./test_data");
    const uint16_t port = PORT_INVALID;
    auto server = Server::create(port, *catalog, sm);
    static_cast<void>(server->start());

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    const int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock >= 0) {
        if (connect(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0) { // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            const std::array<uint32_t, 2> startup = {htonl(static_cast<uint32_t>(STARTUP_PKT_LEN)), htonl(12345)};
            static_cast<void>(send(sock, startup.data(), STARTUP_PKT_LEN, 0));
            
            std::array<char, 1> buffer{};
            const ssize_t n = recv(sock, buffer.data(), 1, 0);
            EXPECT_EQ(n, 0);
        }
        static_cast<void>(close(sock));
    }
    static_cast<void>(server->stop());
}

void test_Server_Terminate() {
    auto catalog = std::make_unique<Catalog>();
    storage::StorageManager sm("./test_data");
    const uint16_t port = PORT_TERM;
    auto server = Server::create(port, *catalog, sm);
    static_cast<void>(server->start());

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    const int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock >= 0) {
        if (connect(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0) { // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            const std::array<uint32_t, 2> startup = {htonl(static_cast<uint32_t>(STARTUP_PKT_LEN)), htonl(static_cast<uint32_t>(PG_STARTUP_CODE))};
            static_cast<void>(send(sock, startup.data(), STARTUP_PKT_LEN, 0));
            
            std::array<char, BUF_SIZE> buffer{};
            static_cast<void>(recv(sock, buffer.data(), AUTH_OK_LEN, 0));  // AuthOK
            static_cast<void>(recv(sock, buffer.data(), READY_LEN, 0));  // ReadyForQuery

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

void test_Server_Handshake() {
    const uint16_t port = PORT_HANDSHAKE;
    storage::StorageManager sm("./test_data");
    auto catalog = std::make_unique<Catalog>();
    auto server = Server::create(port, *catalog, sm);
    static_cast<void>(server->start());

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    const int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock >= 0) {
        if (connect(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0) { // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            // 1. SSL Request
            const std::array<uint32_t, 2> ssl_req = {htonl(static_cast<uint32_t>(STARTUP_PKT_LEN)), htonl(static_cast<uint32_t>(PG_SSL_CODE))};
            static_cast<void>(send(sock, ssl_req.data(), STARTUP_PKT_LEN, 0));
            char response{};
            static_cast<void>(recv(sock, &response, 1, 0));
            EXPECT_EQ(static_cast<int>(response), static_cast<int>('N'));

            // 2. Startup
            const std::array<uint32_t, 2> startup = {htonl(static_cast<uint32_t>(STARTUP_PKT_LEN)), htonl(static_cast<uint32_t>(PG_STARTUP_CODE))};
            static_cast<void>(send(sock, startup.data(), STARTUP_PKT_LEN, 0));
            char type{};
            static_cast<void>(recv(sock, &type, 1, 0));
            EXPECT_EQ(static_cast<int>(type), static_cast<int>('R'));
        }
        static_cast<void>(close(sock));
    }
    static_cast<void>(server->stop());
}

void test_Server_MultiClient() {
    const uint16_t port = PORT_MULTI;
    storage::StorageManager sm("./test_data");
    auto catalog = std::make_unique<Catalog>();
    auto server = Server::create(port, *catalog, sm);
    static_cast<void>(server->start());

    std::vector<std::thread> clients;
    clients.reserve(NUM_CLIENTS);
    std::atomic<int> success_count{0};

    for (int i = 0; i < NUM_CLIENTS; ++i) {
        clients.emplace_back([&success_count]() {
            struct sockaddr_in client_addr{};
            client_addr.sin_family = AF_INET;
            client_addr.sin_port = htons(PORT_MULTI);
            static_cast<void>(inet_pton(AF_INET, "127.0.0.1", &client_addr.sin_addr));

            const int sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock >= 0) {
                if (connect(sock, reinterpret_cast<struct sockaddr*>(&client_addr), sizeof(client_addr)) == 0) { // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                    const std::array<uint32_t, 2> startup = {htonl(static_cast<uint32_t>(STARTUP_PKT_LEN)), htonl(static_cast<uint32_t>(PG_STARTUP_CODE))};
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

    for (auto& t : clients) { t.join(); }
    EXPECT_EQ(success_count.load(), NUM_CLIENTS);

    static_cast<void>(server->stop());
}

} // namespace

int main() {
    std::cout << "Server Unit Tests\n";
    std::cout << "========================\n";

    RUN_TEST(Server_StatusStrings);
    RUN_TEST(Server_SimpleQuery);
    RUN_TEST(Server_InvalidProtocol);
    RUN_TEST(Server_Terminate);
    RUN_TEST(Server_Handshake);
    RUN_TEST(Server_MultiClient);

    std::cout << "========================\n";
    std::cout << "All tests passed: " << tests_passed << "\n";
    if (tests_failed > 0) {
        std::cout << "Tests failed: " << tests_failed << "\n";
        return 1;
    }
    return 0;
}
