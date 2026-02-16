/**
 * @file server_tests.cpp
 * @brief Unit tests for Network Server and PostgreSQL protocol
 */

#include <arpa/inet.h>
#include <unistd.h>

#include <cassert>
#include <chrono>
#include <cstring>
#include <iostream>
#include <thread>
#include <vector>

#include "catalog/catalog.hpp"
#include "executor/types.hpp"
#include "network/server.hpp"
#include "storage/storage_manager.hpp"

using namespace cloudsql;
using namespace cloudsql::network;
using namespace cloudsql::storage;
using namespace cloudsql::executor;

static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) void test_##name()
#define RUN_TEST(name)                                        \
    do {                                                      \
        std::cout << "  " << #name << "... ";                 \
        try {                                                 \
            test_##name();                                    \
            std::cout << "PASSED" << std::endl;               \
            tests_passed++;                                   \
        } catch (const std::exception& e) {                   \
            std::cout << "FAILED: " << e.what() << std::endl; \
            tests_failed++;                                   \
        }                                                     \
    } while (0)

#define EXPECT_EQ(a, b)                                                          \
    do {                                                                         \
        if ((a) != (b)) {                                                        \
            throw std::runtime_error("Expected match but got different values"); \
        }                                                                        \
    } while (0)

#define EXPECT_TRUE(a)                                               \
    do {                                                             \
        if (!(a)) {                                                  \
            throw std::runtime_error("Expected true but got false"); \
        }                                                            \
    } while (0)

#define EXPECT_STREQ(a, b)                                                          \
    do {                                                                            \
        std::string _a = (a);                                                       \
        std::string _b = (b);                                                       \
        if (_a != _b) {                                                             \
            throw std::runtime_error("Expected '" + _b + "' but got '" + _a + "'"); \
        }                                                                           \
    } while (0)

TEST(Server_StatusStrings) {
    auto catalog = Catalog::create();
    StorageManager sm("./test_data");
    Server s(5440, *catalog, sm);

    EXPECT_STREQ(s.get_status_string(), "Stopped");
    s.start();
    EXPECT_STREQ(s.get_status_string(), "Running");
    s.stop();
    EXPECT_STREQ(s.get_status_string(), "Stopped");
}

TEST(Server_SimpleQuery) {
    uint16_t port = 5441;
    auto catalog = Catalog::create();
    StorageManager sm("./test_data");

    /* Pre-create 'dual' table for simple SELECTs */
    catalog->create_table("dual", {});
    std::remove("./test_data/dual.heap");
    HeapTable table("dual", sm, Schema());
    table.create();
    table.insert(Tuple(std::vector<common::Value>{}),
                 0);  // Insert one empty row so SELECT 1 FROM dual returns 1 row

    auto server = Server::create(port, *catalog, sm);
    server->start();

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) == 0) {
        /* 1. Startup */
        uint32_t startup[] = {htonl(8), htonl(196608)};
        send(sock, startup, 8, 0);

        char buffer[1024];
        recv(sock, buffer, 9, 0);  // AuthOK
        recv(sock, buffer, 6, 0);  // ReadyForQuery

        /* 2. Simple Query: SELECT 1 FROM dual */
        std::string sql = "SELECT 1 FROM dual";
        char q_type = 'Q';
        uint32_t q_len = htonl(4 + sql.size() + 1);
        send(sock, &q_type, 1, 0);
        send(sock, &q_len, 4, 0);
        send(sock, sql.c_str(), sql.size() + 1, 0);

        /* Expect: RowDescription ('T'), DataRow ('D'), CommandComplete ('C'), ReadyForQuery ('Z')
         */
        ssize_t n = recv(sock, buffer, 1, 0);
        EXPECT_TRUE(n > 0 && buffer[0] == 'T');

        uint32_t len;
        recv(sock, &len, 4, 0);
        len = ntohl(len);
        std::vector<char> body(len - 4);
        recv(sock, body.data(), len - 4, 0);

        n = recv(sock, buffer, 1, 0);
        EXPECT_TRUE(n > 0 && buffer[0] == 'D');
        recv(sock, &len, 4, 0);
        len = ntohl(len);
        body.resize(len - 4);
        recv(sock, body.data(), len - 4, 0);

        n = recv(sock, buffer, 1, 0);
        EXPECT_TRUE(n > 0 && buffer[0] == 'C');
        recv(sock, &len, 4, 0);
        len = ntohl(len);
        body.resize(len - 4);
        recv(sock, body.data(), len - 4, 0);

        n = recv(sock, buffer, 1, 0);
        EXPECT_TRUE(n > 0 && buffer[0] == 'Z');
    }

    close(sock);
    server->stop();
}

TEST(Server_InvalidProtocol) {
    uint16_t port = 5442;
    auto catalog = Catalog::create();
    StorageManager sm("./test_data");
    auto server = Server::create(port, *catalog, sm);
    server->start();

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) == 0) {
        /* Send invalid protocol version */
        uint32_t startup[] = {htonl(8), htonl(12345)};
        send(sock, startup, 8, 0);

        char buffer[1];
        ssize_t n = recv(sock, buffer, 1, 0);
        EXPECT_EQ(n, 0);  // Server should close connection
    }

    close(sock);
    server->stop();
}

TEST(Server_Terminate) {
    uint16_t port = 5443;
    auto catalog = Catalog::create();
    StorageManager sm("./test_data");
    auto server = Server::create(port, *catalog, sm);
    server->start();

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) == 0) {
        uint32_t startup[] = {htonl(8), htonl(196608)};
        send(sock, startup, 8, 0);

        char buffer[1024];
        recv(sock, buffer, 9, 0);  // AuthOK
        recv(sock, buffer, 6, 0);  // ReadyForQuery

        /* Send Terminate ('X') */
        char t_type = 'X';
        uint32_t t_len = htonl(4);
        send(sock, &t_type, 1, 0);
        send(sock, &t_len, 4, 0);

        char response;
        ssize_t n = recv(sock, &response, 1, 0);
        EXPECT_EQ(n, 0);  // Server should close connection
    }

    close(sock);
    server->stop();
}

int main() {
    std::cout << "Server Unit Tests" << std::endl;
    std::cout << "========================" << std::endl << std::endl;

    RUN_TEST(Server_StatusStrings);
    RUN_TEST(Server_SimpleQuery);
    RUN_TEST(Server_InvalidProtocol);
    RUN_TEST(Server_Terminate);

    std::cout << std::endl << "========================" << std::endl;
    std::cout << "Results: " << tests_passed << " passed, " << tests_failed << " failed"
              << std::endl;

    return (tests_failed > 0);
}
