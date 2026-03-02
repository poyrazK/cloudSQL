/**
 * @file server_tests.cpp
 * @brief Unit tests for PostgreSQL server implementation
 */

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "catalog/catalog.hpp"
#include "common/config.hpp"
#include "network/server.hpp"
#include "storage/buffer_pool_manager.hpp"
#include "storage/storage_manager.hpp"

using namespace cloudsql;
using namespace cloudsql::network;
using namespace cloudsql::storage;

namespace {

constexpr uint16_t PORT_STATUS = 6001;
constexpr uint16_t PORT_CONNECT = 6002;
constexpr uint16_t PORT_STARTUP = 6003;
constexpr size_t STARTUP_PKT_LEN = 8;

TEST(ServerTests, StatusStrings) {
    auto catalog = Catalog::create();
    StorageManager disk_manager("./test_data");
    storage::BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
    config::Config cfg;
    Server s(PORT_STATUS, *catalog, sm, cfg, nullptr);

    EXPECT_STREQ(s.get_status_string().c_str(), "Stopped");
    static_cast<void>(s.start());
    EXPECT_STREQ(s.get_status_string().c_str(), "Running");
    static_cast<void>(s.stop());
}

TEST(ServerTests, Lifecycle) {
    auto catalog = Catalog::create();
    StorageManager disk_manager("./test_data");
    storage::BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
    config::Config cfg;
    uint16_t port = PORT_CONNECT;

    auto server = Server::create(port, *catalog, sm, cfg, nullptr);
    ASSERT_NE(server, nullptr);

    EXPECT_FALSE(server->is_running());
    ASSERT_TRUE(server->start());
    EXPECT_TRUE(server->is_running());

    // Try to connect
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    bool connected = false;
    for (int i = 0; i < 5; ++i) {
        if (connect(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0) {
            connected = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    EXPECT_TRUE(connected);
    close(sock);

    static_cast<void>(server->stop());
    EXPECT_FALSE(server->is_running());
}

TEST(ServerTests, Handshake) {
    auto catalog = Catalog::create();
    StorageManager disk_manager("./test_data");
    storage::BufferPoolManager sm(cloudsql::config::Config::DEFAULT_BUFFER_POOL_SIZE, disk_manager);
    config::Config cfg;
    uint16_t port = PORT_STARTUP;

    auto server = Server::create(port, *catalog, sm, cfg, nullptr);
    ASSERT_TRUE(server->start());

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (connect(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == 0) {
        // Send startup packet
        const std::array<uint32_t, 2> startup = {htonl(static_cast<uint32_t>(STARTUP_PKT_LEN)),
                                                 htonl(196608)};
        send(sock, startup.data(), startup.size() * 4, 0);

        // Receive Auth OK
        std::array<char, 9> buffer{};
        ssize_t n = recv(sock, buffer.data(), 9, 0);
        EXPECT_EQ(n, 9);
        EXPECT_EQ(buffer[0], 'R');

        // Receive ReadyForQuery
        n = recv(sock, buffer.data(), 6, 0);
        EXPECT_EQ(n, 6);
        EXPECT_EQ(buffer[0], 'Z');
    }

    close(sock);
    static_cast<void>(server->stop());
}

}  // namespace
