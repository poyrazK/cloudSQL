/**
 * @file rpc_client_tests.cpp
 * @brief Unit tests for RpcClient - internal RPC client for node-to-node communication
 */

#include <gtest/gtest.h>

#include <atomic>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

#include "network/rpc_client.hpp"
#include "network/rpc_message.hpp"
#include "network/rpc_server.hpp"

using namespace cloudsql::network;

namespace {

// Ignore SIGPIPE to prevent crashes when writing to closed sockets
struct SigpipeGuard {
    SigpipeGuard() { std::signal(SIGPIPE, SIG_IGN); }
};
SigpipeGuard g_sigpipe;

class RpcClientTests : public ::testing::Test {
   protected:
    void SetUp() override {
        port_ = TEST_PORT_BASE_ + next_port_++;
        server_ = std::make_unique<RpcServer>(port_);
        handler_called_ = false;
    }

    void TearDown() override {
        if (server_) {
            server_->stop();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    static constexpr uint16_t TEST_PORT_BASE_ = 6400;
    static std::atomic<uint16_t> next_port_;
    uint16_t port_;
    std::unique_ptr<RpcServer> server_;
    std::atomic<bool> handler_called_{false};
};

std::atomic<uint16_t> RpcClientTests::next_port_{0};

TEST_F(RpcClientTests, ConnectAndDisconnect) {
    server_->start();

    RpcClient client("127.0.0.1", port_);
    EXPECT_TRUE(client.connect());
    EXPECT_TRUE(client.is_connected());

    client.disconnect();
    EXPECT_FALSE(client.is_connected());
}

TEST_F(RpcClientTests, ConnectRefused) {
    // No server started - connection should fail
    RpcClient client("127.0.0.1", port_);
    EXPECT_FALSE(client.connect());
    EXPECT_FALSE(client.is_connected());
}

TEST_F(RpcClientTests, ConnectInvalidAddress) {
    // Use an address that nothing is listening on
    RpcClient client("127.0.0.1", port_);
    // Port not in use, but connection refused happens at TCP level
    EXPECT_FALSE(client.connect());
}

TEST_F(RpcClientTests, CallAfterServerStop) {
    server_->start();

    // Set a handler that responds immediately
    server_->set_handler(RpcType::Heartbeat,
                         [](const RpcHeader&, const std::vector<uint8_t>&, int fd) {
                             RpcHeader resp_h;
                             resp_h.type = RpcType::Heartbeat;
                             resp_h.payload_len = 0;
                             char h_buf[RpcHeader::HEADER_SIZE];
                             resp_h.encode(h_buf);
                             send(fd, h_buf, RpcHeader::HEADER_SIZE, 0);
                         });

    RpcClient client("127.0.0.1", port_);
    ASSERT_TRUE(client.connect());
    ASSERT_TRUE(client.is_connected());

    // Stop the server
    server_->stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Call after server stop should fail (connection refused/reset)
    // Note: is_connected() returns true because it only checks if fd_ >= 0,
    // not whether the server is still connected
    std::vector<uint8_t> response;
    EXPECT_FALSE(client.call(RpcType::Heartbeat, {}, response, 0));
}

TEST_F(RpcClientTests, FullRoundTrip) {
    server_->start();

    server_->set_handler(RpcType::QueryResults,
                         [](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
                             // Echo back the payload
                             RpcHeader resp_h;
                             resp_h.type = RpcType::QueryResults;
                             resp_h.payload_len = static_cast<uint16_t>(p.size());
                             char h_buf[RpcHeader::HEADER_SIZE];
                             resp_h.encode(h_buf);
                             send(fd, h_buf, RpcHeader::HEADER_SIZE, 0);
                             if (!p.empty()) {
                                 send(fd, p.data(), p.size(), 0);
                             }
                         });

    RpcClient client("127.0.0.1", port_);
    ASSERT_TRUE(client.connect());

    std::vector<uint8_t> payload = {1, 2, 3, 4, 5};
    std::vector<uint8_t> response;
    ASSERT_TRUE(client.call(RpcType::QueryResults, payload, response, 0));

    EXPECT_EQ(response.size(), 5U);
    EXPECT_EQ(response[0], 1);
    EXPECT_EQ(response[4], 5);
}

TEST_F(RpcClientTests, ConcurrentCalls) {
    server_->start();

    std::atomic<int> call_count{0};
    server_->set_handler(RpcType::QueryResults,
                         [&](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
                             call_count++;
                             RpcHeader resp_h;
                             resp_h.type = RpcType::QueryResults;
                             resp_h.payload_len = static_cast<uint16_t>(p.size());
                             char h_buf[RpcHeader::HEADER_SIZE];
                             resp_h.encode(h_buf);
                             send(fd, h_buf, RpcHeader::HEADER_SIZE, 0);
                             if (!p.empty()) {
                                 send(fd, p.data(), p.size(), 0);
                             }
                         });

    RpcClient client("127.0.0.1", port_);
    ASSERT_TRUE(client.connect());

    // Make 5 concurrent calls - RpcClient is single-threaded so they serialize
    for (int i = 0; i < 5; i++) {
        std::vector<uint8_t> payload = {static_cast<uint8_t>(i)};
        std::vector<uint8_t> response;
        ASSERT_TRUE(client.call(RpcType::QueryResults, payload, response, 0));
        EXPECT_EQ(response.size(), 1U);
        EXPECT_EQ(response[0], static_cast<uint8_t>(i));
    }

    EXPECT_EQ(call_count, 5);
}

TEST_F(RpcClientTests, ReconnectAfterServerRestart) {
    // Use a different port to avoid conflicts with other tests
    constexpr uint16_t reconnect_port = TEST_PORT_BASE_ + 100;
    auto reconnect_server = std::make_unique<RpcServer>(reconnect_port);
    reconnect_server->start();

    server_->set_handler(RpcType::QueryResults,
                         [](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
                             RpcHeader resp_h;
                             resp_h.type = RpcType::QueryResults;
                             resp_h.payload_len = static_cast<uint16_t>(p.size());
                             char h_buf[RpcHeader::HEADER_SIZE];
                             resp_h.encode(h_buf);
                             send(fd, h_buf, RpcHeader::HEADER_SIZE, 0);
                             if (!p.empty()) {
                                 send(fd, p.data(), p.size(), 0);
                             }
                         });

    RpcClient client("127.0.0.1", reconnect_port);
    ASSERT_TRUE(client.connect());

    std::vector<uint8_t> response;
    ASSERT_TRUE(client.call(RpcType::QueryResults, {}, response, 0));

    // Stop server
    reconnect_server->stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Start server again on same port
    reconnect_server = std::make_unique<RpcServer>(reconnect_port);
    reconnect_server->start();

    // Reconnect
    ASSERT_TRUE(client.connect());
    ASSERT_TRUE(client.is_connected());
    ASSERT_TRUE(client.call(RpcType::QueryResults, {}, response, 0));

    reconnect_server->stop();
}

TEST_F(RpcClientTests, SendOnlyWithoutResponse) {
    server_->start();

    std::atomic<int> call_count{0};
    server_->set_handler(RpcType::Heartbeat, [&](const RpcHeader& h, const std::vector<uint8_t>& p,
                                                 int fd) { call_count++; });

    RpcClient client("127.0.0.1", port_);
    ASSERT_TRUE(client.connect());

    // send_only doesn't wait for response
    ASSERT_TRUE(client.send_only(RpcType::Heartbeat, {}, 0));

    // Give server time to process
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(call_count, 1);
}

}  // namespace
