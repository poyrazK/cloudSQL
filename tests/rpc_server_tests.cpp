/**
 * @file rpc_server_tests.cpp
 * @brief Unit tests for RpcServer - Internal RPC server for node-to-node communication
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

class RpcServerTests : public ::testing::Test {
   protected:
    void SetUp() override {
        // Use a unique port for each test to avoid TIME_WAIT issues
        port_ = TEST_PORT_BASE_ + next_port_++;
        server_ = std::make_unique<RpcServer>(port_);
        handler_called_ = false;
    }

    void TearDown() override {
        if (server_) {
            server_->stop();
        }
        // Small delay to allow socket to settle
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    static constexpr uint16_t TEST_PORT_BASE_ = 6300;
    static std::atomic<uint16_t> next_port_;
    uint16_t port_;
    std::unique_ptr<RpcServer> server_;
    std::atomic<bool> handler_called_{false};
};

std::atomic<uint16_t> RpcServerTests::next_port_{0};

TEST_F(RpcServerTests, LifecycleStartStop) {
    ASSERT_TRUE(server_->start());
    server_->stop();
    ASSERT_TRUE(server_->start());
    server_->stop();
}

TEST_F(RpcServerTests, DoubleStartReturnsFalse) {
    ASSERT_TRUE(server_->start());
    ASSERT_FALSE(server_->start());
    // Force cleanup before next test
    server_->stop();
    // Give socket time to release
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
}

TEST_F(RpcServerTests, SetAndGetHandler) {
    auto handler = [](const RpcHeader&, const std::vector<uint8_t>&, int) {};
    server_->set_handler(RpcType::Heartbeat, handler);
    auto retrieved = server_->get_handler(RpcType::Heartbeat);
    ASSERT_NE(retrieved, nullptr);
}

TEST_F(RpcServerTests, GetHandlerNotSet) {
    auto retrieved = server_->get_handler(RpcType::RegisterNode);
    EXPECT_EQ(retrieved, nullptr);
}

TEST_F(RpcServerTests, HandlerOverride) {
    int call_count = 0;
    auto handler1 = [&](const RpcHeader&, const std::vector<uint8_t>&, int) { call_count++; };
    auto handler2 = [&](const RpcHeader&, const std::vector<uint8_t>&, int) { call_count += 10; };

    server_->set_handler(RpcType::Heartbeat, handler1);
    server_->set_handler(RpcType::Heartbeat, handler2);
    auto retrieved = server_->get_handler(RpcType::Heartbeat);
    ASSERT_NE(retrieved, nullptr);
}

TEST_F(RpcServerTests, ClearHandlersAfterStop) {
    auto handler = [](const RpcHeader&, const std::vector<uint8_t>&, int) {};
    server_->set_handler(RpcType::Heartbeat, handler);
    server_->start();
    server_->stop();
    auto retrieved = server_->get_handler(RpcType::Heartbeat);
    EXPECT_EQ(retrieved, nullptr);
}

TEST_F(RpcServerTests, ZeroPayloadHandler) {
    server_->start();

    bool called = false;
    server_->set_handler(RpcType::Heartbeat,
                         [&called](const RpcHeader& h, const std::vector<uint8_t>& p, int fd) {
                             called = true;
                             EXPECT_EQ(p.size(), 0U);
                         });

    // Connect and send RPC with zero payload
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(fd, 0);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    ASSERT_EQ(connect(fd, (sockaddr*)&addr, sizeof(addr)), 0);

    // Send header
    RpcHeader hdr;
    hdr.type = RpcType::Heartbeat;
    hdr.payload_len = 0;
    char h_buf[RpcHeader::HEADER_SIZE];
    hdr.encode(h_buf);
    send(fd, h_buf, RpcHeader::HEADER_SIZE, 0);

    // Give time for the server to process and call the handler
    for (int i = 0; i < 10 && !called; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    EXPECT_TRUE(called);

    close(fd);
}

TEST_F(RpcServerTests, MultipleConnections) {
    server_->start();

    int call_count = 0;
    server_->set_handler(
        RpcType::Heartbeat,
        [&call_count](const RpcHeader&, const std::vector<uint8_t>&, int) { call_count++; });

    std::vector<int> fds;
    for (int i = 0; i < 5; ++i) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port_);
        inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
        if (connect(fd, (sockaddr*)&addr, sizeof(addr)) == 0) {
            fds.push_back(fd);
        }
    }

    // Send RPCs
    for (int fd : fds) {
        RpcHeader hdr;
        hdr.type = RpcType::Heartbeat;
        hdr.payload_len = 0;
        char h_buf[RpcHeader::HEADER_SIZE];
        hdr.encode(h_buf);
        send(fd, h_buf, RpcHeader::HEADER_SIZE, 0);
    }

    // Give time for the server to process all 5
    for (int i = 0; i < 20 && call_count < 5; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    for (int fd : fds) {
        close(fd);
    }

    EXPECT_EQ(call_count, 5);
}

TEST_F(RpcServerTests, ClientDisconnectMidHeader) {
    server_->start();

    server_->set_handler(RpcType::Heartbeat,
                         [](const RpcHeader&, const std::vector<uint8_t>&, int) {});

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    connect(fd, (sockaddr*)&addr, sizeof(addr));

    // Send partial header then disconnect
    char partial[6];
    std::memset(partial, 0, 6);
    send(fd, partial, 6, 0);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    close(fd);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
}

TEST_F(RpcServerTests, ClientDisconnectMidPayload) {
    server_->start();

    server_->set_handler(RpcType::Heartbeat,
                         [](const RpcHeader&, const std::vector<uint8_t>&, int) {});

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    connect(fd, (sockaddr*)&addr, sizeof(addr));

    // Send full header indicating payload but don't send payload
    RpcHeader hdr;
    hdr.type = RpcType::Heartbeat;
    hdr.payload_len = 100;  // Request 100 bytes but we won't send them
    char h_buf[RpcHeader::HEADER_SIZE];
    hdr.encode(h_buf);
    send(fd, h_buf, RpcHeader::HEADER_SIZE, 0);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    close(fd);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
}

TEST_F(RpcServerTests, FullRoundTripWithClient) {
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

TEST_F(RpcServerTests, NoHandlerRegistered) {
    server_->start();
    // Don't set any handler

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    connect(fd, (sockaddr*)&addr, sizeof(addr));

    // Send an RPC with no handler registered
    RpcHeader hdr;
    hdr.type = RpcType::Error;
    hdr.payload_len = 0;
    char h_buf[RpcHeader::HEADER_SIZE];
    hdr.encode(h_buf);
    send(fd, h_buf, RpcHeader::HEADER_SIZE, 0);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    close(fd);
    server_->stop();
}

TEST_F(RpcServerTests, LargePayload) {
    server_->start();

    // Set a handler that tracks if it was called
    server_->set_handler(RpcType::PushData, [&](const RpcHeader&, const std::vector<uint8_t>&,
                                                int) { handler_called_ = true; });

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    connect(fd, (sockaddr*)&addr, sizeof(addr));

    // Send large payload (10KB)
    RpcHeader hdr;
    hdr.type = RpcType::PushData;
    hdr.payload_len = 10240;
    char h_buf[RpcHeader::HEADER_SIZE];
    hdr.encode(h_buf);
    send(fd, h_buf, RpcHeader::HEADER_SIZE, 0);

    // Send payload data
    std::vector<char> large_data(10240, 'x');
    send(fd, large_data.data(), large_data.size(), 0);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_TRUE(handler_called_);

    close(fd);
    server_->stop();
}

TEST_F(RpcServerTests, ServerOnSpecificPort) {
    // Test server can bind to specific port
    constexpr uint16_t specific_port = 6399;
    auto specific_server = std::make_unique<RpcServer>(specific_port);

    ASSERT_TRUE(specific_server->start());
    specific_server->stop();
}

}  // namespace
