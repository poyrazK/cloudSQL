/**
 * @file rpc_client.cpp
 * @brief Internal RPC client implementation
 */

#include "network/rpc_client.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

#include "network/rpc_message.hpp"

namespace cloudsql::network {

RpcClient::RpcClient(const std::string& address, uint16_t port) : address_(address), port_(port) {}

RpcClient::~RpcClient() {
    disconnect();
}

bool RpcClient::connect() {
    const std::scoped_lock<std::mutex> lock(mutex_);
    if (fd_ >= 0) {
        return true;
    }

    fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ < 0) {
        return false;
    }

    struct sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    static_cast<void>(inet_pton(AF_INET, address_.c_str(), &addr.sin_addr));

    if (::connect(fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        static_cast<void>(close(fd_));
        fd_ = -1;
        return false;
    }

    return true;
}

void RpcClient::disconnect() {
    const std::scoped_lock<std::mutex> lock(mutex_);
    if (fd_ >= 0) {
        static_cast<void>(close(fd_));
        fd_ = -1;
    }
}

bool RpcClient::call(RpcType type, const std::vector<uint8_t>& payload,
                     std::vector<uint8_t>& response_out) {
    if (!send_only(type, payload)) {
        return false;
    }

    std::array<char, 8> header_buf{};
    if (recv(fd_, header_buf.data(), 8, MSG_WAITALL) <= 0) {
        return false;
    }

    const RpcHeader resp_header = RpcHeader::decode(header_buf.data());
    response_out.resize(resp_header.payload_len);
    if (resp_header.payload_len > 0) {
        if (recv(fd_, response_out.data(), resp_header.payload_len, MSG_WAITALL) <= 0) {
            return false;
        }
    }

    return true;
}

bool RpcClient::send_only(RpcType type, const std::vector<uint8_t>& payload) {
    const std::scoped_lock<std::mutex> lock(mutex_);
    if (fd_ < 0 && !connect()) {
        return false;
    }

    RpcHeader header;
    header.type = type;
    header.payload_len = static_cast<uint16_t>(payload.size());

    char header_buf[8];
    header.encode(header_buf);

    if (send(fd_, header_buf, 8, 0) <= 0) {
        return false;
    }
    if (!payload.empty()) {
        if (send(fd_, payload.data(), payload.size(), 0) <= 0) {
            return false;
        }
    }

    return true;
}

}  // namespace cloudsql::network
