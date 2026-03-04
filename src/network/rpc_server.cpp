/**
 * @file rpc_server.cpp
 * @brief Internal RPC server implementation
 */

#include "network/rpc_server.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cstring>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

#include "network/rpc_message.hpp"

namespace cloudsql::network {

bool RpcServer::start() {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        return false;
    }

    int opt = 1;
    static_cast<void>(setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)));

    struct sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port_);

    if (bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        static_cast<void>(close(listen_fd_));
        listen_fd_ = -1;
        return false;
    }

    if (listen(listen_fd_, 10) < 0) {
        static_cast<void>(close(listen_fd_));
        listen_fd_ = -1;
        return false;
    }

    running_ = true;
    accept_thread_ = std::thread(&RpcServer::accept_loop, this);
    return true;
}

void RpcServer::stop() {
    running_ = false;
    if (listen_fd_ >= 0) {
        static_cast<void>(shutdown(listen_fd_, SHUT_RDWR));
        static_cast<void>(close(listen_fd_));
        listen_fd_ = -1;
    }
    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }

    std::vector<std::thread> workers;
    {
        const std::scoped_lock<std::mutex> lock(worker_mutex_);
        workers.swap(worker_threads_);
    }

    for (auto& t : workers) {
        if (t.joinable()) {
            t.join();
        }
    }

    const std::scoped_lock<std::mutex> lock(handlers_mutex_);
    handlers_.clear();
}

void RpcServer::set_handler(RpcType type, RpcHandler handler) {
    const std::scoped_lock<std::mutex> lock(handlers_mutex_);
    handlers_[type] = std::move(handler);
}

void RpcServer::accept_loop() {
    while (running_) {
        fd_set fds;
        FD_ZERO(&fds);
        FD_SET(listen_fd_, &fds);
        struct timeval tv {
            1, 0
        };

        if (select(listen_fd_ + 1, &fds, nullptr, nullptr, &tv) > 0) {
            const int client_fd = accept(listen_fd_, nullptr, nullptr);
            if (client_fd >= 0) {
                const std::scoped_lock<std::mutex> lock(worker_mutex_);
                worker_threads_.emplace_back(&RpcServer::handle_client, this, client_fd);
            }
        }
    }
}

void RpcServer::handle_client(int client_fd) {
    std::array<char, RpcHeader::HEADER_SIZE> header_buf{};
    while (running_) {
        const ssize_t n = recv(client_fd, header_buf.data(), RpcHeader::HEADER_SIZE, MSG_WAITALL);
        if (n <= 0) {
            break;
        }

        const RpcHeader header = RpcHeader::decode(header_buf.data());
        std::vector<uint8_t> payload(header.payload_len);
        if (header.payload_len > 0) {
            if (recv(client_fd, payload.data(), header.payload_len, MSG_WAITALL) <= 0) {
                break;
            }
        }

        RpcHandler handler;
        {
            const std::scoped_lock<std::mutex> lock(handlers_mutex_);
            if (handlers_.count(header.type) != 0U) {
                handler = handlers_[header.type];
            }
        }

        if (handler) {
            handler(header, payload, client_fd);
        }
    }
    static_cast<void>(close(client_fd));
}

}  // namespace cloudsql::network
