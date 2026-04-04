#include <benchmark/benchmark.h>
#include <memory>
#include <vector>
#include <thread>
#include <atomic>
#include <sys/socket.h>
#include <unistd.h>
#include <iostream>
#include "network/rpc_server.hpp"
#include "network/rpc_client.hpp"
#include "network/rpc_message.hpp"

using namespace cloudsql::network;

class NetworkBenchmark : public benchmark::Fixture {
public:
    std::unique_ptr<RpcServer> server;
    std::unique_ptr<RpcClient> client;
    int port = 9000;

    void SetUp(const ::benchmark::State& state) override {
        port = 9000 + state.range(0); // Different payload sizes on different ports
        server = std::make_unique<RpcServer>(port);
        
        server->set_handler(RpcType::AppendEntries, [](const RpcHeader& header, const std::vector<uint8_t>& payload, int client_fd) {
            RpcHeader resp_header = header;
            resp_header.payload_len = static_cast<uint16_t>(payload.size());
            char header_buf[RpcHeader::HEADER_SIZE];
            resp_header.encode(header_buf);
            
            if (send(client_fd, header_buf, RpcHeader::HEADER_SIZE, 0) < 0) {
                std::cerr << "Handler failed to send header to fd=" << client_fd << " errno=" << errno << std::endl;
                return;
            }
            if (send(client_fd, payload.data(), payload.size(), 0) < 0) {
                std::cerr << "Handler failed to send payload to fd=" << client_fd << " errno=" << errno << std::endl;
                return;
            }
        });
        
        if (!server->start()) {
            const_cast<::benchmark::State&>(state).SkipWithError("RPC server failed to start");
            return;
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        client = std::make_unique<RpcClient>("127.0.0.1", port);
        if (!client->connect()) {
            const_cast<::benchmark::State&>(state).SkipWithError("RPC client failed to connect");
            return;
        }
    }

    void TearDown(const ::benchmark::State& state) override {
        client.reset();
        if (server) {
            server->stop();
        }
        server.reset();
    }
};

BENCHMARK_DEFINE_F(NetworkBenchmark, RpcRoundTrip)(benchmark::State& state) {
    if (!client || !client->is_connected()) {
        state.SkipWithError("Client not connected");
        return;
    }

    std::vector<uint8_t> request(state.range(0), 0xAA);
    std::vector<uint8_t> response;

    for (auto _ : state) {
        if (!client->call(RpcType::AppendEntries, request, response)) {
            state.SkipWithError("RPC call failed");
            break;
        }
    }
    
    state.SetBytesProcessed(state.iterations() * state.range(0) * 2);
}
BENCHMARK_REGISTER_F(NetworkBenchmark, RpcRoundTrip)->Arg(64)->Arg(1024)->Arg(16384);
