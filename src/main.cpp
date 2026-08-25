#include "broker.h"
#include "broker_config.h"
#include "replication_manager.h"
#include "storage_engine.h"
#include "tcp_server.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <sstream>
#include <string>
#include <vector>

namespace {

std::atomic<bool> g_running{true};

void handle_signal(int) {
    g_running = false;
}

BrokerType parse_role(const std::string& role) {
    if (role == "leader") {
        return LEADER;
    }
    return FOLLOWER;
}

std::vector<ClusterPeer> parse_peers(const std::string& peers_arg) {
    std::vector<ClusterPeer> peers;
    if (peers_arg.empty()) {
        return peers;
    }

    std::stringstream stream(peers_arg);
    std::string entry;
    while (std::getline(stream, entry, ',')) {
        std::stringstream peer_stream(entry);
        std::string id;
        std::string host;
        std::string port;

        if (!std::getline(peer_stream, id, ':') ||
            !std::getline(peer_stream, host, ':') ||
            !std::getline(peer_stream, port, ':')) {
            continue;
        }

        peers.push_back(
            ClusterPeer{
                std::stoi(id),
                host,
                std::stoi(port),
            }
        );
    }

    return peers;
}

void print_usage() {
    std::cerr
        << "Usage: broker --id <broker_id> --port <port> --host <ip> "
        << "--role <leader|follower> [--leader-id <broker_id>] "
        << "[--peers id:host:port,...] [--data-dir <path>]\n";
}

}  // namespace

int main(int argc, char* argv[]) {
    int broker_id = 0;
    int port = 9092;
    std::string host = "127.0.0.1";
    std::string role = "follower";
    std::string peers_arg;
    std::string data_dir = "./data";
    int leader_id = 0;

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--id" && i + 1 < argc) {
            broker_id = std::stoi(argv[++i]);
        } else if (arg == "--port" && i + 1 < argc) {
            port = std::stoi(argv[++i]);
        } else if (arg == "--host" && i + 1 < argc) {
            host = argv[++i];
        } else if (arg == "--role" && i + 1 < argc) {
            role = argv[++i];
        } else if (arg == "--peers" && i + 1 < argc) {
            peers_arg = argv[++i];
        } else if (arg == "--data-dir" && i + 1 < argc) {
            data_dir = argv[++i];
        } else if (arg == "--leader-id" && i + 1 < argc) {
            leader_id = std::stoi(argv[++i]);
        } else if (arg == "--help") {
            print_usage();
            return 0;
        }
    }

    if (broker_id <= 0) {
        print_usage();
        return 1;
    }

    const BrokerType broker_type = parse_role(role);
    if (broker_type == LEADER) {
        leader_id = broker_id;
    } else if (leader_id <= 0) {
        std::cerr << "Followers must specify --leader-id\n";
        print_usage();
        return 1;
    }

    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    const std::string broker_data_dir =
        data_dir + "/broker-" + std::to_string(broker_id);

    const BrokerConfig config(
        port,
        broker_id,
        host,
        parse_peers(peers_arg),
        broker_data_dir,
        broker_type,
        leader_id
    );

    // Default storage policy: sync after 64 records or five milliseconds.
    StorageEngine storage(config.get_data_directory(), 1024 * 1024);
    // ReplicationManager owns runtime heartbeat/election state for this broker.
    ReplicationManager replication(config, storage);
    Broker broker(config, storage, replication);
    TCPServer server(port, &broker);

    broker.start();
    server.start();

    std::cout << "Broker " << broker_id << " running as " << role
              << " (leader-id=" << leader_id << ") on " << host << ":" << port
              << std::endl;

    while (g_running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    server.stop();
    broker.stop();

    return 0;
}
