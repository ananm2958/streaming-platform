#pragma once

#include "../config/broker_config.h"
#include "../protocol/protocol.h"
#include "../replication/replication_manager.h"
#include "../storage/storage_engine.h"

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

class Broker {
public:
    Broker(
        const BrokerConfig& config,
        StorageEngine& storage,
        ReplicationManager& replication
    );

    ~Broker();

    void start();
    void stop();

    Response handle_request(const Request& request);

    std::string handle_inter_broker_request(
        const Request& request,
        int client_socket
    );

private:
    Response handle_produce(const Request& request);
    Response handle_fetch(const Request& request);
    Response handle_metadata();
    Response forward_produce_to_leader(const Request& request);

    std::string format_leader_redirect() const;

    void heartbeat_loop();

    const BrokerConfig& config_;
    StorageEngine& storage_;
    ReplicationManager& replication_;

    std::atomic<bool> running_;
    std::thread heartbeat_thread_;
};
