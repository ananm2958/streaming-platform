#pragma once

#include "../config/broker_config.h"
#include "../protocol/protocol.h"
#include "../storage/storage_engine.h"

#include <chrono>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <mutex>

class ReplicationManager {
public:
    ReplicationManager(
        const BrokerConfig& config,
        StorageEngine& storage
    );

    bool replicate_message(
        const std::string& topic,
        int partition,
        uint64_t offset,
        const std::string& message
    );
    bool replicate_batch(const std::string& topic, int partition, uint64_t first_offset,
                         const std::vector<std::string>& messages);

    void handle_replication_request(const Request& request, int source_socket);

    bool wait_for_quorum(
        const std::string& topic,
        int partition,
        uint64_t offset,
        std::chrono::milliseconds timeout
    ) const;

    void handle_ack(
        int follower_id,
        const std::string& topic,
        int partition,
        uint64_t offset
    );

    bool quorum_reached(
        const std::string& topic,
        int partition,
        uint64_t offset
    ) const;

    void mark_committed(
        const std::string& topic,
        int partition,
        uint64_t offset
    );

    bool is_committed(
        const std::string& topic,
        int partition,
        uint64_t offset
    ) const;

    uint64_t get_commit_index(
        const std::string& topic,
        int partition
    ) const;

    void broadcast_commit(
        const std::string& topic,
        int partition,
        uint64_t offset
    );

    void handle_commit_request(const Request& request);

    void broadcast_heartbeat();

    void handle_heartbeat(int broker_id);

    void mark_follower_failed(int broker_id);

    bool is_peer_available(int broker_id) const;
    void maybe_elect_leader();
    bool is_leader() const;

private:
    static std::string make_log_key(const std::string& topic, int partition);
    static std::string make_ack_key(
        const std::string& topic,
        int partition,
        uint64_t offset
    );

    const ClusterPeer* find_peer(int broker_id) const;

    bool send_ack(
        int socket_fd,
        uint64_t offset,
        const std::string& topic,
        int partition
    );

    std::string build_replication_request(
        const std::string& topic,
        int partition,
        uint64_t offset,
        const std::string& message
    );

    std::string build_commit_request(
        const std::string& topic,
        int partition,
        uint64_t offset
    );

    const BrokerConfig& config_;
    StorageEngine& storage_;
    std::unordered_map<int, uint64_t> follower_ack_offsets_;
    std::unordered_map<std::string, int> ack_counts_;
    std::unordered_map<std::string, std::unordered_set<int>> ack_followers_;
    std::unordered_map<std::string, uint64_t> commit_index_by_log_;
    std::unordered_map<
        int,
        std::chrono::steady_clock::time_point
    > last_heartbeat_;
    std::unordered_set<int> failed_peers_;
    mutable std::recursive_mutex mutex_;
};
