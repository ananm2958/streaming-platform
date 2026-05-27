#include "replication_manager.h"

#include "../protocol/frame.h"
#include "../protocol/protocol.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <thread>

namespace {

constexpr std::chrono::seconds kHeartbeatTimeout{5};

bool send_one_shot_to_peer(
    const ClusterPeer& peer,
    const std::string& payload,
    std::string* response_payload = nullptr
) {
    int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd < 0) {
        return false;
    }

    sockaddr_in peer_addr{};
    peer_addr.sin_family = AF_INET;
    peer_addr.sin_port = htons(static_cast<uint16_t>(peer.port));

    if (inet_pton(AF_INET, peer.host.c_str(), &peer_addr.sin_addr) <= 0) {
        close(socket_fd);
        return false;
    }

    if (connect(socket_fd, reinterpret_cast<sockaddr*>(&peer_addr), sizeof(peer_addr)) < 0) {
        close(socket_fd);
        return false;
    }

    if (!write_frame(socket_fd, payload)) {
        close(socket_fd);
        return false;
    }

    if (response_payload != nullptr) {
        if (!read_frame(socket_fd, *response_payload)) {
            close(socket_fd);
            return false;
        }
    }

    close(socket_fd);
    return true;
}

bool replicate_to_peer(
    const ClusterPeer& peer,
    const std::string& replication_request,
    const std::string& topic,
    int partition,
    uint64_t offset,
    ReplicationManager& manager
) {
    std::string ack_payload;
    if (!send_one_shot_to_peer(peer, replication_request, &ack_payload)) {
        return false;
    }

    if (ack_payload.empty()) {
        return false;
    }

    const Request ack = parse_request(ack_payload);
    if (ack.type != RequestType::ACK) {
        return false;
    }

    manager.handle_ack(
        ack.broker_id,
        ack.topic.empty() ? topic : ack.topic,
        ack.topic.empty() ? partition : ack.partition,
        ack.fetch_offset
    );

    return ack.fetch_offset >= offset;
}

}  // namespace

std::string ReplicationManager::make_log_key(
    const std::string& topic,
    int partition
) {
    return topic + "-" + std::to_string(partition);
}

std::string ReplicationManager::make_ack_key(
    const std::string& topic,
    int partition,
    uint64_t offset
) {
    return make_log_key(topic, partition) + ":" + std::to_string(offset);
}

ReplicationManager::ReplicationManager(
    const BrokerConfig& config,
    StorageEngine& storage
)
    : config_(config),
      storage_(storage),
      role_(config.get_type()) {
    const auto now = std::chrono::steady_clock::now();
    for (const ClusterPeer& peer : config_.get_peers()) {
        if (peer.broker_id == config_.get_broker_id()) {
            continue;
        }
        last_heartbeat_[peer.broker_id] = now;
    }
}

const ClusterPeer* ReplicationManager::find_peer(int broker_id) const {
    for (const ClusterPeer& peer : config_.get_peers()) {
        if (peer.broker_id == broker_id) {
            return &peer;
        }
    }
    return nullptr;
}

bool ReplicationManager::is_peer_available(int broker_id) const {
    return failed_peers_.find(broker_id) == failed_peers_.end();
}

bool ReplicationManager::replicate_message(
    const std::string& topic,
    int partition,
    uint64_t offset,
    const std::string& message
) {
    if (role_ != BrokerType::LEADER) {
        return false;
    }

    const std::string replication_request =
        build_replication_request(topic, partition, offset, message);

    ack_counts_[make_ack_key(topic, partition, offset)] = 1;
    ack_followers_[make_ack_key(topic, partition, offset)].insert(
        config_.get_broker_id()
    );

    if (config_.get_peers().empty()) {
        return quorum_reached(topic, partition, offset);
    }

    bool dispatched = true;
    for (const ClusterPeer& peer : config_.get_peers()) {
        if (peer.broker_id == config_.get_broker_id()) {
            continue;
        }

        if (!is_peer_available(peer.broker_id)) {
            dispatched = false;
            continue;
        }

        if (!replicate_to_peer(
                peer,
                replication_request,
                topic,
                partition,
                offset,
                *this
            )) {
            mark_follower_failed(peer.broker_id);
            dispatched = false;
        } else {
            last_heartbeat_[peer.broker_id] = std::chrono::steady_clock::now();
            failed_peers_.erase(peer.broker_id);
        }
    }

    return dispatched;
}

void ReplicationManager::handle_replication_request(
    const Request& request,
    int source_socket
) {
    if (role_ != BrokerType::FOLLOWER || request.type != RequestType::REPLICATE) {
        return;
    }

    if (request.topic.empty() || request.partition < 0 || request.message.empty()) {
        return;
    }

    const uint64_t appended_offset = storage_.append(
        request.topic,
        request.partition,
        request.message
    );

    if (appended_offset != request.fetch_offset) {
        return;
    }

    send_ack(
        source_socket,
        request.fetch_offset,
        request.topic,
        request.partition
    );
}

bool ReplicationManager::wait_for_quorum(
    const std::string& topic,
    int partition,
    uint64_t offset,
    std::chrono::milliseconds timeout
) const {
    const auto deadline = std::chrono::steady_clock::now() + timeout;

    while (std::chrono::steady_clock::now() < deadline) {
        if (quorum_reached(topic, partition, offset)) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    return quorum_reached(topic, partition, offset);
}

void ReplicationManager::handle_ack(
    int follower_id,
    const std::string& topic,
    int partition,
    uint64_t offset
) {
    if (role_ != BrokerType::LEADER) {
        return;
    }

    const std::string ack_key = make_ack_key(topic, partition, offset);
    auto& followers = ack_followers_[ack_key];
    if (followers.count(follower_id) > 0) {
        return;
    }

    followers.insert(follower_id);
    follower_ack_offsets_[follower_id] = std::max(
        follower_ack_offsets_[follower_id],
        offset
    );
    ack_counts_[ack_key] += 1;
}

bool ReplicationManager::quorum_reached(
    const std::string& topic,
    int partition,
    uint64_t offset
) const {
    const int total_replicas = 1 + static_cast<int>(config_.get_peers().size());
    const int quorum = total_replicas / 2 + 1;

    const auto count_it = ack_counts_.find(make_ack_key(topic, partition, offset));
    if (count_it == ack_counts_.end()) {
        return false;
    }

    return count_it->second >= quorum;
}

void ReplicationManager::mark_committed(
    const std::string& topic,
    int partition,
    uint64_t offset
) {
    const std::string log_key = make_log_key(topic, partition);
    uint64_t& committed = commit_index_by_log_[log_key];
    if (offset > committed) {
        committed = offset;
    }

    storage_.flush(topic, partition);
}

bool ReplicationManager::is_committed(
    const std::string& topic,
    int partition,
    uint64_t offset
) const {
    const auto it = commit_index_by_log_.find(make_log_key(topic, partition));
    if (it == commit_index_by_log_.end()) {
        return false;
    }

    return offset <= it->second;
}

uint64_t ReplicationManager::get_commit_index(
    const std::string& topic,
    int partition
) const {
    const auto it = commit_index_by_log_.find(make_log_key(topic, partition));
    if (it == commit_index_by_log_.end()) {
        return uint64_t(-1);
    }

    return it->second;
}

void ReplicationManager::broadcast_commit(
    const std::string& topic,
    int partition,
    uint64_t offset
) {
    if (role_ != BrokerType::LEADER) {
        return;
    }

    const std::string commit_request = build_commit_request(topic, partition, offset);

    for (const ClusterPeer& peer : config_.get_peers()) {
        if (peer.broker_id == config_.get_broker_id()) {
            continue;
        }

        if (!is_peer_available(peer.broker_id)) {
            continue;
        }

        if (!send_one_shot_to_peer(peer, commit_request)) {
            mark_follower_failed(peer.broker_id);
        } else {
            last_heartbeat_[peer.broker_id] = std::chrono::steady_clock::now();
            failed_peers_.erase(peer.broker_id);
        }
    }
}

void ReplicationManager::handle_commit_request(const Request& request) {
    if (request.type != RequestType::COMMIT) {
        return;
    }

    if (request.topic.empty() || request.partition < 0) {
        return;
    }

    mark_committed(request.topic, request.partition, request.fetch_offset);
}

void ReplicationManager::broadcast_heartbeat() {
    const auto now = std::chrono::steady_clock::now();

    for (auto it = last_heartbeat_.begin(); it != last_heartbeat_.end();) {
        if (now - it->second > kHeartbeatTimeout) {
            const int broker_id = it->first;
            ++it;
            mark_follower_failed(broker_id);
        } else {
            ++it;
        }
    }

    const Request heartbeat{
        RequestType::HEARTBEAT,
        "",
        0,
        "",
        0,
        config_.get_broker_id()
    };
    const std::string payload = encode_request(heartbeat);

    for (const ClusterPeer& peer : config_.get_peers()) {
        if (peer.broker_id == config_.get_broker_id()) {
            continue;
        }

        if (!send_one_shot_to_peer(peer, payload)) {
            mark_follower_failed(peer.broker_id);
        } else {
            last_heartbeat_[peer.broker_id] = std::chrono::steady_clock::now();
            failed_peers_.erase(peer.broker_id);
        }
    }
}

void ReplicationManager::handle_heartbeat(int broker_id) {
    last_heartbeat_[broker_id] = std::chrono::steady_clock::now();
    failed_peers_.erase(broker_id);
}

void ReplicationManager::mark_follower_failed(int broker_id) {
    failed_peers_.insert(broker_id);
    follower_ack_offsets_.erase(broker_id);
}

bool ReplicationManager::send_ack(
    int socket_fd,
    uint64_t offset,
    const std::string& topic,
    int partition
) {
    if (socket_fd < 0) {
        return false;
    }

    const Request ack{
        RequestType::ACK,
        topic,
        partition,
        "",
        offset,
        config_.get_broker_id()
    };
    return write_frame(socket_fd, encode_request(ack));
}

std::string ReplicationManager::build_replication_request(
    const std::string& topic,
    int partition,
    uint64_t offset,
    const std::string& message
) {
    const Request request{
        RequestType::REPLICATE,
        topic,
        partition,
        message,
        offset,
        0
    };
    return encode_request(request);
}

std::string ReplicationManager::build_commit_request(
    const std::string& topic,
    int partition,
    uint64_t offset
) {
    const Request request{
        RequestType::COMMIT,
        topic,
        partition,
        "",
        offset,
        0
    };
    return encode_request(request);
}
