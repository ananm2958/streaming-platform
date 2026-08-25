#include "broker.h"

#include "../protocol/frame.h"
#include "../protocol/protocol.h"

#include <arpa/inet.h>
#include <cstdint>
#include <exception>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <sstream>
#include <vector>

namespace {

constexpr auto kQuorumTimeout = std::chrono::milliseconds(5000);
constexpr auto kHeartbeatInterval = std::chrono::milliseconds(100);

bool connect_to_host(const std::string& host, int port, int& socket_fd) {
    socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd < 0) {
        return false;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<uint16_t>(port));

    if (inet_pton(AF_INET, host.c_str(), &address.sin_addr) <= 0) {
        close(socket_fd);
        return false;
    }

    if (connect(socket_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        close(socket_fd);
        return false;
    }

    return true;
}

}  

Broker::Broker(
    const BrokerConfig& config,
    StorageEngine& storage,
    ReplicationManager& replication
)
    : config_(config),
      storage_(storage),
      replication_(replication),
      running_(false) {}

Broker::~Broker() {
    stop();
}

void Broker::start() {
    if (running_.exchange(true)) {
        return;
    }

    heartbeat_thread_ = std::thread(&Broker::heartbeat_loop, this);
}

void Broker::stop() {
    if (!running_.exchange(false)) {
        return;
    }

    if (heartbeat_thread_.joinable()) {
        heartbeat_thread_.join();
    }
}

void Broker::heartbeat_loop() {
    while (running_) {
        replication_.broadcast_heartbeat();
        std::this_thread::sleep_for(kHeartbeatInterval);
    }
}

Response Broker::handle_request(const Request& request) {
    switch (request.type) {
        case PRODUCE:
            return handle_produce(request);
        case BATCH_PRODUCE:
            return handle_batch_produce(request);
        case FETCH:
            return handle_fetch(request);
        case METADATA:
            return handle_metadata();
        case JOIN_GROUP:
            return handle_join_group(request);
        case LEAVE_GROUP:
            return handle_leave_group(request);
        case GROUP_FETCH:
            return handle_group_fetch(request);
        case COMMIT_OFFSET:
            return handle_commit_offset(request);
        case GROUP_ASSIGNMENT:
            return handle_group_assignment(request);
        default:
            return Response{false, "unknown_request_type", uint64_t(-1)};
    }
}

std::string Broker::handle_inter_broker_request(
    const Request& request,
    int client_socket
) {
    switch (request.type) {
        case REPLICATE:
        case BATCH_REPLICATE:
            replication_.handle_replication_request(request, client_socket);
            return serialize_response(Response{true, "", 0});
        case ACK:
            replication_.handle_ack(
                request.broker_id,
                request.topic,
                request.partition,
                request.fetch_offset
            );
            return serialize_response(Response{true, "", 0});
        case HEARTBEAT:
            replication_.handle_heartbeat(request.broker_id);
            return serialize_response(Response{true, "", 0});
        case COMMIT:
            replication_.handle_commit_request(request);
            return serialize_response(Response{true, "", 0});
        default:
            return serialize_response(
                Response{false, "unknown_inter_broker_request", uint64_t(-1)}
            );
    }
}

std::string Broker::format_leader_redirect() const {
    LeaderEndpoint leader;
    if (!config_.get_leader_endpoint(leader)) {
        return "not_leader:leader_unknown";
    }

    std::ostringstream redirect;
    redirect << "not_leader:" << leader.broker_id << ":" << leader.host << ":"
             << leader.port;
    return redirect.str();
}

Response Broker::handle_metadata() {
    LeaderEndpoint leader;
    if (!config_.get_leader_endpoint(leader)) {
        return Response{false, "leader_unknown", uint64_t(-1)};
    }

    std::ostringstream payload;
    payload << leader.broker_id << ":" << leader.host << ":" << leader.port;

    const bool is_leader = config_.get_broker_id() == leader.broker_id;
    payload << " role:" << (is_leader ? "leader" : "follower");
    payload << " self:" << config_.get_broker_id();

    return Response{true, payload.str(), 0};
}

Response Broker::forward_produce_to_leader(const Request& request) {
    return forward_to_leader(request);
}

Response Broker::forward_to_leader(const Request& request) {
    LeaderEndpoint leader;
    if (!config_.get_leader_endpoint(leader)) {
        return Response{false, "leader_unknown", uint64_t(-1)};
    }

    int socket_fd = -1;
    if (!connect_to_host(leader.host, leader.port, socket_fd)) {
        return Response{false, "leader_unreachable", uint64_t(-1)};
    }

    if (!write_frame(socket_fd, encode_request(request))) {
        close(socket_fd);
        return Response{false, "leader_write_failed", uint64_t(-1)};
    }

    std::string response_payload;
    if (!read_frame(socket_fd, response_payload)) {
        close(socket_fd);
        return Response{false, "leader_read_failed", uint64_t(-1)};
    }

    close(socket_fd);
    return parse_response(response_payload);
}

std::vector<int> Broker::topic_partitions(const std::string& topic) const {
    return storage_.partitions_for_topic(topic);
}

std::string Broker::format_partitions(const std::vector<int>& partitions) {
    std::ostringstream result;
    for (size_t i = 0; i < partitions.size(); ++i) {
        if (i) result << ',';
        result << partitions[i];
    }
    return result.str();
}

Response Broker::handle_join_group(const Request& request) {
    if (!replication_.is_leader()) return forward_to_leader(request);
    if (request.group_id.empty() || request.member_id.empty() || request.topic.empty())
        return Response{false, "invalid_group_request", uint64_t(-1)};
    const auto partitions = topic_partitions(request.topic);
    if (partitions.empty()) return Response{false, "topic_not_found", uint64_t(-1)};
    const auto assigned = groups_.join(request.group_id, request.member_id, request.topic, partitions);
    return Response{true, "assignment:" + format_partitions(assigned), 0};
}

Response Broker::handle_leave_group(const Request& request) {
    if (!replication_.is_leader()) return forward_to_leader(request);
    if (request.group_id.empty() || request.member_id.empty() || request.topic.empty())
        return Response{false, "invalid_group_request", uint64_t(-1)};
    groups_.leave(request.group_id, request.member_id, topic_partitions(request.topic));
    return Response{true, "", 0};
}

Response Broker::handle_group_assignment(const Request& request) {
    if (!replication_.is_leader()) return forward_to_leader(request);
    if (request.group_id.empty() || request.member_id.empty() || request.topic.empty())
        return Response{false, "invalid_group_request", uint64_t(-1)};
    std::vector<int> assigned;
    for (int partition : topic_partitions(request.topic))
        if (groups_.is_assigned(request.group_id, request.member_id, request.topic, partition)) assigned.push_back(partition);
    return Response{true, "assignment:" + format_partitions(assigned), 0};
}

Response Broker::handle_group_fetch(const Request& request) {
    if (!replication_.is_leader()) return forward_to_leader(request);
    if (request.group_id.empty() || request.member_id.empty() || request.topic.empty() || request.partition < 0)
        return Response{false, "invalid_group_request", uint64_t(-1)};
    if (!groups_.is_assigned(request.group_id, request.member_id, request.topic, request.partition))
        return Response{false, "partition_not_assigned", uint64_t(-1)};
    Request fetch = request;
    fetch.type = FETCH;
    fetch.fetch_offset = groups_.next_offset(request.group_id, request.topic, request.partition);
    return handle_fetch(fetch);
}

Response Broker::handle_commit_offset(const Request& request) {
    if (!replication_.is_leader()) return forward_to_leader(request);
    if (request.group_id.empty() || request.member_id.empty() || request.topic.empty() || request.partition < 0)
        return Response{false, "invalid_group_request", uint64_t(-1)};
    if (!groups_.commit(request.group_id, request.member_id, request.topic, request.partition, request.fetch_offset))
        return Response{false, "partition_not_assigned", uint64_t(-1)};
    return Response{true, "", request.fetch_offset};
}

Response Broker::handle_produce(const Request& request) {
    if (!replication_.is_leader()) {
        return forward_produce_to_leader(request);
    }

    if (request.topic.empty()) {
        return Response{false, "empty_topic", uint64_t(-1)};
    }

    if (request.partition < 0) {
        return Response{false, "invalid_partition", uint64_t(-1)};
    }

    if (request.message.empty()) {
        return Response{false, "empty_message", uint64_t(-1)};
    }

    const uint64_t offset = storage_.append(
        request.topic,
        request.partition,
        request.message
    );
    groups_.refresh_topic(request.topic, topic_partitions(request.topic));

    if (!replication_.replicate_message(
            request.topic,
            request.partition,
            offset,
            request.message
        )) {
        return Response{false, "replication_failed", offset};
    }

    if (!replication_.wait_for_quorum(
            request.topic,
            request.partition,
            offset,
            kQuorumTimeout
        )) {
        return Response{false, "quorum_timeout", offset};
    }

    replication_.mark_committed(request.topic, request.partition, offset);
    replication_.broadcast_commit(request.topic, request.partition, offset);

    return Response{true, "", offset};
}

Response Broker::handle_batch_produce(const Request& request) {
    if (request.messages.empty()) {
        return Response{false, "empty_batch", uint64_t(-1)};
    }
    if (!replication_.is_leader()) {
        return forward_to_leader(request);
    }

    if (request.topic.empty() || request.partition < 0) return Response{false, "invalid_batch", uint64_t(-1)};
    Response result{true, "", 0}; result.appended_offsets.reserve(request.messages.size());
    const uint64_t first_offset = storage_.append(request.topic, request.partition, request.messages.front());
    result.appended_offsets.push_back(first_offset);
    for (size_t i = 1; i < request.messages.size(); ++i)
        result.appended_offsets.push_back(storage_.append(request.topic, request.partition, request.messages[i]));
    groups_.refresh_topic(request.topic, topic_partitions(request.topic));
    const uint64_t last_offset = result.appended_offsets.back();
    if (!replication_.replicate_batch(request.topic, request.partition, first_offset, request.messages) ||
        !replication_.wait_for_quorum(request.topic, request.partition, last_offset, kQuorumTimeout))
        return Response{false, "quorum_timeout", last_offset};
    for (uint64_t offset : result.appended_offsets) {
        replication_.mark_committed(request.topic, request.partition, offset);
        replication_.broadcast_commit(request.topic, request.partition, offset);
    }
    result.appended_offset = last_offset;
    return result;
}

Response Broker::handle_fetch(const Request& request) {
    if (request.topic.empty()) {
        return Response{false, "empty_topic", uint64_t(-1)};
    }

    if (request.partition < 0) {
        return Response{false, "invalid_partition", uint64_t(-1)};
    }

    if (!replication_.is_committed(
            request.topic,
            request.partition,
            request.fetch_offset
        )) {
        return Response{false, "offset_not_committed", uint64_t(-1)};
    }

    try {
        const std::string message = storage_.fetch(
            request.topic,
            request.partition,
            request.fetch_offset
        );

        if (message.empty()) {
            return Response{false, "offset_not_found", uint64_t(-1)};
        }

        return Response{true, message, request.fetch_offset};
    } catch (const std::exception&) {
        return Response{false, "offset_not_found", uint64_t(-1)};
    }
}
