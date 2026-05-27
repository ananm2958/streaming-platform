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

namespace {

constexpr auto kQuorumTimeout = std::chrono::milliseconds(5000);
constexpr auto kHeartbeatInterval = std::chrono::seconds(2);

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
        case FETCH:
            return handle_fetch(request);
        case METADATA:
            return handle_metadata();
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

Response Broker::handle_produce(const Request& request) {
    if (config_.get_type() != LEADER) {
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
