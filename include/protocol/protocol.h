#pragma once

#include <cstdint>
#include <string>
#include <vector>

enum RequestType {
    PRODUCE,
    FETCH,
    REPLICATE,
    ACK,
    HEARTBEAT,
    COMMIT,
    METADATA,
    JOIN_GROUP,
    LEAVE_GROUP,
    GROUP_FETCH,
    COMMIT_OFFSET,
    GROUP_ASSIGNMENT,
    BATCH_PRODUCE,
    BATCH_REPLICATE,
    ERROR
};

RequestType string_to_request(const std::string& s);

struct Request {
    RequestType type;
    std::string topic;
    int partition;
    std::string message;
    uint64_t fetch_offset;
    int broker_id;
    std::string group_id;
    std::string member_id;
    std::vector<std::string> messages;
};

struct Response {
    bool success;
    std::string message;
    uint64_t appended_offset;
    std::vector<uint64_t> appended_offsets;
};

Request parse_request(const std::string& payload);

std::string encode_request(const Request& request);

Response parse_response(const std::string& payload);

std::string serialize_response(const Response& response);
