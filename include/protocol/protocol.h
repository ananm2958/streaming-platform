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
};

struct Response {
    bool success;
    std::string message;
    uint64_t appended_offset;
};

constexpr char kFieldSeparator = '\x1f';

std::vector<std::string> split_fields(const std::string& payload);

std::string join_fields(const std::vector<std::string>& fields);

Request parse_request(const std::string& payload);

std::string encode_request(const Request& request);

Response parse_response(const std::string& payload);

std::string serialize_response(const Response& response);
