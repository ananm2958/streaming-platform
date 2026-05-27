#include "protocol.h"

#include <cstdint>
#include <sstream>
#include <stdexcept>
#include <string>

RequestType string_to_request(const std::string& s) {
    if (s == "PRODUCE") {
        return RequestType::PRODUCE;
    }
    if (s == "FETCH") {
        return RequestType::FETCH;
    }
    if (s == "REPLICATE") {
        return RequestType::REPLICATE;
    }
    if (s == "ACK") {
        return RequestType::ACK;
    }
    if (s == "HEARTBEAT") {
        return RequestType::HEARTBEAT;
    }
    if (s == "COMMIT") {
        return RequestType::COMMIT;
    }
    if (s == "METADATA") {
        return RequestType::METADATA;
    }
    return RequestType::ERROR;
}

std::vector<std::string> split_fields(const std::string& payload) {
    std::vector<std::string> fields;
    std::string current;

    for (char character : payload) {
        if (character == kFieldSeparator) {
            fields.push_back(current);
            current.clear();
        } else {
            current.push_back(character);
        }
    }

    fields.push_back(current);
    return fields;
}

std::string join_fields(const std::vector<std::string>& fields) {
    std::ostringstream joined;
    for (size_t i = 0; i < fields.size(); ++i) {
        if (i > 0) {
            joined << kFieldSeparator;
        }
        joined << fields[i];
    }
    return joined.str();
}

Request parse_request(const std::string& payload) {
    const std::vector<std::string> fields = split_fields(payload);
    if (fields.empty()) {
        return Request{RequestType::ERROR, "", 0, "", 0, 0};
    }

    const RequestType type = string_to_request(fields[0]);
    Request request{type, "", 0, "", 0, 0};

    switch (type) {
        case PRODUCE:
            if (fields.size() >= 5) {
                request.topic = fields[1];
                request.partition = std::stoi(fields[2]);
                request.message = fields[3];
                request.fetch_offset = std::stoull(fields[4]);
            }
            break;
        case FETCH:
            if (fields.size() >= 4) {
                request.topic = fields[1];
                request.partition = std::stoi(fields[2]);
                request.fetch_offset = std::stoull(fields[3]);
            }
            break;
        case REPLICATE:
            if (fields.size() >= 5) {
                request.topic = fields[1];
                request.partition = std::stoi(fields[2]);
                request.fetch_offset = std::stoull(fields[3]);
                request.message = fields[4];
            }
            break;
        case ACK:
            if (fields.size() >= 5) {
                request.fetch_offset = std::stoull(fields[1]);
                request.broker_id = std::stoi(fields[2]);
                request.topic = fields[3];
                request.partition = std::stoi(fields[4]);
            }
            break;
        case HEARTBEAT:
            if (fields.size() >= 2) {
                request.broker_id = std::stoi(fields[1]);
            }
            break;
        case COMMIT:
            if (fields.size() >= 4) {
                request.topic = fields[1];
                request.partition = std::stoi(fields[2]);
                request.fetch_offset = std::stoull(fields[3]);
            }
            break;
        case METADATA:
            break;
        default:
            break;
    }

    return request;
}

std::string encode_request(const Request& request) {
    switch (request.type) {
        case PRODUCE:
            return join_fields(
                {"PRODUCE",
                 request.topic,
                 std::to_string(request.partition),
                 request.message,
                 std::to_string(request.fetch_offset)}
            );
        case FETCH:
            return join_fields(
                {"FETCH",
                 request.topic,
                 std::to_string(request.partition),
                 std::to_string(request.fetch_offset)}
            );
        case REPLICATE:
            return join_fields(
                {"REPLICATE",
                 request.topic,
                 std::to_string(request.partition),
                 std::to_string(request.fetch_offset),
                 request.message}
            );
        case ACK:
            return join_fields(
                {"ACK",
                 std::to_string(request.fetch_offset),
                 std::to_string(request.broker_id),
                 request.topic,
                 std::to_string(request.partition)}
            );
        case HEARTBEAT:
            return join_fields({"HEARTBEAT", std::to_string(request.broker_id)});
        case COMMIT:
            return join_fields(
                {"COMMIT",
                 request.topic,
                 std::to_string(request.partition),
                 std::to_string(request.fetch_offset)}
            );
        case METADATA:
            return "METADATA";
        default:
            return "ERROR";
    }
}

Response parse_response(const std::string& payload) {
    const std::vector<std::string> fields = split_fields(payload);
    if (fields.empty()) {
        return Response{false, "empty_response", uint64_t(-1)};
    }

    if (fields[0] == "OK") {
        const uint64_t offset =
            fields.size() >= 2 ? std::stoull(fields[1]) : uint64_t(0);
        return Response{true, "", offset};
    }

    if (fields[0] == "MESSAGE") {
        const std::string message =
            fields.size() >= 2 ? fields[1] : "";
        return Response{true, message, 0};
    }

    if (fields[0] == "ERROR") {
        const std::string message =
            fields.size() >= 2 ? fields[1] : "unknown_error";
        return Response{false, message, uint64_t(-1)};
    }

    return Response{false, "unknown_response", uint64_t(-1)};
}

std::string serialize_response(const Response& response) {
    if (!response.success) {
        return join_fields({"ERROR", response.message});
    }

    if (!response.message.empty()) {
        return join_fields({"MESSAGE", response.message});
    }

    return join_fields({"OK", std::to_string(response.appended_offset)});
}
