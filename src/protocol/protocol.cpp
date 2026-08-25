#include "protocol.h"

#include <limits>

namespace {
constexpr uint8_t kProtocolVersion = 1;
constexpr size_t kMaxStringBytes = 16 * 1024 * 1024;
constexpr size_t kMaxBatchMessages = 10000;

void put_u8(std::string& out, uint8_t value) { out.push_back(static_cast<char>(value)); }
void put_u16(std::string& out, uint16_t value) {
    put_u8(out, value >> 8); put_u8(out, value);
}
void put_u32(std::string& out, uint32_t value) {
    for (int shift = 24; shift >= 0; shift -= 8) put_u8(out, value >> shift);
}
void put_u64(std::string& out, uint64_t value) {
    for (int shift = 56; shift >= 0; shift -= 8) put_u8(out, value >> shift);
}
bool get_u8(const std::string& in, size_t& pos, uint8_t& value) {
    if (pos >= in.size()) return false; value = static_cast<uint8_t>(in[pos++]); return true;
}
bool get_u16(const std::string& in, size_t& pos, uint16_t& value) {
    uint8_t a, b; if (!get_u8(in,pos,a) || !get_u8(in,pos,b)) return false; value = (uint16_t(a)<<8)|b; return true;
}
bool get_u32(const std::string& in, size_t& pos, uint32_t& value) {
    value = 0; for (int i=0;i<4;++i) { uint8_t byte; if (!get_u8(in,pos,byte)) return false; value=(value<<8)|byte; } return true;
}
bool get_u64(const std::string& in, size_t& pos, uint64_t& value) {
    value = 0; for (int i=0;i<8;++i) { uint8_t byte; if (!get_u8(in,pos,byte)) return false; value=(value<<8)|byte; } return true;
}
void put_string(std::string& out, const std::string& value) { put_u32(out, static_cast<uint32_t>(value.size())); out += value; }
bool get_string(const std::string& in, size_t& pos, std::string& value) {
    uint32_t size; if (!get_u32(in,pos,size) || size > kMaxStringBytes || size > in.size()-pos) return false;
    value.assign(in.data()+pos, size); pos += size; return true;
}
void put_topic_partition(std::string& out, const Request& request) { put_string(out, request.topic); put_u32(out, static_cast<uint32_t>(request.partition)); }
bool get_topic_partition(const std::string& in, size_t& pos, Request& request) {
    uint32_t partition; return get_string(in,pos,request.topic) && get_u32(in,pos,partition) && (request.partition=static_cast<int32_t>(partition), true);
}
}

RequestType string_to_request(const std::string&) { return RequestType::ERROR; }

Request parse_request(const std::string& payload) {
    Request request{RequestType::ERROR, "", 0, "", 0, 0, "", "", {}};
    size_t pos = 0; uint8_t version, type;
    if (!get_u8(payload,pos,version) || version != kProtocolVersion || !get_u8(payload,pos,type) || type > BATCH_REPLICATE) return request;
    request.type = static_cast<RequestType>(type);
    bool valid = true;
    switch (request.type) {
        case PRODUCE: valid = get_topic_partition(payload,pos,request) && get_string(payload,pos,request.message) && get_u64(payload,pos,request.fetch_offset); break;
        case BATCH_PRODUCE: {
            uint32_t count; valid = get_topic_partition(payload,pos,request) && get_u32(payload,pos,count) && count <= kMaxBatchMessages;
            while (valid && request.messages.size() < count) { std::string message; valid = get_string(payload,pos,message); if (valid) request.messages.push_back(std::move(message)); }
            break;
        }
        case BATCH_REPLICATE: {
            uint32_t count; valid = get_topic_partition(payload,pos,request) && get_u64(payload,pos,request.fetch_offset) && get_u32(payload,pos,count) && count <= kMaxBatchMessages;
            while (valid && request.messages.size() < count) { std::string message; valid = get_string(payload,pos,message); if (valid) request.messages.push_back(std::move(message)); }
            break;
        }
        case FETCH: case COMMIT: valid = get_topic_partition(payload,pos,request) && get_u64(payload,pos,request.fetch_offset); break;
        case REPLICATE: valid = get_topic_partition(payload,pos,request) && get_u64(payload,pos,request.fetch_offset) && get_string(payload,pos,request.message); break;
        case ACK: { uint32_t broker; valid = get_u64(payload,pos,request.fetch_offset) && get_u32(payload,pos,broker) && (request.broker_id=static_cast<int32_t>(broker), true) && get_topic_partition(payload,pos,request); break; }
        case HEARTBEAT: { uint32_t broker; valid = get_u32(payload,pos,broker); request.broker_id=static_cast<int32_t>(broker); break; }
        case METADATA: break;
        case JOIN_GROUP: case LEAVE_GROUP: case GROUP_ASSIGNMENT:
            valid = get_string(payload,pos,request.group_id) && get_string(payload,pos,request.member_id) && get_string(payload,pos,request.topic); break;
        case GROUP_FETCH: case COMMIT_OFFSET:
            valid = get_string(payload,pos,request.group_id) && get_string(payload,pos,request.member_id) && get_topic_partition(payload,pos,request) && get_u64(payload,pos,request.fetch_offset); break;
        default: valid = false;
    }
    if (!valid || pos != payload.size()) request.type = RequestType::ERROR;
    return request;
}

std::string encode_request(const Request& request) {
    std::string out; put_u8(out,kProtocolVersion); put_u8(out,static_cast<uint8_t>(request.type));
    switch (request.type) {
        case PRODUCE: put_topic_partition(out,request); put_string(out,request.message); put_u64(out,request.fetch_offset); break;
        case BATCH_PRODUCE: put_topic_partition(out,request); put_u32(out,static_cast<uint32_t>(request.messages.size())); for (const auto& message:request.messages) put_string(out,message); break;
        case BATCH_REPLICATE: put_topic_partition(out,request); put_u64(out,request.fetch_offset); put_u32(out,static_cast<uint32_t>(request.messages.size())); for (const auto& message:request.messages) put_string(out,message); break;
        case FETCH: case COMMIT: put_topic_partition(out,request); put_u64(out,request.fetch_offset); break;
        case REPLICATE: put_topic_partition(out,request); put_u64(out,request.fetch_offset); put_string(out,request.message); break;
        case ACK: put_u64(out,request.fetch_offset); put_u32(out,static_cast<uint32_t>(request.broker_id)); put_topic_partition(out,request); break;
        case HEARTBEAT: put_u32(out,static_cast<uint32_t>(request.broker_id)); break;
        case METADATA: break;
        case JOIN_GROUP: case LEAVE_GROUP: case GROUP_ASSIGNMENT: put_string(out,request.group_id); put_string(out,request.member_id); put_string(out,request.topic); break;
        case GROUP_FETCH: case COMMIT_OFFSET: put_string(out,request.group_id); put_string(out,request.member_id); put_topic_partition(out,request); put_u64(out,request.fetch_offset); break;
        default: return std::string();
    }
    return out;
}

Response parse_response(const std::string& payload) {
    Response response{false,"malformed_response",uint64_t(-1),{}}; size_t pos=0; uint8_t version, success, kind;
    if (!get_u8(payload,pos,version) || version != kProtocolVersion || !get_u8(payload,pos,success) || !get_u8(payload,pos,kind)) return response;
    response.success = success != 0;
    if (!get_u64(payload,pos,response.appended_offset) || !get_string(payload,pos,response.message)) return Response{false,"malformed_response",uint64_t(-1),{}};
    uint32_t count; if (!get_u32(payload,pos,count) || count > kMaxBatchMessages) return Response{false,"malformed_response",uint64_t(-1),{}};
    for (uint32_t i=0;i<count;++i) { uint64_t offset; if (!get_u64(payload,pos,offset)) return Response{false,"malformed_response",uint64_t(-1),{}}; response.appended_offsets.push_back(offset); }
    if (pos != payload.size()) return Response{false,"malformed_response",uint64_t(-1),{}};
    return response;
}

std::string serialize_response(const Response& response) {
    std::string out; put_u8(out,kProtocolVersion); put_u8(out,response.success ? 1 : 0); put_u8(out, response.message.empty() ? 0 : 1);
    put_u64(out,response.appended_offset); put_string(out,response.message); put_u32(out,static_cast<uint32_t>(response.appended_offsets.size()));
    for (uint64_t offset:response.appended_offsets) put_u64(out,offset); return out;
}
