#include "../include/protocol/protocol.h"

#include <cassert>
#include <iostream>
#include <string>

int main() {
    const Request produce{
        RequestType::PRODUCE,
        "orders",
        0,
        "hello world",
        0,
        0
    };

    const std::string encoded = encode_request(produce);
    const Request parsed = parse_request(encoded);

    assert(parsed.type == RequestType::PRODUCE);
    assert(parsed.topic == "orders");
    assert(parsed.partition == 0);
    assert(parsed.message == "hello world");
    assert(encoded.size() > 2);
    assert(encoded[0] == 1);  // binary protocol version, not an ASCII command.

    const Response response{true, "payload with spaces and|pipes", 7};
    const std::string serialized = serialize_response(response);
    const Response roundtrip = parse_response(serialized);

    assert(roundtrip.success);
    assert(roundtrip.message == "payload with spaces and|pipes");

    const Request replicate{
        RequestType::REPLICATE,
        "events",
        1,
        "multi word event body",
        42,
        0
    };
    const Request parsed_replicate = parse_request(encode_request(replicate));
    assert(parsed_replicate.message == "multi word event body");
    assert(parsed_replicate.fetch_offset == 42);

    const Request group_fetch{
        RequestType::GROUP_FETCH, "orders", 2, "", 0, 0, "workers", "consumer-a"
    };
    const Request parsed_group_fetch = parse_request(encode_request(group_fetch));
    assert(parsed_group_fetch.type == RequestType::GROUP_FETCH);
    assert(parsed_group_fetch.group_id == "workers");
    assert(parsed_group_fetch.member_id == "consumer-a");
    assert(parsed_group_fetch.partition == 2);

    const Request batch{
        RequestType::BATCH_PRODUCE, "orders", 1, "", 0, 0, "", "",
        {"first", "second\x1fwith separator", "third"}
    };
    const Request parsed_batch = parse_request(encode_request(batch));
    assert(parsed_batch.type == RequestType::BATCH_PRODUCE);
    assert(parsed_batch.messages == batch.messages);

    const Response batch_response{true, "", 3, {1, 2, 3}};
    const Response parsed_batch_response = parse_response(serialize_response(batch_response));
    assert(parsed_batch_response.success);
    assert(parsed_batch_response.appended_offsets == batch_response.appended_offsets);

    std::cout << "protocol_test passed\n";
    return 0;
}
