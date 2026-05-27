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

    std::cout << "protocol_test passed\n";
    return 0;
}
