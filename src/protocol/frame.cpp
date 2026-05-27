#include "frame.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <sys/socket.h>
#include <unistd.h>

#include <string>

namespace {

bool read_exact(int socket_fd, char* buffer, size_t length) {
    size_t total_read = 0;
    while (total_read < length) {
        const ssize_t bytes = recv(
            socket_fd,
            buffer + total_read,
            length - total_read,
            0
        );
        if (bytes <= 0) {
            return false;
        }
        total_read += static_cast<size_t>(bytes);
    }
    return true;
}

}  // namespace

std::string wrap_frame(const std::string& payload) {
    const uint32_t length = htonl(static_cast<uint32_t>(payload.size()));

    std::string frame;
    frame.resize(sizeof(length) + payload.size());
    std::memcpy(frame.data(), &length, sizeof(length));
    std::memcpy(frame.data() + sizeof(length), payload.data(), payload.size());
    return frame;
}

bool read_frame(int socket_fd, std::string& payload) {
    uint32_t network_length = 0;
    if (!read_exact(
            socket_fd,
            reinterpret_cast<char*>(&network_length),
            sizeof(network_length)
        )) {
        return false;
    }

    const uint32_t length = ntohl(network_length);
    if (length == 0) {
        payload.clear();
        return true;
    }

    payload.resize(length);
    return read_exact(socket_fd, payload.data(), length);
}

bool write_frame(int socket_fd, const std::string& payload) {
    const std::string frame = wrap_frame(payload);
    size_t total_sent = 0;
    while (total_sent < frame.size()) {
        const ssize_t sent = send(
            socket_fd,
            frame.data() + total_sent,
            frame.size() - total_sent,
            0
        );
        if (sent <= 0) {
            return false;
        }
        total_sent += static_cast<size_t>(sent);
    }
    return true;
}
