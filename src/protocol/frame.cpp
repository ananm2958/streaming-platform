#include "frame.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <sys/socket.h>
#include <sys/uio.h>
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
    // Scatter/gather I/O writes the header and caller-owned payload directly;
    // the old implementation allocated and copied a combined frame per send.
    const uint32_t network_length = htonl(static_cast<uint32_t>(payload.size()));
    iovec buffers[2] = {
        {const_cast<uint32_t*>(&network_length), sizeof(network_length)},
        {const_cast<char*>(payload.data()), payload.size()}
    };
    iovec* current = buffers;
    int buffer_count = payload.empty() ? 1 : 2;
    size_t total_sent = 0;
    const size_t frame_size = sizeof(network_length) + payload.size();
    while (total_sent < frame_size) {
        const ssize_t sent = writev(socket_fd, current, buffer_count);
        if (sent <= 0) {
            return false;
        }
        total_sent += static_cast<size_t>(sent);
        size_t consumed = static_cast<size_t>(sent);
        while (buffer_count > 0 && consumed >= current[0].iov_len) {
            consumed -= current[0].iov_len;
            ++current;
            --buffer_count;
        }
        if (buffer_count > 0 && consumed > 0) {
            current[0].iov_base = static_cast<char*>(current[0].iov_base) + consumed;
            current[0].iov_len -= consumed;
        }
    }
    return true;
}
