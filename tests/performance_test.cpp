#include "../include/protocol/frame.h"
#include "../include/protocol/protocol.h"

#include <arpa/inet.h>
#include <chrono>
#include <csignal>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

namespace {
constexpr int kPort = 19100;
constexpr size_t kMessages = 256;

Response send_request(const Request& request) {
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET; address.sin_port = htons(kPort);
    inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);
    if (fd < 0 || connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) return {false, "connect_failed", uint64_t(-1)};
    if (!write_frame(fd, encode_request(request))) { close(fd); return {false, "write_failed", uint64_t(-1)}; }
    std::string response; const bool read = read_frame(fd, response); close(fd);
    return read ? parse_response(response) : Response{false, "read_failed", uint64_t(-1)};
}
}

int main() {
    const pid_t broker = fork();
    if (broker == 0) {
        execl("./broker", "./broker", "--id", "10", "--port", "19100", "--host", "127.0.0.1", "--role", "leader", "--data-dir", "/tmp/streaming-platform-perf", nullptr);
        _exit(1);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    const std::string message(512, 'p');
    Request batch{RequestType::BATCH_PRODUCE, "throughput", 0, "", 0, 0};
    batch.messages.assign(kMessages, message);
    const auto started = std::chrono::steady_clock::now();
    const Response response = send_request(batch);
    if (!response.success || response.appended_offsets.size() != kMessages) {
        kill(broker, SIGTERM); waitpid(broker, nullptr, 0); return 1;
    }
    const double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    std::cout << "localhost TCP batched 512-byte throughput: " << (kMessages * message.size() / 1024.0 / 1024.0 / seconds) << " MiB/s\n";
    kill(broker, SIGTERM); waitpid(broker, nullptr, 0);
    return 0;
}
