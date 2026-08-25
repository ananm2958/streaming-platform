#include "../include/protocol/frame.h"
#include "../include/protocol/protocol.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace {

constexpr int kLeaderPort = 19092;
constexpr int kFollower2Port = 19093;
constexpr int kFollower3Port = 19094;
constexpr char kHost[] = "127.0.0.1";
constexpr char kDataDir[] = "/tmp/streaming-platform-test";
constexpr auto kFailoverDeadline = std::chrono::seconds(12);
constexpr auto kFailoverPollInterval = std::chrono::milliseconds(50);

struct BrokerProcess {
    pid_t pid;
};

bool connect_to_broker(int port, int& socket_fd) {
    socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd < 0) {
        return false;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(static_cast<uint16_t>(port));
    inet_pton(AF_INET, kHost, &address.sin_addr);

    return connect(socket_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0;
}

Response send_request(int port, const Request& request) {
    int socket_fd = -1;
    if (!connect_to_broker(port, socket_fd)) {
        return Response{false, "connect_failed", uint64_t(-1)};
    }

    if (!write_frame(socket_fd, encode_request(request))) {
        close(socket_fd);
        return Response{false, "write_failed", uint64_t(-1)};
    }

    std::string payload;
    if (!read_frame(socket_fd, payload)) {
        close(socket_fd);
        return Response{false, "read_failed", uint64_t(-1)};
    }

    close(socket_fd);
    return parse_response(payload);
}

BrokerProcess start_broker(
    int broker_id,
    int port,
    const char* role,
    const char* leader_id,
    const char* peers
) {
    const pid_t pid = fork();
    if (pid == 0) {
        const std::string id_arg = std::to_string(broker_id);
        const std::string port_arg = std::to_string(port);

        execl(
            "./broker",
            "./broker",
            "--id",
            id_arg.c_str(),
            "--port",
            port_arg.c_str(),
            "--host",
            kHost,
            "--role",
            role,
            "--leader-id",
            leader_id,
            "--peers",
            peers,
            "--data-dir",
            kDataDir,
            nullptr
        );
        std::_Exit(1);
    }

    return BrokerProcess{pid};
}

void stop_brokers(std::vector<BrokerProcess>& processes) {
    for (BrokerProcess& process : processes) {
        if (process.pid > 0) {
            kill(process.pid, SIGTERM);
        }
    }

    for (BrokerProcess& process : processes) {
        if (process.pid > 0) {
            waitpid(process.pid, nullptr, 0);
        }
    }
}

bool wait_for_ports() {
    for (int attempt = 0; attempt < 50; ++attempt) {
        int socket_fd = -1;
        if (connect_to_broker(kLeaderPort, socket_fd)) {
            close(socket_fd);
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
}

std::chrono::milliseconds measure_failover() {
    const auto started = std::chrono::steady_clock::now();
    const auto deadline = started + kFailoverDeadline;

    while (std::chrono::steady_clock::now() < deadline) {
        const Response metadata = send_request(
            kFollower2Port,
            Request{RequestType::METADATA, "", 0, "", 0, 0}
        );
        if (metadata.success &&
            metadata.message.find("2:127.0.0.1:19093 role:leader") != std::string::npos) {
            return std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - started
            );
        }
        std::this_thread::sleep_for(kFailoverPollInterval);
    }

    throw std::runtime_error("leader election exceeded failover deadline");
}

void require(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

}  // namespace

int main() {
    std::system(("rm -rf " + std::string(kDataDir)).c_str());
    std::system(("mkdir -p " + std::string(kDataDir)).c_str());

    std::vector<BrokerProcess> processes;
    processes.push_back(
        start_broker(
            1,
            kLeaderPort,
            "leader",
            "1",
            "2:127.0.0.1:19093,3:127.0.0.1:19094"
        )
    );
    processes.push_back(
        start_broker(
            2,
            kFollower2Port,
            "follower",
            "1",
            "1:127.0.0.1:19092,3:127.0.0.1:19094"
        )
    );
    processes.push_back(
        start_broker(
            3,
            kFollower3Port,
            "follower",
            "1",
            "1:127.0.0.1:19092,2:127.0.0.1:19093"
        )
    );

    try {
        require(wait_for_ports(), "brokers did not start");

        const Response metadata = send_request(
            kFollower2Port,
            Request{RequestType::METADATA, "", 0, "", 0, 0}
        );
        require(metadata.success, "metadata failed");
        require(
            metadata.message.find("1:127.0.0.1:19092") != std::string::npos,
            "metadata missing leader endpoint"
        );

        const Request produce{
            RequestType::PRODUCE,
            "orders",
            0,
            "hello world",
            0,
            0
        };

        const Response leader_produce = send_request(kLeaderPort, produce);
        require(leader_produce.success, "leader produce failed");
        require(leader_produce.appended_offset == 0, "unexpected leader offset");

        const Response forwarded_produce = send_request(kFollower2Port, produce);
        require(forwarded_produce.success, "forwarded produce failed");
        require(
            forwarded_produce.appended_offset == 1,
            "unexpected forwarded offset"
        );

        const Request fetch{
            RequestType::FETCH,
            "orders",
            0,
            "",
            0,
            0
        };

        const Response leader_fetch = send_request(kLeaderPort, fetch);
        require(leader_fetch.success, "leader fetch failed");
        require(leader_fetch.message == "hello world", "leader fetch payload mismatch");

        const Request fetch_second{
            RequestType::FETCH,
            "orders",
            0,
            "",
            1,
            0
        };
        const Response follower_fetch = send_request(kFollower3Port, fetch_second);
        require(follower_fetch.success, "follower fetch failed");
        require(
            follower_fetch.message == "hello world",
            "follower fetch payload mismatch"
        );

        const Response uncommitted_fetch = send_request(
            kLeaderPort,
            Request{RequestType::FETCH, "orders", 0, "", 99, 0}
        );
        require(!uncommitted_fetch.success, "uncommitted fetch should fail");
        require(
            uncommitted_fetch.message == "offset_not_committed",
            "expected offset_not_committed"
        );

        // Simulate an ungraceful leader crash and measure election time.
        kill(processes[0].pid, SIGKILL);
        waitpid(processes[0].pid, nullptr, 0);
        processes[0].pid = -1;
        const std::chrono::milliseconds failover_time = measure_failover();
        std::cout << "localhost leader failover time: " << failover_time.count()
                  << " ms\n";
        require(failover_time < std::chrono::seconds(2),
                "failover exceeded the two-second localhost latency budget");
        const Response post_failover_produce = send_request(
            kFollower2Port,
            Request{RequestType::PRODUCE, "orders", 0, "after failover", 0, 0}
        );
        require(post_failover_produce.success, "produce after failover failed");
        require(post_failover_produce.appended_offset == 2, "failover offset mismatch");

        std::cout << "integration_test passed\n";
        stop_brokers(processes);
        return 0;
    } catch (const std::exception& exception) {
        std::cerr << "integration_test failed: " << exception.what() << std::endl;
        stop_brokers(processes);
        return 1;
    }
}
