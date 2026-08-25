#include "tcp_server.h"

#include "../core/broker.h"
#include "../protocol/frame.h"
#include "../protocol/protocol.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <poll.h>
#include <unistd.h>

#include <cstring>
#include <algorithm>
#include <iostream>
#include <stdexcept>

TCPServer::TCPServer(int port, Broker* broker, size_t worker_count)
    : port(port),
      broker(broker),
      server_fd(-1),
      running(false),
      worker_count_(worker_count == 0 ? std::max<size_t>(1, std::thread::hardware_concurrency())
                                      : worker_count) {}

TCPServer::~TCPServer() {
    stop();
}

void TCPServer::start() {
    server_fd = socket(AF_INET, SOCK_STREAM, 0);

    if (server_fd < 0) {
        throw std::runtime_error("FAILED TO CREATE SOCKET");
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(static_cast<uint16_t>(port));
    server_addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(server_fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) < 0) {
        close(server_fd);
        throw std::runtime_error("BIND FAILED");
    }

    if (listen(server_fd, 10) < 0) {
        close(server_fd);
        throw std::runtime_error("LISTEN FAILED");
    }

    std::cout << "Server listening on port " << port << std::endl;

    running = true;
    workers_.reserve(worker_count_);
    for (size_t i = 0; i < worker_count_; ++i) {
        workers_.emplace_back(&TCPServer::worker_loop, this);
    }
    accept_thread = std::thread(&TCPServer::accept_loop, this);
}

void TCPServer::accept_loop() {
    while (running) {
        // poll keeps the event loop interruptible during shutdown and avoids
        // tying a thread to a blocking accept call.
        pollfd listener{server_fd, POLLIN, 0};
        const int ready = poll(&listener, 1, 200);
        if (ready <= 0 || !(listener.revents & POLLIN)) continue;
        sockaddr_in client_addr{};
        socklen_t len = sizeof(client_addr);

        int client_socket = accept(server_fd, reinterpret_cast<sockaddr*>(&client_addr), &len);

        if (client_socket < 0) {
            if (!running) {
                break;
            }
            continue;
        }

        {
            std::lock_guard<std::mutex> lock(pending_mutex_);
            pending_clients_.push_back(client_socket);
        }
        pending_cv_.notify_one();
    }
}

void TCPServer::worker_loop() {
    while (true) {
        int client_socket = -1;
        {
            std::unique_lock<std::mutex> lock(pending_mutex_);
            pending_cv_.wait(lock, [this] { return !running || !pending_clients_.empty(); });
            if (pending_clients_.empty()) {
                if (!running) return;
                continue;
            }
            client_socket = pending_clients_.front();
            pending_clients_.pop_front();
        }
        handle_client(client_socket);
    }
}

void TCPServer::handle_client(int client_socket) {
    while (running) {
        const std::string request = read_request(client_socket);
        if (request.empty()) break;
        const Request parsed = parse_request(request);
        const std::string response = process_request(request, client_socket);
        send_response(client_socket, response);
        // Replication sends an ACK on the same socket before this response and
        // remains a one-shot exchange; client producers may keep connections
        // open for many requests.
        if (parsed.type == REPLICATE || parsed.type == BATCH_REPLICATE || parsed.type == ACK ||
            parsed.type == HEARTBEAT || parsed.type == COMMIT) break;
    }
    close(client_socket);
}

std::string TCPServer::read_request(int client_socket) {
    std::string payload;
    if (!read_frame(client_socket, payload)) {
        return "";
    }
    return payload;
}

void TCPServer::send_response(int client_socket, const std::string& response) {
    write_frame(client_socket, response);
}

std::string TCPServer::process_request(
    const std::string& request,
    int client_socket
) {
    if (!broker) {
        return serialize_response(Response{false, "broker_unavailable", uint64_t(-1)});
    }

    if (request.empty()) {
        return serialize_response(Response{false, "empty_request", uint64_t(-1)});
    }

    const Request parsed = parse_request(request);

    if (parsed.type == REPLICATE || parsed.type == BATCH_REPLICATE || parsed.type == ACK || parsed.type == HEARTBEAT ||
        parsed.type == COMMIT) {
        return broker->handle_inter_broker_request(parsed, client_socket);
    }

    const Response response = broker->handle_request(parsed);
    return serialize_response(response);
}

void TCPServer::stop() {
    running = false;
    pending_cv_.notify_all();

    if (server_fd >= 0) {
        shutdown(server_fd, SHUT_RDWR);
        close(server_fd);
        server_fd = -1;
    }

    if (accept_thread.joinable()) {
        accept_thread.join();
    }

    for (auto& worker : workers_) {
        if (worker.joinable()) worker.join();
    }
    workers_.clear();

    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        while (!pending_clients_.empty()) {
            close(pending_clients_.front());
            pending_clients_.pop_front();
        }
    }

    std::cout << "Server stopped\n";
}
