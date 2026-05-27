#include "tcp_server.h"

#include "../core/broker.h"
#include "../protocol/frame.h"
#include "../protocol/protocol.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <iostream>
#include <stdexcept>

TCPServer::TCPServer(int port, Broker* broker)
    : port(port),
      broker(broker),
      server_fd(-1),
      running(false) {}

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
    accept_thread = std::thread(&TCPServer::accept_loop, this);
}

void TCPServer::accept_loop() {
    while (running) {
        sockaddr_in client_addr{};
        socklen_t len = sizeof(client_addr);

        int client_socket = accept(server_fd, reinterpret_cast<sockaddr*>(&client_addr), &len);

        if (client_socket < 0) {
            if (!running) {
                break;
            }
            continue;
        }

        std::lock_guard<std::mutex> lock(threads_mutex);
        client_threads.emplace_back(&TCPServer::handle_client, this, client_socket);
    }
}

void TCPServer::handle_client(int client_socket) {
    const std::string request = read_request(client_socket);
    const std::string response = process_request(request, client_socket);
    send_response(client_socket, response);
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
        return join_fields({"ERROR", "broker_unavailable"});
    }

    if (request.empty()) {
        return join_fields({"ERROR", "empty_request"});
    }

    const Request parsed = parse_request(request);

    if (parsed.type == REPLICATE || parsed.type == ACK || parsed.type == HEARTBEAT ||
        parsed.type == COMMIT) {
        return broker->handle_inter_broker_request(parsed, client_socket);
    }

    const Response response = broker->handle_request(parsed);
    return serialize_response(response);
}

void TCPServer::stop() {
    running = false;

    if (server_fd >= 0) {
        shutdown(server_fd, SHUT_RDWR);
        close(server_fd);
        server_fd = -1;
    }

    if (accept_thread.joinable()) {
        accept_thread.join();
    }

    {
        std::lock_guard<std::mutex> lock(threads_mutex);

        for (auto& thread : client_threads) {
            if (thread.joinable()) {
                thread.join();
            }
        }

        client_threads.clear();
    }

    std::cout << "Server stopped\n";
}
