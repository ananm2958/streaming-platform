#pragma once

#include <atomic>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

class Broker;

class TCPServer {
public:
    TCPServer(int port, Broker* broker);
    ~TCPServer();

    void start();
    void stop();

private:
    void accept_loop();
    void handle_client(int client_socket);

    std::string read_request(int client_socket);
    void send_response(int client_socket, const std::string& response);
    std::string process_request(const std::string& request, int client_socket);

    int port;
    Broker* broker;

    int server_fd;

    std::atomic<bool> running;

    std::thread accept_thread;

    std::vector<std::thread> client_threads;
    std::mutex threads_mutex;
};
