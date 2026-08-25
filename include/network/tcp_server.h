#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

class Broker;

class TCPServer {
public:
    // A fixed number of workers prevents a slow client from creating an
    // unbounded number of threads.  Zero selects a sensible host default.
    TCPServer(int port, Broker* broker, size_t worker_count = 0);
    ~TCPServer();

    void start();
    void stop();

private:
    void accept_loop();
    void handle_client(int client_socket);
    void worker_loop();

    std::string read_request(int client_socket);
    void send_response(int client_socket, const std::string& response);
    std::string process_request(const std::string& request, int client_socket);

    int port;
    Broker* broker;

    int server_fd;

    std::atomic<bool> running;

    std::thread accept_thread;
    std::vector<std::thread> workers_;
    std::deque<int> pending_clients_;
    std::mutex pending_mutex_;
    std::condition_variable pending_cv_;
    size_t worker_count_;
};
