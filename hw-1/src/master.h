#pragma once

#include <iostream>
#include <arpa/inet.h>
#include <vector>
#include <map>
#include <unordered_map>
#include <unistd.h>
#include <thread>
#include "mutex"
#include "atomic"

enum {
    BROADCAST_PORT = 33333,
    SLAVES_PORT = 33334,
    CLIENTS_PORT = 32000,
};

struct SlaveInfo {
    size_t id;
    sockaddr_in address;
    int fd;
};

struct Request {
    size_t id;
    sockaddr_in client_address;
    int fd;
    std::pair<int, int> integral_constraints;
    std::map<size_t, int> mapping;
};

class Master {
public:
    Master();

    void broadcastMasterAddress();

    void pollClientConnections();
    void serveClient(int fd, sockaddr_in client_addr);

    void pollSlaveAddresses();
    void pollSlaveResponses(const std::string &host, int port, size_t slave_id);
    void pingSlave(const std::string &host, int port, size_t slave_id);


    std::unordered_map<size_t, SlaveInfo> slaves_;
    std::atomic_size_t slaves_counter_{0};
    std::mutex slaves_mtx_;
    std::unordered_map<size_t, Request> requests_;
    std::atomic_size_t requests_counter_{0};
    std::mutex requests_mtx_;

};