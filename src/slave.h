#pragma once

#include "arpa/inet.h"
#include "unistd.h"
#include <iostream>
#include <thread>
#include <cstring>

enum {
    BROADCAST_PORT = 33333,
    REQUESTS_PORT = 33000,
    PING_PORT = 33001
};

class Slave {
public:
    explicit Slave(const std::string& address);

    void findMaster();

    void pollMasterConnections();

    void pollPings();

    void serveRequests(int fd, sockaddr_in master_address);

    sockaddr_in address_;
    std::string host_;
};
