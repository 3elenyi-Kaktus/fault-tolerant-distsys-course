#pragma once

#include "arpa/inet.h"
#include "unistd.h"
#include <iostream>
#include <thread>
#include <cstring>
#include <syncstream>

enum {
    BROADCAST_PORT = 33333,
    REQUESTS_PORT = 33000,
    PING_PORT = 33001
};

struct Config {
    std::string address;
    int drop_rate;
};

class Slave {
public:
    explicit Slave(Config& config);

    void findMaster();

    void pollMasterConnections();

    void pollPings();

    void serveRequests(int fd, sockaddr_in master_address);

    sockaddr_in address_;
    Config config_;
    bool connected_;
};
