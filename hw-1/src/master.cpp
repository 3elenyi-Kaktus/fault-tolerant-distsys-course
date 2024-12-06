#include <syncstream>
#include "master.h"
#include "helpers.h"

std::string SERVER_ADDRESS = "127.0.0.1";

Master::Master() {
    std::thread broadcaster([&]() {
        try {
            broadcastMasterAddress();
        }
        catch (...) {
            std::cerr << "Caught exception in broadcastMasterAddress\n";
            exit(1);
        }
    });
    std::thread slaves_acceptor([&]() {
        try {
            pollSlaveAddresses();
        }
        catch (...) {
            std::cerr << "Caught exception in pollSlaveAddresses\n";
            exit(1);
        }
    });
    try {
        pollClientConnections();
    }
    catch (...) {
        std::cerr << "Caught exception in pollClientConnections\n";
        exit(1);
    }
    broadcaster.join();
    slaves_acceptor.join();
}

void Master::broadcastMasterAddress() {
    int sock_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock_fd < 0) {
        std::osyncstream(std::cout) << "ERROR on creating socket\n";
        exit(1);
    }

    setSocketOption(sock_fd, SO_BROADCAST);
    sockaddr_in sa = fillAddress(INADDR_BROADCAST, BROADCAST_PORT);

    std::string master_address = SERVER_ADDRESS + ":" + std::to_string(SLAVES_PORT);
    while (true) {
        if (!sendMessageTo(sock_fd, master_address, sa)) {
            std::osyncstream(std::cout) << "Error on broadcasting\n";
        } else {
            std::osyncstream(std::cout) << "Successfully broadcasted master address\n";
        }
        sleep(5);
    }
}

void Master::pollSlaveAddresses() {
    int sock_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock_fd < 0) {
        std::osyncstream(std::cout) << "ERROR on creating socket\n";
        exit(1);
    }
    setSocketOption(sock_fd, SO_REUSEADDR);
    setSocketOption(sock_fd, SO_REUSEPORT);
    sockaddr_in recv_addr = fillAddress(INADDR_ANY, SLAVES_PORT);

    if (bind(sock_fd, (sockaddr *) &recv_addr, sizeof(recv_addr)) < 0) {
        std::osyncstream(std::cout) << "ERROR on binding socket\n";
        exit(1);
    }

    while (true) {
        std::string message;
        sockaddr_in slave_address{};
        std::osyncstream(std::cout) << "Waiting for slave\n";
        try {
            std::tie(message, slave_address) = getMessageFrom(sock_fd);
        }
        catch (...) {
            std::cerr << "Caught exception while polling for new slaves\n";
            exit(1);
        }
        std::osyncstream(std::cout) << "Slave connected from " << inet_ntoa(slave_address.sin_addr) << ":"
                                    << ntohs(slave_address.sin_port)
                                    << "\n";
        auto words = split(message, ":");
        if (words.size() != 2) {
            std::cerr << "Expected 2 vars, got: " << message << "\n";
        }
        std::string host = words[0];
        int port = std::stoi(words[1]);

        size_t slave_id = slaves_counter_.fetch_add(1);

        std::thread servicer([&, host, port, slave_id]() {
            try {
                std::osyncstream(std::cout) << "Call pollSlaveResponses: " << host << ":" << port << ":" << slave_id
                                            << "\n";
                pollSlaveResponses(host, port, slave_id);
            }
            catch (...) {
                std::cerr << "Caught exception in pollSlaveResponses\n";
            }
        });
        servicer.detach();
        std::thread pinger([&, host, port, slave_id]() {
            try {
                std::osyncstream(std::cout) << "Call pingSlave: " << host << ":" << port + 1 << ":" << slave_id << "\n";
                pingSlave(host, port + 1, slave_id);
            }
            catch (...) {
                std::cerr << "Caught exception in pingSlave\n";
            }
        });
        pinger.detach();
    }
}

void Master::pollSlaveResponses(const std::string &host, int port, size_t slave_id) {
    std::osyncstream(std::cout) << "Connecting slave at " << host << ":" << port << "\n";

    int slave_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (slave_fd < 0) {
        std::cerr << "ERROR on creating socket\n";
        exit(1);
    }
    setSocketOption(slave_fd, SO_REUSEADDR);
    setSocketOption(slave_fd, SO_REUSEPORT);
    sockaddr_in slave_addr = fillAddress(host, port);

    if (connect(slave_fd, (struct sockaddr *) &slave_addr, sizeof(slave_addr)) < 0) {
        std::cerr << "ERROR on connecting to slave\n";
    }

    slaves_mtx_.lock();
    slaves_[slave_id] = SlaveInfo(slave_id, slave_addr, slave_fd);
    slaves_mtx_.unlock();
    while (true) {
        std::string message;
        std::osyncstream(std::cout) << "Waiting for messages from slave\n";
        try {
            message = getMessage(slave_fd);
        }
        catch (...) {
            std::cerr << "Caught exception while polling for new messages from slave\n";
            std::lock_guard<std::mutex> guard(slaves_mtx_);
            shutdown(slaves_[slave_id].fd, SHUT_RDWR);
            close(slaves_[slave_id].fd);
            slaves_.erase(slave_id);
            return;
        }
        if (message.empty()) {
            std::osyncstream(std::cout) << "Slave disconnected\n";
            std::lock_guard<std::mutex> guard(slaves_mtx_);
            shutdown(slaves_[slave_id].fd, SHUT_RDWR);
            close(slaves_[slave_id].fd);
            slaves_.erase(slave_id);
            return;
        }
        auto words = split(message, " ");
        if (words.size() != 3) {
            std::cerr << "Expected 3 vars, got: " << message << "\n";
        }
        int id = std::stoi(words[0]);
        int i = std::stoi(words[1]);
        int ct = std::stoi(words[2]);

        std::lock_guard<std::mutex> guard(requests_mtx_);
        requests_[id].mapping[i] = ct;
    }
}

void Master::pingSlave(const std::string &host, int port, size_t slave_id) {
    std::osyncstream(std::cout) << "Pinging slave at " << host << ":" << port << "\n";

    int slave_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (slave_fd < 0) {
        std::osyncstream(std::cout) << "ERROR on creating socket\n";
        exit(1);
    }
    setSocketOption(slave_fd, SO_REUSEADDR);
    setSocketOption(slave_fd, SO_REUSEPORT);
    sockaddr_in slave_addr = fillAddress(host, port);

    int misses = 0;

    while (true) {
        if (misses >= 5) {
            std::cerr << "Slave missed too much pings, disconnecting\n";
            std::lock_guard<std::mutex> guard(slaves_mtx_);
            shutdown(slaves_[slave_id].fd, SHUT_RDWR);
            close(slaves_[slave_id].fd);
            slaves_.erase(slave_id);
            return;
        }
        sleep(2);
        if (!sendMessageTo(slave_fd, std::string(), slave_addr)) {
            std::osyncstream(std::cout) << "Error in sending ping to slave\n";
            ++misses;
            continue;
        }

        std::string message;
        usleep(500000);
        try {
            std::tie(message, slave_addr) = getMessageFrom(slave_fd, MSG_DONTWAIT);
            std::osyncstream(std::cout) << "Received ping from: " << inet_ntoa(slave_addr.sin_addr) << ":"
                                        << ntohs(slave_addr.sin_port) << "\n";
            misses = 0;
        }
        catch (...) {
            std::cerr << "Caught exception while polling for ping replies from slave\n";
            ++misses;
        }
    }
}

void Master::pollClientConnections() {
    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        std::osyncstream(std::cout) << "ERROR on creating socket\n";
        exit(1);
    }
    setSocketOption(sock_fd, SO_REUSEADDR);
    setSocketOption(sock_fd, SO_REUSEPORT);
    sockaddr_in master_addr = fillAddress(INADDR_ANY, CLIENTS_PORT);

    if (bind(sock_fd, (sockaddr *) &master_addr, sizeof(master_addr)) < 0) {
        std::osyncstream(std::cout) << "ERROR on binding socket\n";
        exit(1);
    }

    if (listen(sock_fd, SOMAXCONN) < 0) {
        std::osyncstream(std::cout) << "ERROR on listening\n";
        exit(1);
    }

    while (true) {
        sockaddr_in client_addr{};
        socklen_t init_size = sizeof(client_addr);
        std::osyncstream(std::cout) << "wait\n";
        int initiator_fd = accept(sock_fd, (sockaddr *) &client_addr, &init_size);
        std::osyncstream(std::cout) << "accepted\n";
        if (initiator_fd < 0) {
            std::osyncstream(std::cout) << "ERROR on accepting new client connection\n";
            exit(1);
        }

        std::thread servicer([&, initiator_fd, client_addr]() {
            try {
                serveClient(initiator_fd, client_addr);
            }
            catch (...) {
                std::cerr << "Caught exception in serveClient\n";
            }
        });
        servicer.detach();
    }
}

void Master::serveClient(int fd, sockaddr_in client_addr) {
    std::osyncstream(std::cout) << "Client connected at " << inet_ntoa(client_addr.sin_addr) << ":"
                                << ntohs(client_addr.sin_port)
                                << "\n";

    std::string message;
    try {
        message = recvSafe(fd, 1024);
    }
    catch (...) {
        std::cerr << "Caught exception while getting client request\n";
        return;
    }

    auto words = split(message, " ");
    if (words.size() != 2) {
        std::cerr << "Expected 2 vars, got: " << message << "\n";
    }
    int a = std::stoi(words[0]);
    int b = std::stoi(words[1]);
    std::osyncstream(std::cout) << "Calculate integral from " << a << " to " << b << "\n";
    size_t request_id = requests_counter_.fetch_add(1);
    std::pair<int, int> constraints = std::make_pair(a, b);
    requests_mtx_.lock();
    requests_[request_id] = Request(request_id, client_addr, fd, constraints, {});
    requests_mtx_.unlock();


    int lower_bound = a;
    while (lower_bound < b) {
        slaves_mtx_.lock();
        for (auto [key, slave]: slaves_) {
            if (lower_bound >= b) {
                break;
            }
            std::osyncstream(std::cout) << "Sending req (id: " << request_id << ", at: " << lower_bound << ") to slave "
                                        << inet_ntoa(slave.address.sin_addr) << ":" << ntohs(slave.address.sin_port)
                                        << "\n";
            std::string repack = std::to_string(request_id) + " " + std::to_string(lower_bound);
            if (!sendMessage(slave.fd, repack)) {
                std::osyncstream(std::cout) << "Error while sending integral task to slave\n";
            }
            ++lower_bound;
        }
        slaves_mtx_.unlock();
    }

    while (true) {
        sleep(10);
        std::osyncstream(std::cout) << "Try to collect the results for req id: " << request_id << "\n";
        bool is_counted = true;
        for (size_t i = a; i < b; ++i) {
            std::lock_guard<std::mutex> guard(requests_mtx_);
            if (!requests_[request_id].mapping.contains(i)) {
                is_counted = false;
                slaves_mtx_.lock();
                SlaveInfo slave = slaves_[rand() % slaves_.size()];
                slaves_mtx_.unlock();
                std::osyncstream(std::cout) << "Detected uncounted AT: " << i << " in req: " << request_id
                                            << ", resend to slave: "
                                            << inet_ntoa(slave.address.sin_addr) << ":" << ntohs(slave.address.sin_port)
                                            << "\n";
                std::string repack = std::to_string(request_id) + " " + std::to_string(i);
                if (!sendMessage(slave.fd, repack)) {
                    std::osyncstream(std::cout) << "Error while sending integral task to slave\n";
                }
            }
        }
        if (is_counted) {
            std::osyncstream(std::cout) << "Results for req id: " << request_id << " collected successfully\n";
            break;
        }
    }

    int sum = 0;
    requests_mtx_.lock();
    for (size_t i = a; i < b; ++i) {
        sum += requests_[request_id].mapping[i];
    }
    requests_mtx_.unlock();
    std::string res = std::to_string(sum);

    if (!sendSafe(fd, res)) {
        std::osyncstream(std::cout) << "Error while sending request result to client\n";
    }
    shutdown(fd, SHUT_RDWR);
    close(fd);
}
