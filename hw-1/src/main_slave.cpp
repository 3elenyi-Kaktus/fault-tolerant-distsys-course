#include "iostream"
#include "slave.h"

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: ./slave address(host) drop_rate(0 to 100)\n";
        return 1;
    }
    std::string slave_address(argv[1]);
    char* end;
    int drop_rate = strtol(argv[2], &end, 10);
    std::cout << "Slave startup at address: " << slave_address << " with drop rate: " << drop_rate << "\n";
    Config config{slave_address, drop_rate};
    Slave slave{config};
    std::cout << "Terminate\n";
}