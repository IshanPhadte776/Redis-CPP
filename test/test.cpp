#include <iostream>
#include <string>
#include <vector>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cassert>
#include <thread>

class RedisClient {
private:
    int sock;
public:
    RedisClient() {
        sock = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in serv_addr;
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(6379);
        inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr);
        if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
            std::cerr << "Connection Failed. Is your server running?\n";
            exit(1);
        }
    }

    ~RedisClient() { close(sock); }

    std::string send_cmd(const std::string& cmd) {
        send(sock, cmd.c_str(), cmd.length(), 0);
        char buffer[4096] = {0};
        int bytes = read(sock, buffer, 4096);
        return std::string(buffer, bytes);
    }
};

void log_test(const std::string& name, bool passed) {
    std::cout << (passed ? " [PASS] " : " [FAIL] ") << name << std::endl;
}

int main() {
    RedisClient client;

    std::cout << "--- Starting Redis Clone Test Suite ---\n";

    // 1. Basic Commands
    log_test("PING", client.send_cmd("*1\r\n$4\r\nPING\r\n") == "+PONG\r\n");
    //log_test("ECHO", client.send_cmd("*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n") == "$5\r\nhello\r\n");

    // 2. String & INCR
    std::cout << "Flushing database before String tests...\n";
    client.send_cmd("*1\r\n$8\r\nFLUSHALL\r\n");
    std::cout << "Flushed database before String tests...\n";
    client.send_cmd("*3\r\n$3\r\nSET\r\n$3\r\nnum\r\n$2\r\n10\r\n");
    log_test("INCR existing", client.send_cmd("*2\r\n$4\r\nINCR\r\n$3\r\nnum\r\n") == ":11\r\n");
    log_test("GET after INCR", client.send_cmd("*2\r\n$3\r\nGET\r\n$3\r\nnum\r\n") == "$2\r\n11\r\n");
    //log_test("INCR new key", client.send_cmd("*2\r\n$4\r\nINCR\r\n$7\r\nnew_key\r\n") == ":1\r\n");

    // 3. Lists
    client.send_cmd("*1\r\n$8\r\nFLUSHALL\r\n");
    client.send_cmd("*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\na\r\n");
    client.send_cmd("*3\r\n$5\r\nRPUSH\r\n$6\r\nmylist\r\n$1\r\nb\r\n");
    log_test("LLEN", client.send_cmd("*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n") == ":2\r\n");
    log_test("LPOP", client.send_cmd("*2\r\n$4\r\nLPOP\r\n$6\r\nmylist\r\n") == "$1\r\na\r\n");

    // 4. TTL (Lazy & Active)
    client.send_cmd("*1\r\n$8\r\nFLUSHALL\r\n");
    client.send_cmd("*5\r\n$3\r\nSET\r\n$4\r\ntemp\r\n$3\r\nval\r\n$2\r\nPX\r\n$3\r\n100\r\n");
    log_test("GET before TTL", client.send_cmd("*2\r\n$3\r\nGET\r\n$4\r\ntemp\r\n") == "$3\r\nval\r\n");
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    log_test("GET after TTL (Lazy)", client.send_cmd("*2\r\n$3\r\nGET\r\n$4\r\ntemp\r\n") == "$-1\r\n");

    // 5. Streams
    client.send_cmd("*1\r\n$8\r\nFLUSHALL\r\n");
    client.send_cmd("*5\r\n$4\r\nXADD\r\n$1\r\ns\r\n$3\r\n1-1\r\n$1\r\na\r\n$1\r\nb\r\n");
    log_test("XADD explicit", client.send_cmd("*5\r\n$4\r\nXADD\r\n$1\r\ns\r\n$3\r\n1-2\r\n$1\r\nc\r\n$1\r\nd\r\n") == "$3\r\n1-2\r\n");
    log_test("XADD smaller error", client.send_cmd("*5\r\n$4\r\nXADD\r\n$1\r\ns\r\n$3\r\n0-1\r\n$1\r\nx\r\n$1\r\ny\r\n").find("-ERR") != std::string::npos);
    
    // 6. XRANGE
    //client.send_cmd("*1\r\n$8\r\nFLUSHALL\r\n");
    std::string xrange_res = client.send_cmd("*4\r\n$6\r\nXRANGE\r\n$1\r\ns\r\n$1\r\n-\r\n$1\r\n+\r\n");
    std::cout << "XRANGE Response:\n" << xrange_res << std::endl;
    log_test("XRANGE all", xrange_res.find("*2\r\n") == 0); // Should return array of 2 entries

    // 7. TYPE
    client.send_cmd("*1\r\n$8\r\nFLUSHALL\r\n");
    client.send_cmd("*3\r\n$3\r\nSET\r\n$3\r\nnum\r\n$2\r\n10\r\n");
    std::cout <<  client.send_cmd("*2\r\n$4\r\nTYPE\r\n$3\r\nnum\r\n");
    log_test("TYPE string", client.send_cmd("*2\r\n$4\r\nTYPE\r\n$3\r\nnum\r\n") == "+string\r\n");
    client.send_cmd("*1\r\n$8\r\nFLUSHALL\r\n");
    client.send_cmd("*5\r\n$4\r\nXADD\r\n$1\r\ns\r\n$3\r\n1-1\r\n$1\r\na\r\n$1\r\nb\r\n");
    log_test("TYPE stream", client.send_cmd("*2\r\n$4\r\nTYPE\r\n$1\r\ns\r\n") == "+stream\r\n");
    client.send_cmd("*1\r\n$8\r\nFLUSHALL\r\n");
    log_test("TYPE none", client.send_cmd("*2\r\n$4\r\nTYPE\r\n$7\r\nmissing\r\n") == "+none\r\n");

    std::cout << "--- Test Suite Complete ---\n";
    return 0;
}