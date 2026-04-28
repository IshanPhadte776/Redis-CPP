// TCP integration tests: requires `redis` listening (default 127.0.0.1:6379).
//
//   ./build/redis --port 6379 &
//   ./build/redis_integration_test
//   ./build/redis_integration_test 6380
//
// Port: argv[1], else env REDIS_TEST_PORT, else 6379.

#include <arpa/inet.h>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <unistd.h>

#include <sys/socket.h>

namespace {

// Length of one complete RESP value starting at buf[i] (0 if incomplete / invalid).
std::size_t resp_value_len(const std::string& buf, std::size_t i) {
    if (i >= buf.size()) {
        return 0;
    }
    const char t = buf[i];
    if (t == '+' || t == '-' || t == ':') {
        const std::size_t end = buf.find("\r\n", i);
        if (end == std::string::npos) {
            return 0;
        }
        return end + 2 - i;
    }
    if (t == '$') {
        const std::size_t line_end = buf.find("\r\n", i);
        if (line_end == std::string::npos) {
            return 0;
        }
        long long blen = 0;
        try {
            blen = std::stoll(buf.substr(i + 1, line_end - i - 1));
        } catch (...) {
            return 0;
        }
        if (blen < -1) {
            return 0;
        }
        if (blen == -1) {
            return line_end + 2 - i;
        }
        const std::size_t payload_end = line_end + 2 + static_cast<std::size_t>(blen) + 2;
        if (buf.size() < payload_end) {
            return 0;
        }
        return payload_end - i;
    }
    if (t == '*') {
        const std::size_t line_end = buf.find("\r\n", i);
        if (line_end == std::string::npos) {
            return 0;
        }
        long long count = 0;
        try {
            count = std::stoll(buf.substr(i + 1, line_end - i - 1));
        } catch (...) {
            return 0;
        }
        std::size_t pos = line_end + 2;
        if (count < 0) {
            return pos - i;
        }
        for (long long k = 0; k < count; ++k) {
            const std::size_t piece = resp_value_len(buf, pos);
            if (piece == 0) {
                return 0;
            }
            pos += piece;
        }
        return pos - i;
    }
    return 0;
}

int parse_port(int argc, char** argv) {
    if (argc >= 2) {
        char* end = nullptr;
        const long p = std::strtol(argv[1], &end, 10);
        if (end != argv[1] && *end == '\0' && p > 0 && p <= 65535) {
            return static_cast<int>(p);
        }
        std::cerr << "warning: bad argv[1] port; trying REDIS_TEST_PORT / 6379\n";
    }
    if (const char* env = std::getenv("REDIS_TEST_PORT")) {
        char* end = nullptr;
        const long p = std::strtol(env, &end, 10);
        if (end != env && *end == '\0' && p > 0 && p <= 65535) {
            return static_cast<int>(p);
        }
    }
    return 6379;
}

class RedisClient {
    int sock = -1;
    std::string pending_;

public:
    explicit RedisClient(int port) {
        sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            std::perror("socket");
            std::exit(2);
        }
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(static_cast<std::uint16_t>(port));
        if (inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) != 1) {
            std::cerr << "inet_pton failed\n";
            std::exit(2);
        }
        if (connect(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            std::cerr << "connect(127.0.0.1:" << port
                      << ") failed — start the server, e.g. ./build/redis --port " << port << "\n";
            std::exit(1);
        }
    }

    ~RedisClient() {
        if (sock >= 0) {
            close(sock);
        }
    }

    std::string send_cmd(const std::string& cmd) {
        if (send(sock, cmd.data(), cmd.size(), 0) < 0) {
            return {};
        }
        char buf[65536];
        for (;;) {
            const std::size_t one = resp_value_len(pending_, 0);
            if (one > 0 && one <= pending_.size()) {
                std::string out = pending_.substr(0, one);
                pending_.erase(0, one);
                return out;
            }
            const ssize_t n = recv(sock, buf, sizeof(buf), 0);
            if (n <= 0) {
                break;
            }
            pending_.append(buf, static_cast<std::size_t>(n));
        }
        return {};
    }
};

int g_fails = 0;

void it(const char* name, bool ok) {
    std::cout << (ok ? " [PASS] " : " [FAIL] ") << name << '\n';
    if (!ok) {
        ++g_fails;
    }
}

} // namespace

int main(int argc, char** argv) {
    const int port = parse_port(argc, argv);
    RedisClient c(port);

    std::cout << "--- redis_integration_test (port " << port << ") ---\n";

    // Connection / basics
    it("PING", c.send_cmd("*1\r\n$4\r\nPING\r\n") == "+PONG\r\n");
    it("PING bulk", c.send_cmd("*2\r\n$4\r\nPING\r\n$3\r\nhey\r\n") == "$3\r\nhey\r\n");
    it("ECHO", c.send_cmd("*2\r\n$4\r\nECHO\r\n$5\r\nworld\r\n") == "$5\r\nworld\r\n");
    it("FLUSHALL", c.send_cmd("*1\r\n$8\r\nFLUSHALL\r\n") == "+OK\r\n");

    // Strings
    it("GET missing", c.send_cmd("*2\r\n$3\r\nGET\r\n$2\r\nxx\r\n") == "$-1\r\n");
    c.send_cmd("*3\r\n$3\r\nSET\r\n$3\r\nnum\r\n$2\r\n42\r\n");
    it("GET string", c.send_cmd("*2\r\n$3\r\nGET\r\n$3\r\nnum\r\n") == "$2\r\n42\r\n");
    it("INCR", c.send_cmd("*2\r\n$4\r\nINCR\r\n$3\r\nnum\r\n") == ":43\r\n");
    c.send_cmd("*1\r\n$8\r\nFLUSHALL\r\n");
    it("INCR new key", c.send_cmd("*2\r\n$4\r\nINCR\r\n$5\r\nfresh\r\n") == ":1\r\n");
    c.send_cmd("*3\r\n$3\r\nSET\r\n$3\r\nbad\r\n$2\r\nno\r\n");
    it("INCR non-integer",
       c.send_cmd("*2\r\n$4\r\nINCR\r\n$3\r\nbad\r\n").find("value is not an integer") != std::string::npos);

    // Lists (bulk lengths must match byte count: "lst" is $3)
    c.send_cmd("*1\r\n$8\r\nFLUSHALL\r\n");
    c.send_cmd("*3\r\n$5\r\nRPUSH\r\n$3\r\nlst\r\n$1\r\np\r\n");
    c.send_cmd("*3\r\n$5\r\nRPUSH\r\n$3\r\nlst\r\n$1\r\nq\r\n");
    it("LLEN", c.send_cmd("*2\r\n$4\r\nLLEN\r\n$3\r\nlst\r\n") == ":2\r\n");
    it("LRANGE", c.send_cmd("*4\r\n$6\r\nLRANGE\r\n$3\r\nlst\r\n$1\r\n0\r\n$2\r\n-1\r\n") ==
                    "*2\r\n$1\r\np\r\n$1\r\nq\r\n");
    it("TYPE list", c.send_cmd("*2\r\n$4\r\nTYPE\r\n$3\r\nlst\r\n") == "+list\r\n");
    it("LPOP", c.send_cmd("*2\r\n$4\r\nLPOP\r\n$3\r\nlst\r\n") == "$1\r\np\r\n");

    // Sorted set
    c.send_cmd("*1\r\n$8\r\nFLUSHALL\r\n");
    c.send_cmd("*4\r\n$4\r\nZADD\r\n$1\r\nz\r\n$1\r\n2\r\n$1\r\nb\r\n");
    c.send_cmd("*4\r\n$4\r\nZADD\r\n$1\r\nz\r\n$1\r\n1\r\n$1\r\na\r\n");
    it("ZCARD", c.send_cmd("*2\r\n$5\r\nZCARD\r\n$1\r\nz\r\n") == ":2\r\n");
    it("ZRANGE order", c.send_cmd("*4\r\n$6\r\nZRANGE\r\n$1\r\nz\r\n$1\r\n0\r\n$2\r\n-1\r\n") ==
                          "*2\r\n$1\r\na\r\n$1\r\nb\r\n");
    it("TYPE zset", c.send_cmd("*2\r\n$4\r\nTYPE\r\n$1\r\nz\r\n") == "+zset\r\n");

    // TTL (lazy expiry on read)
    c.send_cmd("*1\r\n$8\r\nFLUSHALL\r\n");
    c.send_cmd("*5\r\n$3\r\nSET\r\n$3\r\nttl\r\n$1\r\nx\r\n$2\r\nPX\r\n$3\r\n120\r\n");
    it("GET before PX expiry", c.send_cmd("*2\r\n$3\r\nGET\r\n$3\r\nttl\r\n") == "$1\r\nx\r\n");
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    it("GET after PX expiry", c.send_cmd("*2\r\n$3\r\nGET\r\n$3\r\nttl\r\n") == "$-1\r\n");

    // Streams
    c.send_cmd("*1\r\n$8\r\nFLUSHALL\r\n");
    c.send_cmd("*5\r\n$4\r\nXADD\r\n$1\r\ns\r\n$3\r\n1-0\r\n$1\r\nf\r\n$1\r\nv\r\n");
    it("XADD auto-ish id",
       c.send_cmd("*5\r\n$4\r\nXADD\r\n$1\r\ns\r\n$3\r\n1-1\r\n$1\r\ng\r\n$1\r\nw\r\n") == "$3\r\n1-1\r\n");
    it("XADD id too small",
       c.send_cmd("*5\r\n$4\r\nXADD\r\n$1\r\ns\r\n$3\r\n0-0\r\n$1\r\na\r\n$1\r\nb\r\n").find("-ERR") !=
           std::string::npos);
    const std::string xr = c.send_cmd("*4\r\n$6\r\nXRANGE\r\n$1\r\ns\r\n$1\r\n-\r\n$1\r\n+\r\n");
    it("XRANGE two entries", xr.starts_with("*2\r\n"));

    // TYPE + KEYS
    c.send_cmd("*1\r\n$8\r\nFLUSHALL\r\n");
    c.send_cmd("*3\r\n$3\r\nSET\r\n$4\r\nkfoo\r\n$1\r\nz\r\n");
    const std::string keys = c.send_cmd("*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n");
    it("KEYS kfoo", keys.find("$4\r\nkfoo\r\n") != std::string::npos);
    it("TYPE string", c.send_cmd("*2\r\n$4\r\nTYPE\r\n$4\r\nkfoo\r\n") == "+string\r\n");
    c.send_cmd("*1\r\n$8\r\nFLUSHALL\r\n");
    c.send_cmd("*5\r\n$4\r\nXADD\r\n$1\r\nsx\r\n$3\r\n1-0\r\n$1\r\na\r\n$1\r\nb\r\n");
    it("TYPE stream", c.send_cmd("*2\r\n$4\r\nTYPE\r\n$2\r\nsx\r\n") == "+stream\r\n");
    c.send_cmd("*1\r\n$8\r\nFLUSHALL\r\n");
    it("TYPE none", c.send_cmd("*2\r\n$4\r\nTYPE\r\n$7\r\nnothing\r\n") == "+none\r\n");

    // ACL / INFO / CONFIG
    it("ACL WHOAMI", c.send_cmd("*2\r\n$3\r\nACL\r\n$6\r\nWHOAMI\r\n") == "$7\r\ndefault\r\n");
    {
        const std::string info = c.send_cmd("*2\r\n$4\r\nINFO\r\n$11\r\nreplication\r\n");
        it("INFO replication", info[0] == '$' && info.find("role:") != std::string::npos);
    }
    {
        const std::string cfg = c.send_cmd("*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$3\r\ndir\r\n");
        it("CONFIG GET dir", cfg.starts_with("*2\r\n") && cfg.find("$3\r\ndir\r\n") != std::string::npos);
    }

    // Transactions
    c.send_cmd("*1\r\n$8\r\nFLUSHALL\r\n");
    it("MULTI", c.send_cmd("*1\r\n$4\r\nMULTI\r\n") == "+OK\r\n");
    {
        const std::string nested = c.send_cmd("*1\r\n$4\r\nMULTI\r\n");
        it("nested MULTI", nested.find("MULTI calls can not be nested") != std::string::npos);
    }
    it("DISCARD", c.send_cmd("*1\r\n$7\r\nDISCARD\r\n") == "+OK\r\n");
    it("MULTI again", c.send_cmd("*1\r\n$4\r\nMULTI\r\n") == "+OK\r\n");
    it("SET queued", c.send_cmd("*3\r\n$3\r\nSET\r\n$2\r\ntx\r\n$1\r\n1\r\n") == "+QUEUED\r\n");
    it("GET queued", c.send_cmd("*2\r\n$3\r\nGET\r\n$2\r\ntx\r\n") == "+QUEUED\r\n");
    it("EXEC SET+GET", c.send_cmd("*1\r\n$4\r\nEXEC\r\n") == "*2\r\n+OK\r\n$1\r\n1\r\n");

    // EXEC without MULTI
    it("EXEC without MULTI", c.send_cmd("*1\r\n$4\r\nEXEC\r\n") == "-ERR EXEC without MULTI\r\n");

    std::cout << "--- done (" << g_fails << " failure(s)) ---\n";
    return g_fails == 0 ? 0 : 1;
}
