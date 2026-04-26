#include <vector>
#include <string>
#include <unordered_map>
#include <cstdint>
#include <mutex>
#include <functional>
#include <iostream>
#include <algorithm>
#include <sys/socket.h>
#include <condition_variable>
#include <queue>
#include <cstring>
#include <stdexcept>
#include "commands.h"
#include "respparser.h" // Wherever your RespValue struct is
#include "dataStructures.h" // Wherever your Node struct and key_value_store are

// Link to the globals in main.cpp
extern std::unordered_map<std::string, Node> key_value_store;
extern std::mutex store_mutex;
extern std::unordered_map<std::string, std::uint64_t> key_versions;
extern std::uint64_t global_flush_epoch;
extern std::priority_queue<ExpiryEntry, std::vector<ExpiryEntry>, std::greater<ExpiryEntry>> expiry_heap;
extern std::condition_variable expiry_cv;
extern bool server_is_replica;
extern std::string server_rdb_dir;
extern std::string server_rdb_dbfilename;

namespace {

// Empty RDB (Redis 7.2 format) — hex from Codecrafters:
// https://github.com/codecrafters-io/redis-tester/blob/main/internal/assets/empty_rdb_hex.md
bool send_all(int fd, const void* data, std::size_t len) {
    const auto* p = static_cast<const char*>(data);
    while (len > 0) {
        const ssize_t n = send(fd, p, len, 0);
        if (n <= 0) {
            return false;
        }
        p += static_cast<std::size_t>(n);
        len -= static_cast<std::size_t>(n);
    }
    return true;
}

static constexpr std::uint8_t kEmptyRdb[] = {
    0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73,
    0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a, 0x72, 0x65, 0x64, 0x69,
    0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74, 0x69, 0x6d, 0x65, 0xc2,
    0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, 0xc2, 0xb0,
    0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61, 0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00, 0xff,
    0xf0, 0x6e, 0x3b, 0xfe, 0xc0, 0xff, 0x5a, 0xa2,
};
constexpr std::size_t kEmptyRdbLen = sizeof(kEmptyRdb);

std::mutex g_repl_targets_mutex;
std::condition_variable g_repl_ack_cv;
std::vector<int> g_repl_targets;
std::unordered_map<int, std::uint64_t> g_repl_ack_offsets;
std::uint64_t g_master_repl_offset = 0;
std::uint64_t g_last_wait_offset = 0;

bool command_propagates_to_replicas(const std::string& cmd_upper) {
    return cmd_upper == "SET" || cmd_upper == "FLUSHALL" || cmd_upper == "INCR"
        || cmd_upper == "RPUSH" || cmd_upper == "LPUSH" || cmd_upper == "LPOP"
        || cmd_upper == "BLPOP" || cmd_upper == "XADD";
}

std::size_t count_acked_replicas_locked(std::uint64_t target_offset) {
    std::size_t acked = 0;
    for (int fd : g_repl_targets) {
        auto it = g_repl_ack_offsets.find(fd);
        if (it != g_repl_ack_offsets.end() && it->second >= target_offset) {
            ++acked;
        }
    }
    return acked;
}

void replication_register_replica(int fd) {
    if (server_is_replica) {
        return;
    }
    std::lock_guard<std::mutex> lock(g_repl_targets_mutex);
    for (int existing : g_repl_targets) {
        if (existing == fd) {
            g_repl_ack_offsets[fd] = g_master_repl_offset;
            return;
        }
    }
    g_repl_targets.push_back(fd);
    g_repl_ack_offsets[fd] = g_master_repl_offset;
}

void replication_propagate(const RespValue& request) {
    if (server_is_replica) {
        return;
    }
    const std::string payload = RespParser::serialize_array(request);
    if (payload.empty()) {
        return;
    }
    std::vector<int> targets;
    {
        std::lock_guard<std::mutex> lock(g_repl_targets_mutex);
        targets = g_repl_targets;
        g_master_repl_offset += payload.size();
    }
    for (int repl_fd : targets) {
        (void)send_all(repl_fd, payload.data(), payload.size());
    }
}

} // namespace

void store_bump_key_revision(const std::string& key) { key_versions[key]++; }

void store_note_database_flush() { global_flush_epoch++; }

void handle_ping(int fd, const RespValue& request) {
    // In Redis, PING can optionally take a message to echo back.
    // For now, we'll keep it simple as per your request.
    if (request.elements.size() > 1) {
        std::string msg = request.elements[1].bulkString;
        std::string resp = "$" + std::to_string(msg.length()) + "\r\n" + msg + "\r\n";
        send(fd, resp.c_str(), resp.length(), 0);
    } else {
        send(fd, "+PONG\r\n", 7, 0);
    }
}

void handle_replconf(int fd, const RespValue& request) {
    if (request.elements.size() >= 3) {
        std::string sub = request.elements[1].bulkString;
        std::transform(sub.begin(), sub.end(), sub.begin(), ::toupper);
        if (sub == "ACK") {
            char* end = nullptr;
            const long long ack = std::strtoll(request.elements[2].bulkString.c_str(), &end, 10);
            if (end != request.elements[2].bulkString.c_str()) {
                std::lock_guard<std::mutex> lock(g_repl_targets_mutex);
                g_repl_ack_offsets[fd] = static_cast<std::uint64_t>(std::max<long long>(0, ack));
                g_repl_ack_cv.notify_all();
            }
            return;
        }
    }
    send(fd, "+OK\r\n", 5, 0);
}

void handle_psync(int fd, const RespValue& request) {
    if (request.elements.size() < 3) {
        const char* err = "-ERR wrong number of arguments for 'psync' command\r\n";
        send(fd, err, strlen(err), 0);
        return;
    }
    static constexpr char kReplId[] = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    std::string resp = "+FULLRESYNC ";
    resp += kReplId;
    resp += " 0\r\n";
    if (!send_all(fd, resp.data(), resp.size())) {
        return;
    }
    std::string bulk_head = "$" + std::to_string(kEmptyRdbLen) + "\r\n";
    if (!send_all(fd, bulk_head.data(), bulk_head.size())) {
        return;
    }
    if (!send_all(fd, kEmptyRdb, kEmptyRdbLen)) {
        return;
    }
    replication_register_replica(fd);
}

void handle_wait(int fd, const RespValue& request) {
    if (request.elements.size() < 3) {
        const char* err = "-ERR wrong number of arguments for 'wait' command\r\n";
        send(fd, err, strlen(err), 0);
        return;
    }

    long long requested = 0;
    long long timeout_ms = 0;
    try {
        requested = std::stoll(request.elements[1].bulkString);
        timeout_ms = std::stoll(request.elements[2].bulkString);
    } catch (...) {
        const char* err = "-ERR value is not an integer or out of range\r\n";
        send(fd, err, strlen(err), 0);
        return;
    }
    if (requested < 0) requested = 0;
    if (timeout_ms < 0) timeout_ms = 0;

    const std::uint64_t target_offset = [&]() {
        std::lock_guard<std::mutex> lock(g_repl_targets_mutex);
        return g_master_repl_offset;
    }();

    bool need_getack = false;
    std::vector<int> targets;
    {
        std::lock_guard<std::mutex> lock(g_repl_targets_mutex);
        need_getack = (target_offset > g_last_wait_offset);
        targets = g_repl_targets;
    }

    if (need_getack && !targets.empty()) {
        static constexpr char kGetAck[] =
            "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
        for (int repl_fd : targets) {
            (void)send_all(repl_fd, kGetAck, sizeof(kGetAck) - 1);
        }
    }

    std::unique_lock<std::mutex> lock(g_repl_targets_mutex);
    auto enough_acked = [&]() {
        return count_acked_replicas_locked(target_offset) >= static_cast<std::size_t>(requested);
    };

    if (!enough_acked()) {
        if (timeout_ms == 0) {
            g_repl_ack_cv.wait(lock, enough_acked);
        } else {
            g_repl_ack_cv.wait_for(lock, std::chrono::milliseconds(timeout_ms), enough_acked);
        }
    }

    const std::size_t acked = count_acked_replicas_locked(target_offset);
    if (target_offset > g_last_wait_offset) {
        g_last_wait_offset = target_offset;
    }
    lock.unlock();

    const std::string resp = ":" + std::to_string(acked) + "\r\n";
    send(fd, resp.c_str(), resp.size(), 0);
}

void handle_echo(int fd, const RespValue& request) {
    if (request.elements.size() < 2) {
        send(fd, "-ERR wrong number of arguments for 'echo' command\r\n", 50, 0);
        return;
    }
    std::string msg = request.elements[1].bulkString;
    std::string resp = "$" + std::to_string(msg.length()) + "\r\n" + msg + "\r\n";
    send(fd, resp.c_str(), resp.length(), 0);
}

void handle_flushall(int fd, const RespValue& request) {
    std::lock_guard<std::mutex> lock(store_mutex);
    store_note_database_flush();
    key_value_store.clear();
    
    while (!expiry_heap.empty()) {
        expiry_heap.pop();
    }
    
    send(fd, "+OK\r\n", 5, 0);
    
    // Wake up any blocked XREAD/BLPOP threads
    expiry_cv.notify_all(); 
}

void handle_set(int fd, const RespValue& request) {
    if (request.elements.size() < 3) {
        send(fd, "-ERR wrong number of arguments for 'set' command\r\n", 49, 0);
        return;
    }

    std::string key = request.elements[1].bulkString;
    std::string val = request.elements[2].bulkString;

    Node n;
    n.value = val;
    n.type = KeyType::String;
    n.hasTTL = false;

    // Handle EX/PX
    if (request.elements.size() >= 5) {
        std::string flag = request.elements[3].bulkString;
        std::transform(flag.begin(), flag.end(), flag.begin(), ::toupper);

        try {
            long long ms = std::stoll(request.elements[4].bulkString);
            if (flag == "EX") ms *= 1000;

            n.hasTTL = true;
            n.expires_at = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
        } catch (...) {
            send(fd, "-ERR value is not an integer or out of range\r\n", 46, 0);
            return;
        }
    }

    {
        std::lock_guard<std::mutex> lock(store_mutex);
        key_value_store[key] = n;
        store_bump_key_revision(key);
        if (n.hasTTL) {
            expiry_heap.push({key, n.expires_at});
        }
    }
    send(fd, "+OK\r\n", 5, 0);
}

// --- GET ---
void handle_get(int fd, const RespValue& request) {
    if (request.elements.size() < 2) {
        send(fd, "-ERR wrong number of arguments for 'get' command\r\n", 50, 0);
        return;
    }
    std::string key = request.elements[1].bulkString;
    std::lock_guard<std::mutex> lock(store_mutex);

    auto it = key_value_store.find(key);
    if (it == key_value_store.end()) {
        send(fd, "$-1\r\n", 5, 0);
    } else {
        Node &node = it->second;
        // Lazy Expiration Check
        if (node.hasTTL && std::chrono::steady_clock::now() >= node.expires_at) {
            store_bump_key_revision(key);
            key_value_store.erase(it);
            send(fd, "$-1\r\n", 5, 0);
        } 
        else if (node.type != KeyType::String) {
            const char* err = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
            send(fd, err, strlen(err), 0);
        } 
        else {
            std::string& result = std::get<std::string>(node.value);
            std::string resp = "$" + std::to_string(result.length()) + "\r\n" + result + "\r\n";
            send(fd, resp.c_str(), resp.length(), 0);
        }
    }
}

void handle_incr(int fd, const RespValue& request) {
    // 1. Argument Check
    if (request.elements.size() < 2) {
        send(fd, "-ERR wrong number of arguments for 'incr' command\r\n", 50, 0);
        return;
    }

    std::string key = request.elements[1].bulkString;

    // 2. Lock the global store
    std::lock_guard<std::mutex> lock(store_mutex);
    
    // 3. Check if key exists
    auto it = key_value_store.find(key);
    if (it == key_value_store.end()) {
        // Case: Key doesn't exist. Create it with "1"
        Node n;
        n.type = KeyType::String;
        n.value = "1"; 
        n.hasTTL = false;
        key_value_store[key] = n;
        store_bump_key_revision(key);

        send(fd, ":1\r\n", 4, 0);
    } 
    else {
        Node &node = it->second;

        // 4. Type Check: Must be a String
        if (node.type != KeyType::String) {
            std::string err = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
            send(fd, err.c_str(), err.length(), 0);
        } 
        else {
            // 5. Extract string and attempt to parse as integer
            std::string& val_str = std::get<std::string>(node.value);
            
            try {
                size_t processed_char_count = 0;
                long long current_val = std::stoll(val_str, &processed_char_count);

                // Strict Check: Redis requires the WHOLE string to be the number.
                if (processed_char_count != val_str.length()) {
                    throw std::invalid_argument("trailing characters");
                }

                // 6. Increment and update
                current_val++;
                node.value = std::to_string(current_val);
                store_bump_key_revision(key);

                // 7. Respond with Integer RESP (starts with ':')
                std::string resp = ":" + std::to_string(current_val) + "\r\n";
                send(fd, resp.c_str(), resp.length(), 0);

            } catch (...) {
                // Catches stoll failures (non-numeric strings) and overflows
                std::string err = "-ERR value is not an integer or out of range\r\n";
                send(fd, err.c_str(), err.length(), 0);
            }
        }
    }
}

// --- RPUSH ---
void handle_rpush(int fd, const RespValue& request) {
    if (request.elements.size() < 3) {
        const char* err = "-ERR wrong number of arguments for 'rpush' command\r\n";
        send(fd, err, strlen(err), 0);
        return;
    }
    std::string key = request.elements[1].bulkString;
    std::lock_guard<std::mutex> lock(store_mutex);
    Node &node = key_value_store[key];

    if (node.type == KeyType::None) {
        node.type = KeyType::List;
        node.value = std::vector<std::string>{};
    }
    if (node.type != KeyType::List) {
        const char* err = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        send(fd, err, strlen(err), 0);
        return;
    }

    auto& list = std::get<std::vector<std::string>>(node.value);
    for (size_t i = 2; i < request.elements.size(); ++i) {
        list.push_back(request.elements[i].bulkString);
    }
    store_bump_key_revision(key);

    std::string resp = ":" + std::to_string(list.size()) + "\r\n";
    send(fd, resp.c_str(), resp.length(), 0);
    expiry_cv.notify_all();
}

// --- LPUSH ---
void handle_lpush(int fd, const RespValue& request) {
    if (request.elements.size() < 3) {
        const char* err = "-ERR wrong number of arguments for 'lpush' command\r\n";
        send(fd, err, strlen(err), 0);
        return;
    }
    std::string key = request.elements[1].bulkString;
    std::lock_guard<std::mutex> lock(store_mutex);
    Node &node = key_value_store[key];

    if (node.type == KeyType::None) {
        node.type = KeyType::List;
        node.value = std::vector<std::string>{};
    }
    if (node.type != KeyType::List) {
        const char* err = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        send(fd, err, strlen(err), 0);
        return;
    }

    auto& list = std::get<std::vector<std::string>>(node.value);
    for (size_t i = 2; i < request.elements.size(); ++i) {
        list.insert(list.begin(), request.elements[i].bulkString);
    }
    store_bump_key_revision(key);

    std::string resp = ":" + std::to_string(list.size()) + "\r\n";
    send(fd, resp.c_str(), resp.length(), 0);
    expiry_cv.notify_all();
}

// --- LRANGE ---
void handle_lrange(int fd, const RespValue& request) {
    if (request.elements.size() < 4) {
        const char* err = "-ERR wrong number of arguments for 'lrange' command\r\n";
        send(fd, err, strlen(err), 0);
        return;
    }
    std::string key = request.elements[1].bulkString;
    long long start;
    long long stop;
    try {
        start = std::stoll(request.elements[2].bulkString);
        stop = std::stoll(request.elements[3].bulkString);
    } catch (...) {
        const char* err = "-ERR value is not an integer or out of range\r\n";
        send(fd, err, strlen(err), 0);
        return;
    }

    std::lock_guard<std::mutex> lock(store_mutex);
    auto it = key_value_store.find(key);
    if (it == key_value_store.end() || it->second.type != KeyType::List) {
        send(fd, "*0\r\n", 4, 0);
    } else {
        auto& list = std::get<std::vector<std::string>>(it->second.value);
        long long size = static_cast<long long>(list.size());

        // Index Normalization
        if (start < 0) start = size + start;
        if (stop < 0) stop = size + stop;
        if (start < 0) start = 0;
        if (stop >= size) stop = size - 1;

        if (start >= size || start > stop) {
            send(fd, "*0\r\n", 4, 0);
        } else {
            long long count = stop - start + 1;
            std::string header = "*" + std::to_string(count) + "\r\n";
            send(fd, header.c_str(), header.length(), 0);
            for (long long i = start; i <= stop; ++i) {
                std::string resp = "$" + std::to_string(list[i].length()) + "\r\n" + list[i] + "\r\n";
                send(fd, resp.c_str(), resp.length(), 0);
            }
        }
    }
}

// --- LLEN ---
void handle_llen(int fd, const RespValue& request) {
    if (request.elements.size() < 2) {
        const char* err = "-ERR wrong number of arguments for 'llen' command\r\n";
        send(fd, err, strlen(err), 0);
        return;
    }
    std::string key = request.elements[1].bulkString;
    std::lock_guard<std::mutex> lock(store_mutex);
    
    long long len = 0;
    auto it = key_value_store.find(key);
    if (it != key_value_store.end() && it->second.type == KeyType::List) {
        len = std::get<std::vector<std::string>>(it->second.value).size();
    }
    std::string resp = ":" + std::to_string(len) + "\r\n";
    send(fd, resp.c_str(), resp.length(), 0);
}

// --- LPOP ---
void handle_lpop(int fd, const RespValue& request) {
    if (request.elements.size() < 2) {
        const char* err = "-ERR wrong number of arguments for 'lpop' command\r\n";
        send(fd, err, strlen(err), 0);
        return;
    }
    std::string key = request.elements[1].bulkString;
    bool has_count = (request.elements.size() >= 3);
    int count = 1;
    if (has_count) {
        try {
            count = std::stoi(request.elements[2].bulkString);
        } catch (...) {
            const char* err = "-ERR value is not an integer or out of range\r\n";
            send(fd, err, strlen(err), 0);
            return;
        }
    }

    std::lock_guard<std::mutex> lock(store_mutex);
    auto it = key_value_store.find(key);
    
    if (it == key_value_store.end()) {
        send(fd, "$-1\r\n", 5, 0);
    } else if (it->second.type != KeyType::List) {
        const char* err = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        send(fd, err, strlen(err), 0);
    } else {
        auto& list = std::get<std::vector<std::string>>(it->second.value);
        if (has_count && count <= 0) {
            send(fd, "*0\r\n", 4, 0);
        } else if (!has_count) {
            // Single Pop
            std::string val = list.front();
            list.erase(list.begin());
            store_bump_key_revision(key);
            if (list.empty()) key_value_store.erase(it);
            std::string resp = "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
            send(fd, resp.c_str(), resp.length(), 0);
        } else {
            // Multi Pop
            int actual_to_pop = std::min((int)list.size(), count);
            std::string header = "*" + std::to_string(actual_to_pop) + "\r\n";
            send(fd, header.c_str(), header.length(), 0);
            for (int i = 0; i < actual_to_pop; ++i) {
                std::string val = list.front();
                list.erase(list.begin());
                std::string resp = "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
                send(fd, resp.c_str(), resp.length(), 0);
            }
            if (actual_to_pop > 0) {
                store_bump_key_revision(key);
            }
            if (list.empty()) key_value_store.erase(it);
        }
    }
}

void handle_blpop(int fd, const RespValue& request) {
    if (request.elements.size() < 3) {
        const char* err = "-ERR wrong number of arguments for 'blpop' command\r\n";
        send(fd, err, strlen(err), 0);
        return;
    }
    std::string key = request.elements[1].bulkString;
    double timeout_sec;
    try {
        timeout_sec = std::stod(request.elements.back().bulkString);
    } catch (...) {
        const char* err = "-ERR value is not a valid float\r\n";
        send(fd, err, strlen(err), 0);
        return;
    }

    std::unique_lock<std::mutex> lock(store_mutex);

    // Predicate: Is there a list and is it non-empty?
    auto check_list = [&]() {
        if (key_value_store.find(key) == key_value_store.end()) return false;
        Node &n = key_value_store[key];
        if (n.type != KeyType::List) return false;
        return !std::get<std::vector<std::string>>(n.value).empty();
    };

    // Type Check
    if (key_value_store.count(key) && key_value_store[key].type != KeyType::List) {
        send(fd, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", 68, 0);
        return;
    }

    bool data_available = true;
    if (!check_list()) {
        if (timeout_sec == 0) {
            expiry_cv.wait(lock, check_list);
        } else {
            data_available = expiry_cv.wait_for(lock, std::chrono::duration<double>(timeout_sec), check_list);
        }
    }

    if (data_available && check_list()) {
        auto& list = std::get<std::vector<std::string>>(key_value_store[key].value);
        std::string val = list.front();
        list.erase(list.begin());
        store_bump_key_revision(key);
        if (list.empty()) key_value_store.erase(key);

        std::string resp = "*2\r\n";
        resp += "$" + std::to_string(key.length()) + "\r\n" + key + "\r\n";
        resp += "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
        send(fd, resp.c_str(), resp.length(), 0);
    } else {
        send(fd, "*-1\r\n", 5, 0); // Timeout case
    }
}

void handle_type(int fd, const RespValue& request) {
    if (request.elements.size() < 2) {
        const char* err = "-ERR wrong number of arguments for 'type' command\r\n";
        send(fd, err, strlen(err), 0);
        return;
    }
    std::string key = request.elements[1].bulkString;
    std::string result = "none";

    std::lock_guard<std::mutex> lock(store_mutex);
    auto it = key_value_store.find(key);
    if (it != key_value_store.end()) {
        Node &node = it->second;
        if (node.hasTTL && std::chrono::steady_clock::now() >= node.expires_at) {
            store_bump_key_revision(key);
            key_value_store.erase(it);
        } else {
            result = typeToString(node.type);
        }
    }
    std::string resp = "+" + result + "\r\n";
    send(fd, resp.c_str(), resp.length(), 0);
}

void handle_info(int fd, const RespValue& request) {
    std::string want;
    if (request.elements.size() >= 2) {
        want = request.elements[1].bulkString;
        std::transform(want.begin(), want.end(), want.begin(), ::tolower);
    }

    std::string payload;
    if (want.empty() || want == "replication") {
        const char* role = server_is_replica ? "slave" : "master";
        payload = std::string("# Replication\r\nrole:") + role + "\r\n"
            "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n"
            "master_repl_offset:0\r\n";
    }

    std::string out = "$" + std::to_string(payload.size()) + "\r\n" + payload + "\r\n";
    send(fd, out.c_str(), out.length(), 0);
}

void handle_config(int fd, const RespValue& request) {
    if (request.elements.size() < 3) {
        const char* err = "-ERR wrong number of arguments for 'config' command\r\n";
        send(fd, err, strlen(err), 0);
        return;
    }

    std::string sub = request.elements[1].bulkString;
    std::transform(sub.begin(), sub.end(), sub.begin(), ::toupper);
    if (sub != "GET") {
        const char* err = "-ERR unsupported CONFIG subcommand\r\n";
        send(fd, err, strlen(err), 0);
        return;
    }

    std::string key = request.elements[2].bulkString;
    std::string lower = key;
    std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);

    std::string value;
    if (lower == "dir") {
        value = server_rdb_dir;
    } else if (lower == "dbfilename") {
        value = server_rdb_dbfilename;
    } else {
        send(fd, "*0\r\n", 4, 0);
        return;
    }

    std::string resp = "*2\r\n";
    resp += "$" + std::to_string(lower.size()) + "\r\n" + lower + "\r\n";
    resp += "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
    send(fd, resp.c_str(), resp.size(), 0);
}

void handle_watch(int fd, const RespValue& request,
                  std::unordered_map<std::string, std::uint64_t>& watch_versions,
                  std::uint64_t& watch_flush_epoch) {
    std::lock_guard<std::mutex> lock(store_mutex);
    if (request.elements.size() > 1) {
        watch_flush_epoch = global_flush_epoch;
    }
    for (size_t i = 1; i < request.elements.size(); ++i) {
        const std::string& k = request.elements[i].bulkString;
        auto it = key_versions.find(k);
        watch_versions[k] = (it == key_versions.end() ? 0 : it->second);
    }
    send(fd, "+OK\r\n", 5, 0);
}

void handle_unwatch(int fd, std::unordered_map<std::string, std::uint64_t>& watch_versions,
                     std::uint64_t& watch_flush_epoch) {
    watch_versions.clear();
    watch_flush_epoch = 0;
    send(fd, "+OK\r\n", 5, 0);
}

void handle_xadd(int fd, const RespValue& request) {
    // XADD key id field value [field value ...] — at least one field/value pair after id.
    if (request.elements.size() < 5 || (request.elements.size() - 3) % 2 != 0) {
        const char* err = "-ERR wrong number of arguments for 'xadd' command\r\n";
        send(fd, err, strlen(err), 0);
        return;
    }

    std::string key = request.elements[1].bulkString;
    std::string id_req = request.elements[2].bulkString;

    std::lock_guard<std::mutex> lock(store_mutex);
    Node &node = key_value_store[key];

    if (node.type == KeyType::None) {
        node.type = KeyType::Stream;
        node.value = std::vector<StreamEntry>{};
    } else if (node.type != KeyType::Stream) {
        const char* err = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        send(fd, err, strlen(err), 0);
        return;
    }

    auto& stream = std::get<std::vector<StreamEntry>>(node.value);
    StreamID last_id = stream.empty() ? StreamID{0, 0} : stream.back().id;
    StreamID final_id;

    try {
        if (id_req == "*") {
            long long now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            final_id.ms = std::max(now_ms, last_id.ms);
            final_id.seq = (final_id.ms == last_id.ms) ? last_id.seq + 1 : 0;
            if (final_id.ms == 0 && final_id.seq == 0) final_id.seq = 1;
        } else if (id_req.find("-*") != std::string::npos) {
            long long req_ms = std::stoll(id_req.substr(0, id_req.find("-*")));
            if (req_ms < last_id.ms) {
                const char* err =
                    "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
                send(fd, err, strlen(err), 0);
                return;
            }
            final_id.ms = req_ms;
            final_id.seq = (req_ms == last_id.ms) ? last_id.seq + 1 : 0;
            if (final_id.ms == 0 && final_id.seq == 0) final_id.seq = 1;
        } else {
            // Explicit ID only (Codecrafters HQ8): Redis distinguishes 0-0 from other invalid IDs.
            final_id = StreamID::parse(id_req);
            if (final_id.ms == 0 && final_id.seq == 0) {
                const char* err = "-ERR The ID specified in XADD must be greater than 0-0\r\n";
                send(fd, err, strlen(err), 0);
                return;
            }
            if (!stream.empty() && final_id <= last_id) {
                const char* err =
                    "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
                send(fd, err, strlen(err), 0);
                return;
            }
        }
    } catch (...) {
        const char* err = "-ERR Invalid stream ID specified for XADD command\r\n";
        send(fd, err, strlen(err), 0);
        return;
    }

    StreamEntry entry;
    entry.id = final_id;
    for (size_t i = 3; i + 1 < request.elements.size(); i += 2) {
        entry.fields.push_back({request.elements[i].bulkString, request.elements[i+1].bulkString});
    }
    stream.push_back(entry);
    store_bump_key_revision(key);

    std::string id_str = final_id.toString();
    std::string resp = "$" + std::to_string(id_str.length()) + "\r\n" + id_str + "\r\n";
    send(fd, resp.c_str(), resp.length(), 0);
    expiry_cv.notify_all();
}

void handle_xrange(int fd, const RespValue& request) {
    // 1. Validation
    if (request.elements.size() < 4) {
        const char* err = "-ERR wrong number of arguments for 'xrange' command\r\n";
        send(fd, err, strlen(err), 0);
        return;
    }

    std::string key = request.elements[1].bulkString;
    
    // 2. Parse range boundaries
    StreamID start_id = StreamID::parseRange(request.elements[2].bulkString, true);
    StreamID end_id = StreamID::parseRange(request.elements[3].bulkString, false);

    std::lock_guard<std::mutex> lock(store_mutex);
    
    auto it = key_value_store.find(key);
    if (it == key_value_store.end() || it->second.type != KeyType::Stream) {
        // Redis returns an empty array if key doesn't exist
        send(fd, "*0\r\n", 4, 0);
        return;
    }

    auto& stream = std::get<std::vector<StreamEntry>>(it->second.value);

    // 3. Binary Search for the range
    // First element >= start_id
    auto start_it = std::lower_bound(stream.begin(), stream.end(), start_id, 
        [](const StreamEntry& e, const StreamID& id) { return e.id < id; });

    // First element > end_id
    auto end_it = std::upper_bound(stream.begin(), stream.end(), end_id, 
        [](const StreamID& id, const StreamEntry& e) { return id < e.id; });

    // 4. Calculate count and send RESP Array Header
    long long count = std::distance(start_it, end_it);
    if (count < 0) count = 0;
    
    std::string header = "*" + std::to_string(count) + "\r\n";
    send(fd, header.c_str(), header.length(), 0);

    // 5. Serialize and Send Entries
    for (auto entry_it = start_it; entry_it != end_it; ++entry_it) {
        const auto& entry = *entry_it;
        
        // Each entry is [ID, [fields]]
        std::string entry_resp = "*2\r\n";
        
        // Send ID
        std::string id_str = entry.id.toString();
        entry_resp += "$" + std::to_string(id_str.length()) + "\r\n" + id_str + "\r\n";
        
        // Send Fields Array
        entry_resp += "*" + std::to_string(entry.fields.size() * 2) + "\r\n";
        for (const auto& pair : entry.fields) {
            entry_resp += "$" + std::to_string(pair.first.length()) + "\r\n" + pair.first + "\r\n";
            entry_resp += "$" + std::to_string(pair.second.length()) + "\r\n" + pair.second + "\r\n";
        }
        
        send(fd, entry_resp.c_str(), entry_resp.length(), 0);
    }
}

std::unordered_map<std::string, std::function<void(int, const RespValue&)>> handlers = {
    // Basic
    {"PING",     handle_ping},
    {"REPLCONF", handle_replconf},
    {"PSYNC",    handle_psync},
    {"ECHO",     handle_echo},
    {"WAIT",     handle_wait},
    {"FLUSHALL", handle_flushall},

    // Strings & Numbers
    {"SET",      handle_set},
    {"GET",      handle_get},
    {"INCR",     handle_incr},

    // Lists
    {"RPUSH",    handle_rpush},
    {"LPUSH",    handle_lpush},
    {"LRANGE",   handle_lrange},
    {"LLEN",     handle_llen},
    {"LPOP",     handle_lpop},
    {"BLPOP",    handle_blpop},

    // // Streams
    {"XADD",     handle_xadd},
    {"XRANGE",   handle_xrange},
    // {"XREAD",    handle_xread},

    // // Metadata
    {"TYPE",     handle_type},
    {"INFO",     handle_info},
    {"CONFIG",   handle_config}
};


void replication_unregister_replica(int client_fd) {
    std::lock_guard<std::mutex> lock(g_repl_targets_mutex);
    g_repl_targets.erase(
        std::remove(g_repl_targets.begin(), g_repl_targets.end(), client_fd),
        g_repl_targets.end());
    g_repl_ack_offsets.erase(client_fd);
    g_repl_ack_cv.notify_all();
}

void execute_command(int client_fd, const RespValue& request) {
    if (request.elements.empty()) return;

    std::string cmd_name = request.elements[0].bulkString;
    std::transform(cmd_name.begin(), cmd_name.end(), cmd_name.begin(), ::toupper);

    auto it = handlers.find(cmd_name);
    if (it != handlers.end()) {
        it->second(client_fd, request);
        if (cmd_name == "SET" || cmd_name == "RPUSH" || cmd_name == "LPUSH") {
            expiry_cv.notify_all(); // Notify after modifying data that BLPOP/XREAD might be waiting on
        }
        if (!server_is_replica && command_propagates_to_replicas(cmd_name)) {
            replication_propagate(request);
        }
    } else {
        std::string err = "-ERR unknown command '" + cmd_name + "'\r\n";
        send(client_fd, err.c_str(), err.length(), 0);

    }
}

void execute_command_for_exec(int client_fd, const RespValue& request) {
    if (request.elements.empty()) {
        const char* err = "-ERR wrong number of arguments for 'exec' command\r\n";
        send(client_fd, err, strlen(err), 0);
        return;
    }
    std::string cmd_name = request.elements[0].bulkString;
    std::transform(cmd_name.begin(), cmd_name.end(), cmd_name.begin(), ::toupper);
    if (cmd_name == "UNWATCH") {
        send(client_fd, "+OK\r\n", 5, 0);
        return;
    }
    try {
        execute_command(client_fd, request);
    } catch (const std::invalid_argument&) {
        const char* err = "-ERR value is not an integer or out of range\r\n";
        send(client_fd, err, strlen(err), 0);
    } catch (const std::out_of_range&) {
        const char* err = "-ERR value is not an integer or out of range\r\n";
        send(client_fd, err, strlen(err), 0);
    } catch (...) {
        const char* err = "-ERR unexpected error processing queued command\r\n";
        send(client_fd, err, strlen(err), 0);
    }
}

void execute_transaction_exec(int client_fd, std::vector<RespValue>& command_queue,
                                std::unordered_map<std::string, std::uint64_t>& watch_versions,
                                std::uint64_t& watch_flush_epoch) {
    const char* null_exec = "*-1\r\n";

    auto abort_exec = [&]() {
        send(client_fd, null_exec, 5, 0);
        watch_versions.clear();
        watch_flush_epoch = 0;
        command_queue.clear();
    };

    std::vector<RespValue> pending;
    {
        std::lock_guard<std::mutex> lock(store_mutex);
        if (!watch_versions.empty()) {
            if (watch_flush_epoch != global_flush_epoch) {
                abort_exec();
                return;
            }
            for (const auto& entry : watch_versions) {
                auto it = key_versions.find(entry.first);
                const std::uint64_t cur = (it == key_versions.end() ? 0 : it->second);
                if (cur != entry.second) {
                    abort_exec();
                    return;
                }
            }
        }

        watch_versions.clear();
        watch_flush_epoch = 0;
        pending = std::move(command_queue);
    }

    if (pending.empty()) {
        send(client_fd, "*0\r\n", 4, 0);
        return;
    }

    std::string header = "*" + std::to_string(pending.size()) + "\r\n";
    send(client_fd, header.c_str(), static_cast<int>(header.length()), 0);
    for (const auto& cmd : pending) {
        execute_command_for_exec(client_fd, cmd);
    }
}

