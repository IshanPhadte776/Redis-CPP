#pragma once
#include <vector>
#include <string>
#include <unordered_map>
#include <functional>
#include <iostream>
#include <algorithm>
#include <sys/socket.h>
#include <condition_variable>
#include <queue>
#include <cstring>
#include "commands.h"
#include "respparser.h" // Wherever your RespValue struct is
#include "dataStructures.h" // Wherever your Node struct and key_value_store are

// Link to the globals in main.cpp
extern std::unordered_map<std::string, Node> key_value_store;
extern std::mutex store_mutex;
extern std::priority_queue<ExpiryEntry, std::vector<ExpiryEntry>, std::greater<ExpiryEntry>> expiry_heap;
extern std::condition_variable expiry_cv;

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
    if (request.elements.size() < 3) return;
    std::string key = request.elements[1].bulkString;
    std::lock_guard<std::mutex> lock(store_mutex);
    Node &node = key_value_store[key];

    if (node.type == KeyType::None) {
        node.type = KeyType::List;
        node.value = std::vector<std::string>{};
    }
    if (node.type != KeyType::List) {
        send(fd, "-WRONGTYPE ...\r\n", 16, 0);
        return;
    }

    auto& list = std::get<std::vector<std::string>>(node.value);
    for (size_t i = 2; i < request.elements.size(); ++i) {
        list.push_back(request.elements[i].bulkString);
    }

    std::string resp = ":" + std::to_string(list.size()) + "\r\n";
    send(fd, resp.c_str(), resp.length(), 0);
    expiry_cv.notify_all();
}

// --- LPUSH ---
void handle_lpush(int fd, const RespValue& request) {
    if (request.elements.size() < 3) return;
    std::string key = request.elements[1].bulkString;
    std::lock_guard<std::mutex> lock(store_mutex);
    Node &node = key_value_store[key];

    if (node.type == KeyType::None) {
        node.type = KeyType::List;
        node.value = std::vector<std::string>{};
    }
    if (node.type != KeyType::List) {
        send(fd, "-WRONGTYPE ...\r\n", 16, 0);
        return;
    }

    auto& list = std::get<std::vector<std::string>>(node.value);
    for (size_t i = 2; i < request.elements.size(); ++i) {
        list.insert(list.begin(), request.elements[i].bulkString);
    }

    std::string resp = ":" + std::to_string(list.size()) + "\r\n";
    send(fd, resp.c_str(), resp.length(), 0);
    expiry_cv.notify_all();
}

// --- LRANGE ---
void handle_lrange(int fd, const RespValue& request) {
    if (request.elements.size() < 4) return;
    std::string key = request.elements[1].bulkString;
    long long start = std::stoll(request.elements[2].bulkString);
    long long stop = std::stoll(request.elements[3].bulkString);

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
    if (request.elements.size() < 2) return;
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
    if (request.elements.size() < 2) return;
    std::string key = request.elements[1].bulkString;
    bool has_count = (request.elements.size() >= 3);
    int count = has_count ? std::stoi(request.elements[2].bulkString) : 1;

    std::lock_guard<std::mutex> lock(store_mutex);
    auto it = key_value_store.find(key);
    
    if (it == key_value_store.end()) {
        send(fd, "$-1\r\n", 5, 0);
    } else if (it->second.type != KeyType::List) {
        send(fd, "-WRONGTYPE ...\r\n", 16, 0);
    } else {
        auto& list = std::get<std::vector<std::string>>(it->second.value);
        if (has_count && count <= 0) {
            send(fd, "*0\r\n", 4, 0);
        } else if (!has_count) {
            // Single Pop
            std::string val = list.front();
            list.erase(list.begin());
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
            if (list.empty()) key_value_store.erase(it);
        }
    }
}

std::unordered_map<std::string, std::function<void(int, const RespValue&)>> handlers = {
    // Basic
    {"PING",     handle_ping},
    {"ECHO",     handle_echo},
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
    {"LPOP",     handle_lpop}
    // {"BLPOP",    handle_blpop},

    // // Streams
    // {"XADD",     handle_xadd},
    // {"XRANGE",   handle_xrange},
    // {"XREAD",    handle_xread},

    // // Metadata
    // {"TYPE",     handle_type}
};


void execute_command(int client_fd, const RespValue& request) {
    if (request.elements.empty()) return;

    std::string cmd_name = request.elements[0].bulkString;
    std::transform(cmd_name.begin(), cmd_name.end(), cmd_name.begin(), ::toupper);

    auto it = handlers.find(cmd_name);
    if (it != handlers.end()) {
        it->second(client_fd, request);
    } else {
        std::string err = "-ERR unknown command '" + cmd_name + "'\r\n";
        send(client_fd, err.c_str(), err.length(), 0);

    }
}

