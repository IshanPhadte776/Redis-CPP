#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <thread>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <cstdint>
#include <queue>
#include <chrono>
#include <algorithm>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <atomic>
#include "respparser.h"
#include "dataStructures.h"
#include "commands.h"

#include <condition_variable>


// Map: Key Name -> Vector of Nodes
std::unordered_map<std::string, Node> key_value_store;
std::unordered_map<std::string, std::uint64_t> key_versions;
std::uint64_t global_flush_epoch = 0;
std::mutex store_mutex;
std::condition_variable expiry_cv;
std::priority_queue<ExpiryEntry, std::vector<ExpiryEntry>, std::greater<ExpiryEntry>> expiry_heap;

// Set by --replicaof; INFO replication reports role:slave when true.
bool server_is_replica = false;

// Upstream TCP connection to master (replica mode); kept open after PING for later REPLCONF/PSYNC.
std::atomic<int> g_replica_master_sock{-1};

// Replica: total bytes of RESP commands consumed from the master replication stream (post-handshake).
std::atomic<std::uint64_t> g_replica_repl_offset{0};

// Replica: responses for commands applied from replication stream are discarded here.
static int g_resp_sink_fd = -1;

namespace {

// Accumulates TCP bytes and returns true when the next complete RESP simple string (+...\r\n)
// matches the expected payload after '+' (e.g. "PONG" for +PONG\r\n, "OK" for +OK\r\n).
bool replica_read_simple_string(int fd, std::string& pending, const char* expect_after_plus) {
    while (true) {
        const std::size_t crlf = pending.find("\r\n");
        if (crlf != std::string::npos) {
            if (crlf < 1 || pending[0] != '+') {
                return false;
            }
            const std::string payload = pending.substr(1, crlf - 1);
            pending.erase(0, crlf + 2);
            return payload == expect_after_plus;
        }
        char chunk[256];
        const ssize_t n = recv(fd, chunk, sizeof(chunk), 0);
        if (n <= 0) {
            return false;
        }
        pending.append(chunk, static_cast<std::size_t>(n));
        if (pending.size() > 4096) {
            return false;
        }
    }
}

// Consumes the next RESP simple string (+...\r\n) without interpreting the payload.
bool replica_discard_simple_string_line(int fd, std::string& pending) {
    while (true) {
        const std::size_t crlf = pending.find("\r\n");
        if (crlf != std::string::npos) {
            if (crlf < 1 || pending[0] != '+') {
                return false;
            }
            pending.erase(0, crlf + 2);
            return true;
        }
        char chunk[256];
        const ssize_t n = recv(fd, chunk, sizeof(chunk), 0);
        if (n <= 0) {
            return false;
        }
        pending.append(chunk, static_cast<std::size_t>(n));
        if (pending.size() > 4096) {
            return false;
        }
    }
}

// After PSYNC, master sends $<len>\r\n then exactly len bytes (RDB; no trailing \r\n).
bool replica_discard_bulk_payload(int fd, std::string& pending) {
    while (true) {
        const std::size_t crlf = pending.find("\r\n");
        if (crlf == std::string::npos) {
            char chunk[512];
            const ssize_t n = recv(fd, chunk, sizeof(chunk), 0);
            if (n <= 0) {
                return false;
            }
            pending.append(chunk, static_cast<std::size_t>(n));
            if (pending.size() > 65536) {
                return false;
            }
            continue;
        }
        if (crlf < 2 || pending[0] != '$') {
            return false;
        }
        const std::string len_str = pending.substr(1, crlf - 1);
        char* parse_end = nullptr;
        const unsigned long long bulk_len = std::strtoull(len_str.c_str(), &parse_end, 10);
        if (parse_end != len_str.c_str() + len_str.size() || bulk_len > 64 * 1024 * 1024) {
            return false;
        }
        const std::size_t body_start = crlf + 2;
        const std::size_t total = body_start + static_cast<std::size_t>(bulk_len);
        while (pending.size() < total) {
            char chunk[1024];
            const ssize_t n = recv(fd, chunk, sizeof(chunk), 0);
            if (n <= 0) {
                return false;
            }
            pending.append(chunk, static_cast<std::size_t>(n));
            if (pending.size() > total + 65536) {
                return false;
            }
        }
        pending.erase(0, total);
        return true;
    }
}

bool replica_is_replconf_getack(const RespValue& cmd) {
    if (cmd.type != RespType::Array || cmd.elements.size() != 3) {
        return false;
    }
    std::string a = cmd.elements[0].bulkString;
    std::string b = cmd.elements[1].bulkString;
    const std::string& star = cmd.elements[2].bulkString;
    for (char& ch : a) {
        ch = static_cast<char>(::toupper(static_cast<unsigned char>(ch)));
    }
    for (char& ch : b) {
        ch = static_cast<char>(::toupper(static_cast<unsigned char>(ch)));
    }
    return a == "REPLCONF" && b == "GETACK" && star == "*";
}

bool replica_send_all(int fd, const void* data, std::size_t len) {
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

std::string replica_format_replconf_ack(std::uint64_t offset) {
    const std::string num = std::to_string(offset);
    return std::string("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$") + std::to_string(num.size()) + "\r\n" + num + "\r\n";
}

void replica_apply_master_stream(int master_fd, std::string initial_pending) {
    g_replica_repl_offset.store(0, std::memory_order_relaxed);

    std::string pending = std::move(initial_pending);
    char buf[4096];
    while (true) {
        const ssize_t n = recv(master_fd, buf, sizeof(buf), 0);
        if (n <= 0) {
            break;
        }
        pending.append(buf, static_cast<std::size_t>(n));
        if (pending.size() > static_cast<std::size_t>(1) << 20) {
            break;
        }
        std::size_t consumed = 0;
        RespValue cmd;
        while (RespParser::try_parse_complete_array(pending, cmd, consumed)) {
            const std::size_t wire_bytes = consumed;
            pending.erase(0, consumed);
            if (replica_is_replconf_getack(cmd)) {
                const std::uint64_t ack_val = g_replica_repl_offset.load(std::memory_order_relaxed);
                const std::string ack_frame = replica_format_replconf_ack(ack_val);
                if (!replica_send_all(master_fd, ack_frame.data(), ack_frame.size())) {
                    close(master_fd);
                    g_replica_master_sock.store(-1);
                    return;
                }
                g_replica_repl_offset.fetch_add(wire_bytes, std::memory_order_relaxed);
                continue;
            }
            execute_command(g_resp_sink_fd, cmd);
            g_replica_repl_offset.fetch_add(wire_bytes, std::memory_order_relaxed);
        }
    }
    close(master_fd);
    g_replica_master_sock.store(-1);
}

std::string replconf_listening_port_payload(int listen_port) {
    const std::string port_str = std::to_string(listen_port);
    std::string s = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$";
    s += std::to_string(port_str.size());
    s += "\r\n";
    s += port_str;
    s += "\r\n";
    return s;
}

void replica_connect_and_send_ping(std::string master_host, int master_port, int replica_listen_port) {
    struct addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo* res = nullptr;
    const std::string port_str = std::to_string(master_port);
    const int gai = getaddrinfo(master_host.c_str(), port_str.c_str(), &hints, &res);
    if (gai != 0 || res == nullptr) {
        std::cerr << "[replica] getaddrinfo(" << master_host << "): " << gai_strerror(gai) << "\n";
        return;
    }

    int fd = -1;
    for (struct addrinfo* p = res; p != nullptr; p = p->ai_next) {
        fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (fd < 0) {
            continue;
        }
        if (connect(fd, p->ai_addr, p->ai_addrlen) == 0) {
            break;
        }
        close(fd);
        fd = -1;
    }
    freeaddrinfo(res);

    if (fd < 0) {
        std::cerr << "[replica] connect to master " << master_host << ":" << master_port << " failed\n";
        return;
    }

    static constexpr char kPing[] = "*1\r\n$4\r\nPING\r\n";
    const ssize_t ping_len = static_cast<ssize_t>(sizeof(kPing) - 1);
    if (send(fd, kPing, static_cast<size_t>(ping_len), 0) != ping_len) {
        std::cerr << "[replica] send PING to master failed\n";
        close(fd);
        return;
    }

    std::string pending;
    if (!replica_read_simple_string(fd, pending, "PONG")) {
        std::cerr << "[replica] expected +PONG after PING\n";
        close(fd);
        return;
    }

    const std::string replconf_port = replconf_listening_port_payload(replica_listen_port);
    if (send(fd, replconf_port.data(), replconf_port.size(), 0) != static_cast<ssize_t>(replconf_port.size())) {
        std::cerr << "[replica] send REPLCONF listening-port failed\n";
        close(fd);
        return;
    }
    if (!replica_read_simple_string(fd, pending, "OK")) {
        std::cerr << "[replica] expected +OK after REPLCONF listening-port\n";
        close(fd);
        return;
    }

    static constexpr char kReplconfCapa[] =
        "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
    const ssize_t capa_len = static_cast<ssize_t>(sizeof(kReplconfCapa) - 1);
    if (send(fd, kReplconfCapa, static_cast<std::size_t>(capa_len), 0) != capa_len) {
        std::cerr << "[replica] send REPLCONF capa psync2 failed\n";
        close(fd);
        return;
    }
    if (!replica_read_simple_string(fd, pending, "OK")) {
        std::cerr << "[replica] expected +OK after REPLCONF capa\n";
        close(fd);
        return;
    }

    static constexpr char kPsync[] = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
    const ssize_t psync_len = static_cast<ssize_t>(sizeof(kPsync) - 1);
    if (send(fd, kPsync, static_cast<std::size_t>(psync_len), 0) != psync_len) {
        std::cerr << "[replica] send PSYNC failed\n";
        close(fd);
        return;
    }
    if (!replica_discard_simple_string_line(fd, pending)) {
        std::cerr << "[replica] expected simple-string reply after PSYNC\n";
        close(fd);
        return;
    }
    if (!replica_discard_bulk_payload(fd, pending)) {
        std::cerr << "[replica] expected RDB bulk payload after FULLRESYNC\n";
        close(fd);
        return;
    }

    g_replica_master_sock.store(fd);
    std::thread(replica_apply_master_stream, fd, std::move(pending)).detach();
}

} // namespace

// ==========================================
// 3. Background Cleanup (Active Expiry)
// ==========================================
void background_cleanup() {
    while (true) {
        // Sleep for a short interval to avoid hammering the CPU
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        
        auto now = std::chrono::steady_clock::now();
        std::lock_guard<std::mutex> lock(store_mutex);

        while (!expiry_heap.empty() && expiry_heap.top().expires_at <= now) {
            // Get the "next to die" entry
            ExpiryEntry entry = expiry_heap.top();
            expiry_heap.pop();

            // 1. Check if the key still exists in the store
            if (key_value_store.count(entry.key)) {
                Node& node = key_value_store[entry.key];

                // 2. The "Double Check" Logic:
                // If a user ran 'SET key val EX 10' then immediately 'SET key val EX 100',
                // the heap now has TWO entries for the same key. We only delete if 
                // the node's current expires_at matches the one we just popped.
                if (node.hasTTL && node.expires_at == entry.expires_at) {
                    store_bump_key_revision(entry.key);
                    key_value_store.erase(entry.key);
                    std::cout << "[Cleanup] Evicted expired key: " << entry.key << "\n";
                    
                    // Optional: If this was a List/Stream, we might need to notify 
                    // blocked clients that the data is gone (though usually not required for TTL)
                    expiry_cv.notify_all(); 
                }
            }
        }
    }
}

// ==========================================
// 4. Client Handler
// ==========================================
void handle_client(int client_fd) {
    char buffer[1024];
    bool in_transaction = false;
    std::vector<RespValue> command_queue;
    std::unordered_map<std::string, std::uint64_t> watch_versions;
    std::uint64_t watch_flush_epoch = 0;
    while (true) {
        ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer), 0);
        if (bytes_received <= 0) break;


        std::string raw_data(buffer, bytes_received);
        RespValue request = RespParser::parse(raw_data);


        std::cout << "[DEBUG] Received raw data: " << raw_data << std::endl;
        std::cout << "[DEBUG] Parsed command: " << (request.elements.empty() ? "None" : request.elements[0].bulkString) << std::endl;

        if (request.type == RespType::Array && !request.elements.empty()) {
            std::string command = request.elements[0].bulkString;
            for (auto &c : command) c = toupper(c);

            if (in_transaction) {
                if (command == "MULTI") {
                    std::cout << "[DEBUG] Received MULTI command (nested)" << std::endl;
                    send(client_fd, "-ERR MULTI calls can not be nested\r\n", 37, 0);
                    continue;
                }
                if (command == "WATCH") {
                    const char* err = "-ERR WATCH inside MULTI is not allowed\r\n";
                    send(client_fd, err, static_cast<int>(strlen(err)), 0);
                    continue;
                }
                if (command == "UNWATCH") {
                    command_queue.push_back(request);
                    send(client_fd, "+QUEUED\r\n", 9, 0);
                    continue;
                }
                if (command == "DISCARD") {
                    in_transaction = false;
                    command_queue.clear();
                    watch_versions.clear();
                    watch_flush_epoch = 0;
                    send(client_fd, "+OK\r\n", 5, 0);
                    continue;
                }
                if (command == "EXEC") {
                    in_transaction = false;
                    execute_transaction_exec(client_fd, command_queue, watch_versions, watch_flush_epoch);
                    continue;
                }
                command_queue.push_back(request);
                send(client_fd, "+QUEUED\r\n", 9, 0);
                continue;
            }

            if (command == "PING") {
                execute_command(client_fd, request);
            }
            else if (command == "REPLCONF") {
                execute_command(client_fd, request);
            }
            else if (command == "PSYNC") {
                execute_command(client_fd, request);
            }
            else if (command == "ECHO" && request.elements.size() > 1) {
                execute_command(client_fd, request);
            } 

            else if (command == "FLUSHALL") {
                execute_command(client_fd, request);
            }
            else if (command == "SET" && request.elements.size() >= 3) {
                execute_command(client_fd, request);
            }
            else if (command == "GET" && request.elements.size() >= 2) {
                execute_command(client_fd, request);
            }
            else if (command == "RPUSH" && request.elements.size() >= 3) {
                execute_command(client_fd, request);
            }

            else if (command == "LPUSH" && request.elements.size() >= 3) {
              execute_command(client_fd, request);
          }

          else if (command == "LRANGE" && request.elements.size() >= 4) {
                execute_command(client_fd, request);
        }

          else if (command == "LLEN" && request.elements.size() >= 2) {
              execute_command(client_fd, request);
          }

          else if (command == "LPOP" && request.elements.size() >= 2) {
                execute_command(client_fd, request);
          }

          else if (command == "BLPOP" && request.elements.size() >= 3) {
                execute_command(client_fd, request);
          }

          else if (command == "TYPE" && request.elements.size() >= 2) {
                execute_command(client_fd, request);
         }

          else if (command == "XADD") {
                execute_command(client_fd, request);
          }

          else if (command == "XRANGE" && request.elements.size() >= 4) {
                execute_command(client_fd, request);
          }
          
          else if (command == "XREAD") {
    std::cout << "[DEBUG] Entering XREAD" << std::endl;
    long long block_ms = -1; 
    int streams_keyword_pos = -1;

    // 1. Parse Arguments
    for (size_t i = 1; i < request.elements.size(); ++i) {
        std::string arg = request.elements[i].bulkString;
        std::transform(arg.begin(), arg.end(), arg.begin(), ::toupper);
        if (arg == "BLOCK" && i + 1 < request.elements.size()) {
            block_ms = std::stoll(request.elements[i + 1].bulkString);
        }
        if (arg == "STREAMS") streams_keyword_pos = i;
    }

    if (streams_keyword_pos == -1) return; // Should not happen with valid RESP

    int num_keys = (request.elements.size() - (streams_keyword_pos + 1)) / 2;
    std::vector<std::string> keys;
    std::vector<std::string> raw_ids;
    for (int i = 0; i < num_keys; ++i) {
        keys.push_back(request.elements[streams_keyword_pos + 1 + i].bulkString);
        raw_ids.push_back(request.elements[streams_keyword_pos + 1 + num_keys + i].bulkString);
    }

    std::unique_lock<std::mutex> lock(store_mutex);

    // 2. Resolve IDs (Handle "$")
    std::vector<StreamID> start_ids;
    for (int i = 0; i < num_keys; ++i) {
        if (raw_ids[i] == "$") {
            if (key_value_store.count(keys[i]) && key_value_store[keys[i]].type == KeyType::Stream) {
                auto& stream = std::get<std::vector<StreamEntry>>(key_value_store[keys[i]].value);
                start_ids.push_back(stream.empty() ? StreamID{0, 0} : stream.back().id);
            } else {
                start_ids.push_back(StreamID{0, 0});
            }
        } else {
            start_ids.push_back(StreamID::parseRange(raw_ids[i], false));
        }
        std::cout << "[DEBUG] XREAD Key: " << keys[i] << " Start ID: " << start_ids[i].toString() << std::endl;
    }

    // 3. The Predicate: Do any of our keys have data > start_id?
    auto has_new_data = [&]() {
        for (int i = 0; i < num_keys; ++i) {
            if (key_value_store.count(keys[i]) && key_value_store[keys[i]].type == KeyType::Stream) {
                auto& stream = std::get<std::vector<StreamEntry>>(key_value_store[keys[i]].value);
                if (!stream.empty() && stream.back().id > start_ids[i]) return true;
            }
        }
        return false;
    };

    // 4. Blocking logic
    if (block_ms >= 0 && !has_new_data()) {
        std::cout << "[DEBUG] No data yet, blocking for " << block_ms << "ms" << std::endl;
        if (block_ms == 0) {
            expiry_cv.wait(lock, has_new_data);
        } else {
            expiry_cv.wait_for(lock, std::chrono::milliseconds(block_ms), has_new_data);
        }
        std::cout << "[DEBUG] Woke up from block!" << std::endl;
    }

    // 5. Final check and Response Generation
    if (!has_new_data()) {
        std::cout << "[DEBUG] Timeout reached, sending NULL" << std::endl;
        send(client_fd, "*-1\r\n", 5, 0); // Null Array is the correct type for XREAD
    } else {
        std::cout << "[DEBUG] Data found! Building response array" << std::endl;
        // Count how many streams actually have new data
        std::vector<int> active_stream_indices;
        for (int i = 0; i < num_keys; ++i) {
            if (key_value_store.count(keys[i])) {
                auto& stream = std::get<std::vector<StreamEntry>>(key_value_store[keys[i]].value);
                if (!stream.empty() && stream.back().id > start_ids[i]) {
                    active_stream_indices.push_back(i);
                }
            }
        }

        std::string final_resp = "*" + std::to_string(active_stream_indices.size()) + "\r\n";
        for (int idx : active_stream_indices) {
            final_resp += "*2\r\n";
            final_resp += "$" + std::to_string(keys[idx].length()) + "\r\n" + keys[idx] + "\r\n";
            
            auto& stream = std::get<std::vector<StreamEntry>>(key_value_store[keys[idx]].value);
            auto start_it = std::upper_bound(stream.begin(), stream.end(), start_ids[idx], 
                [](const StreamID& id, const StreamEntry& e) { return id < e.id; });

            long long entry_count = std::distance(start_it, stream.end());
            final_resp += "*" + std::to_string(entry_count) + "\r\n";

            for (auto it = start_it; it != stream.end(); ++it) {
                final_resp += "*2\r\n";
                std::string id_str = it->id.toString();
                final_resp += "$" + std::to_string(id_str.length()) + "\r\n" + id_str + "\r\n";
                final_resp += "*" + std::to_string(it->fields.size() * 2) + "\r\n";
                for (auto& p : it->fields) {
                    final_resp += "$" + std::to_string(p.first.length()) + "\r\n" + p.first + "\r\n";
                    final_resp += "$" + std::to_string(p.second.length()) + "\r\n" + p.second + "\r\n";
                }
            }
        }
        send(client_fd, final_resp.c_str(), final_resp.length(), 0);
    }
}

          else if (command == "INCR" && request.elements.size() >= 2) {
                execute_command(client_fd, request);
          }

          else if (command == "INFO") {
                execute_command(client_fd, request);
          }

          else if (command == "WATCH") {
                handle_watch(client_fd, request, watch_versions, watch_flush_epoch);
            }

          else if (command == "UNWATCH") {
                handle_unwatch(client_fd, watch_versions, watch_flush_epoch);
            }

          else if (command == "MULTI") {
                std::cout << "[DEBUG] Received MULTI command" << std::endl;
                in_transaction = true;
                send(client_fd, "+OK\r\n", 5, 0);
                std::cout << "[DEBUG] Client entered transaction mode." << std::endl;
                continue;
            }

          else if (command == "DISCARD") {
                send(client_fd, "-ERR DISCARD without MULTI\r\n", 28, 0);
                continue;
            }

          else if (command == "EXEC") {
            send(client_fd, "-ERR EXEC without MULTI\r\n", 25, 0);
            continue;
        }

      }
    }
    replication_unregister_replica(client_fd);
    close(client_fd);
}




// ==========================================
// 5. Main Entry Point
// ==========================================
int main(int argc, char* argv[]) {
    int port = 6379;
    std::string replica_master_host;
    int replica_master_port = 0;

    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--port") == 0) {
            if (i + 1 >= argc) {
                std::cerr << "error: --port requires a value\n";
                return 1;
            }
            char* end = nullptr;
            long parsed = std::strtol(argv[i + 1], &end, 10);
            if (end == argv[i + 1] || *end != '\0' || parsed < 1 || parsed > 65535) {
                std::cerr << "error: invalid port: " << argv[i + 1] << "\n";
                return 1;
            }
            port = static_cast<int>(parsed);
            ++i;
        } else if (std::strcmp(argv[i], "--replicaof") == 0) {
            if (i + 1 >= argc) {
                std::cerr << "error: --replicaof requires host and port\n";
                return 1;
            }
            server_is_replica = true;
            std::string spec(argv[i + 1]);
            if (spec.find(' ') != std::string::npos) {
                const std::size_t sp = spec.find(' ');
                replica_master_host = spec.substr(0, sp);
                std::string port_part = spec.substr(sp + 1);
                const std::size_t not_space = port_part.find_first_not_of(' ');
                if (not_space != std::string::npos) {
                    port_part = port_part.substr(not_space);
                }
                const std::size_t non_digit = port_part.find_first_not_of("0123456789");
                if (non_digit != std::string::npos) {
                    port_part = port_part.substr(0, non_digit);
                }
                char* end = nullptr;
                const long mp = std::strtol(port_part.c_str(), &end, 10);
                if (port_part.empty() || end != port_part.c_str() + port_part.size()
                    || mp < 1 || mp > 65535) {
                    std::cerr << "error: invalid master port in --replicaof\n";
                    return 1;
                }
                replica_master_port = static_cast<int>(mp);
                ++i;
            } else {
                if (i + 2 >= argc) {
                    std::cerr << "error: --replicaof requires host and port\n";
                    return 1;
                }
                replica_master_host = argv[i + 1];
                char* end = nullptr;
                const long mp = std::strtol(argv[i + 2], &end, 10);
                if (end == argv[i + 2] || *end != '\0' || mp < 1 || mp > 65535) {
                    std::cerr << "error: invalid master port for --replicaof\n";
                    return 1;
                }
                replica_master_port = static_cast<int>(mp);
                i += 2;
            }
        }
    }

    if (server_is_replica && (replica_master_host.empty() || replica_master_port == 0)) {
        std::cerr << "error: --replicaof requires host and port\n";
        return 1;
    }

    std::cout << std::unitbuf;

    g_resp_sink_fd = open("/dev/null", O_WRONLY);
    if (g_resp_sink_fd < 0) {
        std::cerr << "error: could not open /dev/null\n";
        return 1;
    }

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(static_cast<std::uint16_t>(port));

    bind(server_fd, (struct sockaddr*)&addr, sizeof(addr));
    listen(server_fd, 5);

    std::thread(background_cleanup).detach();

    std::cout << "Server listening on port " << port << "...\n";

    if (server_is_replica) {
        std::thread(replica_connect_and_send_ping, replica_master_host, replica_master_port, port).detach();
    }

    while (true) {
        struct sockaddr_in client_addr;
        socklen_t len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &len);
        if (client_fd >= 0) {
            std::thread(handle_client, client_fd).detach();
        }
    }
    close(server_fd);
    return 0;
}