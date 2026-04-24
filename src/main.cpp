#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <thread>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <queue>
#include <chrono>
#include <algorithm>
#include <sys/socket.h>
#include <arpa/inet.h>
#include "respparser.h"
#include "dataStructures.h"
#include "commands.h"

#include <condition_variable>


// Map: Key Name -> Vector of Nodes
std::unordered_map<std::string, Node> key_value_store;
std::mutex store_mutex;
std::condition_variable expiry_cv;
std::priority_queue<ExpiryEntry, std::vector<ExpiryEntry>, std::greater<ExpiryEntry>> expiry_heap;

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

void execute_command(int client_fd, const RespValue& request);

// ==========================================
// 4. Client Handler
// ==========================================
void handle_client(int client_fd) {
    char buffer[1024];
    bool in_transaction = false;
    std::vector<RespValue> command_queue;
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

            if (command == "PING") {
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

          else if (command == "XADD" && request.elements.size() >= 3) {
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

          else if (command == "MULTI") {
                std::cout << "[DEBUG] Received MULTI command" << std::endl;
                if (in_transaction) {
                    send(client_fd, "-ERR MULTI calls can not be nested\r\n", 37, 0);
                } else {
                    in_transaction = true;
                    send(client_fd, "+OK\r\n", 5, 0);
                    std::cout << "[DEBUG] Client entered transaction mode." << std::endl;
                }
                continue; 
            }

            // --- 2. DISCARD Command Logic ---
          else if (command == "DISCARD") {
                if (!in_transaction) {
                    send(client_fd, "-ERR DISCARD without MULTI\r\n", 28, 0);
                } else {
                    in_transaction = false;
                    command_queue.clear();
                    send(client_fd, "+OK\r\n", 5, 0);
                }
                continue;
            }

          else if (command == "EXEC") {
            if (!in_transaction) {
                send(client_fd, "-ERR EXEC without MULTI\r\n", 25, 0);
            } else {
                in_transaction = false; // Reset state immediately
                
                if (command_queue.empty()) {
                    send(client_fd, "*0\r\n", 4, 0);
                } else {
                    // Start the response array
                    std::string multi_resp = "*" + std::to_string(command_queue.size()) + "\r\n";
                    send(client_fd, multi_resp.c_str(), multi_resp.length(), 0);

                    // Here is where the refactor pays off!
                    // You can call your execute_command helper for each one.
                    for (const auto& cmd : command_queue) {
                        // Pass the client_fd to your command dispatcher
                        execute_command(client_fd, cmd); 
                    }
                    command_queue.clear();
                }
            }
        }

      }
    }
    close(client_fd);
}




// ==========================================
// 5. Main Entry Point
// ==========================================
int main() {
    std::cout << std::unitbuf;
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(6379);

    bind(server_fd, (struct sockaddr*)&addr, sizeof(addr));
    listen(server_fd, 5);

    std::thread(background_cleanup).detach();

    std::cout << "Server listening on port 6379...\n";

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