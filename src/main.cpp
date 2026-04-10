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
#include "threadManagement.cpp"

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

// This map is your "Directory"
std::unordered_map<std::string, std::function<void(int, const RespValue&)>> handlers = {
    {"SET", handle_set},   // "SET" maps to the handle_set function
    {"GET", handle_get},   // "GET" maps to the handle_get function
    {"PING", handle_ping}  // "PING" maps to the handle_ping function
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
                send(client_fd, "+PONG\r\n", 7, 0);
            } 
            else if (command == "ECHO" && request.elements.size() > 1) {
                std::string msg = request.elements[1].bulkString;
                std::string resp = "$" + std::to_string(msg.length()) + "\r\n" + msg + "\r\n";
                send(client_fd, resp.c_str(), resp.length(), 0);
            } 

            else if (command == "FLUSHALL") {

                std::lock_guard<std::mutex> lock(store_mutex);
                
                // 1. Clear the main data store
                key_value_store.clear();
                
                // 2. Clear the expiry heap (priority_queue doesn't have .clear())
                while (!expiry_heap.empty()) {
                    expiry_heap.pop();
                }
                
                send(client_fd, "+OK\r\n", 5, 0);
            }
            else if (command == "SET" && request.elements.size() >= 3) {
                std::string key = request.elements[1].bulkString;
                std::string val = request.elements[2].bulkString;

                Node n;
                n.value = val;           // Variant automatically becomes std::string
                n.type = KeyType::String;
                n.hasTTL = false;

                // Handle EX (seconds) and PX (milliseconds)
                if (request.elements.size() >= 5) {
                    std::string flag = request.elements[3].bulkString;
                    for (auto &c : flag) c = toupper(c);

                    try {
                        long long ms = std::stoll(request.elements[4].bulkString);
                        if (flag == "EX") ms *= 1000;

                        n.hasTTL = true;
                        n.expires_at = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
                    } catch (...) {
                        send(client_fd, "-ERR value is not an integer or out of range\r\n", 46, 0);
                        continue;
                    }
                }

                {
                    std::lock_guard<std::mutex> lock(store_mutex);
                    // Overwrite whatever was there (String, List, or Stream)
                    key_value_store[key] = n; 
                    
                    if (n.hasTTL) {
                        expiry_heap.push({key, n.expires_at});
                    }
                }
                send(client_fd, "+OK\r\n", 5, 0);
            }
            else if (command == "GET" && request.elements.size() >= 2) {
                std::string key = request.elements[1].bulkString;
                std::lock_guard<std::mutex> lock(store_mutex);

                // 1. Check if key exists
                if (key_value_store.find(key) == key_value_store.end()) {
                    send(client_fd, "$-1\r\n", 5, 0);
                } 
                else {
                    Node &node = key_value_store[key];

                    // 2. Lazy Expiration Check
                    if (node.hasTTL && std::chrono::steady_clock::now() >= node.expires_at) {
                        key_value_store.erase(key);
                        send(client_fd, "$-1\r\n", 5, 0);
                    } 
                    // 3. Type Safety Check (SWE 3 Requirement)
                    else if (node.type != KeyType::String) {
                        const char* err = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                        send(client_fd, err, strlen(err), 0);
                    } 
                    // 4. Extract and Send
                    else {
                        // Use std::get to pull the string out of the variant
                        std::string& result = std::get<std::string>(node.value);
                        std::string resp = "$" + std::to_string(result.length()) + "\r\n" + result + "\r\n";
                        send(client_fd, resp.c_str(), resp.length(), 0);
                    }
                }
            }
            else if (command == "RPUSH" && request.elements.size() >= 3) {
                std::string key = request.elements[1].bulkString;
                std::lock_guard<std::mutex> lock(store_mutex);
                Node &node = key_value_store[key];

                // 1. Initialize if new key
                if (node.type == KeyType::None) {
                    node.type = KeyType::List;
                    node.value = std::vector<std::string>{};
                }

                // 2. Type Check
                if (node.type != KeyType::List) {
                    send(client_fd, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", 68, 0);
                    return;
                }

                // 3. Extract the vector from the variant and push
                auto& list = std::get<std::vector<std::string>>(node.value);
                for (size_t i = 2; i < request.elements.size(); ++i) {
                    list.push_back(request.elements[i].bulkString);
                }

                // 4. Return size
                std::string resp = ":" + std::to_string(list.size()) + "\r\n";
                send(client_fd, resp.c_str(), resp.length(), 0);

                // 5. Signal blocked BLPOP threads
                expiry_cv.notify_all();
            }

            else if (command == "LPUSH" && request.elements.size() >= 3) {
              std::string key = request.elements[1].bulkString;
              std::lock_guard<std::mutex> lock(store_mutex);
              
              Node &node = key_value_store[key];

              if (node.type == KeyType::None) {
                  node.type = KeyType::List;
                  node.value = std::vector<std::string>{};
              }

              if (node.type != KeyType::List) {
                  send(client_fd, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", 68, 0);
                  return;
              }

              auto& list = std::get<std::vector<std::string>>(node.value);
              for (size_t i = 2; i < request.elements.size(); ++i) {
                  // Standard Redis LPUSH behavior: 
                  // LPUSH key a b c -> List becomes [c, b, a]
                  list.insert(list.begin(), request.elements[i].bulkString);
              }

              std::string resp = ":" + std::to_string(list.size()) + "\r\n";
              send(client_fd, resp.c_str(), resp.length(), 0);

              expiry_cv.notify_all();
          }

          else if (command == "LRANGE" && request.elements.size() >= 4) {
            std::string key = request.elements[1].bulkString;
            long long start = std::stoll(request.elements[2].bulkString);
            long long stop = std::stoll(request.elements[3].bulkString);

            std::lock_guard<std::mutex> lock(store_mutex);

            auto it = key_value_store.find(key);
            if (it == key_value_store.end() || it->second.type != KeyType::List) {
                // Redis returns an empty array if key doesn't exist or is the wrong type
                send(client_fd, "*0\r\n", 4, 0);
            } else {
                auto& node = it->second;
                auto& list = std::get<std::vector<std::string>>(node.value);
                long long size = static_cast<long long>(list.size());

                if (start < 0) start = size + start;
                if (stop < 0) stop = size + stop;
                if (start < 0) start = 0;
                if (stop >= size) stop = size - 1;

                if (start >= size || start > stop) {
                    send(client_fd, "*0\r\n", 4, 0);
                } else {
                    long long count = stop - start + 1;
                    std::string header = "*" + std::to_string(count) + "\r\n";
                    send(client_fd, header.c_str(), header.length(), 0);

                    for (long long i = start; i <= stop; ++i) {
                        std::string& val = list[i];
                        std::string resp = "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
                        send(client_fd, resp.c_str(), resp.length(), 0);
                    }
                }
            }
        }

          else if (command == "LLEN" && request.elements.size() >= 2) {
              std::string key = request.elements[1].bulkString;
              std::lock_guard<std::mutex> lock(store_mutex);
              
              long long len = 0;
              auto it = key_value_store.find(key);
              if (it != key_value_store.end() && it->second.type == KeyType::List) {
                  len = std::get<std::vector<std::string>>(it->second.value).size();
              }
              
              std::string resp = ":" + std::to_string(len) + "\r\n";
              send(client_fd, resp.c_str(), resp.length(), 0);
          }

          else if (command == "LPOP" && request.elements.size() >= 2) {
              std::string key = request.elements[1].bulkString;
              bool has_count = (request.elements.size() >= 3);
              int count = has_count ? std::stoi(request.elements[2].bulkString) : 1;

              std::lock_guard<std::mutex> lock(store_mutex);

              auto it = key_value_store.find(key);
              if (it == key_value_store.end()) {
                  send(client_fd, "$-1\r\n", 5, 0);
              } else if (it->second.type != KeyType::List) {
                  send(client_fd, "-WRONGTYPE ...\r\n", 16, 0); // Simplified for brevity
              } else {
                  auto& list = std::get<std::vector<std::string>>(it->second.value);
                  
                  if (has_count && count <= 0) {
                      send(client_fd, "*0\r\n", 4, 0);
                  } else if (!has_count) {
                      // Single Pop
                      std::string val = list.front();
                      list.erase(list.begin());
                      if (list.empty()) key_value_store.erase(it);

                      std::string resp = "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
                      send(client_fd, resp.c_str(), resp.length(), 0);
                  } else {
                      // Multi Pop
                      int actual_to_pop = std::min((int)list.size(), count);
                      std::string header = "*" + std::to_string(actual_to_pop) + "\r\n";
                      send(client_fd, header.c_str(), header.length(), 0);

                      for (int i = 0; i < actual_to_pop; ++i) {
                          std::string val = list.front();
                          list.erase(list.begin());
                          std::string resp = "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
                          send(client_fd, resp.c_str(), resp.length(), 0);
                      }
                      if (list.empty()) key_value_store.erase(it);
                  }
              }
          }

          else if (command == "BLPOP" && request.elements.size() >= 3) {
              std::string key = request.elements[1].bulkString;
              double timeout_sec = std::stod(request.elements.back().bulkString);

              std::unique_lock<std::mutex> lock(store_mutex);

              // Helper to check if key exists AND is a non-empty List
              auto check_list = [&]() {
                  if (key_value_store.find(key) == key_value_store.end()) return false;
                  Node &n = key_value_store[key];
                  if (n.type != KeyType::List) return false;
                  return !std::get<std::vector<std::string>>(n.value).empty();
              };

              // Type Check early if key exists
              if (key_value_store.count(key) && key_value_store[key].type != KeyType::List) {
                  send(client_fd, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", 68, 0);
              } else {
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
                      if (list.empty()) key_value_store.erase(key);

                      std::string resp = "*2\r\n";
                      resp += "$" + std::to_string(key.length()) + "\r\n" + key + "\r\n";
                      resp += "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
                      send(client_fd, resp.c_str(), resp.length(), 0);
                  } else {
                      send(client_fd, "*-1\r\n", 5, 0);
                  }
              }
          }

      else if (command == "TYPE" && request.elements.size() >= 2) {
          std::string key = request.elements[1].bulkString;
          std::string result = "none";

          std::lock_guard<std::mutex> lock(store_mutex);
          auto it = key_value_store.find(key);
          if (it != key_value_store.end()) {
              Node &node = it->second;
              // Lazy Expiration check
              if (node.hasTTL && std::chrono::steady_clock::now() >= node.expires_at) {
                  key_value_store.erase(it);
              } else {
                  result = typeToString(node.type);
              }
          }

          std::string resp = "+" + result + "\r\n";
          send(client_fd, resp.c_str(), resp.length(), 0);
      }

      else if (command == "XADD" && request.elements.size() >= 3) {
        std::string key = request.elements[1].bulkString;
        std::string id_req = request.elements[2].bulkString;
        std::cout << "[DEBUG] Key: " << key << " ID_Req: " << id_req << std::endl;

        std::lock_guard<std::mutex> lock(store_mutex);
        Node &node = key_value_store[key];

        if (node.type == KeyType::None) {
            std::cout << "[DEBUG] New stream detected" << std::endl;
            node.type = KeyType::Stream;
            node.value = std::vector<StreamEntry>{};
        } else if (node.type != KeyType::Stream) {
            std::cout << "[DEBUG] WRONGTYPE error" << std::endl;
            std::string err = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
            send(client_fd, err.c_str(), err.length(), 0);
            return;
        }

        auto& stream = std::get<std::vector<StreamEntry>>(node.value);
        StreamID last_id = stream.empty() ? StreamID{0, 0} : stream.back().id;
        StreamID final_id;
        std::cout << "[DEBUG] Last ID in stream: " << last_id.toString() << std::endl;

        try {
            if (id_req == "*") {
                std::cout << "[DEBUG] Scenario: Full Auto (*)" << std::endl;
                long long now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                final_id.ms = std::max(now_ms, last_id.ms);
                final_id.seq = (final_id.ms == last_id.ms) ? last_id.seq + 1 : 0;
                if (final_id.ms == 0 && final_id.seq == 0) final_id.seq = 1;
            } 
            else if (id_req.find("-*") != std::string::npos) {
                std::cout << "[DEBUG] Scenario: Partial Auto (ms-*)" << std::endl;
                long long req_ms = std::stoll(id_req.substr(0, id_req.find("-*")));
                if (req_ms < last_id.ms) {
                    std::cout << "[DEBUG] Error: ms too small" << std::endl;
                    std::string err = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
                    send(client_fd, err.c_str(), err.length(), 0);
                    return;
                }
                final_id.ms = req_ms;
                final_id.seq = (req_ms == last_id.ms) ? last_id.seq + 1 : 0;
                if (final_id.ms == 0 && final_id.seq == 0) final_id.seq = 1;
            } 
            else {
                std::cout << "[DEBUG] Scenario: Explicit ID" << std::endl;
                final_id = StreamID::parse(id_req);
                std::cout << "[DEBUG] Parsed final_id: " << final_id.toString() << std::endl;

                if (final_id.ms == 0 && final_id.seq == 0) {
                    std::cout << "[DEBUG] Error: ID is 0-0" << std::endl;
                    std::string err = "-ERR The ID specified in XADD must be greater than 0-0\r\n";
                    send(client_fd, err.c_str(), err.length(), 0);
                    continue;
                }
                
                // PAY ATTENTION HERE: This is where 0-3 vs 1-2 should fail
                if (!stream.empty() && !(final_id > last_id)) {
                    std::cout << "[DEBUG] Error: ID not monotonic. final_id <= last_id" << std::endl;
                    std::string err = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
                    send(client_fd, err.c_str(), err.length(), 0);
                    continue;
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "[CRASH] Exception in ID logic: " << e.what() << std::endl;
            return; // This would cause "no content received"
        }

        std::cout << "[DEBUG] Validation passed. Saving entry." << std::endl;
        StreamEntry entry;
        entry.id = final_id;
        for (size_t i = 3; i + 1 < request.elements.size(); i += 2) {
            entry.fields.push_back({request.elements[i].bulkString, request.elements[i+1].bulkString});
        }
        stream.push_back(entry);

        std::string id_str = final_id.toString();
        std::string resp = "$" + std::to_string(id_str.length()) + "\r\n" + id_str + "\r\n";
        std::cout << "[DEBUG] Sending response: " << id_str << std::endl;
        send(client_fd, resp.c_str(), resp.length(), 0);
        
        expiry_cv.notify_all();
    }

    else if (command == "XRANGE" && request.elements.size() >= 4) {
    std::string key = request.elements[1].bulkString;
    StreamID start_id = StreamID::parseRange(request.elements[2].bulkString, true);
    StreamID end_id = StreamID::parseRange(request.elements[3].bulkString, false);

    std::lock_guard<std::mutex> lock(store_mutex);
    
    auto it = key_value_store.find(key);
    if (it == key_value_store.end() || it->second.type != KeyType::Stream) {
        send(client_fd, "*0\r\n", 4, 0);
    } else {
        auto& stream = std::get<std::vector<StreamEntry>>(it->second.value);

        // 1. Binary Search for the start iterator (First element >= start_id)
        auto start_it = std::lower_bound(stream.begin(), stream.end(), start_id, 
            [](const StreamEntry& e, const StreamID& id) { return e.id < id; });

        // 2. Binary Search for the end iterator (First element > end_id)
        auto end_it = std::upper_bound(stream.begin(), stream.end(), end_id, 
            [](const StreamID& id, const StreamEntry& e) { return id < e.id; });

        // 3. Calculate count and send RESP Array Header
        long long count = std::distance(start_it, end_it);
        if (count < 0) count = 0;
        
        std::string header = "*" + std::to_string(count) + "\r\n";
        send(client_fd, header.c_str(), header.length(), 0);

        // 4. Loop through the range
        for (auto entry_it = start_it; entry_it != end_it; ++entry_it) {
            const auto& entry = *entry_it;
            
            // Each entry is an array: [ID, [field1, value1, field2, value2...]]
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
            
            send(client_fd, entry_resp.c_str(), entry_resp.length(), 0);
        }
    }
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
    std::string key = request.elements[1].bulkString;

    std::lock_guard<std::mutex> lock(store_mutex);
    
    // 1. Check if key exists
    if (key_value_store.find(key) == key_value_store.end()) {
        // Case: Key doesn't exist. Create it with "1"
        Node n;
        n.type = KeyType::String;
        n.value = "1"; 
        n.hasTTL = false;
        key_value_store[key] = n;

        send(client_fd, ":1\r\n", 4, 0);
    } 
    else {
        Node &node = key_value_store[key];

        // 2. Type Check: Must be a String
        if (node.type != KeyType::String) {
            std::string err = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
            send(client_fd, err.c_str(), err.length(), 0);
        } 
        else {
            // 3. Extract string and attempt to parse as integer
            std::string& val_str = std::get<std::string>(node.value);
            
            try {
                size_t processed_char_count = 0;
                long long current_val = std::stoll(val_str, &processed_char_count);

                // Strict Check: stoll can parse "10hello" as 10. 
                // Redis requires the WHOLE string to be the number.
                if (processed_char_count != val_str.length()) {
                    throw std::invalid_argument("trailing characters");
                }

                // 4. Increment and update
                current_val++;
                node.value = std::to_string(current_val);

                // 5. Respond with Integer RESP (starts with ':')
                std::string resp = ":" + std::to_string(current_val) + "\r\n";
                send(client_fd, resp.c_str(), resp.length(), 0);

            } catch (...) {
                // Catches stoll failures (non-numeric strings) and overflows
                std::string err = "-ERR value is not an integer or out of range\r\n";
                send(client_fd, err.c_str(), err.length(), 0);
            }
        }
    }
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