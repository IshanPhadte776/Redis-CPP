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


#include <condition_variable>

// ==========================================
// 1. RESP Protocol Parser
// ==========================================
enum class RespType { Array, BulkString, SimpleString, Unknown };

struct RespValue {
    RespType type = RespType::Unknown;
    std::vector<RespValue> elements; 
    std::string bulkString;
};

class RespParser {
public:
    static RespValue parse(const std::string& raw) {
        RespValue value;
        if (raw.empty()) return value;

        if (raw[0] == '*') {
            value.type = RespType::Array;
            size_t pos = raw.find("\r\n");
            if (pos == std::string::npos) return value;

            int num_elements = std::stoi(raw.substr(1, pos - 1));
            size_t current_pos = pos + 2;

            for (int i = 0; i < num_elements; ++i) {
                if (current_pos < raw.length() && raw[current_pos] == '$') {
                    size_t next_crlf = raw.find("\r\n", current_pos);
                    int str_len = std::stoi(raw.substr(current_pos + 1, next_crlf - current_pos - 1));
                    current_pos = next_crlf + 2;

                    RespValue element;
                    element.type = RespType::BulkString;
                    element.bulkString = raw.substr(current_pos, str_len);
                    value.elements.push_back(element);
                    current_pos += str_len + 2;
                }
            }
        }
        return value;
    }
};

// ==========================================
// 2. Data Structures & Global Store
// ==========================================

enum class KeyType {
  None, // Default for non-existent keys
  String, //Created via SET
  List,  // Created via RPUSH/LPUSH
  Set, // Created via SADD
  ZSet, // Created via ZADD
  Hash, // Created via HSET
  Stream, // Created via XADD
  VectorSet, // Created via VADD (Hypothetical Command for Vector Similarity Search)
};


struct StreamEntry {
  std::string id;
  std::unordered_map<std::string, std::string> fields;  
};


using RedisValue = std::variant<
    std::string,                    // For SET
    std::vector<std::string>,       // For LIST (LPUSH/RPUSH)
    std::vector<StreamEntry>        // For STREAM (XADD)
>;


struct Node {
    RedisValue value;    // We definitely still need this for the actual values!
    KeyType type = KeyType::None;
    std::chrono::steady_clock::time_point expires_at;
    bool hasTTL = false;
};


struct ExpiryEntry {
    std::string key;
    std::chrono::steady_clock::time_point expires_at;

    friend bool operator>(const ExpiryEntry& a, const ExpiryEntry& b) {
        return a.expires_at > b.expires_at;
    }
};

// Map: Key Name -> Vector of Nodes
std::unordered_map<std::string, Node> key_value_store;
std::mutex store_mutex;

std::condition_variable expiry_cv;


std::priority_queue<ExpiryEntry, std::vector<ExpiryEntry>, std::greater<ExpiryEntry>> expiry_heap;


// Map the enum to the string response required by Redis
std::string typeToString(KeyType t) {
    switch (t) {
        case KeyType::String:    return "string";
        case KeyType::List:      return "list";
        case KeyType::Set:       return "set";
        case KeyType::ZSet:      return "zset";
        case KeyType::Hash:      return "hash";
        case KeyType::Stream:    return "stream";
        case KeyType::VectorSet: return "vectorset";
        default:                 return "none";
    }
}


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

// ==========================================
// 4. Client Handler
// ==========================================
void handle_client(int client_fd) {
    char buffer[1024];
    while (true) {
        ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer), 0);
        if (bytes_received <= 0) break;

        std::string raw_data(buffer, bytes_received);
        RespValue request = RespParser::parse(raw_data);

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

      else if (command == "XADD" && request.elements.size() >= 4) {
          std::string key = request.elements[1].bulkString;
          std::string id_req = request.elements[2].bulkString;

          std::lock_guard<std::mutex> lock(store_mutex);
          Node &node = key_value_store[key];

          if (node.type == KeyType::None) {
              node.type = KeyType::Stream;
              node.value = std::vector<StreamEntry>{};
          } else if (node.type != KeyType::Stream) {
              send(client_fd, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", 68, 0);
              return;
          }

          auto& stream = std::get<std::vector<StreamEntry>>(node.value);
          
          // Generate ID if '*'
          std::string final_id = id_req;
          if (id_req == "*") {
              auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::system_clock::now().time_since_epoch()).count();
              final_id = std::to_string(now) + "-0";
          }

          // Monotonicity check
          if (!stream.empty() && final_id <= stream.back().id) {
              send(client_fd, "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n", 82, 0);
              return;
          }

          StreamEntry entry;
          entry.id = final_id;
          for (size_t i = 3; i + 1 < request.elements.size(); i += 2) {
              entry.fields[request.elements[i].bulkString] = request.elements[i+1].bulkString;
          }
          stream.push_back(entry);

          std::string resp = "$" + std::to_string(final_id.length()) + "\r\n" + final_id + "\r\n";
          send(client_fd, resp.c_str(), resp.length(), 0);
          expiry_cv.notify_all(); // Notify any XREAD waiters
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