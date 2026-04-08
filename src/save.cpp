// #include <iostream>
// #include <cstdlib>
// #include <string>
// #include <cstring>
// #include <unistd.h>
// #include <thread>
// #include <mutex>
// #include <unordered_map>
// #include <vector>
// #include <queue>
// #include <chrono>
// #include <algorithm>
// #include <sys/socket.h>
// #include <arpa/inet.h>


// #include <condition_variable>

// // ==========================================
// // 1. RESP Protocol Parser
// // ==========================================
// enum class RespType { Array, BulkString, SimpleString, Unknown };

// struct RespValue {
//     RespType type = RespType::Unknown;
//     std::vector<RespValue> elements; 
//     std::string bulkString;
// };

// class RespParser {
// public:
//     static RespValue parse(const std::string& raw) {
//         RespValue value;
//         if (raw.empty()) return value;

//         if (raw[0] == '*') {
//             value.type = RespType::Array;
//             size_t pos = raw.find("\r\n");
//             if (pos == std::string::npos) return value;

//             int num_elements = std::stoi(raw.substr(1, pos - 1));
//             size_t current_pos = pos + 2;

//             for (int i = 0; i < num_elements; ++i) {
//                 if (current_pos < raw.length() && raw[current_pos] == '$') {
//                     size_t next_crlf = raw.find("\r\n", current_pos);
//                     int str_len = std::stoi(raw.substr(current_pos + 1, next_crlf - current_pos - 1));
//                     current_pos = next_crlf + 2;

//                     RespValue element;
//                     element.type = RespType::BulkString;
//                     element.bulkString = raw.substr(current_pos, str_len);
//                     value.elements.push_back(element);
//                     current_pos += str_len + 2;
//                 }
//             }
//         }
//         return value;
//     }
// };

// // ==========================================
// // 2. Data Structures & Global Store
// // ==========================================
// struct Node {
//     std::string value;
//     std::chrono::steady_clock::time_point expires_at;
//     bool hasTTL = false;
// };


// struct ExpiryEntry {
//     std::string key;
//     std::chrono::steady_clock::time_point expires_at;

//     friend bool operator>(const ExpiryEntry& a, const ExpiryEntry& b) {
//         return a.expires_at > b.expires_at;
//     }
// };

// // Map: Key Name -> Vector of Nodes
// std::unordered_map<std::string, std::vector<Node>> key_value_store;
// std::mutex store_mutex;

// std::condition_variable expiry_cv;


// std::priority_queue<ExpiryEntry, std::vector<ExpiryEntry>, std::greater<ExpiryEntry>> expiry_heap;

// // ==========================================
// // 3. Background Cleanup (Active Expiry)
// // ==========================================
// void background_cleanup() {
//     while (true) {
//         std::this_thread::sleep_for(std::chrono::milliseconds(100));
//         auto now = std::chrono::steady_clock::now();
//         std::lock_guard<std::mutex> lock(store_mutex);

//         while (!expiry_heap.empty() && expiry_heap.top().expires_at <= now) {
//             std::string key = expiry_heap.top().key;
//             expiry_heap.pop();

//             if (key_value_store.count(key)) {
//                 auto &vec = key_value_store[key];
//                 // Remove individual expired nodes from the vector
//                 vec.erase(std::remove_if(vec.begin(), vec.end(), [&](const Node& n) {
//                     return n.hasTTL && n.expires_at <= now;
//                 }), vec.end());

//                 if (vec.empty()) key_value_store.erase(key);
//             }
//         }
//     }
// }

// // ==========================================
// // 4. Client Handler
// // ==========================================
// void handle_client(int client_fd) {
//     char buffer[1024];
//     while (true) {
//         ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer), 0);
//         if (bytes_received <= 0) break;

//         std::string raw_data(buffer, bytes_received);
//         RespValue request = RespParser::parse(raw_data);

//         if (request.type == RespType::Array && !request.elements.empty()) {
//             std::string command = request.elements[0].bulkString;
//             for (auto &c : command) c = toupper(c);

//             if (command == "PING") {
//                 send(client_fd, "+PONG\r\n", 7, 0);
//             } 
//             else if (command == "ECHO" && request.elements.size() > 1) {
//                 std::string msg = request.elements[1].bulkString;
//                 std::string resp = "$" + std::to_string(msg.length()) + "\r\n" + msg + "\r\n";
//                 send(client_fd, resp.c_str(), resp.length(), 0);
//             } 
//             else if (command == "SET" && request.elements.size() >= 3) {
//                 std::string key = request.elements[1].bulkString;
//                 std::string val = request.elements[2].bulkString;
//                 Node n; n.value = val; n.hasTTL = false;

//                 if (request.elements.size() >= 5) {
//                     std::string flag = request.elements[3].bulkString;
//                     for (auto &c : flag) c = toupper(c);
//                     long long ms = std::stoll(request.elements[4].bulkString);
//                     if (flag == "EX") ms *= 1000;

//                     n.hasTTL = true;
//                     n.expires_at = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
                    
//                     std::lock_guard<std::mutex> lock(store_mutex);
//                     expiry_heap.push({key, n.expires_at});
//                     key_value_store[key] = { n }; // SET overwrites key with 1-node vector
//                 } else {
//                     std::lock_guard<std::mutex> lock(store_mutex);
//                     key_value_store[key] = { n };
//                 }
//                 send(client_fd, "+OK\r\n", 5, 0);
//             } 
//             else if (command == "GET" && request.elements.size() >= 2) {
//                 std::string key = request.elements[1].bulkString;
//                 std::string result = "";
//                 bool found = false;

//                 std::lock_guard<std::mutex> lock(store_mutex);
//                 if (key_value_store.count(key) && !key_value_store[key].empty()) {
//                     Node &node = key_value_store[key][0];
//                     if (node.hasTTL && std::chrono::steady_clock::now() >= node.expires_at) {
//                         key_value_store.erase(key);
//                     } else {
//                         result = node.value;
//                         found = true;
//                     }
//                 }
//                 if (found) {
//                     std::string resp = "$" + std::to_string(result.length()) + "\r\n" + result + "\r\n";
//                     send(client_fd, resp.c_str(), resp.length(), 0);
//                 } else {
//                     send(client_fd, "$-1\r\n", 5, 0);
//                 }
//             } 
//             else if (command == "RPUSH" && request.elements.size() >= 3) {
//                 std::string key = request.elements[1].bulkString;
//                 std::lock_guard<std::mutex> lock(store_mutex);
//                 auto &vec = key_value_store[key];
//                 for (size_t i = 2; i < request.elements.size(); ++i) {
//                     vec.push_back({request.elements[i].bulkString, {}, false});
//                 }
//                 std::string resp = ":" + std::to_string(vec.size()) + "\r\n";
//                 send(client_fd, resp.c_str(), resp.length(), 0);

//                 expiry_cv.notify_all();
//             }

//             else if (command == "LPUSH" && request.elements.size() >= 3) {
//                 std::string key = request.elements[1].bulkString;
//                 std::lock_guard<std::mutex> lock(store_mutex);
//                 auto &vec = key_value_store[key];
//                 for (size_t i = 2; i < request.elements.size(); ++i) {
//                     vec.insert(vec.begin(), {request.elements[i].bulkString, {}, false});
//                 }
//                 std::string resp = ":" + std::to_string(vec.size()) + "\r\n";
//                 send(client_fd, resp.c_str(), resp.length(), 0);
//             }

//             else if (command == "LRANGE" && request.elements.size() >= 4) {
//               std::string key = request.elements[1].bulkString;
//               long long start = std::stoll(request.elements[2].bulkString);
//               long long stop = std::stoll(request.elements[3].bulkString);

//               std::lock_guard<std::mutex> lock(store_mutex);

//               if (key_value_store.find(key) == key_value_store.end()) {
//                   send(client_fd, "*0\r\n", 4, 0);
//               } else {
//                   auto& list = key_value_store[key];
//                   long long size = static_cast<long long>(list.size());

//                   // 1. Convert negative indices to positive
//                   if (start < 0) start = size + start;
//                   if (stop < 0) stop = size + stop;

//                   // 2. Clamp boundaries (Redis Behavior)
//                   if (start < 0) start = 0;
//                   if (stop >= size) stop = size - 1;

//                   // 3. Final sanity check for empty results
//                   if (start >= size || start > stop) {
//                       send(client_fd, "*0\r\n", 4, 0);
//                   } else {
//                       // 4. Calculate total count for the RESP Array Header
//                       long long count = stop - start + 1;
//                       std::string header = "*" + std::to_string(count) + "\r\n";
//                       send(client_fd, header.c_str(), header.length(), 0);

//                       // 5. Stream the elements
//                       for (long long i = start; i <= stop; ++i) {
//                           std::string& val = list[i].value;
//                           std::string element_resp = "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
//                           send(client_fd, element_resp.c_str(), element_resp.length(), 0);
//                       }
//                   }
//               }
//           }

//           else if (command == "LLEN" && request.elements.size() >= 2) {
//               std::string key = request.elements[1].bulkString;
//               std::lock_guard<std::mutex> lock(store_mutex);
//               long long len = key_value_store.count(key) ? key_value_store[key].size() : 0;
//               std::string resp = ":" + std::to_string(len) + "\r\n";
//               send(client_fd, resp.c_str(), resp.length(), 0);
//           }

//           else if (command == "LPOP" && request.elements.size() >= 2) {
//             std::string key = request.elements[1].bulkString;
            
//             // Determine if 'count' was provided
//             bool has_count = (request.elements.size() >= 3);
//             int count = 1; 
//             if (has_count) {
//                 count = std::stoi(request.elements[2].bulkString);
//             }

//             std::lock_guard<std::mutex> lock(store_mutex);

//             // 1. Check if key exists
//             if (key_value_store.find(key) == key_value_store.end()) {
//                 send(client_fd, "$-1\r\n", 5, 0); // Nil reply
//             } else {
//                 auto& list = key_value_store[key];

//                 // 2. Handle non-positive count (Edge Case)
//                 if (has_count && count <= 0) {
//                     send(client_fd, "*0\r\n", 4, 0);
//                 } 
//                 // 3. Single Pop (Standard LPOP)
//                 else if (!has_count) {
//                     std::string val = list[0].value;
//                     list.erase(list.begin());
//                     if (list.empty()) key_value_store.erase(key);

//                     std::string resp = "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
//                     send(client_fd, resp.c_str(), resp.length(), 0);
//                 }
//                 // 4. Multiple Pop (LPOP key count)
//                 else {
//                     int actual_to_pop = std::min((int)list.size(), count);
//                     std::string header = "*" + std::to_string(actual_to_pop) + "\r\n";
//                     send(client_fd, header.c_str(), header.length(), 0);

//                     for (int i = 0; i < actual_to_pop; ++i) {
//                         std::string val = list[0].value;
//                         list.erase(list.begin()); // Note: O(N) operation in vector
                        
//                         std::string element_resp = "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
//                         send(client_fd, element_resp.c_str(), element_resp.length(), 0);
//                     }
//                     if (list.empty()) key_value_store.erase(key);
//                 }
//             }
//         }

//         else if (command == "BLPOP" && request.elements.size() >= 3) {
//             std::string key = request.elements[1].bulkString;

//             double timeout_sec = std::stod(request.elements.back().bulkString);

//             std::unique_lock<std::mutex> lock(store_mutex);
//             // 1. Check if the list already has items
//             auto check_list = [&]() {
//                 return key_value_store.count(key) && !key_value_store[key].empty();
//             };

//             // 2. If empty, wait for notification or timeout
//             bool data_available = true;
//             if (!check_list()) {
//                 if (timeout_sec == 0) {
//                     expiry_cv.wait(lock, check_list); // Wait forever
//                 } else {
//                     auto timeout_duration = std::chrono::duration<double>(timeout_sec);
//                     data_available = expiry_cv.wait_for(lock, timeout_duration, check_list);
//                 }
//             }

//             if (data_available && check_list()) {
//                 auto& list = key_value_store[key];
//                 std::string val = list.front().value;
//                 list.erase(list.begin()); // Pop the left-most element

//                 // BLPOP returns an array: [key, value]
//                 std::string resp = "*2\r\n";
//                 resp += "$" + std::to_string(key.length()) + "\r\n" + key + "\r\n";
//                 resp += "$" + std::to_string(val.length()) + "\r\n" + val + "\r\n";
//                 send(client_fd, resp.c_str(), resp.length(), 0);
//             } else {
//                 // Timeout reached - return Null Array
//                 send(client_fd, "*-1\r\n", 5, 0);
//             }
//         }
//       }
//     }
//     close(client_fd);
// }


// // ==========================================
// // 5. Main Entry Point
// // ==========================================
// int main() {
//     std::cout << std::unitbuf;
//     int server_fd = socket(AF_INET, SOCK_STREAM, 0);
//     int reuse = 1;
//     setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

//     struct sockaddr_in addr;
//     addr.sin_family = AF_INET;
//     addr.sin_addr.s_addr = INADDR_ANY;
//     addr.sin_port = htons(6379);

//     bind(server_fd, (struct sockaddr*)&addr, sizeof(addr));
//     listen(server_fd, 5);

//     std::thread(background_cleanup).detach();

//     std::cout << "Server listening on port 6379...\n";

//     while (true) {
//         struct sockaddr_in client_addr;
//         socklen_t len = sizeof(client_addr);
//         int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &len);
//         if (client_fd >= 0) {
//             std::thread(handle_client, client_fd).detach();
//         }
//     }
//     close(server_fd);
//     return 0;
// }