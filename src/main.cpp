//This is code for a TCP Server that listens on port 6397 (Redis port) for incoming client connections

//"This server runs on port 6379 and listens on all network interfaces via INADDR_ANY. While it can be reached over localhost or Wi-Fi

//For cin and cout
#include <iostream>
//general purpose functions like exit() 
#include <cstdlib>
//C++ String
#include <string>
//C String functions like memset()
#include <cstring>
//POSIX API for sockets (socket(), bind(), listen(), accept(), close())
#include <unistd.h>

#include <thread>
#include <mutex>
#include <unordered_map>
#include <queue>
#include <chrono>

#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>

#include "respparser.h"

struct Node {
    std::string value;
    std::chrono::steady_clock::time_point expires_at; // Time point when the key expires
    bool hasTTL = false;
};

struct ExpiryEntry {
    std::string key;
    std::chrono::steady_clock::time_point expires_at;

  friend bool operator>(const ExpiryEntry& a, const ExpiryEntry& b) {
        return a.expires_at > b.expires_at;
  }

};

std::unordered_map<std::string, Node> key_value_store; // In-memory key-value store
std::priority_queue<ExpiryEntry, std::vector<ExpiryEntry>, std::greater<ExpiryEntry>> expiry_heap;
std::mutex store_mutex;

void handle_client(int client_fd) {
    // 2. Loop to handle multiple commands from THIS specific client
    //buffer to hold the incoming data from the client, we will read into this buffer and then process the commands. The size of 1024 is arbitrary and can be adjusted based on expected command sizes.
    char buffer[1024];
    while (true) {
      //blocking call that waits for the client to send data, reads the data into the buffer and returns the number of bytes received. If the client disconnects, recv() will return 0 or a negative value, which we can use to break out of the loop and wait for the next client.
        ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer), 0);
        
        if (bytes_received <= 0) {
            // Client disconnected or error
            std::cout << "Client disconnected\n";
            break; 
        }

        // 3. Respond to "PING"
        
        std::string raw_data(buffer, bytes_received);
        RespValue request = RespParser::parse(raw_data);

        //check for a valid Redis Array
        if (request.type == RespType::Array && !request.elements.empty()) {
            RespValue command = request.elements[0]; // Assuming the first element is the command
            for (auto &c : command.bulkString) c = toupper(c);

                // 5. Handle the specific commands
            if (command.bulkString == "PING") {
                const char* pong = "+PONG\r\n";
                send(client_fd, pong, strlen(pong), 0);
            } 
            else if (command.bulkString == "ECHO" && request.elements.size() > 1) {
                // Echo back the second element in the array
                std::string message = request.elements[1].bulkString;
                std::string response = "$" + std::to_string(message.length()) + "\r\n" + message + "\r\n";
                send(client_fd, response.c_str(), response.length(), 0);
            }

            else if (command.bulkString == "SET" && request.elements.size() > 2) {
                std::string key = request.elements[1].bulkString;
                std::string value = request.elements[2].bulkString;

                Node newNode;
                newNode.value = value;
                newNode.hasTTL = false; // By default, no TTL

                if (request.elements.size() >= 5){
                  std::string flag = request.elements[3].bulkString;
                  long long duration_ms = 0;
                  bool valid_ttl = false;

                  std::cout << "Received SET command with potential TTL. Flag: " << flag << "\n";
                  

                  try {
                    if (flag == "EX") {
                        // Seconds to Milliseconds
                        duration_ms = std::stoll(request.elements[4].bulkString) * 1000;
                        valid_ttl = true;
                    } else if (flag == "PX") {
                        // Already Milliseconds
                        duration_ms = std::stoll(request.elements[4].bulkString);
                        valid_ttl = true;
                    }
                  } catch (...) {
                      // Handle cases where the duration isn't a valid number
                      const char* err = "-ERR value is not an integer or out of range\r\n";
                      send(client_fd, err, strlen(err), 0);
                      continue; 
                  }



                  if (valid_ttl) {
                      std::cout << "Setting TTL for key [" << key << "] to " << duration_ms << " ms\n";
                      std::cout << "flag: " << flag << "\n";
                      newNode.hasTTL = true;
                      newNode.expires_at = std::chrono::steady_clock::now() + std::chrono::milliseconds(duration_ms);

                      // Add to the expiry heap
                      ExpiryEntry entry{key, newNode.expires_at};
                      {
                          std::lock_guard<std::mutex> lock(store_mutex);
                          expiry_heap.push(entry);
                      }
                  }

                }
                
                
                else {
                    std::lock_guard<std::mutex> lock(store_mutex);
                    key_value_store[key] = newNode;
                }

                const char* ok = "+OK\r\n";
                send(client_fd, ok, strlen(ok), 0);
                std::cout << "SET command processed for key: " << key << "\n";
                std::cout << "Current store size: " << key_value_store.size() << "\n";
            }
            else if (command.bulkString == "GET" && request.elements.size() > 1) {
                std::string key = request.elements[1].bulkString;
                std::string result;

                {
                    std::lock_guard<std::mutex> lock(store_mutex);
                    if (key_value_store.count(key)) {
                        Node& node = key_value_store[key];
                        if (node.hasTTL && std::chrono::steady_clock::now() >= node.expires_at) {
                          key_value_store.erase(key); // Delete it now (Lazy Expiration)
                        } else {
                          result = node.value;
                        } 
                    }
                }

                if (!result.empty()) {
                    std::string response = "$" + std::to_string(result.length()) + "\r\n" + result + "\r\n";
                    send(client_fd, response.c_str(), response.length(), 0);
                } else {
                    const char* nil = "$-1\r\n";
                    send(client_fd, nil, strlen(nil), 0);
                }
            }

            else {
                // Optional: Handle unknown commands
                const char* err = "-ERR unknown command\r\n";
                send(client_fd, err, strlen(err), 0);
            }
        }


      }

    // 4. Clean up this client and wait for the next one
    close(client_fd);
}

void background_cleanup() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100)); // Check 10 times a second

        auto now = std::chrono::steady_clock::now();
        std::lock_guard<std::mutex> lock(store_mutex);

        while (!expiry_heap.empty() && expiry_heap.top().expires_at <= now) {
            std::string key_to_expire = expiry_heap.top().key;
            
            // Double check: Did the key get updated with a new TTL since it was added to the heap?
            if (key_value_store.count(key_to_expire) && 
                key_value_store[key_to_expire].hasTTL && 
                key_value_store[key_to_expire].expires_at <= now) {
                
                key_value_store.erase(key_to_expire);
                std::cout << "Key [" << key_to_expire << "] expired and removed.\n";
            }
            expiry_heap.pop();
        }
    }
}

int main(int argc, char **argv) {
  // Flush after every std::cout / std::cerr
  // This is to ensure tht even if the server crashs, the buffer is flushed out and we can see the logs in the tester output
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;
  
  //socket creates an endpoint for the communication between client and server
  //AF_INET means we are using the IP_V4 protocol
  //SOCK_STREAM means we are using TCP (as opposed to UDP)
  // 0  means default protocol for the given socket type (TCP in this case)
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
   std::cerr << "Failed to create server socket\n";
   return 1;
  }
  
  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  // SO_REUSEADDR allows the server to bind to an address which is in a TIME_WAIT state. This is useful for development and testing, as it allows you to restart your server without waiting for the OS to release the port.
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    std::cerr << "setsockopt failed\n";
    return 1;
  }
  
  // Struct to hold the server's address information
  struct sockaddr_in server_addr;
  // Server uses IPv4
  server_addr.sin_family = AF_INET;
  //bind to any available network interface 
  server_addr.sin_addr.s_addr = INADDR_ANY;
  //server runs on port 6379 (default Redis port), hton converts the port number to network byte order (big-endian)
  server_addr.sin_port = htons(6379);
  
  //bind() links the socket file descriptor (server_fd) to the address / port specified in server_addr. This allows the server to receive incoming connections on that address and port.
  if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port 6379\n";
    return 1;
  }
  
  // allows for up to 5 pending connections in the queue before the server starts rejecting new connection attempts. This means that if more than 5 clients try to connect at the same time, the server will start rejecting new connection attempts until it has processed some of the existing ones.
  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }
  

  //This allows for multiple clients to connect to the server one after another. The server will handle one client at a time in a sequential manner. After a client disconnects, the server goes back to waiting for the next client to connect.
  while (true) {

    struct sockaddr_in client_addr; // holds connected client address information
    int client_addr_len = sizeof(client_addr); // length of the client address structure, needed for accept()
    std::cout << "Waiting for a client to connect...\n";

    // You can use print statements as follows for debugging, they'll be visible when running tests.
    std::cout << "Logs from your program will appear here!\n";
    
    //blocking call, waits for the client to accept, returns a fd for the accepted connection 
    int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, (socklen_t*)&client_addr_len);

    if (client_fd < 0) {
        std::cerr << "Accept failed\n";
        continue; // Try again for the next client
    }

    std::cout << "Client connected\n";


    std::thread client_thread(handle_client, client_fd);
    client_thread.detach(); // Detach the thread to allow it to run independently

    

  }
  
  close(server_fd);

  return 0;
}
