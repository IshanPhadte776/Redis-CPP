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

#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>

#include "respparser.h"

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
            for (auto &c : command) c = toupper(c);

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
