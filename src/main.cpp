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

#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>

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

    // 2. Loop to handle multiple commands from THIS specific client
    char buffer[1024];
    while (true) {
        ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer), 0);
        
        if (bytes_received <= 0) {
            // Client disconnected or error
            std::cout << "Client disconnected\n";
            break; 
        }

        // 3. Respond to "PING"
        // Note: Real Redis uses RESP protocol, but for basic testing, 
        // we check if the input contains "PING"
        std::string command(buffer, bytes_received);
        if (command.find("PING") != std::string::npos) {
            send(client_fd, "+PONG\r\n", 7, 0);
        }
    }

    // 4. Clean up this client and wait for the next one
    close(client_fd);
  }
  
  close(server_fd);

  return 0;
}
