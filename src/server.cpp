#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <unistd.h>
#include <string.h>
#include <queue>
#include <vector>
#include <algorithm>
#include <iostream>
#include <set>

#include "parse.h"
#include "message.h"
#include "utils.h"
#include "db_types.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024
#define WORKER_THREADS_NUM 5

// holds client sockets and states
typedef struct {
    int client_socket;
} client_job;

// shut sown worker threads
bool shutdown_requested = false;
pthread_mutex_t shutdown_mutex = PTHREAD_MUTEX_INITIALIZER;

/** Step 2 in handle_client_request: 
 * TODO: handle query types according CS265 domain specific language
 **/
char* execute_DbOperator(DbOperator* query) {
    if(!query) {
        return strdup("[SERVER] Error: Invalid DB query.");
    } else if (query->type == PUT) {
        std::cout << "putting " << std::endl;
        for (int i = 0; i < 2; i++) {
            std::cout << query->args[i] << std::endl;
        }
        return strdup("[SERVER] putting.");
    } else if (query->type == GET) {
        std::cout << "getting " << std::endl;
    } else if (query->type == RANGE) {
        std::cout << "range " << std::endl;
    } else if (query->type == DELETE) {
        std::cout << "deleting " << std::endl;
    } else if (query->type == LOAD) {
        std::cout << "loading " << std::endl;
    } else if (query->type == PRINT_STATS) {
        std::cout << "printing stats" << std::endl;
    } else if (query->type == INCORRECT_FORMAT) {
        return strdup("[SERVER] Error: Invalid query argument format.");
    } else {
        return strdup("[SERVER] Error: Invalid query type.");
    }
}

void handle_client_request(int client_socket) {
    // Create two messages, one from which to read and one from which to receive
    message send_message;
    message recv_message;

    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response to the request.
    int length = recv(client_socket, &recv_message, sizeof(message), 0);
    if (length <= 0) {
        if (length < 0) { log_err("Client connection closed!\n"); }
        return;
    }

    char recv_buffer[recv_message.length + 1];
    length = recv(client_socket, recv_buffer, recv_message.length,0);
    recv_message.payload = recv_buffer;
    recv_message.payload[recv_message.length] = '\0';

    // 1. Parse command: client query str -> database operator request
    // TODO: match to the correct command. 
    // If the command is not supported, set the status to UNKNOWN_COMMAND 
    DbOperator* query = parse_command(recv_message.payload, &send_message, client_socket);

    // 2. Handle request: db operator request executed -> get response in send_message
    char* result = execute_DbOperator(query);

    send_message.length = strlen(result);
    char send_buffer[send_message.length + 1];
    strcpy(send_buffer, result);
    send_message.payload = send_buffer;
    send_message.status = OK_WAIT_FOR_RESPONSE;
    
    // 3. send status of the db respnose
    if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
        log_err("Failed to send message.");
        exit(1);
    }

    // 4. Send response to the request
    if (send(client_socket, result, send_message.length, 0) == -1) {
        log_err("Failed to send message.");
        exit(1);
    }
}


// setup_server(): set up server side listening socket & return its file descriptor
int setup_server() {
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    log_info("Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    unlink(local.sun_path);

    /*
    int on = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    {
        log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
        return -1;
    }
    */

    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return -1;
    }

    if (listen(server_socket, 5) == -1) {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return -1;
    }

    return server_socket;
}


/**
* TODO: extend to handle multiple clients
* Think about what is shared between clients and what is not
* Use multiplexing: Accept client connection sockets without blocking,
* use select to wait for activity on any fo the sockets when data can be read/send from any of them.
**/
int main(void)
{
    int server_socket = setup_server();
    if (server_socket < 0) {
        exit(1);
    }
    log_info("[SERVER] server socket %d established.\n", server_socket);

    // map client socket fds to clientContexts
    // std::unordered_map<int, ClientContext*> clientContexts
    std::set<int> clientSockets;
    // fd_set bitmap manages set of fds when using select in multiplexing
    fd_set read_fds;
    // update max_fd always to current max fd number
    int max_fd = server_socket;

    // TODO: implement multiplexing here
    while (true) {
        // clear the set of fds, so end of this it contains only ready fds + server socket
        FD_ZERO(&read_fds);
        FD_SET(server_socket, &read_fds);

        // add active client sockets to read_fds
        for (auto const& client_socket : clientSockets) {
            FD_SET(client_socket, &read_fds);
            max_fd = std::max(max_fd, client_socket);
        }
        // use select to get all socket fds that can be read
        int cur_activity = select(max_fd + 1, &read_fds, NULL, NULL, NULL);
        if (cur_activity < 0) {
            log_err("[SERVER] select() error.\n");
            exit(1);
        }
        // New client connection if server socket is ready
        if (FD_ISSET(server_socket, &read_fds)) {
            // note to self: unix socket only works for IPC on the same machine
            struct sockaddr_un client_addr;
            socklen_t socket_sz = sizeof(client_addr);
            int client_socket = accept(server_socket, (struct sockaddr *)&client_addr, &socket_sz);
            if (client_socket < 0) {
                log_err("[SERVER] Failed to accept new client connection.\n");
                exit(1);
            }

            log_info("[SERVER] Accepted new client connection %d.\n", client_socket);

            clientSockets.insert(client_socket);
        }

        // all all ready client sockets
        std::vector<int> ready_client_sockets;
        for (auto const& client_socket : clientSockets) {
            if (FD_ISSET(client_socket, &read_fds)) {
                ready_client_sockets.push_back(client_socket);
            }
        }
        // check all ready client sockets
        for (int client_socket : ready_client_sockets) {
            if (FD_ISSET(client_socket, &read_fds)) {
                // check for closed
                char buffer;
                // recv return 0 when peeking only on a closed socket
                if (recv(client_socket, &buffer, 1, MSG_PEEK) == 0) {
                    log_info("Client disconnected at socket %d\n", client_socket);
                    close(client_socket);
                    clientSockets.erase(client_socket);
                    continue;
                } else {
                    handle_client_request(client_socket);
                }
            }
        }
    }

    for (auto const& client_socket : clientSockets) {
        close(client_socket);
    }
    close(server_socket);

    return 0;
}
