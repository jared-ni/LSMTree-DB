#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
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
#include <memory>

#include "parse.h"
#include "message.h"
#include "utils.h"
#include "db_types.hh"
#include "lsm_tree.hh"

// --- Configuration ---
const std::string DB_PATH = "./lsm_db_directory";

// --- Global LSM Tree ---
std::unique_ptr<LSMTree> lsm_tree_ptr;

#define DEFAULT_QUERY_BUFFER_SIZE 1024
// #define WORKER_THREADS_NUM 5 // Not used

// shut sown worker threads
bool shutdown_requested = false;
pthread_mutex_t shutdown_mutex = PTHREAD_MUTEX_INITIALIZER;

/** Step 2 in handle_client_request:
 * TODO: handle query types according CS265 domain specific language
 **/
char* execute_DbOperator(DbOperator* query) {
    if (!lsm_tree_ptr) {
         return strdup("[SERVER] Error: Database not initialized.");
    }
    if(!query) {
        return strdup("[SERVER] Error: Invalid DB query object.");
    }

    size_t num_args = query->args.size(); 

    if (query->type == PUT) {
        if (num_args != 2) {
             return strdup("[SERVER] Error: PUT requires 2 arguments (key, value).");
        }
       
        int key = query->args[0];
        int value = query->args[1];

        // Ensure compatibility if LSMTree uses 'long' and args are 'long long'
        DataPair data_to_put(static_cast<int>(key), static_cast<int>(value), false);

        if (lsm_tree_ptr->putData(data_to_put)) {
            return strdup("[SERVER] PUT successful.");
        } else {
            return strdup("[SERVER] Error: PUT operation failed internally.");
        }

    } else if (query->type == GET) {
        if (num_args != 1) {
             return strdup("[SERVER] Error: GET requires 1 argument (key).");
        }
        int key = query->args[0];

        std::optional<DataPair> result = lsm_tree_ptr->getData(key);

        if (result.has_value()) {
            char buffer[64];
            // Use the values from the result DataPair (which are 'long')
            int len = snprintf(buffer, sizeof(buffer), "%d:%d", result.value().key_, result.value().value_);
            if (len < 0 || len >= sizeof(buffer)) {
                return strdup("[SERVER] Error: Failed to format GET result.");
            }
            return strdup(buffer);
        } else {
             char buffer[100];
             // Use the original key requested for the not found message
             snprintf(buffer, sizeof(buffer), "[SERVER] Key %d not found.", key);
            return strdup(buffer);
        }

    } else if (query->type == RANGE) {
        // if (num_args != 2) { // Check using size() if vector
        if (num_args != 2) {    // Check using num_args field
            return strdup("[SERVER] Error: RANGE requires 2 arguments (start_key, end_key).");
        }
        int start_key = query->args[0];
        int end_key = query->args[1];

        if (end_key < start_key) {
            return strdup("[SERVER] Error: RANGE end_key must be greater than or equal to start_key.");
        }

        std::vector<DataPair> results = lsm_tree_ptr->rangeData(start_key, end_key);

        if (results.empty()) {
            return strdup("");
        } else {
            std::string result_str;
            for (size_t i = 0; i < results.size(); ++i) {
                // Append key:value
                result_str += std::to_string(results[i].key_);
                result_str += ":";
                result_str += std::to_string(results[i].value_);
                if (i < results.size() - 1) {
                    result_str += " ";
                }
            }
            // strdup creates a C-style string copy on the heap
            return strdup(result_str.c_str());
        }

    } else if (query->type == DELETE) {
        if (num_args != 1) {
             return strdup("[SERVER] Error: DELETE requires 1 argument (key).");
        }
        int key = query->args[0];
        bool deleted_processed = lsm_tree_ptr->deleteData(key);

        if (deleted_processed) {
            char response_buffer[138];
            snprintf(response_buffer, sizeof(response_buffer), 
                     "[SERVER] Key %d marked for deletion.", key);
            return strdup(response_buffer);
        } else {
            char error_buffer[128];
            snprintf(error_buffer, sizeof(error_buffer),
                     "[SERVER] Error: Failed to process DELETE for key %d internally.", key);
            return strdup(error_buffer);
        }

    } else if (query->type == LOAD) {
        if (query->s_args.empty() || query->s_args[0].empty()) {
            return strdup("[SERVER] Error: LOAD requires a file path argument.");
        }
        const std::string& file_path = query->s_args[0];
        // log_info("[SERVER] LOAD command processing file: %s\n", file_path.c_str()); // Optional: for server logs

        // REMOVE THIS LINE: return strdup("[SERVER] LOAD query not implemented.");

        // open file in binary, and every 4 bytes is a key, and 4 more bytes is a value
        char cwd[1024];
        if (getcwd(cwd, sizeof(cwd)) != NULL) {
            log_info("[SERVER CWD] Current working directory: %s\n", cwd);
        } else {
            log_err("[SERVER CWD] getcwd() error\n");
        }
        log_info("[SERVER] Attempting to load file with path argument: '%s'\n", file_path.c_str());
        
        std::ifstream file(file_path, std::ios::binary | std::ios::ate);
        if (!file.is_open()) {
            char err_buf[FILENAME_MAX + 128]; // Ensure FILENAME_MAX is available (usually from <stdio.h> or <limits.h>)
            snprintf(err_buf, sizeof(err_buf), "[SERVER] Error: Cannot open file '%s': %s",
                     file_path.c_str(), strerror(errno)); // strerror needs <cstring> and errno needs <cerrno>
            return strdup(err_buf);
        }
        std::streamsize file_size = file.tellg();
        file.seekg(0, std::ios::beg);

        if (file_size == 0) {
            file.close();
            char msg_buf[FILENAME_MAX + 64];
            snprintf(msg_buf, sizeof(msg_buf), "[SERVER] LOAD file '%s' is empty. 0 pairs loaded.", file_path.c_str());
            return strdup(msg_buf);
        }

        // file_size must be a multiple of 8 (4+4 bytes key/val pairs)
        if (file_size % 8 != 0) {
            file.close();
            char err_buf[FILENAME_MAX + 128];
            snprintf(err_buf, sizeof(err_buf),
                     "[SERVER] Error: LOAD file '%s' has incorrect size (%lld bytes). Must be a multiple of 8 bytes for key-value pairs.",
                     file_path.c_str(), (long long)file_size);
            return strdup(err_buf);
        }

        int key_from_file;
        int value_from_file;
        unsigned long items_processed_from_file = 0;
        unsigned long items_successfully_put = 0;
        bool read_error_occurred = false;

        // read key/val pairs from file
        while (file.good() && (items_processed_from_file < (unsigned long)(file_size / 8))) {
            // Read 4 bytes for the key
            if (!file.read(reinterpret_cast<char*>(&key_from_file), sizeof(key_from_file))) {
                if (!file.eof()) { // If not EOF, it's an actual read error
                    read_error_occurred = true;
                }
                break; // Exit loop on read failure or EOF for key
            }
            // Read 4 bytes for the value
            if (!file.read(reinterpret_cast<char*>(&value_from_file), sizeof(value_from_file))) {
                if (!file.eof()) { // If not EOF, it's an actual read error for value
                    read_error_occurred = true;
                }
                // This implies we read a key but couldn't read its corresponding value
                log_err("[SERVER] LOAD: Read key but failed to read value from '%s'. File might be truncated or corrupt.\n", file_path.c_str());
                break;
            }
            // track items processed
            items_processed_from_file++;
            DataPair data_to_put(key_from_file, value_from_file, false);

            if (lsm_tree_ptr->putData(data_to_put)) {
                items_successfully_put++;
            } else {
                log_err("[SERVER] LOAD: putData failed for key %d, value %d from file '%s'. Continuing.\n",
                        key_from_file, value_from_file, file_path.c_str());
            }
        } // End of while loop

        // ---- MOVED THIS LOGIC OUTSIDE THE WHILE LOOP ----
        // file.close(); // MOVE THIS LINE to after checking read_error_occurred and sending success/error message for the whole file

        if (read_error_occurred) {
            file.close(); // Close file before returning error
            char err_buf[FILENAME_MAX + 128];
            snprintf(err_buf, sizeof(err_buf),
                    "[SERVER] Error: A read error occurred while processing file '%s' after %lu pairs.",
                    file_path.c_str(), items_processed_from_file);
            return strdup(err_buf);
        }

        file.close(); // Close file after successful processing or non-fatal EOF
        char success_buf[FILENAME_MAX + 128];
        snprintf(success_buf, sizeof(success_buf),
                "[SERVER] LOAD successful. Processed %lu pairs, successfully put %lu pairs into LSM Tree from '%s'.",
                items_processed_from_file, items_successfully_put, file_path.c_str());
        return strdup(success_buf);

    } else if (query->type == PRINT_STATS) {
        std::cout << "printing stats" << std::endl;
        return strdup("[SERVER] PRINT_STATS not implemented.");

    } else if (query->type == INCORRECT_FORMAT) {
        return strdup("[SERVER] Error: Invalid query format detected by parser."); // Modified message slightly
    } else {
        return strdup("[SERVER] Error: Unknown query type.");
    }
}


void handle_client_request(int client_socket) {
    message send_message;
    message recv_message;

    int length = recv(client_socket, &recv_message, sizeof(message), 0);
    if (length <= 0) { return; }

    if (recv_message.length <= 0 || recv_message.length > DEFAULT_QUERY_BUFFER_SIZE * 10) {
        log_info("[SERVER] Received invalid message length: %d on socket %d\n", recv_message.length, client_socket);
        return;
    }

    char recv_buffer[recv_message.length + 1];
    length = recv(client_socket, recv_buffer, recv_message.length, MSG_WAITALL);
    if (length != recv_message.length) {
        log_err("[SERVER] Failed to receive full payload on socket %d. Expected %d, Got %d\n", client_socket, recv_message.length, length);
        return;
    }
    recv_message.payload = recv_buffer;
    recv_message.payload[recv_message.length] = '\0';

    DbOperator* query = parse_command(recv_message.payload, &send_message, client_socket);

    char* result = execute_DbOperator(query);
    // TODO: If query was allocated by parse_command, free it here, e.g., free_DbOperator(query);


    if (strncmp(result, "[SERVER] Error", 14) == 0 || strncmp(result, "[CLIENT] Error", 14) == 0) {
        send_message.status = EXECUTION_ERROR;
    } else if (strstr(result, "not found.") != NULL) { // Adjusted check slightly
         send_message.status = OBJECT_NOT_FOUND;
    } else {
        send_message.status = OK_WAIT_FOR_RESPONSE;
    }
    send_message.length = strlen(result);

    if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
        log_err("[SERVER] Failed to send response header to socket %d: %s\n", client_socket, strerror(errno));
        free(result);
        return;
    }

    if (send_message.length > 0) {
        if (send(client_socket, result, send_message.length, 0) == -1) {
            log_err("[SERVER] Failed to send response payload to socket %d: %s\n", client_socket, strerror(errno));
        }
    }

    free(result);
}


// setup_server(): unchanged
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
    strncpy(local.sun_path, SOCK_PATH, sizeof(local.sun_path) - 1);
    local.sun_path[sizeof(local.sun_path) - 1] = '\0';
    unlink(local.sun_path);

    len = SUN_LEN(&local);

    if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
        log_err("L%d: Socket failed to bind %s: %s\n", __LINE__, local.sun_path, strerror(errno));
        close(server_socket);
        return -1;
    }

    if (listen(server_socket, 5) == -1) {
        log_err("L%d: Failed to listen on socket %s: %s\n", __LINE__, local.sun_path, strerror(errno));
        close(server_socket);
        unlink(local.sun_path);
        return -1;
    }

    return server_socket;
}


int main(void)
{
    // 3. Initialize LSM Tree in main
    log_info("[SERVER INIT] Initializing LSM Tree at path: %s\n", DB_PATH.c_str());
    try {
        lsm_tree_ptr = std::make_unique<LSMTree>(
            DB_PATH
        );
        log_info("[SERVER INIT] LSM Tree initialized successfully.\n");
    } catch (const std::exception& e) {
        log_err("[SERVER INIT] CRITICAL: Failed to initialize LSM Tree: %s\n", e.what());
        exit(EXIT_FAILURE);
    }

    int server_socket = setup_server();
    if (server_socket < 0) {
        exit(EXIT_FAILURE);
    }
    log_info("[SERVER] Server socket %d established, listening on %s\n", server_socket, SOCK_PATH);

    
    std::set<int> clientSockets;
    // fd_set bitmap manages set of fds when using select in multiplexing
    fd_set read_fds;
    // update max_fd always to current max fd number
    int max_fd = server_socket;

    // TODO: implement multiplexing here
    while (true) {
        FD_ZERO(&read_fds);
        FD_SET(server_socket, &read_fds);

        max_fd = server_socket;
        // add active client sockets to read_fds
        for (int client_sock : clientSockets) {
            FD_SET(client_sock, &read_fds);
            max_fd = std::max(max_fd, client_sock);
        }

        // use select to get all socket fds that can be read
        int activity = select(max_fd + 1, &read_fds, NULL, NULL, NULL);

        if (activity < 0 && errno != EINTR) {
            log_err("[SERVER] select() error: %s\n", strerror(errno));
            break;
        }
         if (activity == 0) {
             continue;
         }


        // New client connection if server socket is ready
        if (FD_ISSET(server_socket, &read_fds)) {
            // note to self: unix socket only works for IPC on the same machine
            struct sockaddr_un client_addr;
            socklen_t socket_sz = sizeof(client_addr);
            int client_socket = accept(server_socket, (struct sockaddr *)&client_addr, &socket_sz);

            if (client_socket < 0) {
                log_err("[SERVER] Failed to accept new client connection: %s\n", strerror(errno));
            } else {
                log_info("[SERVER] Accepted new client connection on socket %d.\n", client_socket);
                clientSockets.insert(client_socket);
            }
        }

        // all all ready client sockets (Removed redundant inner loop)

        // check all ready client sockets
        std::vector<int> clients_to_remove;
        for (int client_socket : clientSockets) {
            if (FD_ISSET(client_socket, &read_fds)) {
                // check for closed
                char buffer;
                ssize_t peek_len = recv(client_socket, &buffer, 1, MSG_PEEK | MSG_DONTWAIT);

                if (peek_len == 0) {
                    log_info("[SERVER] Client disconnected (detected by peek): socket %d\n", client_socket);
                    clients_to_remove.push_back(client_socket);
                } else if (peek_len < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                         log_info("[SERVER] Warning: Spurious wakeup or no data on ready socket %d?\n", client_socket);
                    } else {
                        log_err("[SERVER] Error peeking on socket %d: %s\n", client_socket, strerror(errno));
                        clients_to_remove.push_back(client_socket);
                    }
                } else {
                    handle_client_request(client_socket);
                }
            }
        }

        for (int client_socket : clients_to_remove) {
            close(client_socket);
            clientSockets.erase(client_socket);
            log_info("[SERVER] Closed and removed client socket %d\n", client_socket);
        }

    }

    log_info("[SERVER] Shutting down...\n");
    for (int client_socket : clientSockets) {
        close(client_socket);
    }
    if (server_socket >= 0) {
        close(server_socket);
        unlink(SOCK_PATH);
    }
    log_info("[SERVER] Shutdown complete.\n");

    return 0;
}