/* This line at the top is necessary for compilation on the lab machine and many other Unix machines.
Please look up _XOPEN_SOURCE for more details. As well, if your code does not compile on the lab
machine please look into this as a a source of error. */
#define _XOPEN_SOURCE

/**
 * client.c
 *  CS165 Fall 2018
 *
 * This file provides a basic unix socket implementation for a client
 * used in an interactive client-server database.
 * The client receives input from stdin and sends it to the server.
 * No pre-processing is done on the client-side.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <errno.h>

#include "message.h"
#include "utils.h"

#define DEFAULT_STDIN_BUFFER_SIZE 1024

/**
 * connect_client()
 *
 * This sets up the connection on the client side using unix sockets.
 * Returns a valid client socket fd on success, else -1 on failure.
 *
 **/
int connect_client() {
    int client_socket;
    size_t len;
    struct sockaddr_un remote;

    log_info("-- Attempting to connect...\n");

    if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    remote.sun_family = AF_UNIX;
    strncpy(remote.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    len = strlen(remote.sun_path) + sizeof(remote.sun_family) + 1;
    if (connect(client_socket, (struct sockaddr *)&remote, len) == -1) {
        log_err("client connect failed: ");
        return -1;
    }

    log_info("-- Client connected at socket: %d.\n", client_socket);
    return client_socket;
}

/**
 * Getting Started Hint:
 *      What kind of protocol or structure will you use to deliver your results from the server to the client?
 *      What kind of protocol or structure will you use to interpret results for final display to the user?
 *      
**/
int main(void)
{
    int client_socket = connect_client();
    if (client_socket < 0) {
        exit(1);
    }

    message send_message;
    message recv_message;

    // Always output an interactive marker at the start of each command if the
    // input is from stdin. Do not output if piped in from file or from other fd
    const char* prefix = "";
    if (isatty(fileno(stdin))) {
        prefix = "db_client > ";
    }

    const char *output_str = NULL;
    int len = 0;

    // loop and wait for:
    // 1. output interactive marker
    // 2. read from stdin until eof.
    char read_buffer[DEFAULT_STDIN_BUFFER_SIZE];
    send_message.payload = read_buffer;
    send_message.status = OK_DONE;

    while (printf("%s", prefix), output_str = fgets(read_buffer,
           DEFAULT_STDIN_BUFFER_SIZE, stdin), !feof(stdin)) {
        if (output_str == NULL) {
            log_err("fgets failed.\n");
            break;
        }

        // Only process input that is greater than 1 character.
        // convert to message and send the message and the
        // payload directly to the server.
        send_message.length = strlen(read_buffer);
        if (send_message.length > 1) {
            // send the message_header, which tells server payload size
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                log_err("Failed to send message header.");
                exit(1);
            }

            // send the payload (query) to server
            if (send(client_socket, send_message.payload, send_message.length, 0) == -1) {
                log_err("Failed to send query payload.");
                exit(1);
            }

            // always wait for server response
            ssize_t header_len_recv;
            if ((header_len_recv = recv(client_socket, &recv_message, sizeof(message), 0)) > 0) {

                // received the header
                if (recv_message.length < 0) {
                    log_err("Client: Received header with invalid length: %d\n", recv_message.length);
                    exit(1); // Or break, then exit outside loop
                }

                char* server_payload_buffer = NULL;
                if (recv_message.length > 0) {
                    // Allocate buffer for the payload
                    server_payload_buffer = (char*)malloc(recv_message.length + 1);
                    if (!server_payload_buffer) {
                        log_err("Client: Failed to allocate memory for server payload.\n");
                        exit(1);
                    }

                    // Receive the payload from the server
                    ssize_t payload_bytes_actual = recv(client_socket, server_payload_buffer, recv_message.length, MSG_WAITALL);
                    
                    if (payload_bytes_actual > 0) {
                        if (payload_bytes_actual != recv_message.length) {
                             log_err("Client: Incomplete payload. Expected %d, Got %zd\n", recv_message.length, payload_bytes_actual);
                             free(server_payload_buffer);
                             exit(1);
                        }
                        server_payload_buffer[recv_message.length] = '\0';
                        printf("%s\n", server_payload_buffer);
                    } else if (payload_bytes_actual == 0) {
                        log_info("Client: Server closed connection while expecting payload.\n");
                    } else {
                        log_err("Client: Failed to receive payload: %s\n", strerror(errno)); // Need #include <errno.h>
                        // No payload to print
                    }
                    free(server_payload_buffer);
                } else {
                    if (recv_message.status == OK_WAIT_FOR_RESPONSE || recv_message.status == OK_DONE) {
                         printf("\n");
                    }
                }
            } else {
                if (header_len_recv < 0) {
                    log_err("Client: Failed to receive message header: %s\n", strerror(errno)); // Need #include <errno.h>
                } else {
                    log_info("-- Server closed connection\n");
                }
                exit(1);
            }
        }
    }
    close(client_socket);
    return 0;
}
