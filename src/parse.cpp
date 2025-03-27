/* 
 * This file contains methods necessary to parse input from the client.
 * Mostly, functions in parse.c will take in string input and map these
 * strings into database operators. This will require checking that the
 * input from the client is in the correct format and maps to a valid
 * database operator.
 */

#define _DEFAULT_SOURCE
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include "parse.h"
#include "utils.h"
#include "db_types.hh"
#include <vector>
#include <iostream>


/**
 * Takes a pointer to a string.
 * This method returns the original string truncated to where its first comma lies.
 * In addition, the original string now points to the first character after that comma.
 * This method destroys its input.
 **/

char* next_token(char** tokenizer, message_status* status) {
    char* token = strsep(tokenizer, ",");
    if (token == NULL) {
        *status= INCORRECT_FORMAT;
    }
    return token;
}

void parse_args(char* query_command, DbOperator* dbo) {
    char* token = strtok(query_command, " ");
    while (token != nullptr) {
        int num = atoi(token);
        dbo->args.push_back(num);
        token = strtok(nullptr, " ");
    }
}

DbOperator* parse_command(char* query_command, message* send_message, int client_socket) {
    /**
     * commands: put (p), get (g), range (r), delete (d), load (l), print stats (s)
     * put: p [INT1] [INT2]
     * get: g [INT1]
     * range: r [INT1] [INT2]
     * delete: d [INT1]
     * load: l [PATH_TO_FILE_NAME]
     * print stats: s
    **/

    DbOperator *dbo = new DbOperator();
    // dbo->client_fd = client_socket;

    cs165_log(stdout, "FD %i> QUERY: %s\n", client_socket, query_command);

    send_message->status = OK_WAIT_FOR_RESPONSE;

    // case match the commands
    if (!query_command || strlen(query_command) < 1) {
        send_message->status = NO_QUERY_ENTERED;
        delete dbo;
        return NULL;
    }

    // first letter is the command
    char command_char = tolower(query_command[0]);
    send_message->status = OK_WAIT_FOR_RESPONSE;

    switch (command_char) {
        case 'p': {
            query_command += 1;
            dbo->type = PUT;

            // put first and second arguments here:
            parse_args(query_command, dbo);
            if (dbo->args.size() != 2) {
                send_message->status = INCORRECT_FORMAT;
                delete dbo;
                return NULL;
            }
            break;
        }
        case 'g': {
            query_command += 1;
            dbo->type = GET;
            // get first argument here:
            parse_args(query_command, dbo);
            if (dbo->args.size() != 1) {
                send_message->status = INCORRECT_FORMAT;
                delete dbo;
                return NULL;
            }
            break;
        }
        case 'r': {
            query_command += 1;
            dbo->type = RANGE;
            // range first and second arguments here:
            parse_args(query_command, dbo);
            if (dbo->args.size() != 2) {
                send_message->status = INCORRECT_FORMAT;
                delete dbo;
                return NULL;
            }
            break;
        }
        case 'd': {
            query_command += 1;
            dbo->type = DELETE;
            // delete first argument here:
            parse_args(query_command, dbo);
            if (dbo->args.size() != 1) {
                send_message->status = INCORRECT_FORMAT;
                delete dbo;
                return NULL;
            }
            break;
        }
        case 'l': {
            query_command += 1;
            dbo->type = LOAD;
            // load first argument here:
            parse_args(query_command, dbo);
            if (dbo->args.size() != 1) {
                send_message->status = INCORRECT_FORMAT;
                delete dbo;
                return NULL;
            }
            break;
        }
        case 's': {
            query_command += 1;
            dbo->type = PRINT_STATS;
            // print stats: no arguments
            if (strlen(query_command) != 0) {
                send_message->status = INCORRECT_FORMAT;
                delete dbo;
                return NULL;
            }
            break;
        }
        default: {
            send_message->status = UNKNOWN_COMMAND;
            delete dbo;
            return NULL;
        }
    }
    
    dbo->client_fd = client_socket;
    return dbo;
}
