#ifndef PARSE_H__
#define PARSE_H__
#include "message.h"
#include "db_types.h"

DbOperator* parse_command(char* query_command, message* send_message, int client);

#endif
