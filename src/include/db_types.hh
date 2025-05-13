#ifndef DB_TYPES
#define DB_TYPES

#include <vector>

typedef enum OperatorType {
    PUT,
    GET,
    RANGE,
    DELETE,
    LOAD,
    PRINT_STATS,
} OperatorType;

typedef struct DbOperator {
    OperatorType type;
    int client_fd;

    std::vector<int> args;
    // for loading a string path
    std::vector<std::string> s_args;

} DbOperator;

#endif