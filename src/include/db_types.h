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

    std::vector<long long> args;
} DbOperator;

#endif