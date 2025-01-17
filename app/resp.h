#ifndef RESP_H
#define RESP_H
#include "server.h"
#include "str.h"


// Response size constant
#define RESPONSE_ITEM_MAX_SIZE 1024

typedef enum {
    BULK_STRING,
    SIMPLE_STRING,
    SIMPLE_ERROR,
    ARRAY,
} ReqType;

typedef struct {
    s8 str;
} BulkString;

typedef struct {
    s8 raw;
} RdbMessage;

typedef struct {
    s8 str;
} SimpleString;

typedef struct {
    s8 str;
} SimpleError;

typedef struct {
    ReqType type;
    void* val;
    int error;
    int empty;
} Request;

typedef struct {
    Request** elts;
    int count;
} RespArray;

typedef struct {
    char *buffer;
    long   cursor;
    long   length;
    int   capacity;
    int   client_fd;
    // total bytes read until the command is processed
    long total_read;
} RequestParserBuffer;

BulkString parse_bulk_string(Arena *arena, RequestParserBuffer *buffer);
SimpleString parse_simple_string(Arena *arena, RequestParserBuffer *buffer);
RespArray parse_resp_array(Arena *arena, RequestParserBuffer *buffer);
Request parse_request(Arena *arena, RequestParserBuffer *buffer);
RdbMessage parse_initial_rdb_transfer(Arena *arena, RequestParserBuffer *buffer);

// Response functions
void *send_response_bulk_string(Context *ctx, s8 str);
void *respond_null(int client_fd);
long send_response(int client_fd, const char *response);
void send_response_array(int client_fd, char **items, int size);

#endif
