#ifndef RESP_H
#define RESP_H
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
} Element;

typedef struct {
    Element element;
    int error;
    int empty;
    // 0 everything read
    // 1 there is an error
    int status;
} Request;

typedef struct {
    Element** elts;
    int count;
} RespArray;

typedef struct {
    int   client_fd;
    char *buffer;
    long   cursor;
    long   length;
    int   capacity;
    // total bytes read until the command is processed
    long total_read;
    int status;
} BufferReader;

Request try_parse_request(Arena *arena, BufferReader *buffer);

BulkString parse_bulk_string(Arena *arena, BufferReader *buffer);
SimpleString parse_simple_string(Arena *arena, BufferReader *buffer);
RespArray parse_resp_array(Arena *arena, BufferReader *buffer);
Element parse_element(Arena *arena, BufferReader *buffer);
RdbMessage parse_initial_rdb_transfer(Arena *arena, BufferReader *buffer);

// Response functions
void *send_response_bulk_string(int conn_fd, s8 str);
void *respond_null(int client_fd);
long send_response(int client_fd, const char *response);
void send_response_array(int client_fd, char **items, int size);

void append_read_buf(BufferReader *);
#endif
