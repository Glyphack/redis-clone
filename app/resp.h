#ifndef RESP_H
#define RESP_H
#include "str.h"
#include <setjmp.h>


static s8 null_resp = S("$-1\r\n");
static s8 ok_resp = S("+OK\r\n");
static s8 pong_resp = S("+PONG\r\n");
static s8 string_resp = S("+string\r\n");
static s8 stream_resp = S("+stream\r\n");
static s8 none_resp = S("+none\r\n");
static s8 xadd_id_err_resp = S("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
static s8 xadd_id_err_resp_0 = S("-ERR The ID specified in XADD must be greater than 0-0\r\n");

// Error codes for RESP parsing
typedef enum RespError {
    RESP_OK = 0,                  // No error
    RESP_ERR_INVALID_START,        // Invalid RESP type character (not *, $, +, -, :)
    RESP_ERR_INVALID_LENGTH,      // Invalid length specification
    RESP_ERR_ARRAY_LENGTH,        // Invalid array length
    RESP_ERR_UNEXPECTED_EOF,      // Unexpected end of input
    RESP_ERR_BUFFER_OVERFLOW,    // Buffer capacity exceeded
    RESP_ERR_MALFORMED_STRING,   // Malformed string (e.g., missing CRLF)
} RespError;

// Global jump buffer for error handling
extern jmp_buf resp_error_handler;

// Get string representation of RESP error
const char* resp_error_string(RespError err);
extern jmp_buf resp_error_handler;

typedef enum {
    BULK_STRING,
    SIMPLE_STRING,
    SIMPLE_ERROR,
    RDB_MSG,
    ARRAY,
} ReqType;

typedef struct {
    s8 str;
    int is_rdb;
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
} Element;

typedef struct {
    Element element;
    int error;
    int empty;
    // 0 everything read
    // 1 there is an error
    int status;
    // raw bytes of this request
    s8 bytes;
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

s8 serde(Arena *, Element);
s8 serde_bulk_str(Arena *arena, s8 str);
s8 serde_array(Arena *arena, char **items, int item_len);
s8 serde_int(Arena *arena, int val);

void append_read_buf(BufferReader *);
int insert_number(Arena *, char *, i64 , int );
i64 num_len(i64 );
#endif
