#ifndef SERVER_H
#define SERVER_H

#include "arena.h"
#include "hashmap.h"
#include "str.h"
#include "vec.h"
#include <netinet/in.h>

#define RESPONSE_ITEM_MAX_SIZE 1024
#define MAX_PATH 1024

static const char* pongMsg = "+PONG\r\n";
static const char* okMsg = "+OK\r\n";

typedef struct {
    char* dir;
    char* dbfilename;
    int port;
    struct sockaddr_in* master_info;
    char* master_replid;
    int master_repl_offset;
} Config;

typedef struct {
    int conn_fd;
    HashMap* hashmap;
    Config* config;
    Arena* thread_allocator;
    Arena* main_arena;
    vector* replicas;
    int is_connection_to_master;
} Context;

typedef enum {
    BULK_STRING,
    ARRAY,
} ReqType;

typedef struct {
    s8 str;
} BulkString;

typedef struct {
    ReqType type;
    void* val;
    int error;
} Request;

typedef struct {
    vector* elts;
    int count;
    int error;
} RespArray;

typedef struct {
    char *buffer;
    int   cursor;
    int   length;
    int   capacity;
    int   client_fd;
} RequestParserBuffer;

void* connection_handler(void* arg);
void send_response_array(int client_fd, char** items, int size);
int send_response(int client_fd, const char* response);
Request parse_request(Arena *, RequestParserBuffer *);
#endif
