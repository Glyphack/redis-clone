#ifndef SERVER_H
#define SERVER_H

#include "arena.h"
#include "hashmap.h"
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
} Context;

typedef struct {
    char** parts;
    int count;
    int error;
} RespArray;

void* connection_handler(void* arg);
void send_response_array(int client_fd, char** items, int size);
RespArray* parse_resp_array(Arena* arena, int client_fd);
int send_response(int client_fd, const char* response);


#endif
