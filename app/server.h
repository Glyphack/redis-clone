#ifndef USERS_SYSADMIN_PROGRAMMING_REDIS_CLONE_APP_SERVER_H
#define USERS_SYSADMIN_PROGRAMMING_REDIS_CLONE_APP_SERVER_H

#include "arena.h"
#include "hashmap.h"
#include "resp.h"
#include "vec.h"
#include <netinet/in.h>

#define RESPONSE_ITEM_MAX_SIZE 1024
#define MAX_PATH 1024
#define MAX_CLIENTS 10

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
    HashMap** hashmap;
    Config* config;
    Arena* perm;
    vector* replicas;
} ServerContext;

typedef struct {
    int cursor;
    int len;
    s8  buffer;
} BufferWriter;

typedef struct {
    int conn_fd;
    Arena* perm;
    HashMap** hashmap;
    BufferReader reader;
    int want_read;
    int want_write;
    int want_close;
    // bytes to be written to the client
    BufferWriter writer;
} ClientContext;

typedef struct {
    int master_fd;
    HashMap** hashmap;
    Config* config;
    Arena* perm;
    int handshake_done;
} ReplicationContext;

typedef struct {
    int count;
    int size;
    struct pollfd *poll_fds;
    ClientContext *client_contexts;
} Connections;


typedef struct {
    int port;
    int handskahe_done;
    int conn_fd;
} ReplicaConfig;

void* connection_handler(void* arg);
void *master_connection_handler(void *arg);
void send_response_array(int client_fd, char** items, int size);
#endif
