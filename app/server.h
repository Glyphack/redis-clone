#ifndef SERVER_H
#define SERVER_H

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
    int conn_fd;
    HashMap** hashmap;
    Config* config;
    Arena* scratch;
    Arena* perm;
    vector* replicas;
} Context;

typedef struct {
    int master_conn;
    HashMap** hashmap;
    Config* config;
    Arena* scratch;
    Arena* perm;
    int handshake_done;
} ReplicationContext;

typedef struct {
    RequestParserBuffer reader;
} ClientInfo;


typedef struct {
    int port;
    int handskahe_done;
    int conn_fd;
} ReplicaConfig;

void* connection_handler(void* arg);
void *master_connection_handler(void *arg);
void send_response_array(int client_fd, char** items, int size);
#endif
