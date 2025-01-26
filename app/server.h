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


typedef struct {
    size cursor;
    size len;
    s8  buffer;
} BufferWriter;

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

typedef enum  {
    h_replconf,
    h_psync,
    h_done
} handshake_state ;
typedef struct {
    int port;
    handshake_state handskahe_done;
    int conn_fd;
} ReplicaConfig;

typedef struct {
    int master_fd;
    int repl_offset;
    HashMap** hashmap;
    Config* config;
    Arena* perm;
    int handshake_done;
} ReplicationContext;

typedef struct {
    int conn_fd;
    Arena* perm;
    HashMap** hashmap;
    int want_read;
    int want_write;
    int want_close;
    // bytes read from the client
    BufferReader reader;
    // bytes to be written to the client
    BufferWriter writer;
    // If a replica is talking in this connection
    ReplicaConfig *replica;
    // If this a connection to master for replication
    ReplicationContext *replication_context;
} ClientContext;

typedef struct {
    int count;
    int size;
    struct pollfd *poll_fds;
    ClientContext *client_contexts;
} Connections;

void* connection_handler(void* arg);
void *master_connection_handler(void *arg);
#endif
