#ifndef SERVER_H
#define SERVER_H

#include "arena.h"
#include "hashmap.h"
#include "resp.h"
#include "vec.h"
#include <netinet/in.h>
#include <sys/event.h>

#define RESPONSE_ITEM_MAX_SIZE 1024
#define MAX_CLIENTS 1100


typedef struct {
    size cursor;
    size len;
    s8  buffer;
} BufferWriter;

typedef struct {
    s8 dir;
    s8 dbfilename;
    int port;
    struct sockaddr_in* master_info;
    s8 master_replid;
    int master_repl_offset;
} Config;

typedef enum  {
    h_ping,
    h_replconf,
    h_replconf_2,
    h_psync,
    h_fullresync,
    h_done
} handshake_state ;

typedef struct {
    int conn_id;
    int conn_fd;
    int port;
    struct sockaddr_in* address;
    handshake_state handskahe_done;
} ReplicaConfig;

typedef struct {
    int repl_offset;
    handshake_state handshake_state;
} ReplicationContext;

typedef struct {
    i64 deadline;
    i32 num_waiting;
    i32 synced_count;
    i32 client_conn_id;
    i64 repl_offset;
} WaitState;

typedef struct {
    int conn_fd;
    int conn_id;
    Arena* perm;
    Arena temp;
    HashMap** hashmap;
    int want_read;
    int want_write;
    int want_close;
    // bytes read from the client
    BufferReader reader;
    // bytes to be written to the client
    BufferWriter writer;
    // If a replica is talking in this connection. Only master
    ReplicaConfig *replica;
    int is_connection_to_master;
} ClientContext;

typedef struct {
    i32 count;
    i32 size;
    int kq;  // kqueue file descriptor
    struct kevent *events;  // array of kevent structures
    ClientContext *client_contexts;
} Connections;

typedef struct {
    Connections *connections;
    HashMap** hashmap;
    Config* config;
    Arena* perm;
    vector* replicas;
    // If this a connection to master for replication. Only replica
    ReplicationContext *replication_context;
    // If a wait is running;
    WaitState wait_state;
} ServerContext;

void* connection_handler(void* arg);
void *master_connection_handler(void *arg);
#endif
