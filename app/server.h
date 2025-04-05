#ifndef SERVER_H
#define SERVER_H

#include "arena.h"
#include "hashmap.h"
#include "resp.h"
#include "types.h"
#include "vec.h"
#include <netinet/in.h>

#define MAX_CLIENTS 100


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
    // Freed after client disconnects
    // TODO: Create one per client.
    Arena* perm;
    // Freed after req/res cycle
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
    struct pollfd *poll_fds;
    ClientContext *client_contexts;
} Connections;

typedef struct {
    vector* keyvals;
    i64 id_ms;
    i64 id_seq;
    s8 id;
} StreamEntry;

typedef struct {
    i64 id_ms;
    i64 id_seq;
} EntryId;
//
// typedef union {
//     s8 val;
//     EntryId id;
// } StreamEntryFlatItem;

typedef struct {
    // ["key", "val", "1-0", "key", "val2", "0-1", ...]
    // key values cannot have -
    vector* contents;
    // {"100-0": "0", "101-0": "1", "200-0": "2"}
    HashMap* IDIndex;
} StreamEntries;

typedef struct {
    s8 stream_key;
    i64 last_id_ms;
    i64 last_id_seqn;
    StreamEntries stream_entries;
} Stream;

typedef struct {
    //  TODO: map of stream key to Stream
    vector* streams; // vector of streams
} Streams;

void streams_new(Arena *arena, Streams *streams, Stream stream);
Stream* streams_get(Streams*, s8);
void stream_new_entry(Arena*, Stream*, StreamEntry*);
void stream_get_range(Arena *arena, Stream *stream, vector *result, s8 id_begin, s8 id_end);

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
    // Collection of streams in this server
    Streams streams;
} ServerContext;

#endif
