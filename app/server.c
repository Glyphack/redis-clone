#include "server.h"
#include "arena.h"
#include "assert.h"
#include "common.h"
#include "hashmap.h"
#include "rdb.h"
#include "str.h"
#include "types.h"
#include "vec.h"

#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

long long get_current_time() {
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
        return (long long) (ts.tv_sec * 1000 + ts.tv_nsec / 1000000);
    } else {
        return 0;
    }
}

void print_config(Config *config) {
    fprintf(stderr, "config:\n");
    fprintf(stderr, "  dir `%s`\n", config->dir);
    fprintf(stderr, "  dbfilename `%s`\n", config->dbfilename);
    fprintf(stderr, "  port `%d`\n", config->port);
    if (config->master_info != NULL) {
        char ip_str[INET_ADDRSTRLEN];
        if (inet_ntop(AF_INET, &(config->master_info->sin_addr), ip_str, INET_ADDRSTRLEN) == NULL) {
            perror("inet_ntop");
        }
        fprintf(stderr, "  master host `%s`\n", ip_str);
        fprintf(stderr, "  master port `%d`\n", config->master_info->sin_port);
    }
}

void *getConfig(Config *config, char *name) {
    if (strcmp(name, "dir") == 0) {
        return config->dir;
    }
    if (strcmp(name, "dbfilename") == 0) {
        return config->dbfilename;
    }

    return NULL;
}

int handshake(Config *config) {
    int   master_fd = 0;
    char *ping[1]   = {"PING"};

    // Create socket
    if ((master_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        printf("\n Socket creation error \n");
        return -1;
    }

    // Connect to the server
    if (connect(master_fd, (struct sockaddr *) config->master_info, sizeof(struct sockaddr_in)) <
        0) {
        printf("Connection Failed");
        return -1;
    }

    // Initial handshake send a ping
    send_response_array(master_fd, ping, 1);

    char    tmp_str[100];
    ssize_t nbytes = recv(master_fd, tmp_str, sizeof tmp_str, 0);
    if (nbytes <= 0)
        exit(1);
    assert(strncmp(tmp_str, pongMsg, nbytes) == 0);

    // Next send the REPLCONF
    char port[6];
    sprintf(port, "%d", config->port);
    char *repl_conf1[3] = {
        "REPLCONF",
        "listening-port",
        port,
    };
    send_response_array(master_fd, repl_conf1, 3);

    nbytes = recv(master_fd, tmp_str, sizeof tmp_str, 0);
    if (nbytes <= 0)
        exit(1);
    assert(strncmp(tmp_str, okMsg, nbytes) == 0);

    char *repl_conf2[3] = {
        "REPLCONF",
        "capa",
        "psync2",
    };
    send_response_array(master_fd, repl_conf2, 3);

    nbytes = recv(master_fd, tmp_str, sizeof tmp_str, 0);
    if (nbytes <= 0)
        exit(1);
    assert(strncmp(tmp_str, okMsg, nbytes) == 0);

    // Next send the PYSNC
    char *psync[3] = {
        "PSYNC",
        "?",
        "-1",
    };
    send_response_array(master_fd, psync, 3);

    nbytes = recv(master_fd, tmp_str, sizeof tmp_str, 0);
    if (nbytes <= 0)
        exit(1);
    printf("%s", tmp_str);
    // TODO: strtok unsafe do not use it
    char *parts = strtok(tmp_str, " ");
    assert(strcmp(parts, "+FULLRESYNC") == 0);
    parts = strtok(NULL, " ");
    DEBUG_PRINT(parts, s);
    parts             = strtok(NULL, " ");
    int   repl_offset = 0;
    char *curr_pos    = parts;
    while (*parts != '\0') {
        parts++;
        if (*parts == '\r') {
            parts++;
            if (*parts == '\n') {
                strncpy(tmp_str, curr_pos, parts - curr_pos);
                repl_offset = atoi(tmp_str);
            }
        }
    }
    DEBUG_PRINT(repl_offset, d);

    return master_fd;
}

int main(int argc, char *argv[]) {
    // Disable output buffering for testing
    setbuf(stdout, NULL);
    setbuf(stderr, NULL);

    Arena arena = newarena(100 * 1024 * 1024);

    Config *config      = new (&arena, Config);
    config->dbfilename  = NULL;
    config->dir         = NULL;
    config->port        = 6379;
    config->master_info = NULL;

    int print_rdb_and_exit = 0;

    if (argc > 1) {
        for (int i = 0; i < argc; i++) {
            char *flag_name = argv[i];
            if (strcmp(flag_name, "--dir") == 0) {
                char *flag_val = argv[++i];
                DIR  *dp       = opendir(flag_val);
                if (dp == NULL) {
                    perror("Could not open directory passed to -dir");
                }
                closedir(dp);
                config->dir = new (&arena, char, strlength(flag_val));
                strcpy(config->dir, flag_val);
            }
            if (strcmp(flag_name, "--dbfilename") == 0) {
                char *flag_val     = argv[++i];
                config->dbfilename = new (&arena, char, strlength(flag_val));
                strcpy(config->dbfilename, flag_val);
            }
            if (strcmp(flag_name, "--test-rdb") == 0) {
                print_rdb_and_exit = 1;
            }
            if (strcmp(flag_name, "--port") == 0) {
                i++;
                config->port = atoi(argv[i]);
            }
            if (strcmp(flag_name, "--replicaof") == 0) {
                i++;
                struct sockaddr_in *serv_addr = new (&arena, struct sockaddr_in);
                char               *hostinfo  = argv[i];
                char               *host      = strtok(hostinfo, " ");
                if (strcmp(host, "localhost") == 0) {
                    host = "127.0.0.1";
                }
                serv_addr->sin_family = AF_INET;
                int port              = atoi(strtok(NULL, " "));
                serv_addr->sin_port   = htons(port);
                if (inet_pton(AF_INET, host, &serv_addr->sin_addr) <= 0) {
                    printf("\nInvalid address/ Address not supported \n");
                    return -1;
                }
                config->master_info = serv_addr;
            }
        }
    }

    config->master_replid      = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
    config->master_repl_offset = 0;

    print_config(config);

    HashMap *hashmap = hashmap_init();

    if (config->dir && config->dbfilename) {
        char full_path[MAX_PATH];
        snprintf(full_path, sizeof(full_path), "%s%s%s", config->dir, "/", config->dbfilename);
        RdbContent *rdb = parse_rdb(&arena, full_path);
        if (rdb != NULL) {
            RdbDatabase *db = (RdbDatabase *) rdb->databases->items[0];
            hashmap         = db->data;
        }
        if (print_rdb_and_exit) {
            print_rdb(rdb);
            exit(0);
        }
    }

    int                server_fd;
    socklen_t          client_addr_len;
    struct sockaddr_in client_addr;

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == -1) {
        printf("Socket creation failed: %s...\n", strerror(errno));
        return 1;
    }

    // Since the tester restarts your program quite often, setting
    // SO_REUSEADDR ensures that we don't run into 'Address already in use'
    // errors
    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        fprintf(stderr, "SO_REUSEADDR failed: %s \n", strerror(errno));
        return 1;
    }

    struct sockaddr_in serv_addr = {
        .sin_family = AF_INET,
        .sin_port   = htons(config->port),
        .sin_addr   = {htonl(INADDR_ANY)},
    };

    if (bind(server_fd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) != 0) {
        fprintf(stderr, "Bind failed: %s \n", strerror(errno));
        return 1;
    }

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0) {
        fprintf(stderr, "Listen failed: %s \n", strerror(errno));
        return 1;
    }

    if (config->master_info != NULL) {
        int master_fd = handshake(config);
        if (master_fd == -1) {
            printf("handshake failed\n");
            exit(1);
        }

        pthread_t replication_thread_id;
        Arena    *thread_allocator       = new (&arena, Arena);
        *thread_allocator                = newarena(10 * 1024 * 1024);
        Context *context                 = malloc(sizeof(Context));
        context->conn_fd                 = master_fd;
        context->hashmap                 = hashmap;
        context->config                  = config;
        context->thread_allocator        = thread_allocator;
        context->main_arena              = &arena;
        context->is_connection_to_master = 1;
        if (pthread_create(&replication_thread_id, NULL, connection_handler, context) < 0) {
            perror("Could not create thread");
            return 1;
        }
        printf("handshake with master succeeded\n");
        printf("connection handler thread created for replication %lu\n",
               (unsigned long) replication_thread_id);
    }

    printf("Waiting for a client to connect...\n");
    client_addr_len = sizeof(client_addr);
    pthread_t thread_id;

    vector *replicas = initialize_vec();

    int client_fd = accept(server_fd, (struct sockaddr *) &client_addr, &client_addr_len);
    while (client_fd) {
        Arena *thread_allocator          = new (&arena, Arena);
        *thread_allocator                = newarena(10 * 1024 * 1024);
        Context *context                 = malloc(sizeof(Context));
        context->conn_fd                 = client_fd;
        context->hashmap                 = hashmap;
        context->config                  = config;
        context->replicas                = replicas;
        context->thread_allocator        = thread_allocator;
        context->main_arena              = &arena;
        context->is_connection_to_master = 0;
        if (pthread_create(&thread_id, NULL, connection_handler, context) < 0) {
            perror("Could not create thread");
            return 1;
        }
        printf("connection handler thread created %lu\n", (unsigned long) thread_id);
        client_fd = accept(server_fd, (struct sockaddr *) &client_addr, &client_addr_len);
    }
    printf("Client connected\n");

    close(server_fd);

    return 0;
}

void *send_response_bulk_string(int client_fd, char *msg) {
    size_t msg_len      = strlen(msg);
    size_t response_len = msg_len + 32; // Extra space for $, length, \r\n
    char  *response     = malloc(response_len);
    if (!response) {
        fprintf(stderr, "Failed to allocate response buffer\n");
        return NULL;
    }

    snprintf(response, response_len, "$%zu\r\n%s\r\n", msg_len, msg);
    printf("responding with `%s`", response);
    int sent = send(client_fd, response, strlen(response), 0);
    if (sent < 0) {
        fprintf(stderr, "Could not send response: %s\n", strerror(errno));
    } else {
        printf("bytes sent %d\n", sent);
    }
    free(response);
    return NULL;
}

void *respond_null(int client_fd) {
    char response[6];
    response[5] = '\0';
    snprintf(response, sizeof(response), "$%d\r\n", -1);
    printf("responding with `%s`", response);
    int sent = send(client_fd, response, 5, 0);
    if (sent < 0) {
        fprintf(stderr, "Could not send response: %s\n", strerror(errno));
    } else {
        printf("bytes sent %d\n", sent);
    }
    return NULL;
}

int send_response(int client_fd, const char *response) {
    int sent = send(client_fd, response, strlen(response), 0);
    if (sent < 0) {
        fprintf(stderr, "Could not send response: %s\n", strerror(errno));
    } else {
        printf("bytes sent %d\n", sent);
    }
    return sent;
}

void send_response_array(int client_fd, char **items, int size) {
    char response[RESPONSE_ITEM_MAX_SIZE * size + size % 10 + 5];
    sprintf(response, "*%d\r\n", size);
    for (int i = 0; i < size; i++) {
        char formatted[RESPONSE_ITEM_MAX_SIZE];
        snprintf(formatted, RESPONSE_ITEM_MAX_SIZE, "$%d\r\n%s\r\n", (int) strlen(items[i]),
                 items[i]);
        strncat(response, formatted, RESPONSE_ITEM_MAX_SIZE);
    }
    send_response(client_fd, response);
}

char getNextChar(RequestParserBuffer *buffer) {
    if (buffer->cursor < buffer->length) {
        char c = buffer->buffer[buffer->cursor];
        buffer->cursor++;
        return c;
    }
    int bytes_received;

    bytes_received = recv(buffer->client_fd, buffer->buffer, buffer->capacity, 0);
    if (bytes_received <= 0) {
        // TODO another way
        return '^';
    }
    buffer->length = bytes_received;
    buffer->cursor = 0;
    return getNextChar(buffer);
}

BulkString parse_bulk_string(Arena *arena, RequestParserBuffer *buffer) {
    char curr = 0, prev = 0;
    int  len = 0;
    while (curr != '^') {
        curr = getNextChar(buffer);
        // end of the length
        if (curr == '\n' && prev == '\r') {
            break;
        }
        // curr is a digit
        len = len * 10;
        len += curr - '0';
        prev = curr;
    }

    s8 str = (s8) {.len = len, .data = new (arena, u8, len)};

    // TODO memcpy
    for (int i = 0; i < len; i++) {
        str.data[i] = getNextChar(buffer);
    }
    assert(getNextChar(buffer) == '\r');
    assert(getNextChar(buffer) == '\n');

    return (BulkString) {.str = str};
}

RespArray parse_resp_array(Arena *arena, RequestParserBuffer *buffer) {
    RespArray array;
    char      curr = 0, prev = 0;
    int       len = 0;
    while (curr != '^') {
        curr = getNextChar(buffer);
        // end of the length
        if (curr == '\n' && prev == '\r') {
            break;
        }
        // curr is a digit
        len = len * 10;
        len += curr - '0';
        prev = curr;
    }
    array.elts  = initialize_vec();
    array.count = len;
    for (int i = 0; i < len; i++) {
        Request *element = new (arena, Request, 1);
        *element         = parse_request(arena, buffer);
        push_vec(array.elts, element);
    }
    return array;
}

// TODO: split to two functions.
// 1. parse_resp_array that takes in a char* and parses it as resp array. It returns the RespArray
// strcuct.
// 2. read_resp_array_from_socket: that takes a client_fd and reads chunk by chunk and uses the
// parse_resp_array to parse it. If the response is not completed it reads more and then parses it
// 3. Use scratch arena for request parsing
// again until it has full response.
Request parse_request(Arena *arena, RequestParserBuffer *buffer) {
    Request request;
    int     state = 0;

    // Skip the initial part until you see something familiar
    while (state == 0) {
        char c = getNextChar(buffer);
        if (c == '*')
            parse_resp_array(arena, buffer);
        if (c == '$')
            parse_bulk_string(arena, buffer);
        if (c == '^') {
            request.error = 1;
            return request;
        }
    }

    return request;
}

void handle_echo(Context *ctx, Arena *scratch, RespArray *request, int *i) {
    printf("responding to echo\n");
    (*i)++;
    if (*i < request->count) {
        send_response_bulk_string(ctx->conn_fd, request->elts[*i]);
    }
}

void handle_set(Context *ctx, Arena *scratch, RespArray *request, int *i) {
    printf("responding to set\n");
    char     *key = request->elts[++(*i)];
    char     *val = request->elts[++(*i)];
    long long ttl = -1;
    if (request->count > *i + 1) {
        if (strcmp(request->elts[++(*i)], "px") != 0) {
            printf("unknown command arguments");
            return;
        }
        long long current_time = get_current_time();
        printf("current time: %lld\n", current_time);
        ttl = current_time + atoi(request->elts[++(*i)]);
    }
    DEBUG_PRINT(ttl, lld);
    HashMapNode *hNode = hashmap_node_init();
    strcpy(hNode->key, key);
    strcpy(hNode->val, val);
    hNode->ttl = ttl;
    // TODO: add this logic to not respond to all write commands
    hashmap_insert(ctx->hashmap, hNode);
    if (ctx->is_connection_to_master) {
        return;
    }
    send_response_bulk_string(ctx->conn_fd, "OK");
}

void handle_get(Context *ctx, Arena *scratch, RespArray *request, int *i) {
    printf("responding to get\n");
    char        *key  = request->elts[++(*i)];
    HashMapNode *node = hashmap_get(ctx->hashmap, key);
    if (node == NULL) {
        respond_null(ctx->conn_fd);
        return;
    }
    long long current_time = get_current_time();
    printf("current time: %lld\n", current_time);
    if (node->ttl < current_time && node->ttl != -1) {
        printf("item expired ttl: %lld \n", node->ttl);
        respond_null(ctx->conn_fd);
        return;
    }
    send_response_bulk_string(ctx->conn_fd, node->val);
}

void handle_keys(Context *ctx, Arena *scratch, RespArray *request, int *i) {
    char *pattern = request->elts[++(*i)];
    if (strncmp(pattern, "*", 1) != 0) {
        printf("unrecognized pattern");
        exit(1);
    }
    printf("responding to keys\n");
    char **keys = hashmap_keys(ctx->hashmap);
    if (keys == NULL) {
        respond_null(ctx->conn_fd);
        return;
    }
    send_response_array(ctx->conn_fd, keys, ctx->hashmap->size);
}

void handle_ping(Context *ctx, Arena *scratch, RespArray *request, int *i) {
    printf("responding to ping\n");
    send_response(ctx->conn_fd, pongMsg);
}

void handle_config(Context *ctx, Arena *scratch, RespArray *request, int *i) {
    if (strcmp(request->elts[++(*i)], "GET") != 0) {
        printf("Unknown config argument\n");
        return;
    }
    char *arg        = request->elts[++(*i)];
    char *config_val = getConfig(ctx->config, arg);
    if (config_val == NULL) {
        fprintf(stderr, "Unknown Config %s\n", arg);
        return;
    }
    char *resps[2] = {arg, config_val};
    send_response_array(ctx->conn_fd, resps, 2);
}

void handle_info(Context *ctx, Arena *scratch, RespArray *request, int *i) {
    fprintf(stderr, "responding to INFO\n");
    (*i)++;
    if (strcmp(request->elts[*i], "replication")) {
        // no info other than replication supported
        UNREACHABLE();
    }
    int total_size = 13; // "role:master\n" or "role:slave\n" (12 chars + \n)
    total_size += strlen("master_replid:") + 20 + 1;      // replid info + \n
    total_size += strlen("master_repl_offset:") + 20 + 1; // offset info + \n
    total_size += 1;                                      // null terminator

    char *response = new (scratch, byte, total_size);
    int   offset   = 0;

    offset += sprintf(response + offset, "role:%s\n",
                      ctx->config->master_info != NULL ? "slave" : "master");
    offset += sprintf(response + offset, "master_replid:%s\n", ctx->config->master_replid);
    offset +=
        sprintf(response + offset, "master_repl_offset:%d\n", ctx->config->master_repl_offset);

    send_response_bulk_string(ctx->conn_fd, response);
}

typedef struct {
    int port;
    int handskahe_done;
    int conn_fd;
} ReplicaConfig;

void handle_replconf(Context *ctx, Arena *scratch, RespArray *request, int *i,
                     ReplicaConfig *replica) {
    (*i)++;
    if (strcmp(request->elts[*i], "listening-port") == 0) {
        (*i)++;
        int port      = atoi(request->elts[*i]);
        replica->port = port;
    } else if (strcmp(request->elts[*i], "capa") == 0) {
        (*i)++;
        if (strcmp(request->elts[*i], "psync2") != 0) {
            printf("%s is not supported", request->elts[*i]);
            UNREACHABLE();
        }
    }
    send_response(ctx->conn_fd, okMsg);
}

void handle_psync(Context *ctx, Arena *scratch, RespArray *request, int *i) {
    (*i)++;
    if (strcmp(request->elts[*i], "?") == 0) {
        (*i)++;
        if (strcmp(request->elts[*i], "-1") == 0) {
            char response[100];
            sprintf(response, "+FULLRESYNC %s 0\r\n", ctx->config->master_replid);
            send_response(ctx->conn_fd, response);
        }
    }
}

void *connection_handler(void *arg) {
    Context *ctx       = (Context *) arg;
    int      client_fd = ctx->conn_fd;

    int keep_alive = 1;

    // If a replica talks in this connection the information will be recorded.
    ReplicaConfig      *replica  = (ReplicaConfig *) malloc(sizeof(ReplicaConfig));
    int                 req_size = 64 * 1024; // 64KB chunks
    char               *buf      = new (ctx->thread_allocator, byte, req_size);
    RequestParserBuffer buffer   = (RequestParserBuffer) {
          .buffer    = buf,
          .cursor    = 0,
          .length    = 0,
          .capacity  = req_size,
          .client_fd = client_fd,
    };

    while (keep_alive) {
        Request request = parse_request(ctx->thread_allocator, &buffer);
        if (request.error) {
            keep_alive = 0;
        }

        DEBUG_LOG("parsed request");
        DEBUG_PRINT(request->count, d);
        DEBUG_PRINT(request->parts[0], s);

        for (int arg_index = 0; arg_index < request->count; arg_index++) {
            if (strncmp(request->parts[arg_index], "ECHO", 4) == 0) {
                handle_echo(ctx, ctx->thread_allocator, request, &arg_index);
            } else if (strncmp(request->parts[arg_index], "SET", 3) == 0) {
                handle_set(ctx, ctx->thread_allocator, request, &arg_index);
                // TODO: extract this to a function and perform on all write operations
                // Do not propagate from replicas
                if (ctx->replicas != NULL) {
                    for (int i = 0; i < ctx->replicas->total; i++) {
                        ReplicaConfig *replica_to_send = (ReplicaConfig *) ctx->replicas->items[i];
                        if (replica_to_send->handskahe_done == 1) {
                            printf("propagating command to replica %d\n", replica_to_send->port);
                            send_response(replica_to_send->conn_fd, request->raw);
                        }
                    }
                }

            } else if (strncmp(request->parts[arg_index], "GET", 3) == 0) {
                handle_get(ctx, ctx->thread_allocator, request, &arg_index);
            } else if (strncmp(request->parts[arg_index], "KEYS", 4) == 0) {
                handle_keys(ctx, ctx->thread_allocator, request, &arg_index);
            } else if (strncmp(request->parts[arg_index], "PING", 4) == 0) {
                handle_ping(ctx, ctx->thread_allocator, request, &arg_index);
            } else if (strcmp(request->parts[arg_index], "CONFIG") == 0) {
                handle_config(ctx, ctx->thread_allocator, request, &arg_index);
            } else if (strcmp(request->parts[arg_index], "INFO") == 0) {
                handle_info(ctx, ctx->thread_allocator, request, &arg_index);
            } else if (strcmp(request->parts[arg_index], "REPLCONF") == 0) {
                handle_replconf(ctx, ctx->thread_allocator, request, &arg_index, replica);
            } else if (strcmp(request->parts[arg_index], "PSYNC") == 0) {
                handle_psync(ctx, ctx->thread_allocator, request, &arg_index);
                printf("handshake with replica at %d done.\n", replica->port);
                replica->handskahe_done = 1;
                replica->conn_fd        = ctx->conn_fd;
                push_vec(ctx->replicas, replica);
                // transfer rdb
                char *rdbContent = new (ctx->thread_allocator, byte, 1024);
                char *hexString = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469"
                                  "732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0"
                                  "c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
                size_t len      = strlen(hexString);
                for (size_t i = 0; i < len; i += 2) {
                    sscanf(hexString + i, "%2hhx", &rdbContent[i / 2]);
                }
                char *result = new (ctx->thread_allocator, byte, 1024);
                snprintf(result, 1024, "$%lu\r\n%s", strlen(rdbContent), rdbContent);
                send_response(ctx->conn_fd, result);
                recv(ctx->conn_fd, 0, 0, 0);
            } else {
                printf("Unknown command %s\n", request->parts[arg_index]);
                keep_alive = 0;
            }
        }
    }

    droparena(ctx->thread_allocator);
    free(ctx);
    return NULL;
}
