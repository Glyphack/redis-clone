#include "server.h"
#include "arena.h"
#include "assert.h"
#include "common.h"
#include "hashmap.h"
#include "rdb.h"
#include "resp.h"
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
    // TODO: strtok unsafe do not use it
    char *parts = strtok(tmp_str, " ");
    assert(strcmp(parts, "+FULLRESYNC") == 0);
    parts             = strtok(NULL, " ");
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
        if (pthread_create(&replication_thread_id, NULL, connection_handler, context) != 0) {
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
        if (pthread_create(&thread_id, NULL, connection_handler, context) != 0) {
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

void handle_echo(Context *ctx, s8 arg) {
    DEBUG_LOG("responding to echo");
    send_response_bulk_string(ctx, arg);
}

void handle_get(Context *ctx, s8 key) {
    printf("responding to get\n");
    Arena       *scratch = &(*ctx->thread_allocator);
    HashMapNode *node    = hashmap_get(ctx->hashmap, s8_to_cstr(scratch, key));
    if (node == NULL) {
        respond_null(ctx->conn_fd);
        return;
    }
    if (node->ttl != -1 && node->ttl < get_current_time()) {
        printf("item expired ttl: %lld \n", node->ttl);
        respond_null(ctx->conn_fd);
        return;
    }
    send_response_bulk_string(ctx, s8_from_cstr(scratch, node->val));
}

// TODO if msg is not empty reply back
void handle_ping(Context *ctx, s8 msg) {
    DEBUG_LOG("responding to ping");
    send_response(ctx->conn_fd, pongMsg);
}

void *connection_handler(void *arg) {
    Context *ctx       = (Context *) arg;
    int      client_fd = ctx->conn_fd;

    int keep_alive = 1;

    // If a replica talks in this connection the information will be recorded.
    ReplicaConfig *replica = (ReplicaConfig *) malloc(sizeof(ReplicaConfig));

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
        DEBUG_LOG("parsed request");
        if (request.empty) {
            break;
        }
        if (request.error) {
            printf("Request invalid\n");
            continue;
        }

        // TODO: handle rdb file transfer
        // for now ignore RDB
        if (request.type == BULK_STRING) {
            continue;
        }

        if (request.type != ARRAY) {
            printf("only supporting array requests\n");
            continue;
        }
        RespArray *resp_array = (RespArray *) request.val;
        if (resp_array->count == 0) {
            printf("Request invalid\n");
            keep_alive = 0;
        }
        Request *command_val = resp_array->elts[0];
        assert(command_val->type == BULK_STRING);
        BulkString *command = (BulkString *) command_val->val;
        s8print(command->str);

        if (s8equals(command->str, S("PING")) == true) {
            s8 arg = S("");
            if (resp_array->count == 2) {
                Request *arg_val = resp_array->elts[1];
                assert(arg_val->type == BULK_STRING);
                arg = ((BulkString *) arg_val->val)->str;
            }
            handle_ping(ctx, arg);
        } else if (s8equals(command->str, S("ECHO")) == true) {
            assert(resp_array->count == 2);
            Request *command_val = resp_array->elts[1];
            assert(command_val->type == BULK_STRING);
            BulkString *command = (BulkString *) command_val->val;
            handle_echo(ctx, command->str);
        } else if (s8equals(command->str, S("GET")) == true) {
            assert(resp_array->count == 2);
            Request *command_val = resp_array->elts[1];
            assert(command_val->type == BULK_STRING);
            BulkString *key = (BulkString *) command_val->val;
            handle_get(ctx, key->str);
        } else if (s8equals(command->str, S("SET")) == true) {
            // TODO: extract this to a function and perform on all write operations
            // Do not propagate from replicas
            if (ctx->replicas != NULL) {
                for (int i = 0; i < ctx->replicas->total; i++) {
                    ReplicaConfig *replica_to_send = (ReplicaConfig *) ctx->replicas->items[i];
                    if (replica_to_send->handskahe_done == 1) {
                        printf("propagating command to replica %d\n", replica_to_send->port);
                        // TODO: Do not use buf directly instead the buffer should keep the whole
                        // request to send it to the replica. The buf might be overwritten by the
                        // next request.
                        send_response(replica_to_send->conn_fd, buf);
                    }
                }
            }
            assert(resp_array->count >= 3);
            Request *key_val = resp_array->elts[1];
            Request *val_val = resp_array->elts[2];
            assert(key_val->type == BULK_STRING);
            assert(val_val->type == BULK_STRING);
            BulkString *key = (BulkString *) key_val->val;
            BulkString *val = (BulkString *) val_val->val;

            long long ttl = -1;
            if (resp_array->count > 3) {
                Request *px_val = resp_array->elts[3];
                assert(px_val->type == BULK_STRING);
                BulkString *px = (BulkString *) px_val->val;
                if (!s8equals(px->str, S("px"))) {
                    printf("unknown command arguments");
                    keep_alive = 0;
                    continue;
                }
                Request *ttl_val = resp_array->elts[4];
                assert(ttl_val->type == BULK_STRING);
                BulkString *ttl_str      = (BulkString *) ttl_val->val;
                Arena      *scratch      = &(*ctx->thread_allocator);
                long long   current_time = get_current_time();
                ttl                      = current_time + atoi(s8_to_cstr(scratch, ttl_str->str));
            }

            HashMapNode *hNode = hashmap_node_init();
            strcpy(hNode->key, s8_to_cstr(ctx->thread_allocator, key->str));
            strcpy(hNode->val, s8_to_cstr(ctx->thread_allocator, val->str));
            hNode->ttl = ttl;
            hashmap_insert(ctx->hashmap, hNode);

            if (!ctx->is_connection_to_master) {
                send_response_bulk_string(ctx, S("OK"));
            }
        } else if (s8equals(command->str, S("KEYS")) == true) {
            assert(resp_array->count == 2);
            Request *pattern_val = resp_array->elts[1];
            assert(pattern_val->type == BULK_STRING);
            BulkString *pattern = (BulkString *) pattern_val->val;

            if (!s8equals(pattern->str, S("*"))) {
                printf("unrecognized pattern");
                keep_alive = 0;
                continue;
            }

            char **keys = hashmap_keys(ctx->hashmap);
            if (keys == NULL) {
                respond_null(ctx->conn_fd);
                continue;
            }
            send_response_array(ctx->conn_fd, keys, ctx->hashmap->size);
        } else if (s8equals(command->str, S("CONFIG")) == true) {
            assert(resp_array->count == 3);
            Request *get_val = resp_array->elts[1];
            assert(get_val->type == BULK_STRING);
            BulkString *get = (BulkString *) get_val->val;

            if (!s8equals(get->str, S("GET"))) {
                printf("Unknown config argument\n");
                keep_alive = 0;
                continue;
            }

            Request *arg_val = resp_array->elts[2];
            assert(arg_val->type == BULK_STRING);
            BulkString *arg = (BulkString *) arg_val->val;

            char *config_val = getConfig(ctx->config, s8_to_cstr(ctx->thread_allocator, arg->str));
            if (config_val == NULL) {
                fprintf(stderr, "Unknown Config %s\n", s8_to_cstr(ctx->thread_allocator, arg->str));
                continue;
            }

            char *resps[2] = {s8_to_cstr(ctx->thread_allocator, arg->str), config_val};
            send_response_array(ctx->conn_fd, resps, 2);
        } else if (s8equals(command->str, S("INFO")) == true) {
            assert(resp_array->count == 2);
            Request *section_val = resp_array->elts[1];
            assert(section_val->type == BULK_STRING);
            BulkString *section = (BulkString *) section_val->val;

            if (!s8equals(section->str, S("replication"))) {
                // no info other than replication supported
                UNREACHABLE();
            }

            Arena *scratch    = &(*ctx->thread_allocator);
            int    total_size = 13; // "role:master\n" or "role:slave\n" (12 chars + \n)
            total_size += strlen("master_replid:") + 20 + 1;      // replid info + \n
            total_size += strlen("master_repl_offset:") + 20 + 1; // offset info + \n
            total_size += 1;                                      // null terminator

            char *response = new (scratch, byte, total_size);
            int   offset   = 0;

            offset += sprintf(response + offset, "role:%s\n",
                              ctx->config->master_info != NULL ? "slave" : "master");
            offset += sprintf(response + offset, "master_replid:%s\n", ctx->config->master_replid);
            offset += sprintf(response + offset, "master_repl_offset:%d\n",
                              ctx->config->master_repl_offset);

            send_response_bulk_string(ctx, s8_from_cstr(scratch, response));
        } else if (s8equals(command->str, S("REPLCONF")) == true) {
            assert(resp_array->count == 3);
            Request *arg_val = resp_array->elts[1];
            assert(arg_val->type == BULK_STRING);
            BulkString *arg = (BulkString *) arg_val->val;

            if (s8equals(arg->str, S("listening-port"))) {
                Request *port_val = resp_array->elts[2];
                assert(port_val->type == BULK_STRING);
                BulkString *port_str = (BulkString *) port_val->val;
                replica->port        = atoi(s8_to_cstr(ctx->thread_allocator, port_str->str));
            } else if (s8equals(arg->str, S("capa"))) {
                Request *capa_val = resp_array->elts[2];
                assert(capa_val->type == BULK_STRING);
                BulkString *capa = (BulkString *) capa_val->val;
                if (!s8equals(capa->str, S("psync2"))) {
                    printf("%s is not supported", s8_to_cstr(ctx->thread_allocator, capa->str));
                    UNREACHABLE();
                }
            } else if (s8equals(arg->str, S("GETACK"))) {
                // ["replconf", "getack", "*"]
                assert(ctx->is_connection_to_master);
                DEBUG_LOG("responding to replconf get ack");
                assert(resp_array->count == 3);
                assert(resp_array->elts[2]->type == BULK_STRING);
                s8 arg_2_value = ((BulkString *) resp_array->elts[2]->val)->str;
                assert(s8equals(arg_2_value, S("*")));
                char *items[3] = {"REPLCONF", "ACK", "0"};
                send_response_array(ctx->conn_fd, items, 3);
            }
            send_response(ctx->conn_fd, okMsg);
        } else if (s8equals(command->str, S("PSYNC")) == true) {
            assert(resp_array->count == 3);
            Request *arg1_val = resp_array->elts[1];
            Request *arg2_val = resp_array->elts[2];
            assert(arg1_val->type == BULK_STRING);
            assert(arg2_val->type == BULK_STRING);
            BulkString *arg1 = (BulkString *) arg1_val->val;
            BulkString *arg2 = (BulkString *) arg2_val->val;

            if (s8equals(arg1->str, S("?")) && s8equals(arg2->str, S("-1"))) {
                char response[100];
                sprintf(response, "+FULLRESYNC %s 0\r\n", ctx->config->master_replid);
                send_response(ctx->conn_fd, response);

                printf("handshake with replica at %d done.\n", replica->port);
                replica->handskahe_done = 1;
                replica->conn_fd        = ctx->conn_fd;
                push_vec(ctx->replicas, replica);

                // transfer rdb
                Arena *scratch    = &(*ctx->thread_allocator);
                char  *rdbContent = new (scratch, byte, 1024);
                char *hexString = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469"
                                  "732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0"
                                  "c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
                size_t len      = strlen(hexString);
                for (size_t i = 0; i < len; i += 2) {
                    sscanf(hexString + i, "%2hhx", &rdbContent[i / 2]);
                }
                char *result = new (scratch, byte, 1024);
                snprintf(result, 1024, "$%lu\r\n%s", strlen(rdbContent), rdbContent);
                send_response(ctx->conn_fd, result);
                recv(ctx->conn_fd, 0, 0, 0);
            }
        } else {
            printf("Unknown command\n");
            keep_alive = 0;
        }
    }

    printf("quitting thread_id %lu\n", (unsigned long) pthread_self());
    droparena(ctx->thread_allocator);
    free(ctx);
    return NULL;
}
