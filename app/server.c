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
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

BufferReader buffered_reader(Arena *arena, int conn_fd) {
    int          capacity = 64 * 1024; // 64KB chunks
    byte        *buf      = new (arena, byte, capacity);
    BufferReader buffer   = (BufferReader) {
          .buffer     = buf,
          .cursor     = 0,
          .length     = 0,
          .capacity   = capacity,
          .client_fd  = conn_fd,
          .total_read = 0,
    };
    return buffer;
}

void append_write_buff(BufferWriter *writer, s8 *data) {
    if (writer->len + data->len > writer->buffer.len) {
        printf("Buffer full: writer len %zu, data len %zu, buffer len %zu\n", writer->len,
               data->len, writer->buffer.len);
        abort();
    }
    DEBUG_LOG("sending");
    s8print(*data);
    memcpy(writer->buffer.data + writer->len, data->data, data->len);
    writer->len += data->len;
}

void write_response(ClientContext *c_context, s8 *response) {
    append_write_buff(&c_context->writer, response);
}

int send_writes(int fd, BufferWriter *writer) {
    if (writer->cursor >= writer->len) {
        return 0; // Everything written
    }
    u8  *to_write     = writer->buffer.data + writer->cursor;
    size to_write_len = writer->len - writer->cursor;
    DBG_F("Writing response to %d\n", fd);
    size written = send(fd, to_write, to_write_len, 0);
    if (written < 0) {
        perror("send");
        return -1;
    }
    writer->cursor += written;
    if (writer->cursor == writer->len) {
        writer->cursor = 0;
        writer->len    = 0;
        return 0; // Finished
    }
    return 1; // Not finished
}

int add_client(Arena *arena, ServerContext *sv_context, Connections *connections, int newfd) {
    if (connections->count == connections->size) {
        UNREACHABLE();
    }
    fcntl(newfd, F_SETFL, fcntl(newfd, F_GETFL, 0) | O_NONBLOCK);

    short event = POLLERR;
    int   id    = connections->count;
    if (id == 0) {
        event |= POLLIN;
    }
    connections->poll_fds[id]        = (struct pollfd) {.fd = newfd, .events = event, .revents = 0};
    connections->client_contexts[id] = (ClientContext) {
        .conn_fd    = newfd,
        .perm       = arena,
        .hashmap    = sv_context->hashmap,
        .reader     = buffered_reader(arena, newfd),
        .writer     = (BufferWriter) {.cursor = 0,
                                      .len    = 0,
                                      .buffer = (s8) {.data = new (arena, u8, 1024), .len = 1024}},
        .want_read  = 1,
        .want_write = 0,
        .want_close = 0,
    };
    connections->client_contexts[id].conn_id = id;
    connections->count++;
    return id;
}

void del_connection(Connections *connections, int i) {
    DBG_F("client %d removed\n", i);
    connections->poll_fds[i]        = connections->poll_fds[connections->count - 1];
    connections->client_contexts[i] = connections->client_contexts[connections->count - 1];
    // ClientContext del_con           = connections->client_contexts[i];
    connections->count--;
}

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
    if (strcmp(name, "appendonly") == 0) {
        return "no"; // Explicitly disable AOF persistence
    }
    if (strcmp(name, "save") == 0) {
        return ""; // Empty string means RDB persistence is disabled
    }
    return NULL;
}

void handle_get(ClientContext *ctx, s8 key) {
    printf("responding to get\n");
    HashMapNode node = hashmap_get(*ctx->hashmap, key);
    if (node.key.len == 0) {
        write_response(ctx, &null_resp);
        return;
    }
    if (node.ttl != -1 && node.ttl < get_current_time()) {
        printf("item expired ttl: %lld \n", node.ttl);
        write_response(ctx, &null_resp);
        return;
    }
    s8 response = serde_bulk_str(&(*(ctx->perm)), node.val);
    write_response(ctx, &response);
}

void handle_master_request(ServerContext *sv_context, ClientContext *c_context, Request request) {
    ClientContext *ctx = c_context;

    // TODO: do not use perm arena for short lived req/resp
    Arena  *scratch = c_context->perm;
    Element element = request.element;

    if (element.type == SIMPLE_STRING) {
        SimpleString *res = ((SimpleString *) element.val);
        if (s8equals_nocase(res->str, S("pong")) == true) {
            DEBUG_LOG("handshake first");
            char port[6];
            sprintf(port, "%d", sv_context->config->port);
            char *repl_conf1[3] = {
                "REPLCONF",
                "listening-port",
                port,
            };
            s8 request = serde_array(scratch, repl_conf1, 3);
            write_response(ctx, &request);
            ctx->replication_context->handshake_state = h_replconf;
        } else if (s8equals_nocase(res->str, S("ok")) == true) {
            if (ctx->replication_context->handshake_state == h_replconf) {
                char *repl_conf2[3] = {
                    "REPLCONF",
                    "capa",
                    "psync2",
                };
                s8 request = serde_array(scratch, repl_conf2, 3);
                write_response(ctx, &request);
                ctx->replication_context->handshake_state = h_replconf_2;
            } else if (ctx->replication_context->handshake_state == h_replconf_2) {
                char *psync[3] = {
                    "PSYNC",
                    "?",
                    "-1",
                };
                s8 request = serde_array(scratch, psync, 3);
                write_response(ctx, &request);
                ctx->replication_context->handshake_state = h_psync;
            } else {
                UNREACHABLE();
            }
        } else if (s8startswith(res->str, S("FULLRESYNC")) == true) {
            DEBUG_LOG("got full resync");
            ctx->replication_context->handshake_state = h_done;
        }
        return;
    } else if (element.type == RDB_MSG) {
        DEBUG_LOG("Got empty rdb");
        return; // ignore rdb message
    }

    DBG_F("replication parsed request %d\n", element.type);
    DBG(c_context->reader.total_read, lu);

    if (element.type == BULK_STRING || element.type == SIMPLE_ERROR) {
        return;
    } else if (element.type == SIMPLE_STRING) {
        ctx->replication_context->handshake_state = 1;
        return;
    }

    assert(element.type == ARRAY);
    RespArray *resp_array = (RespArray *) element.val;
    if (resp_array->count == 0) {
        printf("Request invalid\n");
        return;
    }
    Element *command_val = resp_array->elts[0];
    assert(command_val->type == BULK_STRING);
    BulkString *command = (BulkString *) command_val->val;

    if (s8equals_nocase(command->str, S("SET")) == true) {
        assert(resp_array->count >= 3);
        Element *key_val = resp_array->elts[1];
        Element *val_val = resp_array->elts[2];
        assert(key_val->type == BULK_STRING);
        assert(val_val->type == BULK_STRING);
        BulkString *key = (BulkString *) key_val->val;
        BulkString *val = (BulkString *) val_val->val;

        long long ttl = -1;
        if (resp_array->count > 3) {
            Element *px_val = resp_array->elts[3];
            assert(px_val->type == BULK_STRING);
            BulkString *px = (BulkString *) px_val->val;
            if (!s8equals_nocase(px->str, S("px"))) {
                printf("unknown command arguments");
                return;
            }
            Element *ttl_val = resp_array->elts[4];
            assert(ttl_val->type == BULK_STRING);
            BulkString *ttl_str      = (BulkString *) ttl_val->val;
            long long   current_time = get_current_time();
            ttl                      = current_time + atoi(s8_to_cstr(scratch, ttl_str->str));
        }

        HashMapNode hNode = {.key = s8malloc(key->str), .val = s8malloc(val->str), .ttl = ttl};
        hashmap_upsert(ctx->hashmap, ctx->perm, &hNode);
    } else if (s8equals_nocase(command->str, S("REPLCONF")) == true) {
        assert(resp_array->count == 3);
        Element *arg_val = resp_array->elts[1];
        assert(arg_val->type == BULK_STRING);
        BulkString *arg = (BulkString *) arg_val->val;

        if (s8equals_nocase(arg->str, S("GETACK"))) {
            // ["replconf", "getack", "*"]
            DEBUG_LOG("responding to replconf get ack");
            DBG(ctx->replication_context->repl_offset, d);
            assert(resp_array->count == 3);
            assert(resp_array->elts[2]->type == BULK_STRING);
            s8 arg_2_value = ((BulkString *) resp_array->elts[2]->val)->str;
            assert(s8equals_nocase(arg_2_value, S("*")));
            char *offset_str = new (scratch, char, 10);
            snprintf(offset_str, 10, "%d", ctx->replication_context->repl_offset);
            char *items[3] = {"REPLCONF", "ACK", offset_str};
            s8    resp     = serde_array(scratch, items, 3);
            write_response(c_context, &resp);
        }
    } else if (s8equals_nocase(command->str, S("INFO")) == true) {
        assert(resp_array->count == 2);
        Element *section_val = resp_array->elts[1];
        assert(section_val->type == BULK_STRING);
        BulkString *section = (BulkString *) section_val->val;

        if (!s8equals_nocase(section->str, S("replication"))) {
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
                          sv_context->config->master_info != NULL ? "slave" : "master");
        offset +=
            sprintf(response + offset, "master_replid:%s\n", sv_context->config->master_replid);
        offset += sprintf(response + offset, "master_repl_offset:%d\n",
                          sv_context->config->master_repl_offset);

        serde_bulk_str(ctx->perm, s8_from_cstr(scratch, response));
    } else {
        printf("Unknown request type\n");
    }

    ctx->replication_context->repl_offset += request.bytes.len;
    DBG(ctx->replication_context->repl_offset, d);
}

void handle_request(ServerContext *sv_context, ClientContext *c_context, Request request) {
    Element element = request.element;
    // TODO: do not use perm arena for short lived req/resp
    Arena *scratch = c_context->perm;
    if (element.type != ARRAY) {
        printf("Request invalid\n");
        return;
    }
    RespArray *resp_array = (RespArray *) element.val;
    if (resp_array->count == 0) {
        printf("Request invalid\n");
        return;
    }
    Element *command_val = resp_array->elts[0];
    assert(command_val->type == BULK_STRING);
    BulkString    *command = (BulkString *) command_val->val;
    ClientContext *ctx     = c_context;

    if (s8equals_nocase(command->str, S("PING")) == true) {
        DEBUG_LOG("responding to ping");
        s8 arg = S("");
        if (resp_array->count == 2) {
            Element *arg_val = resp_array->elts[1];
            assert(arg_val->type == BULK_STRING);
            arg = ((BulkString *) arg_val->val)->str;
        }
        if (arg.len == 0) {
            write_response(c_context, &pong_resp);
        } else {
            // Arg passed to echo
            UNREACHABLE();
        }
    } else if (s8equals_nocase(command->str, S("ECHO")) == true) {
        DEBUG_LOG("responding to echo");
        assert(resp_array->count == 2);
        Element *command_val = resp_array->elts[1];
        assert(command_val->type == BULK_STRING);
        BulkString *command  = (BulkString *) command_val->val;
        s8          response = serde_bulk_str(scratch, command->str);
        write_response(c_context, &response);
    } else if (s8equals_nocase(command->str, S("GET")) == true) {
        assert(resp_array->count == 2);
        Element *command_val = resp_array->elts[1];
        assert(command_val->type == BULK_STRING);
        BulkString *key = (BulkString *) command_val->val;
        handle_get(ctx, key->str);
    } else if (s8equals_nocase(command->str, S("SET")) == true) {
        // TODO: extract this to a function and perform on all write operations
        if (sv_context->replicas->total != 0) {
            for (int i = 0; i < sv_context->replicas->total; i++) {
                ReplicaConfig *replica_to_send = (ReplicaConfig *) sv_context->replicas->items[i];
                printf("propagating command to replica %d\n", replica_to_send->port);
                ClientContext *replica_context =
                    &sv_context->connections->client_contexts[replica_to_send->conn_id];
                write_response(replica_context, &request.bytes);
                replica_context->want_write = 1;
            }
            DEBUG_LOG("propagated");
        }
        assert(resp_array->count >= 3);
        Element *key_val = resp_array->elts[1];
        Element *val_val = resp_array->elts[2];
        assert(key_val->type == BULK_STRING);
        assert(val_val->type == BULK_STRING);
        BulkString *key = (BulkString *) key_val->val;
        BulkString *val = (BulkString *) val_val->val;

        long long ttl = -1;
        if (resp_array->count > 3) {
            Element *px_val = resp_array->elts[3];
            assert(px_val->type == BULK_STRING);
            BulkString *px = (BulkString *) px_val->val;
            if (!s8equals_nocase(px->str, S("px"))) {
                printf("unknown command arguments");
                return;
            }
            Element *ttl_val = resp_array->elts[4];
            assert(ttl_val->type == BULK_STRING);
            BulkString *ttl_str      = (BulkString *) ttl_val->val;
            long long   current_time = get_current_time();
            ttl                      = current_time + atoi(s8_to_cstr(scratch, ttl_str->str));
        }
        DEBUG_LOG("ttl set");

        HashMapNode hNode = {.key = s8malloc(key->str), .val = s8malloc(val->str), .ttl = ttl};
        DEBUG_LOG("upserted message");
        hashmap_upsert(ctx->hashmap, ctx->perm, &hNode);
        write_response(c_context, &ok_resp);
    } else if (s8equals_nocase(command->str, S("keys")) == true) {
        assert(resp_array->count == 2);
        Element *pattern_val = resp_array->elts[1];
        assert(pattern_val->type == BULK_STRING);
        BulkString *pattern = (BulkString *) pattern_val->val;

        if (!s8equals_nocase(pattern->str, S("*"))) {
            printf("unrecognized pattern");
            return;
        }
        vector *keys = initialize_vec();
        hashmap_keys(*ctx->hashmap, keys);
        if (keys->total == 0) {
            write_response(c_context, &null_resp);
            return;
        }
        char *key_chars[keys->total];
        for (int i = 0; i < keys->total; i++) {
            key_chars[i] = keys->items[i];
        }
        s8 response = serde_array(ctx->perm, key_chars, keys->total);
        write_response(c_context, &response);
    } else if (s8equals_nocase(command->str, S("CONFIG")) == true) {
        assert(resp_array->count == 3);
        Element *get_val = resp_array->elts[1];
        assert(get_val->type == BULK_STRING);
        BulkString *get = (BulkString *) get_val->val;

        if (!s8equals_nocase(get->str, S("GET"))) {
            printf("Unknown config argument\n");
            return;
        }

        Element *arg_val = resp_array->elts[2];
        assert(arg_val->type == BULK_STRING);
        BulkString *arg = (BulkString *) arg_val->val;

        char *config_val = getConfig(sv_context->config, s8_to_cstr(scratch, arg->str));
        if (config_val == NULL) {
            fprintf(stderr, "Unknown Config %s\n", s8_to_cstr(scratch, arg->str));
            return;
        }

        char *resps[2] = {s8_to_cstr(scratch, arg->str), config_val};
        s8    response = serde_array(scratch, resps, 2);
        write_response(c_context, &response);
    } else if (s8equals_nocase(command->str, S("INFO")) == true) {
        assert(resp_array->count == 2);
        Element *section_val = resp_array->elts[1];
        assert(section_val->type == BULK_STRING);
        BulkString *section = (BulkString *) section_val->val;

        if (!s8equals_nocase(section->str, S("replication"))) {
            // no info other than replication supported
            UNREACHABLE();
        }

        int total_size = 13; // "role:master\n" or "role:slave\n" (12 chars + \n)
        total_size += strlen("master_replid:") + 20 + 1;      // replid info + \n
        total_size += strlen("master_repl_offset:") + 20 + 1; // offset info + \n
        total_size += 1;                                      // null terminator

        char *content = new (scratch, byte, total_size);
        int   offset  = 0;

        offset += sprintf(content + offset, "role:%s\n",
                          sv_context->config->master_info != NULL ? "slave" : "master");
        offset +=
            sprintf(content + offset, "master_replid:%s\n", sv_context->config->master_replid);
        offset += sprintf(content + offset, "master_repl_offset:%d\n",
                          sv_context->config->master_repl_offset);

        s8 response = serde_bulk_str(&(*c_context->perm), s8_from_cstr(scratch, content));
        write_response(c_context, &response);
    } else if (s8equals_nocase(command->str, S("REPLCONF")) == true) {
        assert(resp_array->count == 3);
        Element *arg_val = resp_array->elts[1];
        assert(arg_val->type == BULK_STRING);
        BulkString *arg = (BulkString *) arg_val->val;
        if (c_context->replica == NULL) {
            c_context->replica = new (c_context->perm, ReplicaConfig);
        }

        if (s8equals_nocase(arg->str, S("listening-port"))) {
            Element *port_val = resp_array->elts[2];
            assert(port_val->type == BULK_STRING);
            BulkString *port_str     = (BulkString *) port_val->val;
            c_context->replica->port = atoi(s8_to_cstr(scratch, port_str->str));
            write_response(c_context, &ok_resp);
        } else if (s8equals_nocase(arg->str, S("capa"))) {
            Element *capa_val = resp_array->elts[2];
            assert(capa_val->type == BULK_STRING);
            // port must be set before setting the capabilities
            assert(c_context->replica->port != 0);
            BulkString *capa = (BulkString *) capa_val->val;
            if (!s8equals_nocase(capa->str, S("psync2"))) {
                printf("%s is not supported", s8_to_cstr(scratch, capa->str));
                UNREACHABLE();
            }
            write_response(c_context, &ok_resp);
            c_context->replica->handskahe_done = h_psync;
        } else {
            UNREACHABLE();
        }
    } else if (s8equals_nocase(command->str, S("PSYNC")) == true) {
        assert(resp_array->count == 3);
        Element *arg1_val = resp_array->elts[1];
        Element *arg2_val = resp_array->elts[2];
        assert(arg1_val->type == BULK_STRING);
        assert(arg2_val->type == BULK_STRING);
        BulkString *arg1 = (BulkString *) arg1_val->val;
        BulkString *arg2 = (BulkString *) arg2_val->val;

        if (s8equals_nocase(arg1->str, S("?")) && s8equals_nocase(arg2->str, S("-1"))) {
            char response[100];
            sprintf(response, "+FULLRESYNC %s 0\r\n", sv_context->config->master_replid);
            s8 resp_s8 = (s8) {.len = strlen(response), .data = (u8 *) response};
            write_response(c_context, &resp_s8);

            c_context->replica->handskahe_done = h_done;
            c_context->replica->conn_fd        = ctx->conn_fd;
            c_context->replica->conn_id        = ctx->conn_id;
            push_vec(sv_context->replicas, c_context->replica);

            // transfer rdb
            char  *rdbContent = new (scratch, byte, 1024);
            char  *hexString  = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469"
                                "732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0"
                                "c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
            size_t len        = strlen(hexString);
            for (size_t i = 0; i < len; i += 2) {
                sscanf(hexString + i, "%2hhx", &rdbContent[i / 2]);
            }
            char *result = new (scratch, byte, 1024);
            snprintf(result, 1024, "$%lu\r\n%s", strlen(rdbContent), rdbContent);
            len = strlen(result);
            fprintf(stderr, "DEBUGPRINT[22]: server.c:550: len=%zu\n", len);
            s8 rdb_response = (s8) {.len = len, .data = (u8 *) result};
            write_response(c_context, &rdb_response);
            DBG_F("handshake with replica at %d done. conn id: %d\n", c_context->replica->port,
                  c_context->conn_id);
        }

    } else if (s8equals_nocase(command->str, S("wait")) == true) {
        s8 response = serde_int(c_context->perm, 0);
        write_response(c_context, &response);

    } else {
        printf("Unknown request type\n");
    }
}

int main(int argc, char *argv[]) {
    // Disable output buffering for testing
    setbuf(stdout, NULL);
    setbuf(stderr, NULL);

    Arena arena = newarena(10000 * 1024);

    Config *config = new (&arena, Config);
    *config        = (Config) {
               .dbfilename  = NULL,
               .dir         = NULL,
               .port        = 6379,
               .master_info = NULL,
    };

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

    HashMap *hashmap = 0;

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
    struct sockaddr_in client_addr, serv_addr = {
                                        .sin_family = AF_INET,
                                        .sin_port   = htons(config->port),
                                        .sin_addr   = {htonl(INADDR_ANY)},
                                    };

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

    if (bind(server_fd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) != 0) {
        fprintf(stderr, "Bind failed: %s \n", strerror(errno));
        return 1;
    }

    if (listen(server_fd, MAX_CLIENTS) != 0) {
        fprintf(stderr, "Listen failed: %s \n", strerror(errno));
        return 1;
    }

    printf("Waiting for a client to connect...\n");

    client_addr_len      = sizeof(client_addr);
    vector     *replicas = initialize_vec();
    Connections conns    = {
           .poll_fds        = malloc(sizeof(struct pollfd) * 5),
           .client_contexts = malloc(sizeof(ClientContext) * 5),
           .count           = 0,
           .size            = MAX_CLIENTS,
    };
    ServerContext sv_context = {
        .hashmap     = &hashmap,
        .config      = config,
        .perm        = &arena,
        .replicas    = replicas,
        .connections = &conns,
    };
    add_client(&arena, &sv_context, &conns, server_fd);

    if (config->master_info != NULL) {
        int master_conn_fd;
        if ((master_conn_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
            printf("\n Socket creation error \n");
            return -1;
        }
        if (connect(master_conn_fd, (struct sockaddr *) config->master_info,
                    sizeof(struct sockaddr_in)) < 0) {
            printf("Connection Failed");
            return -1;
        }

        ReplicationContext *repl_context = new (&arena, ReplicationContext);
        repl_context->repl_offset        = 0;
        repl_context->handshake_state    = 0;
        int            id                = add_client(&arena, &sv_context, &conns, master_conn_fd);
        ClientContext *context           = &conns.client_contexts[id];
        context->replication_context     = repl_context;
        conns.poll_fds[id].events |= POLLOUT;
        // write ping
        char *ping[1] = {"PING"};
        s8    request = serde_array(&arena, ping, 1);
        write_response(context, &request);
    }

    while (1) {
        int poll_count = poll(conns.poll_fds, conns.count, 1000);

        if (poll_count < 0 && errno == EINTR) {
            continue;
        }
        if (poll_count == -1) {
            perror("poll");
            abort();
        }

        for (int i = 1; i < conns.count; i++) {
            uint32_t       ready = conns.poll_fds[i].revents;
            ClientContext *conn  = &conns.client_contexts[i];
            struct pollfd *pfd   = &conns.poll_fds[i];
            if ((ready & POLLERR) || conn->want_close) {
                DBG_F("client %d removed\n", conn->conn_fd);
                del_connection(&conns, i);
            } else if (ready & POLLOUT) {
                int result = send_writes(pfd->fd, &conn->writer);
                DBG_F("write response result %d\n", result);
                switch (result) {
                case -1:
                    conn->want_close = 1;
                    break;
                case 0:
                    conn->want_write = 0;
                    conn->want_read  = 1;
                    break;
                case 1:
                    conn->want_write = 1;
                    break;
                default:
                    UNREACHABLE();
                }
            } else if (ready & POLLIN) {
                append_read_buf(&conn->reader);
                switch (conn->reader.status) {
                case 0:
                case -1:
                    conn->want_close = 1;
                    continue;
                }

                while (conn->reader.length > 0) {
                    Request request = try_parse_request(&arena, &conn->reader);
                    if (request.empty) {
                        break;
                    }
                    printf("read request from client %d\n", pfd->fd);
                    // if everything is read
                    if (conn->reader.cursor == conn->reader.length) {
                        conn->reader.cursor = 0;
                        conn->reader.length = 0;
                        conn->want_read     = 0;
                        conn->want_write    = 1;
                    }
                    if (conn->replication_context) {
                        handle_master_request(&sv_context, conn, request);
                    } else {
                        handle_request(&sv_context, conn, request);
                    }
                }
            }
        }

        for (int i = 0; i < conns.count; i++) {
            ClientContext *conn = &conns.client_contexts[i];
            if (conn->want_close) {
                del_connection(&conns, i);
            }
        }

        for (int i = 0; i < conns.count; i++) {
            struct pollfd *pfd = &conns.poll_fds[i];
            if (pfd->fd == server_fd && pfd->revents) {
                int new_fd = accept(server_fd, (struct sockaddr *) &client_addr, &client_addr_len);
                if (new_fd == -1) {
                    perror("accept");
                    continue;
                } else {
                    add_client(&arena, &sv_context, &conns, new_fd);
                    printf("Client %d connected\n", new_fd);
                }
                continue;
            }
            ClientContext *conn = &conns.client_contexts[i];
            pfd->events         = POLLERR;
            if (conn->want_read) {
                pfd->events |= POLLIN;
            }
            if (conn->want_write) {
                pfd->events |= POLLOUT;
            }
        }
    }

    for (int i = 0; i < conns.count; i++) {
        close(conns.poll_fds[i].fd);
    }
    return 0;
}
