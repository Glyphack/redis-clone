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
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/event.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

BufferReader buffered_reader(Arena *arena, int conn_fd) {
    int          capacity = 64 * 1024; // 64KB chunks
    char        *buf      = new (arena, char, capacity);
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
    size written = write(fd, to_write, to_write_len);
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

void update_events(int kq, int fd, int filter, int flags, void *udata) {
    struct kevent ev;
    EV_SET(&ev, fd, filter, flags, 0, 0, udata);
    if (kevent(kq, &ev, 1, NULL, 0, NULL) == -1) {
        perror("kevent");
        exit(1);
    }
}

// Batch update multiple events
void update_events_batch(int kq, struct kevent *events, int nevents) {
    if (kevent(kq, events, nevents, NULL, 0, NULL) == -1) {
        perror("kevent batch");
        exit(1);
    }
}

int add_client(Arena *arena, ServerContext *sv_context, Connections *connections, int newfd) {
    if (connections->count == connections->size) {
        fprintf(stderr, "Max client limit (%d) reached. Cannot add more clients.\n",
                connections->size);
        close(newfd);
        return -1;
    }
    fcntl(newfd, F_SETFL, fcntl(newfd, F_GETFL, 0) | O_NONBLOCK);

    int          id         = connections->count;
    int          FULL_MEM   = 100 * 1024;
    Arena        temp_arena = newarena(FULL_MEM);
    BufferWriter writer     = (BufferWriter) {
            .cursor = 0,
            .len    = 0,
            .buffer = (s8) {.data = new (&temp_arena, u8, FULL_MEM / 10), .len = 1024},
    };

    connections->client_contexts[id] = (ClientContext) {
        .conn_fd                 = newfd,
        .perm                    = arena,
        .temp                    = temp_arena,
        .hashmap                 = sv_context->hashmap,
        .reader                  = buffered_reader(arena, newfd),
        .writer                  = writer,
        .want_read               = 1,
        .want_write              = 0,
        .want_close              = 0,
        .is_connection_to_master = 0,
        .conn_id                 = id,
    };

    // Batch event registration
    struct kevent evs[2];
    int           nevents = 0;

    if (id == 0) {
        // For server socket, only monitor read events
        EV_SET(&evs[nevents++], newfd, EVFILT_READ, EV_ADD, 0, 0,
               &connections->client_contexts[id]);
    } else {
        // For client sockets, monitor both read and write events initially
        EV_SET(&evs[nevents++], newfd, EVFILT_READ, EV_ADD, 0, 0,
               &connections->client_contexts[id]);
        EV_SET(&evs[nevents++], newfd, EVFILT_WRITE, EV_ADD | EV_DISABLE, 0, 0,
               &connections->client_contexts[id]);
    }

    update_events_batch(connections->kq, evs, nevents);
    connections->count++;
    return id;
}

void del_connection(Connections *connections, int i) {
    DBG_F("client %d removed\n", i);
    ClientContext *del_con = &connections->client_contexts[i];

    // Batch delete events
    struct kevent evs[2];
    EV_SET(&evs[0], del_con->conn_fd, EVFILT_READ, EV_DELETE, 0, 0, NULL);
    EV_SET(&evs[1], del_con->conn_fd, EVFILT_WRITE, EV_DELETE, 0, 0, NULL);
    update_events_batch(connections->kq, evs, 2);

    close(del_con->conn_fd);
    droparena(&del_con->temp);

    // Move the last client to this slot if not already the last
    if (i < connections->count - 1) {
        connections->client_contexts[i] = connections->client_contexts[connections->count - 1];
        // Update events for the moved client to point to new location
        struct kevent  evs[2];
        ClientContext *moved = &connections->client_contexts[i];
        EV_SET(&evs[0], moved->conn_fd, EVFILT_READ, EV_ADD, 0, 0, moved);
        EV_SET(&evs[1], moved->conn_fd, EVFILT_WRITE, EV_ADD | (moved->want_write ? 0 : EV_DISABLE),
               0, 0, moved);
        update_events_batch(connections->kq, evs, 2);
    }
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
    printf("config:\n");
    printf("  dir: ");
    s8print(config->dir);
    printf("  dbfilename: ");
    s8print(config->dbfilename);
    printf("  port `%d`\n", config->port);
    if (config->master_info != NULL) {
        char ip_str[INET_ADDRSTRLEN];
        if (inet_ntop(AF_INET, &(config->master_info->sin_addr), ip_str, INET_ADDRSTRLEN) == NULL) {
            perror("inet_ntop");
        }
        printf("  master host `%s`\n", ip_str);
        printf("  master port `%d`\n", config->master_info->sin_port);
    }
}

void *getConfig(Config *config, s8 name) {
    if (s8equals(name, S("dir"))) {
        return config->dir.data;
    }
    if (s8equals(name, S("dbfilename"))) {
        return config->dbfilename.data;
    }
    if (s8equals(name, S("appendonly"))) {
        return "no"; // Explicitly disable AOF persistence
    }
    if (s8equals(name, S("save"))) {
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
            sv_context->replication_context->handshake_state = h_replconf;
        } else if (s8equals_nocase(res->str, S("ok")) == true) {
            if (sv_context->replication_context->handshake_state == h_replconf) {
                char *repl_conf2[3] = {
                    "REPLCONF",
                    "capa",
                    "psync2",
                };
                s8 request = serde_array(scratch, repl_conf2, 3);
                write_response(ctx, &request);
                sv_context->replication_context->handshake_state = h_replconf_2;
            } else if (sv_context->replication_context->handshake_state == h_replconf_2) {
                char *psync[3] = {
                    "PSYNC",
                    "?",
                    "-1",
                };
                s8 request = serde_array(scratch, psync, 3);
                write_response(ctx, &request);
                sv_context->replication_context->handshake_state = h_psync;
            } else {
                UNREACHABLE();
            }
        } else if (s8startswith(res->str, S("FULLRESYNC")) == true) {
            DEBUG_LOG("got full resync");
            sv_context->replication_context->handshake_state = h_done;
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
        sv_context->replication_context->handshake_state = 1;
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

        HashMapNode hNode = {
            .key = s8clone(ctx->perm, key->str), .val = s8clone(ctx->perm, val->str), .ttl = ttl};
        hashmap_upsert(ctx->hashmap, ctx->perm, &hNode);
    } else if (s8equals_nocase(command->str, S("REPLCONF")) == true) {
        assert(resp_array->count == 3);
        Element *arg_val = resp_array->elts[1];
        assert(arg_val->type == BULK_STRING);
        BulkString *arg = (BulkString *) arg_val->val;

        if (s8equals_nocase(arg->str, S("GETACK"))) {
            // ["replconf", "getack", "*"]
            DEBUG_LOG("responding to replconf get ack");
            DBG(sv_context->replication_context->repl_offset, d);
            assert(resp_array->count == 3);
            assert(resp_array->elts[2]->type == BULK_STRING);
            s8 arg_2_value = ((BulkString *) resp_array->elts[2]->val)->str;
            assert(s8equals_nocase(arg_2_value, S("*")));
            char *offset_str = new (scratch, char, 10);
            snprintf(offset_str, 10, "%d", sv_context->replication_context->repl_offset);
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

        // TODO: Not useful for replicas
        int total_size = 13; // "role:master\n" or "role:slave\n" (12 chars + \n)
        total_size += strlen("master_replid:") + 20 + 1;      // replid info + \n
        total_size += strlen("master_repl_offset:") + 20 + 1; // offset info + \n
        total_size += 1;                                      // null terminator

        s8  response = (s8) {.len = total_size, .data = new (scratch, u8, total_size)};
        int offset   = 0;

        s8 type;
        type    = sv_context->config->master_info != NULL ? S("slave") : S("master");
        s8 role = S("role:");
        memcpy(response.data, role.data, role.len);
        offset += role.len;
        memcpy(response.data + offset, type.data, type.len);
        offset += type.len;
        response.data[offset] = '\n';
        offset++;
        s8 repl_id = S("master_replid:");
        memcpy(response.data, repl_id.data, repl_id.len);
        offset += repl_id.len;
        memcpy(response.data + offset, sv_context->config->master_replid.data,
               sv_context->config->master_replid.len);
        offset += type.len;
        response.data[offset] = '\n';
        s8 repl_offset        = S("master_repl_offset:");
        memcpy(response.data, repl_offset.data, repl_offset.len);
        offset += repl_offset.len;
        insert_number(scratch, response.data, sv_context->config->master_repl_offset, offset);
        response.data[offset] = '\n';

        serde_bulk_str(ctx->perm, response);
    } else {
        printf("Unknown request type\n");
    }

    sv_context->replication_context->repl_offset += request.bytes.len;
    DBG(sv_context->replication_context->repl_offset, d);
}

void handle_request(ServerContext *sv_context, ClientContext *c_context, Request request) {
    Element element = request.element;
    Arena  *scratch = &c_context->temp;
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
            sv_context->config->master_repl_offset += request.bytes.len;
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

        HashMapNode hNode = {
            .key = s8clone(ctx->perm, key->str), .val = s8clone(ctx->perm, val->str), .ttl = ttl};
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
        char *key_chars = new (scratch, char, keys->total);
        for (int i = 0; i < keys->total; i++) {
            key_chars[i] = *(char *) keys->items[i];
        }
        free(keys->items);
        free(keys);
        s8 response = serde_array(scratch, &key_chars, keys->total);
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

        char *config_val = getConfig(sv_context->config, arg->str);
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

        char *content = new (scratch, char, total_size);
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
            BulkString *port_str          = (BulkString *) port_val->val;
            c_context->replica->port      = atoi(s8_to_cstr(scratch, port_str->str));
            struct sockaddr_in *serv_addr = new (sv_context->perm, struct sockaddr_in);
            // TODO: host info is not always right
            char *host            = "127.0.0.1";
            serv_addr->sin_family = AF_INET;
            serv_addr->sin_port   = htons(c_context->replica->port);
            if (inet_pton(AF_INET, host, &serv_addr->sin_addr) <= 0) {
                printf("\nInvalid address/ Address not supported \n");
                abort();
            }
            c_context->replica->address = serv_addr;
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
        } else if (s8equals_nocase(arg->str, S("GETACK"))) {
            DEBUG_LOG("responding to replconf get ack");
            DBG(sv_context->replication_context->repl_offset, d);
            assert(resp_array->count == 3);
            assert(resp_array->elts[2]->type == BULK_STRING);
            s8 arg_2_value = ((BulkString *) resp_array->elts[2]->val)->str;
            assert(s8equals_nocase(arg_2_value, S("*")));
            char *offset_str = new (scratch, char, 10);
            snprintf(offset_str, 10, "%d", sv_context->replication_context->repl_offset);
            char *items[3] = {"REPLCONF", "ACK", offset_str};
            s8    resp     = serde_array(scratch, items, 3);
            write_response(c_context, &resp);
        } else if (s8equals_nocase(arg->str, S("ACK"))) {
            DEBUG_LOG("processing ACK from replica");
            WaitState *wait_state = &sv_context->wait_state;
            i64 offset = s8to_i64(((BulkString *) ((RespArray *) element.val)->elts[2]->val)->str);
            if (offset == wait_state->repl_offset + 37 || (offset == wait_state->repl_offset)) {
                wait_state->synced_count++;
                DEBUG_LOG("Found a synced replica");
            }
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
            s8    replid     = sv_context->config->master_replid;
            s8    fullresync = S("+FULLRESYNC ");
            s8    end        = S(" 0\r\n");
            char *response   = new (scratch, char, fullresync.len + end.len + replid.len);
            memcpy(response, fullresync.data, fullresync.len);
            i64 offset = fullresync.len;
            memcpy(response + offset, replid.data, replid.len);
            offset += replid.len;
            memcpy(response + offset, end.data, end.len);
            s8 resp_s8 = (s8) {.len = fullresync.len + end.len + replid.len, .data = response};

            write_response(c_context, &resp_s8);
            c_context->replica->handskahe_done = h_done;
            c_context->replica->conn_fd        = ctx->conn_fd;
            c_context->replica->conn_id        = ctx->conn_id;
            push_vec(sv_context->replicas, c_context->replica);

            // transfer rdb
            char  *rdbContent = new (scratch, char, 1024);
            char  *hexString  = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469"
                                "732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0"
                                "c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
            size_t len        = strlen(hexString);
            for (size_t i = 0; i < len; i += 2) {
                sscanf(hexString + i, "%2hhx", &rdbContent[i / 2]);
            }
            char *result = new (scratch, char, 1024);
            snprintf(result, 1024, "$%lu\r\n%s", strlen(rdbContent), rdbContent);
            len = strlen(result);
            fprintf(stderr, "DEBUGPRINT[22]: server.c:550: len=%zu\n", len);
            s8 rdb_response = (s8) {.len = len, .data = (u8 *) result};
            write_response(c_context, &rdb_response);
            DBG_F("handshake with replica at %d done. conn id: %d\n", c_context->replica->port,
                  c_context->conn_id);
        }

    } else if (s8equals_nocase(command->str, S("wait")) == true) {
        Element *arg1_val   = resp_array->elts[1];
        i32      wait_count = (i32) s8to_i64(((BulkString *) arg1_val->val)->str);
        i64      timeout    = 0;
        if (resp_array->count >= 2) {
            void *arg2_val = resp_array->elts[2]->val;
            timeout        = s8to_i64(((BulkString *) arg2_val)->str);
        }
        if (sv_context->config->master_repl_offset == 0) {
            s8 response = serde_int(c_context->perm, sv_context->replicas->total);
            write_response(c_context, &response);
            return;
        }
        char *items[3] = {"REPLCONF", "GETACK", "*"};
        s8    req      = serde_array(&ctx->temp, items, 3);
        for (int i = 0; i < sv_context->replicas->total; i++) {
            ReplicaConfig *replica = get_vec(sv_context->replicas, i);
            ClientContext *replica_context =
                &sv_context->connections->client_contexts[replica->conn_id];
            write_response(replica_context, &req);
            replica_context->want_write = 1;
        }

        WaitState wait = (WaitState) {
            .deadline       = get_current_time() + timeout,
            .num_waiting    = wait_count,
            .synced_count   = 0,
            .client_conn_id = c_context->conn_id,
            .repl_offset    = sv_context->config->master_repl_offset,
        };
        sv_context->wait_state = wait; // Only one wait at any given time is possible

    } else {
        printf("Unknown request type\n");
    }
}

int main(int argc, char *argv[]) {
    // Disable output buffering for testing
    setbuf(stdout, NULL);
    setbuf(stderr, NULL);

    Arena arena = newarena(1000000 * 1024);

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
                config->dir = s8_from_cstr(&arena, flag_val);
            }
            if (strcmp(flag_name, "--dbfilename") == 0) {
                char *flag_val     = argv[++i];
                config->dbfilename = s8_from_cstr(&arena, flag_val);
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

    config->master_replid      = S("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb");
    config->master_repl_offset = 0;

    print_config(config);

    HashMap *hashmap = 0;

    if (config->dir.len && config->dbfilename.len) {
        usize path_size = config->dir.len + config->dbfilename.len + 1;
        Arena scratch   = newarena(1000 * 1024);
        s8    full_path = {.len = path_size, .data = new (&scratch, u8, path_size)};
        memcpy(full_path.data, config->dir.data, config->dir.len);
        size pos            = config->dir.len + 1;
        full_path.data[pos] = '/';
        pos++;
        memcpy(full_path.data + pos, config->dbfilename.data, config->dbfilename.len);
        RdbContent *rdb = parse_rdb(&arena, &scratch, full_path);
        droparena(&scratch);
        if (rdb != NULL) {
            // TODO: something not right
            RdbDatabase *db = (RdbDatabase *) rdb->database;
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

    printf("Server started\n");

    client_addr_len  = sizeof(client_addr);
    vector *replicas = initialize_vec();

    // Initialize kqueue
    int kq = kqueue();
    if (kq == -1) {
        perror("kqueue");
        return 1;
    }

    Connections conns = {
        .count           = 0,
        .size            = MAX_CLIENTS,
        .kq              = kq,
        .events          = malloc(sizeof(struct kevent) * MAX_CLIENTS),
        .client_contexts = malloc(sizeof(ClientContext) * MAX_CLIENTS),
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
        context->is_connection_to_master = 1;
        sv_context.replication_context   = repl_context;

        // Enable write events for master connection
        update_events(kq, master_conn_fd, EVFILT_WRITE, EV_ENABLE, context);

        // write ping
        char *ping[1] = {"PING"};
        s8    request = serde_array(&arena, ping, 1);
        write_response(context, &request);
    }

    struct timespec timeout = {
        .tv_sec  = 0,
        .tv_nsec = 10000000 // 10ms
    };

    while (1) {
        // Handle wait_state (if any)
        if (sv_context.wait_state.num_waiting &&
            sv_context.wait_state.deadline < get_current_time()) {
            ClientContext *client_context =
                &conns.client_contexts[sv_context.wait_state.client_conn_id];
            s8 response = serde_int(&client_context->temp, sv_context.wait_state.synced_count);
            write_response(client_context, &response);
            client_context->want_write = 1;

            struct kevent ev;
            EV_SET(&ev, client_context->conn_fd, EVFILT_WRITE, EV_ENABLE, 0, 0, client_context);
            update_events_batch(conns.kq, &ev, 1);

            sv_context.wait_state = (WaitState) {0};
        }

        int nev = kevent(kq, NULL, 0, conns.events, conns.size, &timeout);
        if (nev < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("kevent");
            break;
        }

        for (int i = 0; i < nev; i++) {
            struct kevent *ev = &conns.events[i];
            int            fd = (int) ev->ident;

            // Direct access to client context through udata
            ClientContext *conn = (ClientContext *) ev->udata;
            if (conn == NULL) {
                fprintf(stderr, "No client context found for fd %d\n", fd);
                continue;
            }

            if (ev->flags & EV_ERROR) {
                fprintf(stderr, "Event error: %s\n", strerror(ev->data));
                conn->want_close = 1;
            }

            if (fd == server_fd) {
                // Handle new connection
                int new_fd = accept(server_fd, (struct sockaddr *) &client_addr, &client_addr_len);
                if (new_fd == -1) {
                    perror("accept");
                } else {
                    add_client(&arena, &sv_context, &conns, new_fd);
                    DBG_F("Client %d connected\n", new_fd);
                }
                continue;
            }

            if (ev->filter == EVFILT_READ) {
                append_read_buf(&conn->reader);
                switch (conn->reader.status) {
                case 0:
                case -1:
                    conn->want_close = 1;
                    continue;
                }

                while (conn->reader.length > 0) {
                    Arena   temp_arena_bu = conn->temp;
                    Request request       = try_parse_request(&conn->temp, &conn->reader);
                    if (request.empty) {
                        conn->temp = temp_arena_bu;
                        break;
                    }
                    DBG_F("read request from client %d\n", fd);

                    if (conn->reader.cursor == conn->reader.length) {
                        conn->reader.cursor = 0;
                        conn->reader.length = 0;

                        // Batch update read/write events
                        struct kevent evs[2];
                        EV_SET(&evs[0], fd, EVFILT_READ, EV_DISABLE, 0, 0, conn);
                        EV_SET(&evs[1], fd, EVFILT_WRITE, EV_ENABLE, 0, 0, conn);
                        update_events_batch(conns.kq, evs, 2);
                    }

                    if (conn->is_connection_to_master) {
                        handle_master_request(&sv_context, conn, request);
                    } else {
                        handle_request(&sv_context, conn, request);
                    }
                    conn->temp = temp_arena_bu;
                }
            }

            if (ev->filter == EVFILT_WRITE) {
                int result = send_writes(fd, &conn->writer);
                DBG_F("write response result %d\n", result);
                switch (result) {
                case -1:
                    conn->want_close = 1;
                    break;
                case 0:
                    // Batch update read/write events
                    struct kevent evs[2];
                    EV_SET(&evs[0], fd, EVFILT_WRITE, EV_DISABLE, 0, 0, conn);
                    EV_SET(&evs[1], fd, EVFILT_READ, EV_ENABLE, 0, 0, conn);
                    update_events_batch(conns.kq, evs, 2);
                    break;
                case 1:
                    // Keep write events enabled
                    break;
                default:
                    UNREACHABLE();
                }
            }

            if (conn->want_close) {
                // Find the index of this connection
                int idx;
                for (idx = 0; idx < conns.count; idx++) {
                    if (&conns.client_contexts[idx] == conn)
                        break;
                }
                if (idx < conns.count) {
                    del_connection(&conns, idx);
                }
            }
        }
    }

    // Cleanup
    close(kq);
    free(conns.events);
    free(conns.client_contexts);
    for (int i = 0; i < conns.count; i++) {
        close(conns.client_contexts[i].conn_fd);
    }
    return 0;
}
