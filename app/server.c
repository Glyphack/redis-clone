#include "server.h"
#include "rdb.h"
#include "vec.h"
#include <arpa/inet.h>
#include <stdint.h>
#include <sys/stat.h>

#include <dirent.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <unistd.h>

long long get_current_time() {
  struct timespec ts;
  if (clock_gettime(CLOCK_REALTIME, &ts) == 0) {
    return (long long)(ts.tv_sec * 1000 + ts.tv_nsec / 1000000);
  } else {
    return 0;
  }
}

HashMapNode *hashmap_node_init() {
  HashMapNode *hNode = (HashMapNode *)malloc(sizeof(HashMapNode));
  hNode->key = (char *)malloc(MAX_ENTRY_STR_SIZE);
  hNode->val = (char *)malloc(MAX_ENTRY_STR_SIZE);
  return hNode;
}

HashMap *hashmap_init() {
  HashMap *h = (HashMap *)malloc(sizeof(HashMap));
  for (int i = 0; i < MAX_MAP_SIZE; i++) {
    HashMapNode *node = h->nodes[i] = malloc(sizeof(HashMapNode));
    node->key = malloc(MAX_ENTRY_STR_SIZE);
    node->val = malloc(MAX_ENTRY_STR_SIZE);
  }
  h->size = 0;
  h->capacity = MAX_MAP_SIZE;
  return h;
}

void hashmap_insert(HashMap *h, HashMapNode *node) {
  int index = -1;
  for (int i = 0; i < h->size; i++) {
    HashMapNode *existingNode = h->nodes[i];
    if (strcmp(existingNode->key, node->key) == 0) {
      index = i;
      break;
    }
  }
  if (index != -1) {
    h->nodes[index] = node;
  }

  if (h->size == h->capacity) {
    printf("hashamp capacity reached");
    exit(1);
  }

  h->nodes[h->size++] = node;
}

HashMapNode *hashmap_get(HashMap *h, char *key) {
  int index = -1;
  for (int i = 0; i < h->size; i++) {
    if (strcmp(h->nodes[i]->key, key) == 0) {
      index = i;
      break;
    }
  }
  if (index == -1) {
    return NULL;
  }
  return h->nodes[index];
}

char **hashmap_keys(HashMap *h) {
  char **array = (char **)malloc(h->size * sizeof(char *));
  for (int i = 0; i < h->size; i++) {
    HashMapNode *node = h->nodes[i];
    array[i] = (char *)malloc(strlen(node->key) * sizeof(char));
    strcpy(array[i], node->key);
    printf("%s\n", array[i]);
  }
  return array;
}

void printConfig(Config *config) {
  fprintf(stderr, "config:\n");
  fprintf(stderr, "  dir `%s`\n", config->dir);
  fprintf(stderr, "  dbfilename `%s`\n", config->dbfilename);
  fprintf(stderr, "  port `%d`\n", config->port);
  if (config->master_info != NULL) {
    char ip_str[INET_ADDRSTRLEN];
    if (inet_ntop(AF_INET, &(config->master_info->sin_addr), ip_str,
                  INET_ADDRSTRLEN) == NULL) {
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

  fprintf(stderr, "Unknown Config %s\n", name);
  return NULL;
}

int handshake(Config *config) {

  int sock = 0;
  struct sockaddr_in serv_addr = *config->master_info;
  char *message = "*1\r\n$4\r\nPING\r\n";

  // Create socket
  if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    printf("\n Socket creation error \n");
    return -1;
  }

  // Connect to the server
  if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    printf("\nConnection Failed \n");
    return -1;
  }

  // Send data
  send(sock, message, strlen(message), 0);

  close(sock);
  return 0;
}

int main(int argc, char *argv[]) {
  // Disable output buffering for testing
  setbuf(stdout, NULL);
  setbuf(stderr, NULL);

  Config *config = (Config *)malloc(sizeof(Config));
  config->dbfilename = NULL;
  config->dir = NULL;
  config->port = 6379;
  config->master_info = NULL;

  int print_rdb_and_exit = 0;

  if (argc > 1) {
    for (int i = 0; i < argc; i++) {
      char *flag_name = argv[i];
      if (strcmp(flag_name, "--dir") == 0) {
        char *flag_val = argv[++i];
        DIR *dp = opendir(flag_val);
        if (dp == NULL) {
          perror("Could not open directory passed to -dir");
        }
        closedir(dp);
        config->dir = (char *)malloc(strlen(flag_val));
        strcpy(config->dir, flag_val);
        printf("dir: %s\n", flag_val);
      }
      if (strcmp(flag_name, "--dbfilename") == 0) {
        char *flag_val = argv[++i];
        config->dbfilename = (char *)malloc(strlen(flag_val));
        strcpy(config->dbfilename, flag_val);
        printf("dbfilename: %s\n", flag_val);
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
        struct sockaddr_in *serv_addr =
            (struct sockaddr_in *)malloc(sizeof(struct sockaddr_in));
        char *hostinfo = argv[i];
        char *host = strtok(hostinfo, " ");
        if (strcmp(host, "localhost") == 0) {
          host = "127.0.0.1";
        }
        serv_addr->sin_family = AF_INET;
        int port = atoi(strtok(NULL, " "));
        serv_addr->sin_port = htons(port);
        if (inet_pton(AF_INET, host, &serv_addr->sin_addr) <= 0) {
          printf("\nInvalid address/ Address not supported \n");
          return -1;
        }
        config->master_info = serv_addr;
      }
    }
  }

  // config->master_replid = malloc(40);
  config->master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
  config->master_repl_offset = 0;

  printConfig(config);

  HashMap *hashmap = hashmap_init();

  if (config->dir && config->dbfilename) {
    char full_path[MAX_PATH];
    snprintf(full_path, sizeof(full_path), "%s%s%s", config->dir, "/",
             config->dbfilename);
    RdbContent *rdb = parse_rdb(full_path);
    if (rdb != NULL) {
      RdbDatabase *db = (RdbDatabase *)rdb->databases->items[0];
      hashmap = db->data;
    }
    if (print_rdb_and_exit) {
      print_rdb(rdb);
      exit(0);
    }
  }

  int server_fd;
  socklen_t client_addr_len;
  struct sockaddr_in client_addr;

  server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd == -1) {
    printf("Socket creation failed: %s...\n", strerror(errno));
    return 1;
  }

  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) <
      0) {
    fprintf(stderr, "SO_REUSEADDR failed: %s \n", strerror(errno));
    return 1;
  }

  struct sockaddr_in serv_addr = {
      .sin_family = AF_INET,
      .sin_port = htons(config->port),
      .sin_addr = {htonl(INADDR_ANY)},
  };

  if (bind(server_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) != 0) {
    fprintf(stderr, "Bind failed: %s \n", strerror(errno));
    return 1;
  }

  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    fprintf(stderr, "Listen failed: %s \n", strerror(errno));
    return 1;
  }

  if (config->master_info != NULL) {
    if (handshake(config) == -1) {
      printf("handshake failed\n");
      exit(1);
    }
    printf("handshake with master succeeded\n");
  }

  printf("Waiting for a client to connect...\n");
  client_addr_len = sizeof(client_addr);
  pthread_t thread_id;

  int client_fd =
      accept(server_fd, (struct sockaddr *)&client_addr, &client_addr_len);
  while (client_fd) {
    Context *context = malloc(sizeof(Context));
    context->conn_fd = client_fd;
    context->hashmap = hashmap;
    context->config = config;
    if (pthread_create(&thread_id, NULL, connection_handler, (void *)context) <
        0) {
      perror("Could not create thread");
      return 1;
    }
    printf("connection handler thread created %lu\n", (unsigned long)thread_id);
    client_fd =
        accept(server_fd, (struct sockaddr *)&client_addr, &client_addr_len);
  }
  printf("Client connected\n");

  close(server_fd);

  return 0;
}

void *send_response_bulk_string(int client_fd, char *msg) {
  char response[256];
  response[0] = '\0';
  snprintf(response, sizeof(response), "$%d\r\n%s\r\n", (int)strlen(msg), msg);
  printf("responding with `%s`", response);
  int sent = send(client_fd, response, strlen(response), 0);
  if (sent < 0) {
    fprintf(stderr, "Could not send response: %s\n", strerror(errno));
  } else {
    printf("bytes sent %d\n", sent);
  }
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

int send_response(int client_fd, char *response) {
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
    snprintf(formatted, RESPONSE_ITEM_MAX_SIZE, "$%d\r\n%s\r\n",
             (int)strlen(items[i]), items[i]);
    strncat(response, formatted, RESPONSE_ITEM_MAX_SIZE);
  }
  send_response(client_fd, response);
}

void *connection_handler(void *arg) {
  Context *ctx = (Context *)arg;
  int client_fd = ctx->conn_fd;

  while (1) {
    int req_size = 2048;
    char request[req_size];
    int bytes_received = recv(client_fd, request, req_size, 0);

    if (bytes_received > 0) {
      request[bytes_received] = '\0';
      printf("Received request: `%s`\n", request);

      int part_count = atoi(&request[1]);
      char *parts[part_count];

      printf("message has %d parts\n", part_count);

      int msg_len = 0;
      int cursor = 0;
      int part_index = 0;
      while (cursor < bytes_received) {
        char ch = request[cursor];
        cursor = cursor + 1;
        if (ch == '$') {
          int size_start = cursor;
          int size_section_len = 0;
          while (ch != '\r') {
            size_section_len++;
            ch = request[size_start + size_section_len];
          }
          char *msg_len_str = (char *)malloc(size_section_len * sizeof(char));
          strncpy(msg_len_str, request + size_start, size_section_len);
          msg_len = atoi(msg_len_str);
          printf("part len: %d\n", msg_len);
          parts[part_index] = malloc((msg_len) * sizeof(char));
          strncpy(parts[part_index],
                  request + size_start + size_section_len + 2, msg_len);
          char *part = parts[part_index];
          part[msg_len] = '\0';
          printf("part content: %s\n", parts[part_index]);
          part_index += 1;
          cursor += size_section_len + 2;
        }
      }

      for (int i = 0; i < part_count; i++) {
        if (strncmp(parts[i], "ECHO", 4) == 0) {
          printf("responding to echo\n");
          i = i + 1;
          int argument_len = strlen(parts[i]);
          send_response_bulk_string(ctx->conn_fd, parts[i]);
        } else if (strncmp(parts[i], "SET", 3) == 0) {
          printf("responding to set\n");
          char *key = parts[++i];
          char *val = parts[++i];
          long long ttl = -1;
          if (part_count > 3) {
            if (strcmp(parts[++i], "px") != 0) {
              perror("unknown command arguments");
              break;
            }
            long long current_time = get_current_time();
            printf("current time: %lld\n", current_time);
            ttl = current_time + atoi(parts[++i]);
          }
          printf("ttl: %lld", ttl);
          HashMapNode *hNode = hashmap_node_init();
          strcpy(hNode->key, key);
          strcpy(hNode->val, val);
          hNode->ttl = ttl;
          hashmap_insert(ctx->hashmap, hNode);
          send_response_bulk_string(ctx->conn_fd, "OK");
        } else if (strncmp(parts[i], "GET", 3) == 0) {
          printf("responding to get\n");
          char *key = parts[++i];
          HashMapNode *node = hashmap_get(ctx->hashmap, key);
          if (node == NULL) {
            respond_null(ctx->conn_fd);
            continue;
          }
          long long current_time = get_current_time();
          printf("current time: %lld", current_time);
          if (node->ttl < current_time && node->ttl != -1) {
            printf("item expired ttl: %lld \n", node->ttl);
            respond_null(ctx->conn_fd);
            continue;
          }
          send_response_bulk_string(ctx->conn_fd, node->val);
        } else if (strncmp(parts[i], "KEYS", 4) == 0) {
          char *pattern = parts[++i];
          if (strncmp(pattern, "*", 1) != 0) {
            printf("unrecognized pattern");
            exit(1);
          }
          printf("responding to keys\n");
          char **keys = hashmap_keys(ctx->hashmap);
          if (keys == NULL) {
            respond_null(ctx->conn_fd);
            continue;
          }
          send_response_array(ctx->conn_fd, keys, ctx->hashmap->size);
        } else if (strncmp(parts[i], "PING", 4) == 0) {
          printf("responding to ping\n");
          char message[7] = "+PONG\r\n";
          int sent = send(client_fd, message, 7, 0);
          if (sent < 0) {
            fprintf(stderr, "Could not send response: %s\n", strerror(errno));
          } else {
            printf("bytes sent %d\n", sent);
          }
        } else if (strcmp(parts[i], "CONFIG") == 0) {
          // Should I be increased if we cannot handle the argument?
          if (strcmp(parts[++i], "GET") != 0) {
            perror("Unknown command\n");
            break;
          }

          char *arg = parts[++i];
          char *config_val = getConfig(ctx->config, arg);
          if (config_val == NULL) {
            perror("Unknown command\n");
            break;
          }

          char *resps[2] = {arg, config_val};
          send_response_array(ctx->conn_fd, resps, 2);
        } else if (strcmp(parts[i], "INFO") == 0) {
          i++;
          if (strcmp(parts[i], "replication")) {
            // no info other than replication supported
            UNREACHABLE();
          }
          int size = 3;
          char *msg;
          char *resps[size];

          char *role_info = malloc(11);
          if (ctx->config->master_info != NULL) {
            strcpy(role_info, "role:slave");
          } else {
            strcpy(role_info, "role:master");
          }
          resps[0] = role_info;

          DEBUG_LOG("hi");
          char *replid_info = malloc(strlen("master_replid:") +
                                     strlen(ctx->config->master_replid) + 1);
          strcpy(replid_info, "master_replid:");
          strcat(replid_info, ctx->config->master_replid);
          resps[1] = replid_info;

          resps[2] = malloc(strlen("master_repl_offset:") + 20 + 1);
          strcpy(resps[2], "master_repl_offset:");
          char repl_offset[20];
          sprintf(repl_offset, "%d", ctx->config->master_repl_offset);
          strcat(resps[2], repl_offset);

          // Allocate enough buffer for the whole message concatenated
          int total_size = 0;
          for (int i = 0; i < size; i++) {
            // Plus 2 for the \n
            total_size += strlen(resps[i]) + 2;
          }
          msg = malloc(total_size);

          for (int i = 0; i < size; i++) {
            strcat(msg, resps[i]);
            if (i != size) {
              strcat(msg, "\n");
            }
          }

          send_response_bulk_string(ctx->conn_fd, msg);
        } else {
          perror("Unknown command\n");
          break;
        }
      }
    } else if (bytes_received == 0) {
      printf("Client did not send any data");
      break;
    } else {
      perror("Could not read from connection");
    }
  }

  return NULL;
}

void print_mystr(Mystr *s) {
  for (size_t i = 0; i < s->len; i++) {
    putchar(s->data[i]);
  }
  putchar('\n');
}

Mystr *new_mystr() { return (Mystr *)malloc(sizeof(Mystr)); }

char *convertToCStr(Mystr *s) {
  char *c = (char *)malloc(s->len + 1);
  for (int i = 0; i < s->len; i++) {
    c[i] = s->data[i];
  }
  c[s->len] = '\0';
  return c;
}
