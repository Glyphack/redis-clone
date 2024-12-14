#include <errno.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

void *connection_handler(void *arg);

#define MAX_ENTRY_STR_SIZE 128
#define MAX_MAP_SIZE 1000

struct HashMap {
  char *keys[MAX_MAP_SIZE];
  char *values[MAX_MAP_SIZE];
  int size;
  int capacity;
};

struct HashMap *hashmap_init() {
  struct HashMap *h = (struct HashMap *)malloc(sizeof(struct HashMap));
  for (int i = 0; i < MAX_MAP_SIZE; i++) {
    h->keys[i] = malloc(MAX_ENTRY_STR_SIZE);
    h->values[i] = malloc(MAX_ENTRY_STR_SIZE);
  }
  h->size = 0;
  h->capacity = MAX_MAP_SIZE;
  return h;
}

void hashmap_insert(struct HashMap *h, char *key, char *val) {
  int index = -1;
  for (int i = 0; i < h->size; i++) {
    if (strcmp(h->keys[i], key) == 0) {
      index = i;
      break;
    }
  }
  if (index != -1) {
    strcpy(h->values[index], val);
  }

  if (h->size == h->capacity) {
    printf("hashamp capacity reached");
  }

  h->keys[h->size] = (char *)malloc(MAX_ENTRY_STR_SIZE);
  strcpy(h->keys[h->size], key);

  h->values[h->size] = (char *)malloc(MAX_ENTRY_STR_SIZE);
  strcpy(h->values[h->size], val);
  h->size++;
}

char *hashmap_get(struct HashMap *h, char *key) {
  int index = -1;
  for (int i = 0; i < h->size; i++) {
    if (strcmp(h->keys[i], key) == 0) {
      index = i;
      break;
    }
  }
  if (index == -1) {
    return NULL;
  }
  return h->values[index];
}

struct Context {
  int conn_fd;
  struct HashMap *hashmap;
};

int main() {
  // Disable output buffering for testing
  setbuf(stdout, NULL);
  setbuf(stderr, NULL);

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
      .sin_port = htons(6379),
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

  struct HashMap *hashmap = hashmap_init();

  printf("Waiting for a client to connect...\n");
  client_addr_len = sizeof(client_addr);
  pthread_t thread_id;

  int client_fd =
      accept(server_fd, (struct sockaddr *)&client_addr, &client_addr_len);
  while (client_fd) {
    struct Context *context = malloc(sizeof(struct Context));
    context->conn_fd = client_fd;
    context->hashmap = hashmap;
    fprintf(stderr, "DEBUGPRINT[3]: server.c:60: client_fd=%d\n", client_fd);
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

void *send_response(int client_fd, char *msg) {
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

void *connection_handler(void *arg) {
  struct Context *ctx = (struct Context *)arg;
  int client_fd = ctx->conn_fd;
  fprintf(stderr, "DEBUGPRINT[1]: server.c:78: client_fd=%d\n", client_fd);

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
        printf("%s\n", parts[i]);
        if (strncmp(parts[i], "ECHO", 4) == 0) {
          printf("responding to echo\n");
          i = i + 1;
          int argument_len = strlen(parts[i]);
          send_response(ctx->conn_fd, parts[i]);
        } else if (strncmp(parts[i], "SET", 3) == 0) {
          printf("responding to set\n");
          char *key = parts[++i];
          char *val = parts[++i];
          hashmap_insert(ctx->hashmap, key, val);
          send_response(ctx->conn_fd, "OK");
        } else if (strncmp(parts[i], "GET", 3) == 0) {
          printf("responding to get\n");
          char *key = parts[++i];
          char *val = hashmap_get(ctx->hashmap, key);
          // if (val == NULL) {
          //   send_response(ctx->conn_fd, "-1");
          //   continue;
          // }
          send_response(ctx->conn_fd, val);
        } else if (strncmp(parts[i], "PING", 4) == 0) {
          printf("responding to ping\n");
          char message[7] = "+PONG\r\n";
          int sent = send(client_fd, message, 7, 0);
          if (sent < 0) {
            fprintf(stderr, "Could not send response: %s\n", strerror(errno));
          } else {
            printf("bytes sent %d\n", sent);
          }
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
