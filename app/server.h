#ifndef SERVER_H
#define SERVER_H

#include "./vec.h"
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


#define MAX_ENTRY_STR_SIZE 128
#define MAX_MAP_SIZE 1000
#define RESPONSE_ITEM_MAX_SIZE 256
#define MAX_PATH 256
#define MAX_METADATA_SIZE 100


#define DEBUG_PRINT(var, fmt) fprintf(stderr, "DEBUG: (%s:%d) %s = %" #fmt "\n", __FILE__, __LINE__, #var, var)
#define DEBUG_LOG(msg) fprintf(stderr, "DEBUG: (%s:%d) %s\n", __FILE__, __LINE__, #msg)

#define UNREACHABLE()                                                          \
  do {                                                                         \
    fprintf(stderr, "Unreachable code reached at %s:%d\n", __FILE__,           \
            __LINE__);                                                         \
    exit(EXIT_FAILURE);                                                        \
  } while (0)

typedef struct {
  char* data;
  size_t len;
} Mystr;

void print_mystr(Mystr *s);
Mystr *new_mystr();
char* convertToCStr(Mystr *s);

typedef struct {
  char *dir;
  char *dbfilename;
  int port;
  // master server information
  struct sockaddr_in *master_info;

  char* master_replid;
  int master_repl_offset;
} Config;

void printConfig(Config *config);
void *getConfig(Config *config, char *name);

typedef struct {
  char *key;
  char *val;
  // Expiration time in milliseconds
  int64_t ttl;
} HashMapNode;

HashMapNode *hashmap_node_init();

typedef struct {
  HashMapNode *nodes[MAX_MAP_SIZE];
  int size;
  int capacity;
} HashMap;

HashMap *hashmap_init();
void hashmap_insert(HashMap *h, HashMapNode *node);
HashMapNode *hashmap_get(HashMap *, char *);
char **hashmap_keys(HashMap *h);

typedef struct {
  int conn_fd;
  HashMap *hashmap;
  Config *config;
} Context;


void *connection_handler(void *arg);

void send_response_array(int client_fd, char **items, int size);
void *send_response_bulk_string(int client_fd, char *msg);
#endif
