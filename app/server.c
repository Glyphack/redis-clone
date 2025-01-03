#include "./server.h"
#include "vec.h"
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

#define UNREACHABLE()                                                          \
  do {                                                                         \
    fprintf(stderr, "Unreachable code reached at %s:%d\n", __FILE__,           \
            __LINE__);                                                         \
    exit(EXIT_FAILURE);                                                        \
  } while (0)

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
  printf("config:\n");
  printf("  dir `%s`\n", config->dir);
  printf("  dbfilename `%s`\n", config->dbfilename);
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

char *read_source_file(FILE *fp) {
  char *data = NULL;
  struct stat st;

  if (fstat(fileno(fp), &st) == -1)
    goto ret;

  data = calloc(st.st_size + 1, sizeof(char));
  if (!data)
    goto ret;

  int rd = fread(data, sizeof(char), st.st_size, fp);
  if (rd != st.st_size) {
    data = NULL;
    goto ret;
  }
  data[st.st_size] = '\0';

ret:
  return data;
}

uint16_t read_little_endian_16(char *bytes) {
  return (uint16_t)(bytes[0] | (bytes[1] << 8));
}

uint32_t read_little_endian_32(char *bytes) {
  return (uint32_t)(bytes[0] | (bytes[1] << 8) | (bytes[2] << 16) |
                    (bytes[3] << 24));
}

int64_t read_little_endian_64(char *bytes) {
  int64_t result;
  memcpy(&result, bytes, 8);
  return result;
}

typedef struct {
  int int_value;
  Mystr *str_value;
} AuxValue;

void print_rdb_value(AuxValue *aux_value) {
  if (aux_value->str_value == NULL) {
    printf("%d\n", aux_value->int_value);
  } else {
    print_mystr(aux_value->str_value);
  }
}

int parse_rdb_len_encoding(char **cursor, int *special_format) {
  char *p = *cursor;
  *special_format = -1;
  int mode = (*p & 0b11000000) >> 6;
  int len = 0;
  // The next 6 bits represent the length
  if (mode == 0) {
    len = (unsigned char)*p;
    p++;
  }
  //  Read one additional byte.The combined 14 bits represent the length
  else if (mode == 1) {
    uint8_t byte1 = *p;
    p++;
    uint8_t byte2 = *p;
    // Mask the lower 6 bits of byte1 (0x3F = 00111111 in binary)
    // Then shift them left by 8 bits, leaving room for the next 8 bits from
    // byte2.
    uint16_t combined = ((uint16_t)(byte1 & 0x3F) << 8) | byte2;
    len = (int)combined;
  }
  // Discard the remaining 6 bits. The next 4 bytes from the stream represent
  // the length
  else if (mode == 2) {
    p++;
    uint8_t byte1 = *p;
    p++;
    uint8_t byte2 = *p;
    p++;
    uint8_t byte3 = *p;
    p++;
    uint8_t byte4 = *p;
    uint32_t combined = ((uint32_t)(byte1) << 24) | ((uint32_t)byte2 << 16) |
                        ((uint32_t)byte3 << 8) | byte4;
    len = (int)combined;
  }
  // The next object is encoded in a special format. The remaining 6 bits
  // indicate the format. May be used to store numbers or Strings.
  else if (mode == 3) {
    int rem_6_bits = (uint8_t)*p & 0b00111111;
    *special_format = rem_6_bits;
    p++;
  }

  *cursor = p;
  return len;
}

int parse_len_encoded_string(char **cursor, int size, AuxValue *aux_value) {
  char *p = *cursor;
  if (size <= 0) {
    fprintf(stderr, "invalid size %d\n", size);
    return 1;
  }
  aux_value->str_value = (Mystr *)malloc(sizeof(Mystr));
  aux_value->str_value->data = p;
  aux_value->str_value->len = size;
  p = p + size;
  *cursor = p;
  return 0;
}

int parse_integer_encoded_as_string(char **cursor, int special_mode) {
  // 0 indicates that an 8 bit integer follows
  int result = 0;
  char *p = *cursor;
  if (special_mode == 0) {
    result = (int)*p;
    p++;
  } else if (special_mode == 1) {
    uint16_t num = read_little_endian_16(p);
    result = num;
    p += 2;
  } else if (special_mode == 2) {
    uint32_t num = read_little_endian_32(p);
    result = num;
    p += 4;
  } else {
    printf("unreachable");
    exit(1);
    UNREACHABLE();
  }

  *cursor = p;
  return result;
}

void printRdb(const RdbContent *rdb) {
  printf("header is\n");
  print_mystr(rdb->header);

  int is_name = 1;

  printf("auxiliary fields\n");
  for (int i = 0; i < rdb->metadata->total; i++) {
    if (is_name) {
      print_mystr(rdb->metadata->items[i]);
      is_name = 0;
    } else {
      AuxValue *aux_value = (AuxValue *)rdb->metadata->items[i];
      print_rdb_value(aux_value);
      is_name = 1;
    }
  }

  printf("databases\n");
  for (int i = 0; i < rdb->databases->total; i++) {
    RdbDatabase *db = (RdbDatabase *)rdb->databases->items[i];
    printf("database number: %d\n", db->num);
    printf("hash size: %d\n", db->resizedb_hash);
    printf("expiry size: %d\n", db->resizedb_expiry);
    for (int j = 0; j < db->data->size; j++) {
      printf("key : %s\n", db->data->nodes[j]->key);
      printf("value : %s\n", db->data->nodes[j]->val);
      printf("expiration: %lld\n", db->data->nodes[j]->ttl);
    }
  }
}

RdbContent *parse_rdb(char *path) {
  RdbContent *rdbContent = (RdbContent *)malloc(sizeof(RdbContent));
  rdbContent->header = NULL;
  rdbContent->metadata = initialize_vec();

  FILE *fp;
  char *content;
  fp = fopen(path, "rb");
  if (fp == NULL) {
    fprintf(stderr, "opening file: %s\n", path);
    perror("Error opening file");
    return NULL;
  }
  content = read_source_file(fp);
  int content_size = strlen(content);
  char *p = content;

  // Keep track of what section of the rdb file we are at
  // Starting at 0 which is the header, the first section.
  int section_number = 0;

  while (*p != '\0') {
    if (0 == section_number) {
      Mystr *header = new_mystr();
      printf("reading header\n");
      header->data = p;
      int header_len = 0;
      while ((unsigned char)*p != 0xFA) {
        header_len++;
        p++;
      }
      header->len = header_len;
      rdbContent->header = header;
      section_number++;
      // Consuming the fa
      p++;
    }
    if (1 == section_number) {
      printf("reading metadata\n");
      while ((unsigned char)*p != 0xFE) {
        AuxValue *name = (AuxValue *)malloc(sizeof(AuxValue));
        AuxValue *value = (AuxValue *)malloc(sizeof(AuxValue));
        value->int_value = 0;
        value->str_value = NULL;

        // TODO error
        int special_format;
        int len = parse_rdb_len_encoding(&p, &special_format);
        parse_len_encoded_string(&p, len, name);
        push_vec(rdbContent->metadata, name->str_value);

        len = parse_rdb_len_encoding(&p, &special_format);
        if (special_format == -1) {
          parse_len_encoded_string(&p, len, value);
        } else {
          int result = parse_integer_encoded_as_string(&p, special_format);
          value->int_value = result;
        }
        push_vec(rdbContent->metadata, value);

        if ((unsigned char)*p == 0xFA) {
          p++;
        } else {
          if ((unsigned char)*p == 0xFE) {
            section_number++;
            // Consume FE
            p++;
            break;
          }
          fprintf(stderr, "expected fa at beginning of auxiliary field");
          return NULL;
        }
      }
    }

    // Database Selection
    if (2 == section_number) {
      rdbContent->databases = initialize_vec();
      printf("reading database section\n");
      // TODO: There can be multiple databases
      RdbDatabase *rdb_db = (RdbDatabase *)malloc(sizeof(RdbDatabase));
      rdb_db->data = hashmap_init();
      int special_format;
      int db_num = parse_rdb_len_encoding(&p, &special_format);
      if (special_format != -1) {
        fprintf(stderr, "expected special format to be 0 for database id");
      }
      rdb_db->num = db_num;
      // resizedb information
      if ((unsigned char)*p == 0xFB) {
        // Consume FB
        p++;
        int sf;
        int db_hash_size = parse_rdb_len_encoding(&p, &sf);
        if (sf != -1) {
          fprintf(stderr, "expected special format to be 0 for database id");
        }
        int db_expiry_size = parse_rdb_len_encoding(&p, &sf);
        if (sf != -1) {
          fprintf(stderr, "expected special format to be 0 for database id");
        }
        // The sizes are always integers
        rdb_db->resizedb_hash = db_hash_size;
        rdb_db->resizedb_expiry = db_expiry_size;
      }

      // read db keys
      printf("reading key val section\n");
      while ((unsigned char)*p != 0xFE && (unsigned char)*p != 0xFF) {
        int expire_time_kind = RDB_KEY_EXP_KIND_NO_TTL;
        int64_t expire_time = -1;

        // "expiry time in seconds", followed by 4 byte unsigned int
        if ((unsigned char)*p == 0xFD) {
          p++;
          expire_time_kind = RDB_KEY_EXP_KIND_S;
          expire_time = read_little_endian_32(p);
          p += 4;
        }

        // "expiry time in ms", followed by 8 byte unsigned long
        if ((unsigned char)*p == 0xFC) {
          p++;
          expire_time_kind = RDB_KEY_EXP_KIND_MS;
          expire_time = read_little_endian_64(p);
          p += 8;
        }

        fprintf(stderr, "expiration time: %lld\n", expire_time);
        DEBUG_PRINT(expire_time, lld);

        // 0 = String Encoding
        // 1 = List Encoding
        // 2 = Set Encoding
        // 3 = Sorted Set Encoding
        // 4 = Hash Encoding
        // 9 = Zipmap Encoding
        // 10 = Ziplist Encoding
        // 11 = Intset Encoding
        // 12 = Sorted Set in Ziplist Encoding
        // 13 = Hashmap in Ziplist Encoding (Introduced in RDB version 4)
        // 14 = List in Quicklist encoding (Introduced in RDB version 7)
        int value_type = (int)*p;

        fprintf(stderr, "value type %d\n", (int)*p);
        // read type
        p++;

        HashMapNode *node = hashmap_node_init();
        switch (value_type) {
        case 0:
          printf("key type string\n");
          AuxValue key, value;
          int sf, len;
          len = parse_rdb_len_encoding(&p, &sf);
          if (sf != -1) {
            fprintf(stderr,
                    "expected special format to be 0 for string key val");
          }
          parse_len_encoded_string(&p, len, &key);
          sf = 0;
          len = parse_rdb_len_encoding(&p, &sf);
          if (sf != -1) {
            fprintf(stderr,
                    "expected special format to be 0 for string key val");
          }
          parse_len_encoded_string(&p, len, &value);
          node->key = convertToCStr(key.str_value);
          node->val = convertToCStr(value.str_value);
          break;
        default:
          UNREACHABLE();
          break;
        }

        switch (expire_time_kind) {
        case RDB_KEY_EXP_KIND_NO_TTL:
          node->ttl = -1;
          break;
        case RDB_KEY_EXP_KIND_S:
          node->ttl = expire_time * 1000;
          break;
        case RDB_KEY_EXP_KIND_MS:
          node->ttl = expire_time;
          break;
        default:
          fprintf(stderr, "unknown expiration time");
          break;
        }

        hashmap_insert(rdb_db->data, node);
      }

      push_vec(rdbContent->databases, rdb_db);

      if ((unsigned char)*p == 0xFE) {
        // TODO: Support more than 1 database section
        printf("only one database section is supported");
        exit(1);
      }

      // End of RDB
      if ((unsigned char)*p == 0xFF) {
        section_number++;
      }
    }

    // end of RDB
    if (3 == section_number) {
      // CRC checksum
      p += 8;
    }
    p++;
  }

  printf("parsing RDB finished\n");

  fclose(fp);
  return rdbContent;
}

int main(int argc, char *argv[]) {
  // Disable output buffering for testing
  setbuf(stdout, NULL);
  setbuf(stderr, NULL);

  Config *config = (Config *)malloc(sizeof(Config));
  config->dbfilename = (char *)malloc(0);
  config->dir = (char *)malloc(0);

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
    }
  }

  printConfig(config);

  HashMap *hashmap = hashmap_init();

  char full_path[MAX_PATH];

  snprintf(full_path, sizeof(full_path), "%s%s%s", config->dir, "/",
           config->dbfilename);
  if (config->dir && config->dbfilename) {
    RdbContent *rdb = parse_rdb(full_path);
    if (rdb != NULL) {
      printRdb(rdb);
      RdbDatabase *db = (RdbDatabase *)rdb->databases->items[0];
      hashmap = db->data;
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

void *respond_with_msg(int client_fd, char *msg) {
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
          respond_with_msg(ctx->conn_fd, parts[i]);
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
          respond_with_msg(ctx->conn_fd, "OK");
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
          respond_with_msg(ctx->conn_fd, node->val);
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
