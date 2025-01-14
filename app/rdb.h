#ifndef RDB_H
#define RDB_H

#include "hashmap.h"
#include "vec.h"
#include "str.h"

#define RDB_KEY_EXP_KIND_NO_TTL 0
#define RDB_KEY_EXP_KIND_S 1
#define RDB_KEY_EXP_KIND_MS 2
#define RDB_SIZE_8_BIT 1
#define RDB_SIZE_32_BIT 1
#define RDB_SIZE_6_BIT_LEN 1

typedef struct RdbDatabase {
  int num;
  int resizedb_hash;
  int resizedb_expiry;
  HashMap *data;
} RdbDatabase;

typedef struct RdbContent {
  s8 header;
  vector *metadata;
  vector *databases;
} RdbContent;

RdbContent *parse_rdb(Arena *, char *);
void print_rdb(const RdbContent*);

#endif
