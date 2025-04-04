#ifndef HASHMAP_H
#define HASHMAP_H

#include "str.h"
#include "vec.h"
#include <stdint.h>

typedef struct {
    s8 key;
    s8 val;
    long long ttl;
} HashMapNode;

// Capacity 4^32
struct HashMap {
    struct HashMap *children[4];
    HashMapNode node;
};

typedef struct HashMap HashMap;


// Function declarations
void hashmap_upsert(HashMap**,Arena *, HashMapNode*);
void hashmap_upsert_atomic(HashMap**,Arena *, HashMapNode*);
HashMapNode hashmap_get(HashMap *, s8);
void hashmap_keys(HashMap*, Arena*, vector*);

void hashmap_print(HashMap*);
void hashmapnode_print(HashMapNode *node);

uint64_t hash(s8);

#endif 
