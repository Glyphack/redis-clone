#ifndef HASHMAP_H
#define HASHMAP_H

#include "arena.h"

#define MAX_ENTRY_STR_SIZE 1024
#define MAX_MAP_SIZE 1024

typedef struct {
    char* key;
    char* val;
    long long ttl;
} HashMapNode;

typedef struct {
    HashMapNode* nodes[MAX_MAP_SIZE];
    int size;
    int capacity;
    Arena* arena;  // Added arena field
} HashMap;

// Function declarations
HashMap* hashmap_init();
HashMapNode* hashmap_node_init();
void hashmap_insert(HashMap* h, HashMapNode* node);
HashMapNode* hashmap_get(HashMap* h, char* key);
char** hashmap_keys(HashMap* h);

#endif 
