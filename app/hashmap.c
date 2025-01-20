#include "hashmap.h"
#include "common.h"
#include "str.h"
#include "types.h"
#include "vec.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void hashmap_upsert(HashMap **map, Arena *arena, HashMapNode *node) {
    for (u64 h = hash(node->key); *map; h <<= 2) {
        if (s8equals((*map)->node.key, node->key)) {
            (*map)->node.val = node->val;
            (*map)->node.ttl = node->ttl;
            return;
        }
        map = (HashMap **) &(*map)->children[h >> 62];
    }
    *map             = new (arena, HashMap);
    (*map)->node.key = node->key;
    (*map)->node.val = node->val;
    (*map)->node.ttl = node->ttl;
}

void hashmap_upsert_atomic(HashMap **map, Arena *arena, HashMapNode *input) {
    for (u64 h = hash(input->key);; h <<= 2) {
        HashMap *n = __atomic_load_n(map, __ATOMIC_ACQUIRE);
        if (!n) {
            Arena rollback = *arena;
            HashMap *new   = new (arena, HashMap);
            new->node.key  = input->key;
            new->node.val  = input->val;
            new->node.ttl  = input->ttl;
            int pass       = __ATOMIC_RELEASE;
            int fail       = __ATOMIC_ACQUIRE;
            if (__atomic_compare_exchange_n(map, &n, new, false, pass, fail)) {
                return;
            }
            *arena = rollback;
        } else if (s8equals(n->node.key, input->key)) {
            return;
        }
        map = n->children + (h >> 62);
    }
}

HashMapNode hashmap_get(HashMap *map, s8 key) {
    for (u64 h = hash(key); map; h <<= 2) {
        if (s8equals(map->node.key, key)) {
            return (HashMapNode) {.key = map->node.key, .val = map->node.val, .ttl = map->node.ttl};
        }
        map = (HashMap *) (map)->children[h >> 62];
    }
    return (HashMapNode) {0};
}

void hashmap_keys(HashMap *h, vector *result) {
    if (!h) {
        return;
    }
    if (h->node.key.len == 0) {
        return;
    }
    char *key = malloc(h->node.key.len + 1);
    memcpy(key, h->node.key.data, h->node.key.len);
    key[h->node.key.len] = '\0';
    push_vec(result, key);
    for (int i = 0; i < 4; i++) {
        if (h->children[i]) {
            hashmap_keys((HashMap *) h->children[i], result);
        }
    }
}

u64 hash(s8 s) {
    u64 h = 0x100;
    for (ptrdiff_t i = 0; i < s.len; i++) {
        h ^= s.data[i];
        h *= 1111111111111111111u;
    }
    return h;
}

void hashmap_print(HashMap *h) {
    if (h == NULL) {
        return;
    }
    printf("key: %s, val: %s exp: %lld\n", h->node.key.data, h->node.val.data, h->node.ttl);
    for (int i = 0; i < 4; i++) {
        hashmap_print((HashMap *) h->children[i]);
    }
}
