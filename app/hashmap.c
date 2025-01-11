#include "hashmap.h"
#include <string.h>
#include <stdio.h>

HashMapNode* hashmap_node_init(Arena* arena) {
    HashMapNode* hNode = new(arena, HashMapNode);
    hNode->key = new(arena, char, MAX_ENTRY_STR_SIZE);
    hNode->val = new(arena, char, MAX_ENTRY_STR_SIZE);
    return hNode;
}

HashMap* hashmap_init(Arena* arena) {
    HashMap* h = new(arena, HashMap);
    h->arena = arena;
    
    for (int i = 0; i < MAX_MAP_SIZE; i++) {
        h->nodes[i] = hashmap_node_init(arena);
    }
    h->size = 0;
    h->capacity = MAX_MAP_SIZE;
    return h;
}

void hashmap_insert(HashMap* h, HashMapNode* node) {
    int index = -1;
    for (int i = 0; i < h->size; i++) {
        HashMapNode* existingNode = h->nodes[i];
        if (strcmp(existingNode->key, node->key) == 0) {
            index = i;
            break;
        }
    }
    if (index != -1) {
        h->nodes[index] = node;
        return;
    }

    if (h->size == h->capacity) {
        printf("hashmap capacity reached");
        return;
    }

    h->nodes[h->size++] = node;
}

HashMapNode* hashmap_get(HashMap* h, char* key) {
    for (int i = 0; i < h->size; i++) {
        if (strcmp(h->nodes[i]->key, key) == 0) {
            return h->nodes[i];
        }
    }
    return NULL;
}

char** hashmap_keys(HashMap* h) {
    char** array = new(h->arena, char*, h->size);
    for (int i = 0; i < h->size; i++) {
        HashMapNode* node = h->nodes[i];
        array[i] = new(h->arena, char, strlen(node->key) + 1);
        strcpy(array[i], node->key);
    }
    return array;
} 