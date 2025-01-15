#include "hashmap.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

HashMapNode *hashmap_node_init() {
    HashMapNode *hNode = (HashMapNode *) malloc(sizeof(HashMapNode));
    hNode->key         = (char *) malloc(MAX_ENTRY_STR_SIZE);
    hNode->val         = (char *) malloc(MAX_ENTRY_STR_SIZE);
    return hNode;
}

HashMap *hashmap_init() {
    HashMap *h = malloc(sizeof(HashMap));

    for (int i = 0; i < MAX_MAP_SIZE; i++) {
        h->nodes[i] = 0;
    }
    h->size     = 0;
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
        return;
    }

    if (h->size == h->capacity) {
        printf("hashmap capacity reached");
        return;
    }

    h->nodes[h->size++] = node;
}

HashMapNode *hashmap_get(HashMap *h, char *key) {
    for (int i = 0; i < h->size; i++) {
        if (strcmp(h->nodes[i]->key, key) == 0) {
            return h->nodes[i];
        }
    }
    return NULL;
}

char **hashmap_keys(HashMap *h) {
    char **array = (char **) malloc(h->size * sizeof(char *));
    for (int i = 0; i < h->size; i++) {
        HashMapNode *node = h->nodes[i];
        array[i]          = (char *) malloc((strlen(node->key) + 1) * sizeof(char));
        strcpy(array[i], node->key);
    }
    return array;
}
