#include "vec.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// TODO: Make this work with arena
vector *initialize_vec() {
    vector *v   = (vector *) malloc(sizeof(vector));
    v->total    = 0;
    v->capacity = 1;
    v->items    = malloc(v->capacity * sizeof(void *));

    return v;
}

void push_vec(vector *vec, void *item) {
    if (vec->total == vec->capacity) {
        int   new_cap   = vec->capacity * 2;
        void *new_items = realloc(vec->items, new_cap * sizeof(void *));
        if (new_items == NULL) {
            fprintf(stderr, "cannot reallocate vector");
            return;
        }
        vec->items    = new_items;
        vec->capacity = new_cap;
    }
    vec->items[vec->total] = item;
    vec->total++;
}

void *get_vec(vector *vec, int index) {
    if (index >= vec->total) {
        fprintf(stderr, "tried to access index greater than vector items: %d\n", index);
        exit(1);
        return NULL;
    }
    void *val = vec->items[index];
    return val;
}
