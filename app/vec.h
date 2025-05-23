#ifndef VEC_H
#define VEC_H
#include <stdlib.h>

typedef struct {
    void ** items;
    int capacity;
    int total;
    int item_size;
} vector;

vector *initialize_vec();
void push_vec(vector*, void*);
void *get_vec(vector*, int );

#endif
