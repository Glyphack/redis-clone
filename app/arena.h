#ifndef ARENA_H
#define ARENA_H
#include "stdlib.h"

typedef struct {
    unsigned char *data;
    int length;
    int offset;
} Arena;

Arena arena_init(int sz);
void *aalloc(Arena *a, size_t sz);
void *aalloc_str(Arena *a, size_t sz);
#endif
