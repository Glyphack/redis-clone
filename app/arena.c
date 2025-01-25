#include "arena.h"
#include "stdlib.h"
#include "types.h"
#include <stddef.h>
#include <stdio.h>
#include <string.h>

Arena newarena(size cap) {
    Arena a  = {0};
    a.offset = malloc(cap);
    a.begin  = a.offset;
    a.end    = a.offset ? a.offset + cap : 0;
    return a;
}

__attribute((malloc, alloc_align(3))) void *alloc(Arena *a, size sz, size align, size count) {
    size padding   = -(uptr) a->offset & (align - 1);
    size available = a->end - a->offset - padding;
    if (available < 0 || count > available / sz) {
        printf("Out of memory");
        abort();
    }
    void *p = a->offset + padding;
    a->offset += padding + count * sz;
    return memset(p, 0, count * sz);
}

void droparena(Arena *a) {
    free(a->begin);
}
