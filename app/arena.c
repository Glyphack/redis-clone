#include "arena.h"
#include "common.h"
#include "stdlib.h"
#include "types.h"

#include <stddef.h>
#include <stdio.h>
#include <string.h>

#if __APPLE__
#define MAX_FRAMES 100
#include <execinfo.h>
void print_stacktrace() {
    void  *buffer[MAX_FRAMES];
    int    nptrs   = backtrace(buffer, MAX_FRAMES);
    char **strings = backtrace_symbols(buffer, nptrs);
    if (strings == NULL) {
        perror("backtrace_symbols");
        return;
    }

    printf("Stack trace (most recent call first):\n");
    for (int i = 0; i < nptrs; i++) {
        printf("%s\n", strings[i]);
    }
    free(strings);
}
#else
void print_stacktrace() {
}
#endif

Arena newarena(size cap) {
    Arena a  = {0};
    a.offset = malloc(cap);
    a.begin  = a.offset;
    a.end    = a.offset ? a.offset + cap : 0;
    return a;
}

__attribute((malloc, alloc_align(3))) void *alloc(Arena *a, size sz, size align, size count) {
    size padding   = -(uintptr_t) a->offset & (align - 1);
    size available = a->end - a->offset - padding;

    if (available < 0 || count > available / sz) {
        printf("Out of memory\n");
        print_stacktrace();
        abort();
    }
    void *p = a->offset + padding;
    a->offset += padding + count * sz;
    return p;
}

void droparena(Arena *a) {
    free(a->begin);
}
