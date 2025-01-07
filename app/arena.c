#include "arena.h"
#include "stdlib.h"

Arena arena_init(int sz) {
  unsigned char *memory = malloc(sz);
  Arena arena = {.data = memory, .length = sz, .offset = 0};
  return arena;
}

void *aalloc(Arena *a, size_t sz) {
  if (a->offset + sz > a->length) {
    return NULL; // Out of memory
  }
  void *p = &a->data[a->offset];
  a->offset += sz;
  return p;
}

void *aalloc_str(Arena *a, size_t sz) { return aalloc(a, sz + 1); }
