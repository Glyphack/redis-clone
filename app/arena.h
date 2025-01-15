#ifndef USERS_LIGHT_PROGRAMMING_REDIS_CLONE_APP_ARENA_H
#define USERS_LIGHT_PROGRAMMING_REDIS_CLONE_APP_ARENA_H
#include "types.h"

#define mysizeof(x)    sizeof(x)
#define countof(a)   (mysizeof(a) / mysizeof(*(a)))
#define lengthof(s)  (countof(s) - 1)
#define strlength(s) (strlen(s) + 1)

#define new(...) newx(__VA_ARGS__, new4, new3, new2)(__VA_ARGS__)
#define newx(a, b, c, d, e, ...) e
#define new2(a, t) (t *)alloc(a, mysizeof(t), __alignof__(t), 1)
#define new3(a, t, n) (t *)alloc(a, mysizeof(t), __alignof__(t), n)
#define new4(a, t, n, f) (t *)alloc(a, mysizeof(t), __alignof__(t), n, f)

typedef struct {
  byte *offset;
  byte *begin;
  byte *end;
} Arena;

Arena newarena(size cap);
void droparena(Arena *a);
void *alloc(Arena *a, size sz, size align, size count);

#endif
