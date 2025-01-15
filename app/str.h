#ifndef STR_H
#define STR_H

#include "types.h"
#include "arena.h"

#define S(s) (s8){.data=(u8 *)s, .len=lengthof(s)}
typedef struct {
    u8  *data;
    size len;
} s8;

s8 s8_from_cstr(Arena *arena, const char *);
char *s8_to_cstr(Arena *arena, s8 );
s8   s8span(u8 *, u8 *); 
b32  s8equals(s8, s8);
size s8compare(s8, s8);
u64  s8hash(s8);
s8   s8trim(s8);
s8   s8clone(s8, Arena *);
void s8print(s8);
#endif
