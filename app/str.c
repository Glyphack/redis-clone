#include "str.h"
#include "arena.h"
#include "types.h"
#include <stdio.h>
#include <string.h>

s8 s8_from_cstr(Arena *arena, const char *cstr) {
    size len = strlen(cstr);
    s8   str;
    str.data = new (arena, u8, len + 1);
    memcpy(str.data, cstr, len);
    str.data[len] = '\0';
    str.len       = len;
    return str;
}

char *s8_to_cstr(Arena *arena, s8 str) {
    char *result = new (arena, char, str.len + 1);
    memcpy(result, str.data, str.len);
    result[str.len] = '\0';
    return result;
}

// Creates a string slice between two pointers
s8 s8span(u8 *start, u8 *end) {
    s8 str;
    str.data = start;
    str.len  = end - start;
    return str;
}

// Checks if two strings are equal
b32 s8equals(s8 a, s8 b) {
    if (a.len != b.len) {
        return false;
    }
    return memcmp(a.data, b.data, a.len) == 0;
}

// Compares two strings lexicographically
size s8compare(s8 a, s8 b) {
    size min_len = a.len < b.len ? a.len : b.len;
    int  result  = memcmp(a.data, b.data, min_len);

    if (result != 0) {
        return result;
    }

    // If the common prefix is the same, longer string is greater
    if (a.len < b.len)
        return -1;
    if (a.len > b.len)
        return 1;
    return 0;
}

// Computes a hash value for the string
u64 s8hash(s8 str) {
    u64 hash = 5381;
    for (size i = 0; i < str.len; i++) {
        hash = ((hash << 5) + hash) + str.data[i];
    }
    return hash;
}

// Removes whitespace from both ends of the string
s8 s8trim(s8 str) {
    u8 *start = str.data;
    u8 *end   = str.data + str.len;

    // Trim leading whitespace
    while (start < end && (*start == ' ' || *start == '\t' || *start == '\n' || *start == '\r')) {
        start++;
    }

    // Trim trailing whitespace
    while (end > start &&
           (*(end - 1) == ' ' || *(end - 1) == '\t' || *(end - 1) == '\n' || *(end - 1) == '\r')) {
        end--;
    }

    return s8span(start, end);
}

// Creates a copy of the string in the provided arena
s8 s8clone(s8 str, Arena *arena) {
    s8 result;
    result.len  = str.len;
    result.data = new (arena, u8, str.len);
    memcpy(result.data, str.data, str.len);
    return result;
}

void s8print(s8 str) {
    printf("%.*s\n", (int) str.len, str.data);
}
