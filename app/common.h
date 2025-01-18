#ifndef COMMON_H
#define COMMON_H

#include <stddef.h>
#include <stdio.h>

#define DEBUG_PRINT(x, format) /* printf("%s: %" #format "\n", #x, x) */
#define DEBUG_LOG(msg) /* printf("%s\n", msg) */
#define DEBUG_PRINT_F(format, ...) /* printf(format, __VA_ARGS__) */
#define UNREACHABLE() __builtin_unreachable()
#define assert_with_print(cond, format, ...) \
    if (!(cond)) { \
        printf(format, __VA_ARGS__); \
        assert(cond); \
    }


#endif 
