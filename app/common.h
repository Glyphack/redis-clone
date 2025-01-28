#ifndef COMMON_H
#define COMMON_H

#include <stddef.h>
#include <stdio.h>

#ifdef NODEBUG
#define DBG(x, format) /* printf("%s: %" #format "\n", #x, x) */
#define DEBUG_LOG(msg) /* printf("%s\n", msg) */
#define DBG_F(format, ...) /* printf(format, __VA_ARGS__) */
#else
#define DBG(x, format) printf("%s: %" #format "\n", #x, x)
#define DEBUG_LOG(msg) printf("%s\n", msg)
#define DBG_F(format, ...) printf(format, __VA_ARGS__)
#endif

#define UNREACHABLE() __builtin_unreachable()
#define assert_with_print(cond, format, ...) \
    if (!(cond)) { \
        printf(format, __VA_ARGS__); \
        assert(cond); \
    }


#endif 
