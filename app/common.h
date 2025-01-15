#ifndef COMMON_H
#define COMMON_H

#include <stddef.h>
#include <stdio.h>

#define DEBUG_PRINT(x, format) printf("%s: %" #format "\n", #x, x)
#define DEBUG_LOG(msg) printf("%s\n", msg)
#define UNREACHABLE() __builtin_unreachable()


#endif 
