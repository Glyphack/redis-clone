cmake_minimum_required(VERSION 3.13)

# debug build
set(CMAKE_C_FLAGS
    "${CMAKE_C_FLAGS} -march=native -m64 -O3 -flto -Wall -Wextra -Wconversion -Wno-incompatible-pointer-types-discards-qualifiers -Wno-pointer-sign -Wno-sign-conversion -Wdouble-promotion -Wvla"
)

# set(CMAKE_C_FLAGS
#     "${CMAKE_C_FLAGS} -march=native -m64 -O3 -Wall -Wextra -Wconversion -Wno-incompatible-pointer-types-discards-qualifiers -Wno-pointer-sign -Wno-sign-conversion -Wdouble-promotion -Wvla"
# )
# add_definitions(-DNODEBUG)

project(redis)

file(GLOB_RECURSE SOURCE_FILES app/*.c app/*.h)

set(CMAKE_C_STANDARD 23)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_executable(redis ${SOURCE_FILES})
