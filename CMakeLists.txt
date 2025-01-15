cmake_minimum_required(VERSION 3.13)
set(CMAKE_C_COMPILER gcc)
set(CMAKE_CXX_COMPILER gcc)
set(CMAKE_MAKE_PROGRAM make)

set(CMAKE_C_FLAGS
    "${CMAKE_C_FLAGS} -march=native -m64 -Wall -Wextra -Wconversion -Wno-incompatible-pointer-types-discards-qualifiers -Wno-pointer-sign -Wno-sign-conversion -Wdouble-promotion -Wvla -fsanitize=undefined"
)

project(redis)

file(GLOB_RECURSE SOURCE_FILES app/*.c app/*.h)

set(CMAKE_C_STANDARD 23)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_executable(redis ${SOURCE_FILES})
