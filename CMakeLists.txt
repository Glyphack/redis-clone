cmake_minimum_required(VERSION 3.13)
set(CMAKE_C_COMPILER gcc)
set(CMAKE_CXX_COMPILER gcc)
set(CMAKE_MAKE_PROGRAM make)

project(redis)

file(GLOB_RECURSE SOURCE_FILES app/*.c app/*.h)

set(CMAKE_C_STANDARD 23)
set(CMAKE_C_STANDARD_REQUIRED ON)

add_executable(redis ${SOURCE_FILES})
