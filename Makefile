CC = clang
CFLAGS = -std=c23 -Wall -Wextra -O2

OUTPUT ?= bin

build: app/*.c app/*.h
	cmake -B build -D CMAKE_BUILD_TYPE=Debug -S .
	cmake --build ./build
	cp ./build/redis $(OUTPUT)

test: 
	python test/test.py

.PHONY: test run build
