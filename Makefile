CC = clang

OUTPUT ?= bin

build: app/*.c app/*.h
	cmake -B build -D CMAKE_BUILD_TYPE=Debug -S .
	cmake --build ./build
	cp ./build/redis $(OUTPUT)

test: 
	python test/test.py

benchmark:
	redis-benchmark -t set -c 50 -n 10000

.PHONY: test run build
