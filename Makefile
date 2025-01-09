CC = clang
CFLAGS = -std=c23 -Wall -Wextra -O2

OUTPUT ?= bin

build: app/*.c app/*.h
	@$(CC) $(CFLAGS) app/*.c -o $(OUTPUT)

run:
	./your_program.sh $(ARGS)
	rm /tmp/codecrafters-build-redis-c

test: 
	python test/test.py

.PHONY: test run
