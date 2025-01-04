CC = clang
CFLAGS = -std=c23 -Wall -Wextra -O2

OUTPUT ?= build/bin

build: app/*.c app/*.h
	@$(CC) $(CFLAGS) app/*.c -o $(OUTPUT)

run:
	./your_program.sh

test: 
	python test/test.py

.PHONY: test
