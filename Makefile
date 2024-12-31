CC = clang
CFLAGS = -std=c23 -Wall -Wextra -O2

build: app/*.c app/*.h
	@$(CC) $(CFLAGS) app/*.c app/*.h

run:
	./your_program.sh
