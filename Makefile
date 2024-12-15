CC = gcc
CFLAGS = -std=c2x -Wall -Wextra -O2

build: app/server.c
	$(CC) $(CFLAGS) -o run app/server.c
