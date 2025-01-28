#include <errno.h>
#include <netdb.h>
#include <stdio.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

static int request(int c) {
    for (char *s = "*1\r\n$4\r\nPING\r\n"; *s; s++) {
        if (write(c, s, 1) == -1) {
            return -1;
        }
        if (usleep(100) == -1) {
            return -1;
        }
    }
    return 0;
}

static int response(int c) {
    char buf[1 << 12];
    for (;;) {
        int len = read(c, buf, sizeof(buf));
        switch (len) {
        case -1:
            switch (errno) {
            case EINTR:
                continue;
            default:
                return -1;
            }
        case 0:
            return !fflush(stdout) ? -1 : 0;
        default:
            if (!fwrite(buf, len, 1, stdout)) {
                return -1;
            }
        }
    }
}

int main(void) {
    struct addrinfo *r = 0, hints = {
                                .ai_family   = AF_UNSPEC,
                                .ai_socktype = SOCK_STREAM,
                            };

    if (getaddrinfo("localhost", "6379", &hints, &r)) {
        perror("getaddrinfo()");
        return 1;
    }

    for (struct addrinfo *a = r; a; a = a->ai_next) {
        int c = socket(a->ai_family, a->ai_socktype, a->ai_protocol);
        if (c == -1) {
            perror("socket()");
            continue;
        }

        if (connect(c, a->ai_addr, a->ai_addrlen) == -1) {
            perror("connect()");
            close(c);
            continue;
        }
        freeaddrinfo(r);

        if (request(c) == -1) {
            perror("HTTP request");
        }
        if (response(c) == -1) {
            perror("HTTP response");
        }
        close(c);
        return 0;
    }
    return 1;
}
