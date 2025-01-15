#include "resp.h"
#include "common.h"
#include "server.h"
#include "str.h"
#include <assert.h>
#include <errno.h>
#include <string.h>
#include <sys/socket.h>

char getCurrChar(RequestParserBuffer *buffer) {
    if (buffer->cursor < buffer->length) {
        return buffer->buffer[buffer->cursor];
    }
    return '\0';
}

char getNextChar(RequestParserBuffer *buffer) {
    if (buffer->cursor < buffer->length) {
        char c = buffer->buffer[buffer->cursor];
        buffer->cursor++;
        return c;
    }
    int bytes_received;

    bytes_received = recv(buffer->client_fd, buffer->buffer, buffer->capacity, 0);
    if (bytes_received == 0) {
        // Indicate we did not get anything
        DEBUG_LOG("read everything from client");
        return '\0';
    }
    if (bytes_received < 0) {
        UNREACHABLE();
    }
    buffer->length = bytes_received;
    buffer->cursor = 0;
    return getNextChar(buffer);
}

int read_len(RequestParserBuffer *buffer) {
    char curr = getCurrChar(buffer), prev = 0;
    int  len = 0;

    while (curr != '\0') {
        curr = getNextChar(buffer);
        // end of the length
        if (curr == '\n' && prev == '\r') {
            break;
        }
        if (curr != '\r') {
            // assert curr is a digit
            assert(curr >= '0' && curr <= '9');
            len = len * 10;
            len += curr - '0';
        }
        prev = curr;
    }
    assert(len >= 0);
    return len;
}

BulkString parse_bulk_string(Arena *arena, RequestParserBuffer *buffer) {
    DEBUG_LOG("parse bulk string");
    int len = read_len(buffer);
    s8  str = (s8) {.len = len, .data = new (arena, u8, len)};

    // TODO memcpy
    for (int i = 0; i < len; i++) {
        str.data[i] = getNextChar(buffer);
    }
    assert(getNextChar(buffer) == '\r');
    assert(getNextChar(buffer) == '\n');

    DEBUG_LOG("parsed bulk string");
    return (BulkString) {.str = str};
}

// Simple strings are encoded as a plus (+) character, followed by a string. The string mustn't
// contain a CR (\r) or LF (\n) character and is terminated by CRLF (i.e., \r\n).
SimpleString parse_simple_string(Arena *arena, RequestParserBuffer *buffer) {
    char         prev;
    char         curr         = getNextChar(buffer);
    s8           str          = {0};
    SimpleString simpleString = {0};
    // TODO: simple strings have no length. We max it to 50 here.
    str.data = new (arena, u8, 50);
    int i    = 0;

    while (curr != '\0') {
        if (curr == '\n' && prev == '\r') {
            return simpleString;
        }
        if (i > 50) {
            // We reached max size of simple string and cannot add anymore. Something is wrong.
            UNREACHABLE();
        }
        if (curr != '\r') {
            str.data[i] = curr;
        }
        i++;
        prev = curr;
        curr = getNextChar(buffer);
    }
    // EOF while expecting the string to be finished
    UNREACHABLE();
}

RespArray parse_resp_array(Arena *arena, RequestParserBuffer *buffer) {
    DEBUG_LOG("parsing resp array");
    RespArray array = {0};
    int       len   = read_len(buffer);
    array.elts      = new (arena, Request *, len);
    array.count     = len;
    for (int i = 0; i < len; i++) {
        Request *element = new (arena, Request, 1);
        *element         = parse_request(arena, buffer);
        array.elts[i]    = element;
    }
    return array;
}

// TODO: Use scratch arena for request parsing.
Request parse_request(Arena *arena, RequestParserBuffer *buffer) {
    Request request = {0};
    char    c       = getNextChar(buffer);
    if (c == '\0') {
        request.empty = 1;
        return request;
    }

    // *1\r\n$4\r\nPING\r\n
    // Skip the initial part until you see something familiar
    while (c != '\0') {
        if (c == '*') {
            request.val                = new (arena, RespArray);
            *(RespArray *) request.val = parse_resp_array(arena, buffer);
            request.type               = ARRAY;
            return request;
        } else if (c == '$') {
            request.val                 = new (arena, BulkString);
            *(BulkString *) request.val = parse_bulk_string(arena, buffer);
            request.type                = BULK_STRING;
            return request;
        } else if (c == '+') {
            SimpleString  simple_str    = parse_simple_string(arena, buffer);
            SimpleString *simple_string = new (arena, SimpleString);
            simple_string->str          = simple_str.str;
            request.val                 = simple_string;
            request.type                = SIMPLE_STRING;
            return request;
        } else if (c == '-') {
            SimpleString simple_str = parse_simple_string(arena, buffer);
            SimpleError *simple_err = new (arena, SimpleError);
            simple_err->str         = simple_str.str;
            request.val             = simple_err;
            request.type            = SIMPLE_ERROR;
            return request;
        } else {
            // A character that we don't recognize
            DEBUG_PRINT(c, c);
            UNREACHABLE();
        }
    }

    request.error = 1;
    return request;
}

void *send_response_bulk_string(Context *ctx, s8 str) {
    // 1 (for '$') + number of digits in str.len + 2 (\r\n) + str.len + 2 (\r\n) + 1 (null
    // terminator)
    int num_len  = 1;
    int len_copy = (int) str.len;
    while (len_copy /= 10)
        num_len++;
    int   response_len = 1 + num_len + 2 + (int) str.len + 2 + 1;
    char *response     = new (&(*ctx->thread_allocator), char, response_len);

    snprintf(response, response_len, "$%zu\r\n%.*s\r\n", str.len, (int) str.len, str.data);
    response[response_len - 1] = '\0';
    printf("responding with `%s`", response);
    int sent = send(ctx->conn_fd, response, response_len - 1, 0);
    if (sent < 0) {
        fprintf(stderr, "Could not send response: %s\n", strerror(errno));
    } else {
        printf("bytes sent %d\n", sent);
    }
    return NULL;
}

void *respond_null(int client_fd) {
    char response[6];
    response[5] = '\0';
    snprintf(response, sizeof(response), "$%d\r\n", -1);
    printf("responding with `%s`", response);
    int sent = send(client_fd, response, 5, 0);
    if (sent < 0) {
        fprintf(stderr, "Could not send response: %s\n", strerror(errno));
    } else {
        printf("bytes sent %d\n", sent);
    }
    return NULL;
}

int send_response(int client_fd, const char *response) {
    int sent = send(client_fd, response, strlen(response), 0);
    if (sent < 0) {
        fprintf(stderr, "Could not send response: %s\n", strerror(errno));
    } else {
        printf("bytes sent %d\n", sent);
    }
    return sent;
}

void send_response_array(int client_fd, char **items, int size) {
    char response[RESPONSE_ITEM_MAX_SIZE * size + size % 10 + 5];
    sprintf(response, "*%d\r\n", size);
    for (int i = 0; i < size; i++) {
        char formatted[RESPONSE_ITEM_MAX_SIZE];
        snprintf(formatted, RESPONSE_ITEM_MAX_SIZE, "$%d\r\n%s\r\n", (int) strlen(items[i]),
                 items[i]);
        strncat(response, formatted, RESPONSE_ITEM_MAX_SIZE);
    }
    send_response(client_fd, response);
}
