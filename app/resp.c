#include "resp.h"
#include "common.h"
#include "server.h"
#include "str.h"
#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>

char getCurrChar(BufferReader *buffer) {
    if (buffer->cursor < buffer->length) {
        return buffer->buffer[buffer->cursor];
    }
    return '\0';
}

char rewindChar(BufferReader *buffer) {
    if (buffer->cursor > 0) {
        buffer->cursor--;
        buffer->total_read--;
    }
    return buffer->buffer[buffer->cursor];
}

u8 getNextChar(BufferReader *buffer) {
    if (buffer->cursor >= buffer->length) {
        return '\0';
    }
    char c = buffer->buffer[buffer->cursor];
    buffer->cursor++;
    buffer->total_read++;
    return c;
}

void append_read_buf(BufferReader *buffer) {
    if (buffer->length >= buffer->capacity) {
        // Buffer full
        UNREACHABLE();
    }
    long bytes_received = recv(buffer->client_fd, buffer->buffer + buffer->length,
                               buffer->capacity - buffer->length, 0);
    if (bytes_received == 0) {
        DEBUG_LOG("read everything from client");
        buffer->status = 0;
        return;
    }
    if (bytes_received < 0) {
        if (errno == EAGAIN) {
            buffer->status = 1;
            return;
        }
        perror("cannot read from client");
        buffer->status = -1;
        return;
    }
    DEBUG_PRINT_F("received request: %.*s\n", (int) bytes_received, buffer->buffer);
    buffer->length += bytes_received;
    buffer->status = 1;
    return;
}

int read_len(BufferReader *buffer) {
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
            assert_with_print(curr >= '0' && curr <= '9', "%c", curr);
            len = len * 10;
            len += curr - '0';
        }
        prev = curr;
    }
    assert(len >= 0);
    return len;
}

long min(long a, long b) {
    return a < b ? a : b;
}

RdbMessage parse_initial_rdb_transfer(Arena *arena, BufferReader *buffer) {
    char curr = getNextChar(buffer);
    if (curr != '$') {
        printf("Expected $ but got %c\n", curr);
    }
    long len = read_len(buffer);
    s8   raw = (s8) {.len = len, .data = new (arena, u8, len)};

    long len_to_copy = min(buffer->length - buffer->cursor, len);
    memcpy(raw.data, buffer->buffer + buffer->cursor, len_to_copy);
    buffer->cursor += len_to_copy;
    buffer->total_read += len_to_copy;
    len_to_copy = len - len_to_copy;
    if (len_to_copy > 0) {
        for (int i = 0; i < len_to_copy; i++) {
            raw.data[i] = getNextChar(buffer);
        }
    }

    return (RdbMessage) {.raw = raw};
}

BulkString parse_bulk_string(Arena *arena, BufferReader *buffer) {
    long len = read_len(buffer);
    s8   str = (s8) {.len = len, .data = new (arena, u8, len)};

    long len_to_copy = min(buffer->length - buffer->cursor, len);
    memcpy(str.data, buffer->buffer + buffer->cursor, len_to_copy);
    buffer->cursor += len_to_copy;
    buffer->total_read += len_to_copy;
    len_to_copy = len - len_to_copy;
    if (len_to_copy > 0) {
        for (int i = 0; i < len_to_copy; i++) {
            str.data[i] = getNextChar(buffer);
        }
    }

    if (getNextChar(buffer) == '\r') {
        assert(getNextChar(buffer) == '\n');
    } else {
        // TODO: RDB files don't have this
        rewindChar(buffer);
    }

    return (BulkString) {.str = str};
}

// Simple strings are encoded as a plus (+) character, followed by a string. The string mustn't
// contain a CR (\r) or LF (\n) character and is terminated by CRLF (i.e., \r\n).
SimpleString parse_simple_string(Arena *arena, BufferReader *buffer) {
    char         prev;
    char         curr         = getNextChar(buffer);
    s8           str          = {0};
    SimpleString simpleString = {0};
    // TODO: simple strings have no length. We max it to 50 here.
    int MAX_SIMPLE_STRING_SIZE = 1024;
    str.data                   = new (arena, u8, MAX_SIMPLE_STRING_SIZE);
    int i                      = 0;

    while (curr != '\0') {
        if (curr == '\n' && prev == '\r') {
            return simpleString;
        }
        if (i > MAX_SIMPLE_STRING_SIZE) {
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

RespArray parse_resp_array(Arena *arena, BufferReader *buffer) {
    RespArray array = {0};
    int       len   = read_len(buffer);
    array.elts      = new (arena, Element *, len);
    array.count     = len;
    for (int i = 0; i < len; i++) {
        Element *element = new (arena, Element, 1);
        *element         = parse_element(arena, buffer);
        array.elts[i]    = element;
    }
    return array;
}

// TODO return if parse failed quickly. Check paths and return as soon as it's not what expected.
Request try_parse_request(Arena *arena, BufferReader *buffer) {
    Element element = parse_element(arena, buffer);
    int     empty   = 0;
    if (element.error) {
        empty = 1;
    }
    return (Request) {.empty = empty, .element = element};
}

// TODO: Use scratch arena for request parsing.
Element parse_element(Arena *arena, BufferReader *buffer) {
    Element element = {0};
    char    c       = getNextChar(buffer);
    if (c == '\0') {
        return element;
    }

    // Skip the initial part until you see something familiar
    while (c != '\0') {
        if (c == '*') {
            element.val                = new (arena, RespArray);
            *(RespArray *) element.val = parse_resp_array(arena, buffer);
            element.type               = ARRAY;
            return element;
        } else if (c == '$') {
            element.val                 = new (arena, BulkString);
            *(BulkString *) element.val = parse_bulk_string(arena, buffer);
            element.type                = BULK_STRING;
            return element;
        } else if (c == '+') {
            SimpleString  simple_str    = parse_simple_string(arena, buffer);
            SimpleString *simple_string = new (arena, SimpleString);
            simple_string->str          = simple_str.str;
            element.val                 = simple_string;
            element.type                = SIMPLE_STRING;
            return element;
        } else if (c == '-') {
            SimpleString simple_str = parse_simple_string(arena, buffer);
            SimpleError *simple_err = new (arena, SimpleError);
            simple_err->str         = simple_str.str;
            element.val             = simple_err;
            element.type            = SIMPLE_ERROR;
            return element;
        } else {
            // A character that we don't recognize
            DEBUG_PRINT(c, c);
            UNREACHABLE();
        }
    }

    return element;
}

s8 serde_bulk_str(Arena *arena, s8 str) {
    // 1 (for '$') + number of digits in str.len + 2 (\r\n) + str.len + 2 (\r\n)
    int num_len  = 1;
    int len_copy = (int) str.len;
    while (len_copy /= 10)
        num_len++;
    int len      = 1 + num_len + 2 + (int) str.len + 2;
    s8  response = (s8) {.len = len, .data = new (arena, u8, len)};

    int pos              = 0;
    response.data[pos++] = '$';

    char len_str[num_len];
    int  len_pos  = 0;
    int  temp_len = (int) str.len;
    do {
        len_str[len_pos++] = '0' + (temp_len % 10);
        temp_len /= 10;
    } while (temp_len > 0);
    while (len_pos > 0) {
        response.data[pos++] = len_str[--len_pos];
    }

    response.data[pos++] = '\r';
    response.data[pos++] = '\n';

    memcpy(response.data + pos, str.data, str.len);
    pos += str.len;

    response.data[pos++] = '\r';
    response.data[pos++] = '\n';

    DEBUG_LOG("responding with");
    s8print(response);
    return response;
}

void *respond_null(int client_fd) {
    char response[6];
    response[5] = '\0';
    snprintf(response, sizeof(response), "$%d\r\n", -1);
    DEBUG_PRINT_F("responding with `%s`", response);
    long sent = send(client_fd, response, 5, 0);
    if (sent < 0) {
        fprintf(stderr, "Could not send response: %s\n", strerror(errno));
    } else {
    }
    return NULL;
}

long send_response(int client_fd, const char *response) {
    DEBUG_PRINT(response, s);
    DEBUG_PRINT(strlen(response), lu);
    long sent = send(client_fd, response, strlen(response), 0);
    if (sent < 0) {
        fprintf(stderr, "Could not send response: %s\n", strerror(errno));
    }
    return sent;
}

// TODO: use s8 instead of char**
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
