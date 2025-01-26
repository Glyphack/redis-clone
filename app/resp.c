#include "resp.h"
#include "common.h"
#include "server.h"
#include "str.h"
#include <assert.h>
#include <errno.h>
#include <setjmp.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>

// Error string lookup function
const char *resp_error_string(RespError err) {
    switch (err) {
    case RESP_OK:
        return "RESP_OK";
    case RESP_ERR_INVALID_START:
        return "RESP_ERR_INVALID_START";
    case RESP_ERR_INVALID_LENGTH:
        return "RESP_ERR_INVALID_LENGTH";
    case RESP_ERR_ARRAY_LENGTH:
        return "RESP_ERR_ARRAY_LENGTH";
    case RESP_ERR_UNEXPECTED_EOF:
        return "RESP_ERR_UNEXPECTED_EOF";
    case RESP_ERR_BUFFER_OVERFLOW:
        return "RESP_ERR_BUFFER_OVERFLOW";
    case RESP_ERR_MALFORMED_STRING:
        return "RESP_ERR_MALFORMED_STRING";
    default:
        return "UNKNOWN_ERROR";
    }
}

// Define the jump buffer
jmp_buf resp_error_handler;

// Helper function to raise parsing errors
static void resp_error(RespError code) {
    longjmp(resp_error_handler, code);
}

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
        printf("read buffer full\n");
        abort();
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
    DEBUG_PRINT_F("received: %.*s\n", (int) bytes_received, buffer->buffer + buffer->length);
    buffer->length += bytes_received;
    buffer->status = 1;
    return;
}

int read_len(BufferReader *buffer) {
    char curr = getNextChar(buffer);
    int  len  = 0;

    while (curr >= '0' && curr <= '9') {
        len = len * 10;
        len += curr - '0';
        fprintf(stderr, "DEBUGPRINT[24]: resp.c:101: len=%d\n", len);
        curr = getNextChar(buffer);
    }
    // end of the length
    if (curr == '\0') {
        resp_error(RESP_ERR_UNEXPECTED_EOF);
    }
    char next = getNextChar(buffer);
    if (curr != '\r' && next != '\n') {
        resp_error(RESP_ERR_INVALID_LENGTH);
    }

    if (len <= 0) {
        fprintf(stderr, "DEBUGPRINT[23]: resp.c:115: len=%d\n", len);
        resp_error(RESP_ERR_INVALID_LENGTH);
    }

    return len;
}

long min(long a, long b) {
    return a < b ? a : b;
}

RdbMessage parse_initial_rdb_transfer(Arena *arena, BufferReader *buffer) {
    char curr = getNextChar(buffer);
    if (curr != '$') {
        resp_error(RESP_ERR_MALFORMED_STRING);
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

    if (getNextChar(buffer) != '\r' || getNextChar(buffer) != '\n') {
        resp_error(RESP_ERR_MALFORMED_STRING);
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

    while (curr == '\n' && prev == '\r') {
        if (curr != '\0') {
            resp_error(RESP_ERR_UNEXPECTED_EOF);
        }
        if (i > MAX_SIMPLE_STRING_SIZE) {
            // We reached max size of simple string and cannot add anymore. Something is wrong.
            resp_error(RESP_ERR_BUFFER_OVERFLOW);
        }
        if (curr != '\r') {
            str.data[i] = curr;
        }
        i++;
        prev = curr;
        curr = getNextChar(buffer);
    }
    resp_error(RESP_ERR_UNEXPECTED_EOF);
    return simpleString;
}

RespArray parse_resp_array(Arena *arena, BufferReader *buffer) {
    DEBUG_LOG("parse array");
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

    RespError err = setjmp(resp_error_handler);
    if (err) {
        DEBUG_PRINT_F("RESP parsing error: %s\n", resp_error_string(err));
        Request result = {0};
        result.empty   = 1;
        result.error   = 1;
        buffer->cursor = 0;
        return result;
    }

    Request result       = {0};
    long    before_parse = buffer->cursor;
    s8      raw          = {0};
    Element element      = parse_element(arena, buffer);
    raw.len              = buffer->cursor - before_parse;
    raw.data             = (u8 *) buffer->buffer + before_parse;
    result.element       = element;
    result.empty         = 0;
    return result;
}

// TODO: Use scratch arena for request parsing.
Element parse_element(Arena *arena, BufferReader *buffer) {
    Element element = {0};
    char    c       = getNextChar(buffer);

    if (c == '*') {
        element.val                = new (arena, RespArray);
        *(RespArray *) element.val = parse_resp_array(arena, buffer);
        element.type               = ARRAY;
    } else if (c == '$') {
        element.val                 = new (arena, BulkString);
        *(BulkString *) element.val = parse_bulk_string(arena, buffer);
        element.type                = BULK_STRING;
    } else if (c == '+') {
        SimpleString  simple_str    = parse_simple_string(arena, buffer);
        SimpleString *simple_string = new (arena, SimpleString);
        simple_string->str          = simple_str.str;
        element.val                 = simple_string;
        element.type                = SIMPLE_STRING;
    } else if (c == '-') {
        SimpleString simple_str = parse_simple_string(arena, buffer);
        SimpleError *simple_err = new (arena, SimpleError);
        simple_err->str         = simple_str.str;
        element.val             = simple_err;
        element.type            = SIMPLE_ERROR;
    } else {
        resp_error(RESP_ERR_UNEXPECTED_EOF);
    }

    return element;
}

int fill_len(Arena *arena, char *dest, int len, int start_pos) {
    int num_len  = 1;
    int len_copy = len;
    while (len_copy /= 10)
        num_len++;
    int   pos      = start_pos;
    char *len_str  = new (&(*arena), char, num_len);
    int   len_pos  = 0;
    int   temp_len = len;
    do {
        len_str[len_pos++] = '0' + (temp_len % 10);
        temp_len /= 10;
    } while (temp_len > 0);
    while (len_pos > 0) {
        dest[pos++] = len_str[--len_pos];
    }
    return pos;
}

s8 serde_bulk_str(Arena *arena, s8 str) {
    int  num_len  = 1;
    size len_copy = str.len;
    while (len_copy /= 10)
        num_len++;
    // 1 (for '$') + number of digits in str.len + 2 (\r\n) + str.len + 2 (\r\n)
    int len              = 1 + num_len + 2 + (int) str.len + 2;
    s8  response         = (s8) {.len = len, .data = new (arena, u8, len)};
    int pos              = 0;
    response.data[pos++] = '$';
    pos                  = fill_len(arena, (char *) response.data, str.len, pos);

    response.data[pos++] = '\r';
    response.data[pos++] = '\n';

    memcpy(response.data + pos, str.data, str.len);
    pos += str.len;

    response.data[pos++] = '\r';
    response.data[pos++] = '\n';

    return response;
}

// TODO: use s8 instead of char**
s8 serde_array(Arena *arena, char **items, int item_len) {
    size resp_len        = RESPONSE_ITEM_MAX_SIZE * item_len + item_len % 10 + 5;
    s8   response        = (s8) {.len = resp_len, .data = new (arena, u8, resp_len)};
    int  pos             = 0;
    response.data[pos++] = '*';
    pos                  = fill_len(arena, (char *) response.data, item_len, pos);

    response.data[pos++] = '\r';
    response.data[pos++] = '\n';

    for (int i = 0; i < item_len; i++) {
        s8 item_formatted = serde_bulk_str(arena, cstr_as_s8(items[i]));
        memcpy(response.data + pos, item_formatted.data, item_formatted.len);
        pos += item_formatted.len;
    }
    response.len = pos;
    return response;
}
