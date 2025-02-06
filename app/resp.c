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
#include <unistd.h>

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

jmp_buf resp_error_handler;

static void resp_error(RespError code) {
    longjmp(resp_error_handler, code);
}

char getCurrChar(BufferReader *buffer) {
    if (buffer->cursor < buffer->length) {
        return buffer->buffer[buffer->cursor];
    }
    return '\0';
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

u8 peekChar(BufferReader *buffer) {
    int cursor     = buffer->cursor;
    u8  curr       = getNextChar(buffer);
    buffer->cursor = cursor;
    return curr;
}

void append_read_buf(BufferReader *buffer) {
    if (buffer->length >= buffer->capacity) {
        printf("read buffer full\n");
        abort();
    }
    long bytes_received =
        read(buffer->client_fd, buffer->buffer + buffer->length, buffer->capacity - buffer->length);
    if (bytes_received == 0) {
        DEBUG_LOG("read everything from client");
        buffer->status = 0;
        return;
    }
    if (bytes_received < 0) {
        if (errno == EAGAIN) {
            buffer->status = 2;
            return;
        }
        perror("cannot read from client");
        buffer->status = -1;
        return;
    }
    DBG_F("received from %d:", buffer->client_fd);
    s8print((s8) {.len = bytes_received, .data = (u8 *) buffer->buffer + buffer->length});
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
    for (int i = 0; i < len_to_copy; i++) {
        str.data[i] = getNextChar(buffer);
    }

    int is_rdb = 1;
    if (peekChar(buffer) == '\r') {
        getNextChar(buffer);
        if (peekChar(buffer) == '\n') {
            is_rdb = 0;
            getNextChar(buffer);
        }
    }
    return (BulkString) {.str = str, .is_rdb = is_rdb};
}

// Simple strings are encoded as a plus (+) character, followed by a string. The string mustn't
// contain a CR (\r) or LF (\n) character and is terminated by CRLF (i.e., \r\n).
SimpleString parse_simple_string(Arena *arena, BufferReader *buffer) {
    char curr = getNextChar(buffer);
    char prev = curr;
    // TODO: simple strings have no length. We max it to 50 here.
    int MAX_SIMPLE_STRING_SIZE = 1024;
    s8  str                    = {.data = new (arena, u8, MAX_SIMPLE_STRING_SIZE), .len = 0};
    int i                      = 0;

    while (!(prev == '\r' && curr == '\n')) {
        if (curr == '\0') {
            // TODO:Can jump back when we have more data?
            resp_error(RESP_ERR_UNEXPECTED_EOF);
        }
        if (i > MAX_SIMPLE_STRING_SIZE) {
            // We reached max size of simple string and cannot add anymore. Something is wrong.
            resp_error(RESP_ERR_BUFFER_OVERFLOW);
        }
        if (curr != '\r') {
            str.data[i] = curr;
            str.len++;
        }
        i++;
        prev = curr;
        curr = getNextChar(buffer);
    }
    SimpleString simpleString = {.str = str};
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

Request try_parse_request(Arena *arena, BufferReader *buffer) {
    Request result       = {0};
    long    before_parse = buffer->cursor;
    s8      raw          = {0};

    RespError err = setjmp(resp_error_handler);
    if (err) {
        DBG_F("RESP parsing error: %s", resp_error_string(err));
        Request result = {.empty = 1};
        buffer->cursor = 0;
        return result;
    }
    Element element = parse_element(arena, buffer);
    raw.len         = buffer->cursor - before_parse;
    raw.data        = (u8 *) buffer->buffer + before_parse;
    result.element  = element;
    result.empty    = 0;
    result.bytes    = raw;
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
        if (((BulkString *) element.val)->is_rdb) {
            element.type = RDB_MSG;
        } else {
            element.type = BULK_STRING;
        }
    } else if (c == '+') {
        element.val                   = new (arena, SimpleString);
        *(SimpleString *) element.val = parse_simple_string(arena, buffer);
        element.type                  = SIMPLE_STRING;
    } else if (c == '-') {
        element.val                   = new (arena, SimpleString);
        *(SimpleString *) element.val = parse_simple_string(arena, buffer);
        element.type                  = SIMPLE_ERROR;
    } else {
        resp_error(RESP_ERR_UNEXPECTED_EOF);
    }

    return element;
}

int insert_number(Arena *arena, char *dest, int len, int start_pos) {
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
    pos                  = insert_number(arena, (char *) response.data, str.len, pos);

    response.data[pos++] = '\r';
    response.data[pos++] = '\n';

    memcpy(response.data + pos, str.data, str.len);
    pos += str.len;

    response.data[pos++] = '\r';
    response.data[pos++] = '\n';

    return response;
}

s8 serde_int(Arena *arena, int val) {
    int val_size = 1;
    int val_copy = val;
    while (val_copy /= 10)
        val_size++;
    int len         = 1 + val_size + 2;
    s8  str         = (s8) {.data = new (arena, u8, len), .len = len};
    str.data[0]     = ':';
    int pos         = insert_number(arena, (char *) str.data, val, 1);
    str.data[pos++] = '\r';
    str.data[pos++] = '\n';
    return str;
}

// TODO: use s8 instead of char**
s8 serde_array(Arena *arena, char **items, int item_len) {
    size resp_len        = RESPONSE_ITEM_MAX_SIZE * item_len + item_len % 10 + 5;
    s8   response        = (s8) {.len = resp_len, .data = new (arena, u8, resp_len)};
    int  pos             = 0;
    response.data[pos++] = '*';
    pos                  = insert_number(arena, (char *) response.data, item_len, pos);

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
