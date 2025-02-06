#include "rdb.h"
#include "common.h"
#include "hashmap.h"
#include "str.h"
#include "types.h"
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

u8 *read_source_file(Arena *arena, FILE *fp) {
    u8         *data = NULL;
    struct stat st;

    if (fstat(fileno(fp), &st) == -1)
        goto ret;

    data = new (arena, u8, st.st_size + 1);
    if (!data)
        goto ret;

    i64 rd = fread(data, sizeof(u8), st.st_size, fp);
    if (rd != st.st_size) {
        data = NULL;
        goto ret;
    }
    data[st.st_size] = '\0';

ret:
    return data;
}

uint16_t read_little_endian_16(u8 *bytes) {
    return (uint16_t) (bytes[0] | (bytes[1] << 8));
}

uint32_t read_little_endian_32(u8 *bytes) {
    return (uint32_t) (bytes[0] | (bytes[1] << 8) | (bytes[2] << 16) | (bytes[3] << 24));
}

int64_t read_little_endian_64(u8 *bytes) {
    int64_t result;
    memcpy(&result, bytes, 8);
    return result;
}

typedef enum { AUX_TYPE_INT, AUX_TYPE_STRING } AuxType;

typedef union {
    int int_value;
    s8  str_value;
} AuxUnion;

typedef struct {
    AuxType  type;
    AuxUnion value;
} AuxValue;

// Function to print the value stored in AuxValue
void print_rdb_value(const AuxValue *aux) {
    if (aux->type == AUX_TYPE_INT) {
        printf("%d\n", aux->value.int_value);
    } else if (aux->type == AUX_TYPE_STRING) {
        printf("%.*s\n", (int) aux->value.str_value.len, aux->value.str_value.data);
    } else {
        printf("Unknown type\n");
    }
}
// TODO: prefer u8 over char
int parse_rdb_len_encoding(u8 **cursor, int *special_format) {
    u8 *p           = *cursor;
    *special_format = -1;
    int mode        = (*p & 0b11000000) >> 6;
    int len         = 0;
    // The next 6 bits represent the length
    if (mode == 0) {
        len = *p;
        p++;
    }
    //  Read one additional byte.The combined 14 bits represent the length
    else if (mode == 1) {
        uint8_t byte1 = *p;
        p++;
        uint8_t byte2 = *p;
        // Mask the lower 6 bits of byte1 (0x3F = 00111111 in binary)
        // Then shift them left by 8 bits, leaving room for the next 8 bits from
        // byte2.
        uint16_t combined = (uint16_t) ((byte1 & 0x3F) << 8) | byte2;
        len               = (int) combined;
    }
    // Discard the remaining 6 bits. The next 4 bytes from the stream represent
    // the length
    else if (mode == 2) {
        p++;
        uint8_t byte1 = *p;
        p++;
        uint8_t byte2 = *p;
        p++;
        uint8_t byte3 = *p;
        p++;
        uint8_t  byte4 = *p;
        uint32_t combined =
            ((uint32_t) (byte1) << 24) | ((uint32_t) byte2 << 16) | ((uint32_t) byte3 << 8) | byte4;
        len = (int) combined;
    }
    // The next object is encoded in a special format. The remaining 6 bits
    // indicate the format. May be used to store numbers or Strings.
    else if (mode == 3) {
        int rem_6_bits  = (uint8_t) *p & 0b00111111;
        *special_format = rem_6_bits;
        p++;
    }

    *cursor = p;
    return len;
}

// TODO: prefer u8 over char
int parse_len_encoded_string(u8 **cursor, int size, AuxValue *aux_value) {
    char *p = *cursor;
    if (size <= 0) {
        fprintf(stderr, "invalid size %d\n", size);
        return 1;
    }
    aux_value->value.str_value.data = (u8 *) p;
    aux_value->value.str_value.len  = size;
    p                               = p + size;
    *cursor                         = p;
    return 0;
}

// TODO: prefer u8 over char
int parse_integer_encoded_as_string(u8 **cursor, int special_mode) {
    // 0 indicates that an 8 bit integer follows
    int   result = 0;
    char *p      = *cursor;
    if (special_mode == 0) {
        result = (int) *p;
        p++;
    } else if (special_mode == 1) {
        uint16_t num = read_little_endian_16(p);
        result       = num;
        p += 2;
    } else if (special_mode == 2) {
        uint32_t num = read_little_endian_32(p);
        result       = num;
        p += 4;
    } else {
        printf("unreachable");
        exit(1);
        UNREACHABLE();
    }

    *cursor = p;
    return result;
}

void print_rdb(const RdbContent *rdb) {
    printf("loaded rdb content:\n");
    printf("header: ");
    s8print(rdb->header);

    printf("auxiliary fields\n");
    for (int i = 0; i < rdb->metadata->total; i++) {
        print_rdb_value((AuxValue *) rdb->metadata->items[i]);
    }

    printf("database\n");
    RdbDatabase *db = (RdbDatabase *) rdb->database;
    printf("database number: %d\n", db->num);
    printf("hash size: %d\n", db->resizedb_hash);
    printf("expiry size: %d\n", db->resizedb_expiry);
    hashmap_print(db->data);
}

// TODO: arena for RDB information like header, fields, etc. should be different than the arena used
// for hashmap since that's long lived.
RdbContent *parse_rdb(Arena *perm, Arena *scratch, s8 path) {
    RdbContent *rdbContent = new (perm, RdbContent);
    rdbContent->metadata   = initialize_vec();

    FILE *fp = fopen((char *) path.data, "rb");
    if (fp == NULL) {
        perror("Error opening file");
        s8print(path);
        return NULL;
    }
    u8 *content = read_source_file(scratch, fp);
    if (content == NULL) {
        fclose(fp);
        return NULL;
    }
    u8 *p = content;

    int section_number = 0; // Tracks the current section being parsed:
                            // 0: RDB file header
                            // 1: Auxiliary fields (metadata)
                            // 2: Database key-value pairs and expiry info
                            // 3: RDB file checksum

    while (*p != '\0') {
        if (0 == section_number) {
            size header_len = 0;
            s8   header     = {.len = 0, .data = p};
            while (*p != 0xFA && *p != '\0') {
                header_len++;
                p++;
            }
            header.len         = header_len;
            rdbContent->header = header;
            section_number++;
            p++;
        }
        if (1 == section_number) {
            while (*p != 0xFE) {
                AuxValue *name  = new (perm, AuxValue);
                AuxValue *value = new (perm, AuxValue);

                int special_format;
                int len = parse_rdb_len_encoding(&p, &special_format);
                parse_len_encoded_string(&p, len, name);
                // name is always str
                name->type = AUX_TYPE_STRING;
                push_vec(rdbContent->metadata, name);

                len = parse_rdb_len_encoding(&p, &special_format);
                if (special_format == -1) {
                    value->type = AUX_TYPE_STRING;
                    parse_len_encoded_string(&p, len, value);
                } else {
                    value->type = AUX_TYPE_INT;
                    int result  = parse_integer_encoded_as_string(&p, special_format);
                    DBG(result, d);
                    value->value.int_value = result;
                }
                push_vec(rdbContent->metadata, value);

                if (*p == 0xFA) {
                    p++;
                } else {
                    if (*p == 0xFE) {
                        section_number++;
                        p++;
                        break;
                    }
                    fprintf(stderr, "expected fa at beginning of auxiliary field");
                    return NULL;
                }
            }
        }

        if (2 == section_number) {
            RdbDatabase *rdb_db = new (perm, RdbDatabase);
            rdb_db->data        = 0;
            int special_format;
            int db_num = parse_rdb_len_encoding(&p, &special_format);
            if (special_format != -1) {
                fprintf(stderr, "expected special format to be 0 for database id");
            }
            rdb_db->num = db_num;
            if (*p == 0xFB) {
                p++;
                int sf;
                int db_hash_size = parse_rdb_len_encoding(&p, &sf);
                if (sf != -1) {
                    fprintf(stderr, "expected special format to be 0 for database id");
                }
                int db_expiry_size = parse_rdb_len_encoding(&p, &sf);
                if (sf != -1) {
                    fprintf(stderr, "expected special format to be 0 for database id");
                }
                rdb_db->resizedb_hash   = db_hash_size;
                rdb_db->resizedb_expiry = db_expiry_size;
            }

            while (*p != 0xFE && *p != 0xFF) {
                int     expire_time_kind = RDB_KEY_EXP_KIND_NO_TTL;
                int64_t expire_time      = -1;

                if (*p == 0xFD) {
                    p++;
                    expire_time_kind = RDB_KEY_EXP_KIND_S;
                    expire_time      = read_little_endian_32((char *) p);
                    p += 4;
                }

                if (*p == 0xFC) {
                    p++;
                    expire_time_kind = RDB_KEY_EXP_KIND_MS;
                    expire_time      = read_little_endian_64((char *) p);
                    p += 8;
                }

                int value_type = (int) *p;
                p++;

                HashMapNode *node = new (perm, HashMapNode);
                switch (value_type) {
                case 0:; // cannot define after case
                    AuxValue key, value;
                    int      sf, len;
                    len = parse_rdb_len_encoding(&p, &sf);
                    if (sf != -1) {
                        UNREACHABLE();
                    }
                    parse_len_encoded_string(&p, len, &key);
                    sf  = 0;
                    len = parse_rdb_len_encoding(&p, &sf);
                    if (sf != -1) {
                        UNREACHABLE();
                    }
                    parse_len_encoded_string(&p, len, &value);
                    node->key = key.value.str_value;
                    node->val = value.value.str_value;
                    break;
                default:
                    UNREACHABLE();
                    break;
                }

                switch (expire_time_kind) {
                case RDB_KEY_EXP_KIND_NO_TTL:
                    node->ttl = -1;
                    break;
                case RDB_KEY_EXP_KIND_S:
                    node->ttl = expire_time * 1000;
                    break;
                case RDB_KEY_EXP_KIND_MS:
                    node->ttl = expire_time;
                    break;
                default:
                    fprintf(stderr, "unknown expiration time");
                    UNREACHABLE();
                    break;
                }
                hashmap_upsert(&rdb_db->data, perm, node);
            }

            rdbContent->database = rdb_db;

            if (*p == 0xFE) {
                printf("Only one DB supported");
                UNREACHABLE();
            }
            if (*p == 0xFF) {
                section_number++;
            }
        }

        if (3 == section_number) {
            p += 8;
        }
        p++;
    }

    fclose(fp);
    return rdbContent;
}
