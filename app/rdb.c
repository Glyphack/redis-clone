#include "rdb.h"
#include "common.h"
#include "types.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

char *read_source_file(Arena *arena, FILE *fp) {
    char       *data = NULL;
    struct stat st;

    if (fstat(fileno(fp), &st) == -1)
        goto ret;

    data = new (arena, byte, st.st_size + 1);
    if (!data)
        goto ret;

    int rd = fread(data, sizeof(char), st.st_size, fp);
    if (rd != st.st_size) {
        data = NULL;
        goto ret;
    }
    data[st.st_size] = '\0';

ret:
    return data;
}

uint16_t read_little_endian_16(char *bytes) {
    return (uint16_t) (bytes[0] | (bytes[1] << 8));
}

uint32_t read_little_endian_32(char *bytes) {
    return (uint32_t) (bytes[0] | (bytes[1] << 8) | (bytes[2] << 16) | (bytes[3] << 24));
}

int64_t read_little_endian_64(char *bytes) {
    int64_t result;
    memcpy(&result, bytes, 8);
    return result;
}

typedef struct {
    int    int_value;
    Mystr *str_value;
} AuxValue;

void print_rdb_value(AuxValue *aux_value) {
    if (aux_value->str_value == NULL) {
        printf("%d\n", aux_value->int_value);
    } else {
        print_mystr(aux_value->str_value);
    }
}

int parse_rdb_len_encoding(char **cursor, int *special_format) {
    char *p         = *cursor;
    *special_format = -1;
    int mode        = (*p & 0b11000000) >> 6;
    int len         = 0;
    // The next 6 bits represent the length
    if (mode == 0) {
        len = (unsigned char) *p;
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
        uint16_t combined = ((uint16_t) (byte1 & 0x3F) << 8) | byte2;
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

int parse_len_encoded_string(Arena *arena, char **cursor, int size, AuxValue *aux_value) {
    char *p = *cursor;
    if (size <= 0) {
        fprintf(stderr, "invalid size %d\n", size);
        return 1;
    }
    aux_value->str_value       = new (arena, Mystr);
    aux_value->str_value->data = p;
    aux_value->str_value->len  = size;
    p                          = p + size;
    *cursor                    = p;
    return 0;
}

int parse_integer_encoded_as_string(char **cursor, int special_mode) {
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
    printf("header is\n");
    print_mystr(rdb->header);

    int is_name = 1;

    printf("auxiliary fields\n");
    for (int i = 0; i < rdb->metadata->total; i++) {
        if (is_name) {
            print_mystr(rdb->metadata->items[i]);
            is_name = 0;
        } else {
            AuxValue *aux_value = (AuxValue *) rdb->metadata->items[i];
            print_rdb_value(aux_value);
            is_name = 1;
        }
    }

    printf("databases\n");
    for (int i = 0; i < rdb->databases->total; i++) {
        RdbDatabase *db = (RdbDatabase *) rdb->databases->items[i];
        printf("database number: %d\n", db->num);
        printf("hash size: %d\n", db->resizedb_hash);
        printf("expiry size: %d\n", db->resizedb_expiry);
        for (int j = 0; j < db->data->size; j++) {
            printf("key : %s\n", db->data->nodes[j]->key);
            printf("value : %s\n", db->data->nodes[j]->val);
            printf("expiration: %lld\n", db->data->nodes[j]->ttl);
        }
    }
}

RdbContent *parse_rdb(Arena *arena, char *path) {
    RdbContent *rdbContent = new (arena, RdbContent);
    rdbContent->header     = NULL;
    rdbContent->metadata   = initialize_vec();

    char *content;
    FILE *fp = fopen(path, "rb");
    if (fp == NULL) {
        printf("opening file: %s\n", path);
        perror("Error opening file");
        return NULL;
    }
    content = read_source_file(arena, fp);
    if (content == NULL) {
        fclose(fp);
        return NULL;
    }
    char *p = content;

    int section_number = 0;

    while (*p != '\0') {
        if (0 == section_number) {
            Mystr *header  = new (arena, Mystr);
            header->data   = p;
            int header_len = 0;
            while ((unsigned char) *p != 0xFA) {
                header_len++;
                p++;
            }
            header->len        = header_len;
            rdbContent->header = header;
            section_number++;
            p++;
        }
        if (1 == section_number) {
            while ((unsigned char) *p != 0xFE) {
                AuxValue *name   = new (arena, AuxValue);
                AuxValue *value  = new (arena, AuxValue);
                value->int_value = 0;
                value->str_value = NULL;

                int special_format;
                int len = parse_rdb_len_encoding(&p, &special_format);
                parse_len_encoded_string(arena, &p, len, name);
                push_vec(rdbContent->metadata, name->str_value);

                len = parse_rdb_len_encoding(&p, &special_format);
                if (special_format == -1) {
                    parse_len_encoded_string(arena, &p, len, value);
                } else {
                    int result       = parse_integer_encoded_as_string(&p, special_format);
                    value->int_value = result;
                }
                push_vec(rdbContent->metadata, value);

                if ((unsigned char) *p == 0xFA) {
                    p++;
                } else {
                    if ((unsigned char) *p == 0xFE) {
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
            rdbContent->databases = initialize_vec();
            RdbDatabase *rdb_db   = new (arena, RdbDatabase);
            rdb_db->data          = hashmap_init();
            int special_format;
            int db_num = parse_rdb_len_encoding(&p, &special_format);
            if (special_format != -1) {
                fprintf(stderr, "expected special format to be 0 for database id");
            }
            rdb_db->num = db_num;
            if ((unsigned char) *p == 0xFB) {
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

            while ((unsigned char) *p != 0xFE && (unsigned char) *p != 0xFF) {
                int     expire_time_kind = RDB_KEY_EXP_KIND_NO_TTL;
                int64_t expire_time      = -1;

                if ((unsigned char) *p == 0xFD) {
                    p++;
                    expire_time_kind = RDB_KEY_EXP_KIND_S;
                    expire_time      = read_little_endian_32(p);
                    p += 4;
                }

                if ((unsigned char) *p == 0xFC) {
                    p++;
                    expire_time_kind = RDB_KEY_EXP_KIND_MS;
                    expire_time      = read_little_endian_64(p);
                    p += 8;
                }

                int value_type = (int) *p;
                p++;

                HashMapNode *node = hashmap_node_init();
                switch (value_type) {
                case 0:; // cannot define after case
                    AuxValue key, value;
                    int      sf, len;
                    len = parse_rdb_len_encoding(&p, &sf);
                    if (sf != -1) {
                        UNREACHABLE();
                    }
                    parse_len_encoded_string(arena, &p, len, &key);
                    sf  = 0;
                    len = parse_rdb_len_encoding(&p, &sf);
                    if (sf != -1) {
                        UNREACHABLE();
                    }
                    parse_len_encoded_string(arena, &p, len, &value);
                    node->key = convertToCStr(key.str_value);
                    node->val = convertToCStr(value.str_value);
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

                hashmap_insert(rdb_db->data, node);
            }

            push_vec(rdbContent->databases, rdb_db);

            if ((unsigned char) *p == 0xFE) {
                UNREACHABLE();
            }

            if ((unsigned char) *p == 0xFF) {
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
