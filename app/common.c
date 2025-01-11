#include "common.h"
#include <stdlib.h>

void print_mystr(Mystr *s) {
  for (size_t i = 0; i < s->len; i++) {
    putchar(s->data[i]);
  }
  putchar('\n');
}

char *convertToCStr(Mystr *s) {
  char *c = (char *)malloc(s->len + 1);
  for (usize i = 0; i < s->len; i++) {
    c[i] = s->data[i];
  }
  c[s->len] = '\0';
  return c;
}
