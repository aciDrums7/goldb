#ifndef GOLDB_DICT_H
#define GOLDB_DICT_H

#include <stddef.h>
#include <stdint.h>

/* Binary-safe string -> string hash table with separate chaining.
 * The bucket count is always a power of two and doubles when the load
 * factor reaches 1. */

typedef struct entry {
    struct entry *next;
    char *key;
    size_t klen;
    char *val;
    size_t vlen;
    int64_t expire; /* Absolute unix time in milliseconds, -1 = never. */
} entry;

typedef struct dict {
    entry **table;
    size_t size;
    size_t used;
} dict;

dict *dictCreate(void);
void dictFree(dict *d);
void dictEmpty(dict *d);
entry *dictFind(dict *d, const char *key, size_t klen);
/* Insert or overwrite. Overwriting clears any expire, like Redis SET. */
entry *dictSet(dict *d, const char *key, size_t klen, const char *val, size_t vlen);
/* Returns 1 if the key existed and was removed, 0 otherwise. */
int dictDelete(dict *d, const char *key, size_t klen);

#endif
