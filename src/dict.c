#include "dict.h"

#include <stdlib.h>
#include <string.h>

static void *xmalloc(size_t n) {
    void *p = malloc(n ? n : 1);
    if (!p) abort();
    return p;
}

static entry **newTable(size_t size) {
    entry **t = calloc(size, sizeof(entry *));
    if (!t) abort();
    return t;
}

/* FNV-1a, 64 bit. */
static uint64_t hash(const char *s, size_t len) {
    uint64_t h = 1469598103934665603ULL;
    for (size_t i = 0; i < len; i++) {
        h ^= (unsigned char)s[i];
        h *= 1099511628211ULL;
    }
    return h;
}

static char *dupBytes(const char *p, size_t n) {
    char *c = xmalloc(n);
    if (n) memcpy(c, p, n);
    return c;
}

dict *dictCreate(void) {
    dict *d = xmalloc(sizeof(*d));
    d->size = 16;
    d->used = 0;
    d->table = newTable(d->size);
    return d;
}

static void freeEntry(entry *e) {
    free(e->key);
    free(e->val);
    free(e);
}

void dictEmpty(dict *d) {
    for (size_t i = 0; i < d->size; i++) {
        entry *e = d->table[i];
        while (e) {
            entry *next = e->next;
            freeEntry(e);
            e = next;
        }
        d->table[i] = NULL;
    }
    d->used = 0;
}

void dictFree(dict *d) {
    dictEmpty(d);
    free(d->table);
    free(d);
}

static void grow(dict *d) {
    size_t nsize = d->size * 2;
    entry **nt = newTable(nsize);
    for (size_t i = 0; i < d->size; i++) {
        entry *e = d->table[i];
        while (e) {
            entry *next = e->next;
            size_t idx = hash(e->key, e->klen) & (nsize - 1);
            e->next = nt[idx];
            nt[idx] = e;
            e = next;
        }
    }
    free(d->table);
    d->table = nt;
    d->size = nsize;
}

entry *dictFind(dict *d, const char *key, size_t klen) {
    entry *e = d->table[hash(key, klen) & (d->size - 1)];
    for (; e; e = e->next)
        if (e->klen == klen && memcmp(e->key, key, klen) == 0) return e;
    return NULL;
}

entry *dictSet(dict *d, const char *key, size_t klen, const char *val, size_t vlen) {
    entry *e = dictFind(d, key, klen);
    if (e) {
        free(e->val);
        e->val = dupBytes(val, vlen);
        e->vlen = vlen;
        e->expire = -1;
        return e;
    }
    if (d->used >= d->size) grow(d);
    e = xmalloc(sizeof(*e));
    e->key = dupBytes(key, klen);
    e->klen = klen;
    e->val = dupBytes(val, vlen);
    e->vlen = vlen;
    e->expire = -1;
    size_t idx = hash(key, klen) & (d->size - 1);
    e->next = d->table[idx];
    d->table[idx] = e;
    d->used++;
    return e;
}

int dictDelete(dict *d, const char *key, size_t klen) {
    entry **link = &d->table[hash(key, klen) & (d->size - 1)];
    for (; *link; link = &(*link)->next) {
        entry *e = *link;
        if (e->klen == klen && memcmp(e->key, key, klen) == 0) {
            *link = e->next;
            freeEntry(e);
            d->used--;
            return 1;
        }
    }
    return 0;
}
