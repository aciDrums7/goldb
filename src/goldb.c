/* goldb: a small key-value server that speaks the Redis protocol (RESP).
 *
 * Single threaded, one poll() event loop, append-only file persistence.
 * Every write is appended to the AOF before its reply is sent and the file
 * is fsync()ed once per second, so a process crash loses nothing and a
 * power loss loses at most about one second of writes. */

#define _POSIX_C_SOURCE 200809L

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "dict.h"

#define MAX_ARGS 1024
#define MAX_BULK (64LL * 1024 * 1024)
#define MAX_INLINE (64 * 1024)
#define MAX_QUERY ((size_t)128 * 1024 * 1024)
#define MAX_OUTPUT ((size_t)256 * 1024 * 1024)
#define MAX_EXPIRE 1000000000000000LL /* Bound user times to avoid overflow. */
#define READ_CHUNK (16 * 1024)
#define EXPIRE_BUCKETS_PER_CYCLE 64

typedef struct client {
    int fd;
    char *q; /* Query buffer: bytes read but not yet executed. */
    size_t qlen, qcap;
    char *o; /* Output buffer: replies not yet written. */
    size_t olen, ocap, opos;
    int closing; /* Close once the output buffer is flushed. */
    int dead;
} client;

static struct {
    dict *db;
    int aof_fd; /* -1 while loading or when persistence is disabled. */
    int aof_dirty;
    int64_t last_fsync;
    size_t expire_cursor;
    client **clients;
    size_t nclients, cclients;
} server = {.aof_fd = -1};

static volatile sig_atomic_t stop_requested = 0;

/* Parsed arguments point into the query buffer of the command being run. */
static const char *argv_[MAX_ARGS];
static size_t argl_[MAX_ARGS];

static void fatal(const char *what) {
    perror(what);
    exit(1);
}

static int64_t mstime(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void bufAppend(char **buf, size_t *len, size_t *cap, const char *p, size_t n) {
    if (*len + n > *cap) {
        size_t c = *cap ? *cap : 256;
        while (c < *len + n) c *= 2;
        char *nb = realloc(*buf, c);
        if (!nb) abort();
        *buf = nb;
        *cap = c;
    }
    if (n) memcpy(*buf + *len, p, n);
    *len += n;
}

/* ---------------------------------------------------------------- Replies.
 * c is NULL while replaying the AOF: replies are discarded. */

static void addRaw(client *c, const char *p, size_t n) {
    if (!c || c->dead) return;
    if (n > MAX_OUTPUT - c->olen) {
        c->dead = 1; /* Reply backlog over the limit: drop the client. */
        return;
    }
    bufAppend(&c->o, &c->olen, &c->ocap, p, n);
}

static void addStr(client *c, const char *s) { addRaw(c, s, strlen(s)); }

static void addInt(client *c, long long v) {
    char b[32];
    int n = snprintf(b, sizeof(b), ":%lld\r\n", v);
    addRaw(c, b, (size_t)n);
}

static void addBulk(client *c, const char *p, size_t len) {
    char b[32];
    int n = snprintf(b, sizeof(b), "$%zu\r\n", len);
    addRaw(c, b, (size_t)n);
    addRaw(c, p, len);
    addRaw(c, "\r\n", 2);
}

static void addError(client *c, const char *msg) {
    addStr(c, "-ERR ");
    addStr(c, msg);
    addStr(c, "\r\n");
}

/* Error that quotes the command name, which is untrusted input: keep it short
 * and printable so it cannot break the reply framing. */
static void addCommandError(client *c, const char *fmt, const char *name, size_t len) {
    char safe[33], msg[128];
    size_t n = len < 32 ? len : 32;
    for (size_t i = 0; i < n; i++) safe[i] = (name[i] >= 32 && name[i] < 127) ? name[i] : '?';
    safe[n] = '\0';
    snprintf(msg, sizeof(msg), fmt, safe);
    addError(c, msg);
}

/* ---------------------------------------------------------------- Protocol. */

/* Read "<prefix><digits>\r\n" at *pos. Returns 1 on success, 0 if more data
 * is needed, -1 on a protocol error (with *err set). */
static int readLength(const char *buf, size_t len, size_t *pos, char prefix, long long *out,
                      const char **err) {
    if (*pos >= len) return 0;
    if (buf[*pos] != prefix) {
        *err = prefix == '*' ? "expected '*'" : "expected '$'";
        return -1;
    }
    const char *start = buf + *pos + 1;
    const char *cr = memchr(start, '\r', len - *pos - 1);
    if (!cr) {
        if (len - *pos > 32) {
            *err = "invalid length";
            return -1;
        }
        return 0;
    }
    if ((size_t)(cr - buf) + 1 >= len) return 0; /* Wait for the '\n'. */
    if (cr[1] != '\n' || cr == start) {
        *err = "invalid length";
        return -1;
    }
    long long v = 0;
    for (const char *p = start; p < cr; p++) {
        if (*p < '0' || *p > '9') {
            *err = "invalid length";
            return -1;
        }
        v = v * 10 + (*p - '0');
        if (v > MAX_BULK) {
            *err = "length too large";
            return -1;
        }
    }
    *out = v;
    *pos = (size_t)(cr - buf) + 2;
    return 1;
}

/* Parse one request: a RESP array of bulk strings, or an inline command
 * ("SET k v\r\n") as typed in telnet. Returns bytes consumed, 0 if the
 * request is incomplete, -1 on protocol error. argv_ points into buf. */
static long parseCommand(const char *buf, size_t len, int *argc, const char **err) {
    if (len == 0) return 0;
    if (buf[0] != '*') {
        const char *nl = memchr(buf, '\n', len);
        if (!nl) {
            if (len > MAX_INLINE) {
                *err = "inline request too long";
                return -1;
            }
            return 0;
        }
        size_t end = (size_t)(nl - buf), linelen = end, i = 0;
        if (linelen && buf[linelen - 1] == '\r') linelen--;
        int n = 0;
        while (i < linelen) {
            while (i < linelen && (buf[i] == ' ' || buf[i] == '\t')) i++;
            if (i == linelen) break;
            if (n == MAX_ARGS) {
                *err = "too many arguments";
                return -1;
            }
            size_t s = i;
            while (i < linelen && buf[i] != ' ' && buf[i] != '\t') i++;
            argv_[n] = buf + s;
            argl_[n] = i - s;
            n++;
        }
        *argc = n;
        return (long)end + 1;
    }

    size_t pos = 0;
    long long count;
    int r = readLength(buf, len, &pos, '*', &count, err);
    if (r <= 0) return r;
    if (count > MAX_ARGS) {
        *err = "too many arguments";
        return -1;
    }
    for (long long i = 0; i < count; i++) {
        long long blen;
        r = readLength(buf, len, &pos, '$', &blen, err);
        if (r <= 0) return r;
        if (len - pos < (size_t)blen + 2) return 0;
        if (buf[pos + blen] != '\r' || buf[pos + blen + 1] != '\n') {
            *err = "expected CRLF after bulk string";
            return -1;
        }
        argv_[i] = buf + pos;
        argl_[i] = (size_t)blen;
        pos += (size_t)blen + 2;
    }
    *argc = (int)count;
    return (long)pos;
}

/* ---------------------------------------------------------------- AOF. */

static void aofWrite(const char *p, size_t n) {
    while (n) {
        ssize_t w = write(server.aof_fd, p, n);
        if (w < 0) {
            if (errno == EINTR) continue;
            /* Refuse to keep acknowledging writes that are not persisted. */
            fatal("goldb: AOF write failed");
        }
        p += w;
        n -= (size_t)w;
    }
    server.aof_dirty = 1;
}

static void feedAof(int argc, const char **argv, const size_t *argl) {
    static char *buf;
    static size_t cap;
    size_t len = 0;
    char h[32];
    if (server.aof_fd == -1) return;
    int n = snprintf(h, sizeof(h), "*%d\r\n", argc);
    bufAppend(&buf, &len, &cap, h, (size_t)n);
    for (int i = 0; i < argc; i++) {
        n = snprintf(h, sizeof(h), "$%zu\r\n", argl[i]);
        bufAppend(&buf, &len, &cap, h, (size_t)n);
        bufAppend(&buf, &len, &cap, argv[i], argl[i]);
        bufAppend(&buf, &len, &cap, "\r\n", 2);
    }
    aofWrite(buf, len);
}

/* Log "<cmd> key <absolute ms>" so replaying never extends a TTL. */
static void feedAofAbsolute(const char *cmd, const char *key, size_t klen, int64_t when,
                            const char *val, size_t vlen) {
    char ts[32];
    int n = snprintf(ts, sizeof(ts), "%lld", (long long)when);
    if (val) {
        const char *argv[] = {cmd, key, val, "PXAT", ts};
        size_t argl[] = {strlen(cmd), klen, vlen, 4, (size_t)n};
        feedAof(5, argv, argl);
    } else {
        const char *argv[] = {cmd, key, ts};
        size_t argl[] = {strlen(cmd), klen, (size_t)n};
        feedAof(3, argv, argl);
    }
}

/* ---------------------------------------------------------------- Commands. */

static int argIs(const char *p, size_t n, const char *name) {
    return strlen(name) == n && strncasecmp(p, name, n) == 0;
}

static int parseLL(const char *p, size_t n, long long *out) {
    char b[32], *end;
    if (n == 0 || n >= sizeof(b) || !(p[0] == '-' || (p[0] >= '0' && p[0] <= '9'))) return 0;
    memcpy(b, p, n);
    b[n] = '\0';
    errno = 0;
    long long v = strtoll(b, &end, 10);
    if (errno || *end) return 0;
    *out = v;
    return 1;
}

/* Lazy expiration: an expired key is deleted the moment it is touched. */
static entry *lookup(const char *key, size_t klen, int64_t now) {
    entry *e = dictFind(server.db, key, klen);
    if (e && e->expire != -1 && e->expire <= now) {
        dictDelete(server.db, key, klen);
        return NULL;
    }
    return e;
}

static void execCommand(client *c, int argc, const char **argv, const size_t *argl) {
    const char *cmd = argv[0];
    size_t clen = argl[0];
    int64_t now = mstime();

#define ARITY(ok)                                                                        \
    do {                                                                                 \
        if (!(ok)) {                                                                     \
            addCommandError(c, "wrong number of arguments for '%s' command", cmd, clen); \
            return;                                                                      \
        }                                                                                \
    } while (0)

    if (argIs(cmd, clen, "ping")) {
        ARITY(argc <= 2);
        if (argc == 1)
            addStr(c, "+PONG\r\n");
        else
            addBulk(c, argv[1], argl[1]);
    } else if (argIs(cmd, clen, "echo")) {
        ARITY(argc == 2);
        addBulk(c, argv[1], argl[1]);
    } else if (argIs(cmd, clen, "command")) {
        addStr(c, "*0\r\n"); /* redis-cli asks for command docs on connect. */
    } else if (argIs(cmd, clen, "get")) {
        ARITY(argc == 2);
        entry *e = lookup(argv[1], argl[1], now);
        if (e)
            addBulk(c, e->val, e->vlen);
        else
            addStr(c, "$-1\r\n");
    } else if (argIs(cmd, clen, "set")) {
        ARITY(argc >= 3);
        int64_t expire = -1;
        for (int i = 3; i < argc; i += 2) {
            int ex = argIs(argv[i], argl[i], "ex"), px = argIs(argv[i], argl[i], "px"),
                pxat = argIs(argv[i], argl[i], "pxat");
            long long v;
            if (!(ex || px || pxat) || i + 1 >= argc || expire != -1) {
                addError(c, "syntax error");
                return;
            }
            if (!parseLL(argv[i + 1], argl[i + 1], &v) || v <= 0 || v > MAX_EXPIRE) {
                addError(c, "invalid expire time in 'set' command");
                return;
            }
            expire = ex ? now + v * 1000 : px ? now + v : v;
        }
        entry *e = dictSet(server.db, argv[1], argl[1], argv[2], argl[2]);
        if (expire == -1) {
            feedAof(3, argv, argl);
        } else {
            feedAofAbsolute("SET", argv[1], argl[1], expire, argv[2], argl[2]);
            if (expire <= now)
                dictDelete(server.db, argv[1], argl[1]);
            else
                e->expire = expire;
        }
        addStr(c, "+OK\r\n");
    } else if (argIs(cmd, clen, "del")) {
        ARITY(argc >= 2);
        long long n = 0;
        for (int i = 1; i < argc; i++)
            if (lookup(argv[i], argl[i], now)) n += dictDelete(server.db, argv[i], argl[i]);
        if (n) feedAof(argc, argv, argl);
        addInt(c, n);
    } else if (argIs(cmd, clen, "exists")) {
        ARITY(argc >= 2);
        long long n = 0;
        for (int i = 1; i < argc; i++) n += lookup(argv[i], argl[i], now) != NULL;
        addInt(c, n);
    } else if (argIs(cmd, clen, "expire") || argIs(cmd, clen, "pexpireat")) {
        ARITY(argc == 3);
        long long v;
        if (!parseLL(argv[2], argl[2], &v) || v > MAX_EXPIRE || v < -MAX_EXPIRE) {
            addError(c, "value is not an integer or out of range");
            return;
        }
        entry *e = lookup(argv[1], argl[1], now);
        if (!e) {
            addInt(c, 0);
            return;
        }
        int64_t when = argIs(cmd, clen, "expire") ? now + v * 1000 : v;
        feedAofAbsolute("PEXPIREAT", argv[1], argl[1], when, NULL, 0);
        if (when <= now)
            dictDelete(server.db, argv[1], argl[1]);
        else
            e->expire = when;
        addInt(c, 1);
    } else if (argIs(cmd, clen, "ttl")) {
        ARITY(argc == 2);
        entry *e = lookup(argv[1], argl[1], now);
        if (!e)
            addInt(c, -2);
        else if (e->expire == -1)
            addInt(c, -1);
        else
            addInt(c, (e->expire - now + 500) / 1000);
    } else if (argIs(cmd, clen, "persist")) {
        ARITY(argc == 2);
        entry *e = lookup(argv[1], argl[1], now);
        if (!e || e->expire == -1) {
            addInt(c, 0);
            return;
        }
        e->expire = -1;
        feedAof(argc, argv, argl);
        addInt(c, 1);
    } else if (argIs(cmd, clen, "dbsize")) {
        ARITY(argc == 1);
        addInt(c, (long long)server.db->used);
    } else if (argIs(cmd, clen, "flushall")) {
        ARITY(argc == 1);
        dictEmpty(server.db);
        feedAof(argc, argv, argl);
        addStr(c, "+OK\r\n");
    } else {
        addCommandError(c, "unknown command '%s'", cmd, clen);
    }
#undef ARITY
}

/* Active expiration: sample a few buckets per loop iteration, so keys that
 * are never read again do not keep memory forever. */
static void activeExpire(int64_t now) {
    dict *d = server.db;
    for (int i = 0; i < EXPIRE_BUCKETS_PER_CYCLE; i++) {
        entry *e = d->table[server.expire_cursor++ & (d->size - 1)];
        while (e) {
            entry *next = e->next;
            if (e->expire != -1 && e->expire <= now) dictDelete(d, e->key, e->klen);
            e = next;
        }
    }
}

/* Replay the AOF through the normal command path. A truncated final command
 * (crash in the middle of a write) is discarded; any other damage is fatal. */
static void loadAof(const char *path) {
    int fd = open(path, O_RDONLY);
    if (fd == -1) {
        if (errno == ENOENT) return;
        fatal("goldb: open AOF");
    }
    struct stat st;
    if (fstat(fd, &st) == -1) fatal("goldb: stat AOF");
    size_t len = (size_t)st.st_size, got = 0;
    char *buf = malloc(len ? len : 1);
    if (!buf) abort();
    while (got < len) {
        ssize_t r = read(fd, buf + got, len - got);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) fatal("goldb: read AOF");
        got += (size_t)r;
    }
    close(fd);

    size_t pos = 0;
    long commands = 0;
    while (pos < len) {
        int argc = 0;
        const char *err = NULL;
        if (buf[pos] != '*') {
            fprintf(stderr, "goldb: corrupt AOF at offset %zu: expected '*'\n", pos);
            exit(1);
        }
        long r = parseCommand(buf + pos, len - pos, &argc, &err);
        if (r < 0) {
            fprintf(stderr, "goldb: corrupt AOF at offset %zu: %s\n", pos, err);
            exit(1);
        }
        if (r == 0) break;
        pos += (size_t)r;
        if (argc) execCommand(NULL, argc, argv_, argl_);
        commands++;
    }
    if (pos < len) {
        fprintf(stderr, "goldb: AOF ends with %zu bytes of a truncated command, discarding them\n",
                len - pos);
        if (truncate(path, (off_t)pos) == -1) fatal("goldb: truncate AOF");
    }
    free(buf);
    fprintf(stderr, "goldb: loaded %ld commands from %s, %zu keys\n", commands, path,
            server.db->used);
}

/* ---------------------------------------------------------------- Networking. */

static void setNonBlocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) fatal("goldb: fcntl");
}

static int listenOn(const char *address, int port) {
    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, address, &sa.sin_addr) != 1) {
        fprintf(stderr, "goldb: invalid bind address %s\n", address);
        exit(1);
    }
    int fd = socket(AF_INET, SOCK_STREAM, 0), yes = 1;
    if (fd == -1) fatal("goldb: socket");
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    if (bind(fd, (struct sockaddr *)&sa, sizeof(sa)) == -1) fatal("goldb: bind");
    if (listen(fd, 511) == -1) fatal("goldb: listen");
    setNonBlocking(fd);
    return fd;
}

static void acceptClients(int lfd) {
    for (;;) {
        int fd = accept(lfd, NULL, NULL);
        if (fd == -1) return; /* EAGAIN, or a transient error: retry next loop. */
        setNonBlocking(fd);
        client *c = calloc(1, sizeof(*c));
        if (!c) abort();
        c->fd = fd;
        if (server.nclients == server.cclients) {
            server.cclients = server.cclients ? server.cclients * 2 : 16;
            server.clients = realloc(server.clients, server.cclients * sizeof(client *));
            if (!server.clients) abort();
        }
        server.clients[server.nclients++] = c;
    }
}

static void processInput(client *c) {
    size_t pos = 0;
    while (pos < c->qlen && !c->closing && !c->dead) {
        int argc = 0;
        const char *err = NULL;
        long r = parseCommand(c->q + pos, c->qlen - pos, &argc, &err);
        if (r == 0) break;
        if (r < 0) {
            addStr(c, "-ERR Protocol error: ");
            addStr(c, err);
            addStr(c, "\r\n");
            c->closing = 1;
            break;
        }
        pos += (size_t)r;
        if (argc) execCommand(c, argc, argv_, argl_);
    }
    memmove(c->q, c->q + pos, c->qlen - pos);
    c->qlen -= pos;
}

static void readFromClient(client *c) {
    if (c->qcap - c->qlen < READ_CHUNK) {
        c->qcap = c->qcap ? c->qcap * 2 : READ_CHUNK * 2;
        c->q = realloc(c->q, c->qcap);
        if (!c->q) abort();
    }
    ssize_t n = read(c->fd, c->q + c->qlen, c->qcap - c->qlen);
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) return;
    if (n <= 0) {
        c->dead = 1;
        return;
    }
    c->qlen += (size_t)n;
    processInput(c);
    if (c->qlen > MAX_QUERY) c->dead = 1;
}

static void writeToClient(client *c) {
    while (c->opos < c->olen) {
        ssize_t n = write(c->fd, c->o + c->opos, c->olen - c->opos);
        if (n < 0) {
            if (errno == EINTR) continue;
            if (errno != EAGAIN && errno != EWOULDBLOCK) c->dead = 1;
            break;
        }
        c->opos += (size_t)n;
    }
    if (c->opos == c->olen) {
        c->opos = c->olen = 0;
        if (c->closing) c->dead = 1;
    } else if (c->olen - c->opos > MAX_OUTPUT) {
        c->dead = 1; /* Client is not reading its replies. */
    }
}

static void reapClients(void) {
    for (size_t i = 0; i < server.nclients; i++) {
        client *c = server.clients[i];
        if (!c->dead) continue;
        close(c->fd);
        free(c->q);
        free(c->o);
        free(c);
        server.clients[i--] = server.clients[--server.nclients];
    }
}

static void onSignal(int sig) {
    (void)sig;
    stop_requested = 1;
}

static void usage(void) {
    fprintf(stderr, "usage: goldb [--port N] [--bind ADDR] [--aof PATH | --no-aof]\n");
    exit(1);
}

int main(int argc, char **argv) {
    int port = 6380;
    const char *bind = "127.0.0.1", *aof = "goldb.aof";
    for (int i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "--port") && i + 1 < argc)
            port = atoi(argv[++i]);
        else if (!strcmp(argv[i], "--bind") && i + 1 < argc)
            bind = argv[++i];
        else if (!strcmp(argv[i], "--aof") && i + 1 < argc)
            aof = argv[++i];
        else if (!strcmp(argv[i], "--no-aof"))
            aof = NULL;
        else
            usage();
    }
    if (port < 1 || port > 65535) usage();

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = onSignal;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);

    server.db = dictCreate();
    if (aof) {
        loadAof(aof);
        server.aof_fd = open(aof, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (server.aof_fd == -1) fatal("goldb: open AOF");
    }
    int lfd = listenOn(bind, port);
    fprintf(stderr, "goldb: listening on %s:%d, AOF %s\n", bind, port, aof ? aof : "disabled");

    struct pollfd *pfds = NULL;
    size_t pcap = 0;
    server.last_fsync = mstime();
    while (!stop_requested) {
        size_t n = server.nclients + 1;
        if (n > pcap) {
            pcap = n * 2;
            pfds = realloc(pfds, pcap * sizeof(*pfds));
            if (!pfds) abort();
        }
        pfds[0] = (struct pollfd){.fd = lfd, .events = POLLIN};
        for (size_t i = 0; i < server.nclients; i++) {
            client *c = server.clients[i];
            pfds[i + 1] = (struct pollfd){
                .fd = c->fd, .events = (short)(POLLIN | (c->olen > c->opos ? POLLOUT : 0))};
        }
        int ready = poll(pfds, (nfds_t)n, 100);
        if (ready < 0 && errno != EINTR) fatal("goldb: poll");
        if (ready > 0) {
            for (size_t i = 0; i + 1 < n; i++) {
                client *c = server.clients[i];
                short re = pfds[i + 1].revents;
                if (re & (POLLIN | POLLHUP | POLLERR)) readFromClient(c);
                if (!c->dead && c->olen > c->opos) writeToClient(c);
            }
            if (pfds[0].revents & POLLIN) acceptClients(lfd);
        }
        reapClients();
        int64_t now = mstime();
        activeExpire(now);
        if (server.aof_dirty && now - server.last_fsync >= 1000) {
            if (fsync(server.aof_fd) == -1) fatal("goldb: fsync AOF");
            server.aof_dirty = 0;
            server.last_fsync = now;
        }
    }
    if (server.aof_fd != -1 && fsync(server.aof_fd) == -1) fatal("goldb: fsync AOF");
    fprintf(stderr, "goldb: shutting down\n");
    return 0;
}
