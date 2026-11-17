# goldb

A small single-threaded key-value database in C. New personal systems project,
started November 2026 with AI coding assistance. Not a Redis replacement or a
production database. Inspired by Salvatore Sanfilippo's systems programming
videos; implementation is not a copy of a course project.

## Run

Requires a POSIX system (macOS or Linux), C11 compiler and make. Tests use only
Python 3's standard library (Python >= 3.9).

```sh
make
./goldb                         # loopback port 6380; creates goldb.aof
redis-cli -p 6380 SET greeting hello
redis-cli -p 6380 GET greeting
redis-cli -p 6380 SET session token EX 30
redis-cli -p 6380 TTL session
make test
make sanitize                  # rebuild and run with ASan + UBSan
```

Options: `--port N`, `--bind IPv4`, `--aof PATH`, `--no-aof`.
Stop with SIGINT or SIGTERM; the AOF is fsynced on shutdown.

## Current scope

- Binary-safe strings and a separate-chaining hash table that grows at load 1.
- Nonblocking sockets with a single `poll()` loop, fragmented RESP2 requests,
  pipelining, and a basic whitespace-separated inline protocol (no quoting).
- PING, ECHO, GET, SET (EX/PX/PXAT), DEL, EXISTS, EXPIRE, PEXPIREAT, TTL,
  PERSIST, DBSIZE, FLUSHALL. COMMAND returns an empty array for redis-cli.
- Lazy and incremental active expiration. DBSIZE may count expired entries
  until the active expiration sweep reaches them.
- Append-only persistence with absolute expiration timestamps. Incomplete final
  records are truncated on startup. The AOF is fsynced approximately every second,
  not per write; acknowledged writes may be lost on power failure. I/O or fsync
  errors terminate the server rather than continuing to acknowledge writes.
- Integration tests for binary data, fragmented/pipelined requests, malformed
  protocol isolation, seeded randomized dictionary operations, TTL replay and
  partial AOF tail recovery.

## Limits

Local experimental use only: no authentication, TLS, replication, transactions,
cluster, eviction, AOF compaction, checksums or performance claims. Do not bind
public interfaces or use untrusted AOF files. Full AOF replay loads the log into
memory; memory allocation failure exits the process. Non-keyed hashing is not
hardened against adversarial collisions. AOF fsync blocks the event loop.

## Design and next work

`src/dict.c` owns key/value memory; `src/goldb.c` contains framing, command
execution, AOF and the event loop. Keep this small enough to read. Next areas to
explore: streaming replay and validated AOF recovery, bounded event-loop work,
AOF compaction, and randomized differential tests against Redis. These are not
implemented features.

Inspiration: antirez's [Learning C channel](https://www.youtube.com/@antirez),
[Writing System Software 1: AOF](https://www.youtube.com/watch?v=VBrnmciV9fM),
[Random Testing](https://www.youtube.com/watch?v=XOz_arKM1WY),
[smallchat](https://www.youtube.com/watch?v=eT02gzeLmF0).
