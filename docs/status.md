# goldb status

2026-11-17: owner approved a small public personal C database, named goldb,
for continued development and possible Alignerr Project Creation submission.
Keep scope minimal; do not fabricate prior development history or experience.

Implemented: RESP2 key-value server, poll event loop, binary-safe hash table,
TTL, append-only persistence and partial-tail recovery.

Verification on macOS arm64 / Apple clang 21:
- `make && make test`: 6 integration tests passed, including 2,500 seeded
  random mutations followed by replay and key comparisons, and slow-reader
  output-limit disconnection with continued service for other clients.
- `make sanitize`: same 6 tests passed under AddressSanitizer + UBSan.
- Actual `redis-cli` smoke on loopback 16389: PING -> PONG; SET/GET -> goldb;
  SET EX / TTL -> 30; DEL -> 1. Disposable server exited 0.

Native read-only Workhorse review ctx_6804746571ae found one output-buffer
bound gap. Fixed reply allocation before append and stopped pipeline execution
for disconnected clients; regression test passes normally and under sanitizers.
Alignerr must describe this as a new AI-assisted project. No interviews or
assessments may be performed by an agent.
