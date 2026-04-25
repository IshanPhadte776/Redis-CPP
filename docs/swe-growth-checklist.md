# SWE growth checklist

Short checklist of artifacts and work that tend to read as **mid-level (SWE 2)** vs **senior (SWE 3)**. Ladders differ by company; use this as a **project and career** guide, not an official job level spec.

Use for your Redis-style server, or any systems / backend project.

---

## SWE 2 (mid-level) — ownership, reliability, evidence

### Design & communication

- [ ] **Design note (1–3 pages)** for a non-trivial change: problem, goals, non-goals, approach, rejected alternatives, risks
- [ ] **Explicit API or protocol contract** documented (commands, errors, invariants)
- [ ] **Lock ordering / concurrency note** for shared state (what mutex protects, what CV wakes, deadlock avoidance)

### Testing & correctness

- [ ] **Integration tests** against a real running server (spawn binary, send RESP, assert bytes)
- [ ] **Regression tests** for bugs you fixed (transaction edge cases, TTL races, blocking timeouts)
- [ ] **Sanitizer runs** in CI or locally: ASan / UBSan; TSan for concurrency-heavy changes
- [ ] **Clear test plan** in the design note for the feature you shipped

### Performance & load

- [ ] **Repeatable benchmark** script (fixed workload, same machine, recorded commit hash)
- [ ] **Throughput + latency** reported (at least avg; better: p50 / p95 or min / max)
- [ ] **Stress test** (many clients, mixed commands, long run) with no crashes / obvious leaks
- [ ] **One profiling pass** (e.g. `perf`) with a takeaway tied to a small, measured improvement

### Production habits

- [ ] **Structured or leveled logging** (reduce printf noise in hot paths; optional debug flag)
- [ ] **Basic resource limits** (max message size, max bulk string length, connection cap)
- [ ] **Graceful shutdown** path (stop accept, drain or timeout clients, flush if persistence exists)
- [ ] **Config surface** documented (flags, env, or file — what each knob does)

### Scope

- [ ] **One vertical owned end-to-end** (e.g. persistence slice, replication slice, or proper request buffering) — not only scattered commands

---

## SWE 3 (senior) — systems shape, failure, operability at scale

### Architecture & consistency

- [ ] **Multi-component design** (e.g. accept loop + worker pool + store shards) with documented tradeoffs
- [ ] **Consistency / durability story** written down (what clients can assume after crash, failover, or partition)
- [ ] **Replication**: full sync + ongoing command stream; reconnect; optional partial resync / backlog
- [ ] **Durability**: RDB and/or AOF-style log with **fsync policy** and recovery tests after `kill -9`
- [ ] **Corruption / bad input paths**: detect, fail safe, optional repair or admin tooling

### Reliability & abuse

- [ ] **Backpressure**: bounded queues; overload degrades gracefully (errors/latency), not unbounded memory growth
- [ ] **Eviction or cap** when over memory budget (policy documented: LRU sample, random, etc.)
- [ ] **Slow command logging** or latency thresholds for ops debugging
- [ ] **Chaos or fault injection** (master dies under load, disk full, partial writes) with expected outcomes documented

### Observability & operations

- [ ] **Metrics** (Prometheus text, or file): throughput, errors, connections, replication lag, store size
- [ ] **Per-connection or request correlation** in logs where it helps debug multi-client issues
- [ ] **`INFO` (or equivalent)** rich enough to debug replication/persistence in the field
- [ ] **Runbook-style doc**: how to deploy, verify health, common failures, rollback

### Concurrency & performance (senior depth)

- [ ] **Profiling-driven** refactor: smaller critical sections, sharded locks, or dedicated I/O vs worker model
- [ ] **Long TSan / stress** runs on CI or documented manual gate before merge
- [ ] **Property-based or model tests** for a critical subsystem (streams, transactions, ordering)

### Security

- [ ] **AUTH** (or TLS story) and threat model note (who is trusted, what is exposed)
- [ ] **Dangerous commands** gated, renamed, or disabled in “safe” mode
- [ ] **Input validation** on every untrusted path (size, depth, encoding)

### Leadership (often expected at senior, not only code)

- [ ] **Mentoring**: review others’ designs/diffs with concrete suggestions
- [ ] **Cross-cutting migration** or deprecation (old path → new path with rollout plan)
- [ ] **Incident-style postmortem** (even for a simulated failure): timeline, root cause, action items

---

## How to use this file

1. Pick **one SWE 2 vertical** and finish it with design + tests + benchmark.
2. Add **SWE 3** items only when that vertical needs durability, scale, or ops — avoid checklist theater.
3. Re-check boxes when you **ship** the behavior, not when you “plan to.”

---

*Last updated: personal growth checklist for side projects and leveling conversations.*
