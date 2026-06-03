# taskboss

🇷🇺 [Русский](README.ru.md)

A native PostgreSQL job-queue extension written in Rust using [pgrx](https://github.com/pgcentralfoundation/pgrx). Inspired by [pg-boss](https://github.com/timgit/pg-boss).

Unlike pg-boss (a Node.js library), this extension lives entirely inside PostgreSQL — no external processes, no extra dependencies.

## Features (v1 / MVP)

- Queue registry: `boss.create_queue` / `boss.delete_queue` / `boss.get_queues`
- Reliable job delivery via `SKIP LOCKED` — exactly-once claim by competing consumers
- Push delivery of new jobs via built-in `LISTEN`/`NOTIFY`
- Priorities, deferred start (`startAfter`), basic retry with delay
- Background worker: automatic expiry of stuck jobs and retention-based cleanup

Deferred to future versions: cron schedules, pub/sub, queue policies (singleton/short/stately),
partitioning, heartbeat monitoring, throttle/debounce, dead-letter queues.

## Requirements

- PostgreSQL 18
- Rust toolchain + `cargo pgrx`
- For the maintenance background worker: `shared_preload_libraries = 'taskboss'` in `postgresql.conf`
  (requires a PostgreSQL restart) and the `taskboss.database` GUC set to the database where the extension is installed.

## Quick Start

### Docker

```bash
docker run -d --name taskboss \
  -e POSTGRES_PASSWORD=secret \
  -p 5432:5432 \
  ghcr.io/sashaaro/taskboss:latest
```

Connect to the running container:

```bash
docker exec -it taskboss psql -U postgres
```

### From source

```bash
# Install cargo-pgrx
cargo install cargo-pgrx

# Initialise managed PostgreSQL installations
cargo pgrx init

# Run the extension inside PostgreSQL 18
cargo pgrx run pg18
```

Once connected to psql:

```sql
CREATE EXTENSION taskboss;

-- create a queue and send a job
SELECT boss.create_queue('email-welcome');
SELECT boss.send('email-welcome', '{"to": "a@b.c"}');

-- consumer: atomically claim and complete a job
SELECT * FROM boss.fetch('email-welcome', 1);
SELECT boss.complete('email-welcome', '<job-id>', '{"ok": true}');
```

### Push delivery via LISTEN/NOTIFY

Instead of polling in a loop, a consumer subscribes to the queue channel and wakes up
on notification, then atomically claims the job via `fetch`:

```sql
LISTEN boss_email_welcome;                       -- channel = boss_<queue_name>
-- ... client blocks until NOTIFY fired by boss.send() ...
SELECT * FROM boss.fetch('email-welcome', 1);
```

## Function Reference

- `boss.send(name, data jsonb, options jsonb)` — `options`: `priority`, `startAfter`
  (seconds or ISO string), `retryLimit`, `retryDelay`, `expireInSeconds`.
- `boss.create_queue(name, options jsonb)` — `options`: `retryLimit`, `retryDelay`,
  `expireInSeconds`, `retentionSeconds` (default values for jobs in this queue).
- `boss.fetch(name, batch_size)` → `SETOF boss.job`.
- `boss.complete(name, id, output jsonb)` / `boss.fail(name, id, output jsonb)` → `boolean`.

## Development

```bash
# Build
cargo pgrx build

# Tests (spins up a temporary PostgreSQL instance)
cargo pgrx test pg18

# Benchmarks
cargo pgrx bench pg18
```

## Scenario Tests (DSL)

In addition to `pg_test` unit tests, the repository includes declarative integration tests written
in a small DSL. Scenarios live in the [`scenarios/`](scenarios) directory; the
[`dsltest`](dsltest) runner (a parser built with [winnow](https://github.com/winnow-rs/winnow))
executes them against a **running** instance. Each client `#N` is an independent session, so
consumer competition (`SKIP LOCKED`) and cross-session `LISTEN`/`NOTIFY` wakeups are both covered.

```bash
# 1. start an instance with the extension (port 28818, DB taskboss); \q keeps it alive
cargo pgrx run pg18

# 2. run all scenarios (or pass specific files)
cargo run -p dsltest -- scenarios
cargo run -p dsltest -- scenarios/basic_delivery.scenario

# Override the DSN via TASKBOSS_DSN
```

Coverage: delivery and push (`basic_delivery`, `notify_wakeup`); queue correctness
(`priority_ordering`, `fifo_ordering`, `delayed_start`, `retry_then_succeed`,
`retry_exhaustion`, `retry_delay`, `expire_via_maintain`, `retention_purge`); concurrency
(`competing_consumers`, `multi_consumer_exactly_once`, `concurrent_producers`).

Full DSL grammar description and scenario list: [dsltest/README.md](dsltest/README.md).

## Inspiration

[pg-boss](https://github.com/timgit/pg-boss) is an excellent PostgreSQL-backed job queue for Node.js. This project pursues the same goal but implements the queue logic as a native server-side PostgreSQL extension, eliminating the overhead of an external process.
