# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`taskboss` is a native PostgreSQL job-queue extension written in Rust using [pgrx](https://github.com/pgcentralfoundation/pgrx) (v0.18.1), targeting PostgreSQL 18 by default and compiled as a `cdylib`. It is inspired by [pg-boss](https://github.com/timgit/pg-boss) but runs entirely inside PostgreSQL (no external operator process). The crate/extension/library are all named `taskboss`; the user-facing SQL objects live in the `boss` schema.

## Commands

### Build

```bash
cargo pgrx build
```

### Run in a managed PostgreSQL instance (starts pg if needed)

```bash
cargo pgrx run pg18
```

### Run pgrx-based tests (spins up a temporary PostgreSQL instance)

```bash
cargo pgrx test pg18
```

### Run a single test by name

```bash
cargo pgrx test pg18 -- send_fetch_complete
```

### Run pg_regress integration tests

```bash
cargo pgrx test pg18 --features pg_test
```

### Run benchmarks

```bash
cargo pgrx bench pg18
```

### Install the extension into a local PostgreSQL installation

```bash
cargo pgrx install
```

## Architecture

This is a native job-queue extension inspired by [pg-boss](https://github.com/timgit/pg-boss),
but running entirely inside PostgreSQL (no external operator process). All objects live in a
dedicated `boss` schema. See `docs`/plan and `README.md` for the user-facing API.

- **[src/schema.rs](src/schema.rs)** — the data model and SQL-level operations shipped as one
  `extension_sql!` block: the `boss` schema, `boss.job_state` enum, `boss.queue`/`boss.job`
  tables, the fetch hot-path partial index, and the SQL/plpgsql functions `boss.channel`,
  `boss.send` (insert + `pg_notify`), `boss.fetch` (atomic claim via `FOR UPDATE SKIP LOCKED`),
  `boss.complete`, `boss.fail` (retry logic), `boss.get_queues`, and `boss.maintain`.
- **[src/lib.rs](src/lib.rs)** — Rust entry points: `#[pg_schema] mod boss` with the `#[pg_extern]`
  control functions `create_queue`/`delete_queue` (over `Spi`); `_PG_init` registering GUCs
  (`taskboss.database`, `taskboss.maintenance_interval`) and the maintenance background
  worker; `background_worker_main` which calls `boss.maintain()` on a timer. `#[pg_test]` tests
  live in `mod tests`.
- **Delivery model**: consumers `LISTEN boss_<queue>` and are woken by the `pg_notify` issued in
  `boss.send`; the actual job claim still goes through `boss.fetch` (SKIP LOCKED → exactly-once).
  Without listeners, the queue degrades to plain polling via `boss.fetch`.
- **Background worker** requires `shared_preload_libraries = 'taskboss'` (needs a restart) and
  attaches to the single database named by the `taskboss.database` GUC. `pg_test` sets the
  preload via `postgresql_conf_options()`.
- **[taskboss.control](taskboss.control)** — PostgreSQL extension control file; `@CARGO_VERSION@` is substituted by pgrx at build time.
- **[sql/](sql/)** — SQL migration files loaded by pgrx to define or upgrade the extension's SQL-level objects.
- **[tests/pg_regress/](tests/pg_regress/)** — pg_regress integration test scripts; the setup file runs `CREATE EXTENSION taskboss` to bootstrap the test database.
- **[.cargo/config.toml](.cargo/config.toml)** — sets `-Wl,-undefined,dynamic_lookup` on macOS so PostgreSQL symbols resolve at runtime rather than link time.

## Key pgrx conventions

- `::pgrx::pg_module_magic!(name, version)` must appear at the crate root — it emits the PostgreSQL module magic number.
- PostgreSQL features are selected via Cargo features (`pg13`–`pg18`). The `default` feature is `pg18`.
- `panic = "unwind"` is required in both dev and release profiles so Rust panics are caught by PostgreSQL's error-handling machinery instead of aborting the process.
