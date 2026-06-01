# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a PostgreSQL extension written in Rust using [pgrx](https://github.com/pgcentralfoundation/pgrx) (v0.18.1), targeting PostgreSQL 18 by default. The extension is scaffolded via `cargo pgrx new` and compiled as a `cdylib`.

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
cargo pgrx test pg18 -- test_hello_my_extension
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

- **[src/lib.rs](src/lib.rs)** — single entry point for all extension logic. Functions exposed to PostgreSQL are annotated with `#[pg_extern]`. Tests inside `#[pg_schema] mod tests` use `#[pg_test]` and run inside a live PostgreSQL session via `cargo pgrx test`. Benchmarks use `#[pg_bench]` under the `pg_bench` feature.
- **[my_extension.control](my_extension.control)** — PostgreSQL extension control file; `@CARGO_VERSION@` is substituted by pgrx at build time.
- **[sql/](sql/)** — SQL migration files loaded by pgrx to define or upgrade the extension's SQL-level objects.
- **[tests/pg_regress/](tests/pg_regress/)** — pg_regress integration test scripts; the setup file runs `CREATE EXTENSION my_extension` to bootstrap the test database.
- **[.cargo/config.toml](.cargo/config.toml)** — sets `-Wl,-undefined,dynamic_lookup` on macOS so PostgreSQL symbols resolve at runtime rather than link time.

## Key pgrx conventions

- `::pgrx::pg_module_magic!(name, version)` must appear at the crate root — it emits the PostgreSQL module magic number.
- PostgreSQL features are selected via Cargo features (`pg13`–`pg18`). The `default` feature is `pg18`.
- `panic = "unwind"` is required in both dev and release profiles so Rust panics are caught by PostgreSQL's error-handling machinery instead of aborting the process.
