//! `taskboss` — a native PostgreSQL job-queue extension inspired by
//! [pg-boss](https://github.com/timgit/pg-boss).
//!
//! Unlike pg-boss (a Node.js library that polls and maintains the queue from an
//! external process), all logic lives inside PostgreSQL:
//!
//! - The schema, the hot-path `boss.send`/`boss.fetch` (using `SKIP LOCKED`),
//!   and `boss.maintain` are defined in [`schema`].
//! - `boss.create_queue` / `boss.delete_queue` are Rust functions over SPI
//!   (see [`boss`]).
//! - A background worker runs maintenance (expiry + retention) on a timer.
//! - New jobs are pushed to consumers via `LISTEN`/`NOTIFY` (see `boss.send`).

use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;
use std::ffi::CString;
use std::time::Duration;

mod schema;

::pgrx::pg_module_magic!(name, version);

/// Database the maintenance worker connects to. A background worker can only
/// attach to a single database, so this must point at the database where the
/// extension is installed.
static MAINTENANCE_DB: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"postgres"));

/// How often (seconds) the maintenance worker runs `boss.maintain()`.
static MAINTENANCE_INTERVAL: GucSetting<i32> = GucSetting::<i32>::new(60);

#[pg_schema]
mod boss {
    use pgrx::prelude::*;

    /// Create a queue (idempotent). `options` (jsonb) may set `retryLimit`,
    /// `retryDelay`, `expireInSeconds`, `retentionSeconds`.
    #[pg_extern]
    fn create_queue(name: &str, options: default!(pgrx::Json, "'{}'")) -> bool {
        let o = &options.0;
        let as_i32 = |key: &str, default: i32| {
            o.get(key).and_then(|v| v.as_i64()).map(|v| v as i32).unwrap_or(default)
        };
        let retry_limit = as_i32("retryLimit", 2);
        let retry_delay = as_i32("retryDelay", 0);
        let expire = as_i32("expireInSeconds", 900);
        let retention = as_i32("retentionSeconds", 60 * 60 * 24 * 14);

        Spi::run_with_args(
            "INSERT INTO boss.queue
                 (name, retry_limit, retry_delay, expire_seconds, retention_seconds)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (name) DO NOTHING",
            &[
                name.into(),
                retry_limit.into(),
                retry_delay.into(),
                expire.into(),
                retention.into(),
            ],
        )
        .expect("failed to create queue");
        true
    }

    /// Delete a queue and all of its jobs (via `ON DELETE CASCADE`).
    #[pg_extern]
    fn delete_queue(name: &str) -> bool {
        Spi::run_with_args("DELETE FROM boss.queue WHERE name = $1", &[name.into()])
            .expect("failed to delete queue");
        true
    }
}

/// Registers GUCs and the maintenance background worker. Runs once when the
/// library is loaded via `shared_preload_libraries`.
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    GucRegistry::define_string_guc(
        c"taskboss.database",
        c"Database the queue maintenance worker connects to",
        c"A background worker attaches to a single database; set this to the database where taskboss is installed.",
        &MAINTENANCE_DB,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"taskboss.maintenance_interval",
        c"Seconds between queue maintenance runs",
        c"How often the background worker runs boss.maintain() to expire stale jobs and purge old ones.",
        &MAINTENANCE_INTERVAL,
        1,
        86_400,
        GucContext::Sighup,
        GucFlags::default(),
    );

    BackgroundWorkerBuilder::new("taskboss: queue maintenance")
        .set_function("background_worker_main")
        .set_library("taskboss")
        .set_restart_time(Some(Duration::from_secs(5)))
        .enable_spi_access()
        .load();
}

/// Entry point for the maintenance background worker. Calls `boss.maintain()`
/// on the configured interval until the postmaster asks it to stop.
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn background_worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let dbname = MAINTENANCE_DB
        .get()
        .map(|c| c.to_string_lossy().into_owned())
        .unwrap_or_else(|| "postgres".to_string());
    BackgroundWorker::connect_worker_to_spi(Some(&dbname), None);

    let interval = Duration::from_secs(MAINTENANCE_INTERVAL.get().max(1) as u64);

    while BackgroundWorker::wait_latch(Some(interval)) {
        // Guarded so the worker survives databases where the extension isn't
        // installed yet (or has been dropped) without crash-looping.
        BackgroundWorker::transaction(|| {
            Spi::run(
                "DO $$ BEGIN
                     IF to_regproc('boss.maintain') IS NOT NULL THEN
                         PERFORM boss.maintain();
                     END IF;
                 END $$;",
            )
            .expect("queue maintenance failed");
        });
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_send_fetch_complete() {
        Spi::run("SELECT boss.create_queue('welcome')").unwrap();

        let id: Option<pgrx::Uuid> = Spi::get_one(
            "SELECT boss.send('welcome', '{\"to\": \"a@b.c\"}')",
        )
        .unwrap();
        assert!(id.is_some(), "send should return a job id");

        // fetch claims the job and flips it to active
        let state: Option<String> = Spi::get_one(
            "SELECT state::text FROM boss.fetch('welcome', 1)",
        )
        .unwrap();
        assert_eq!(state.as_deref(), Some("active"));

        // a second fetch finds nothing (the only job is now active)
        let again: Option<i64> =
            Spi::get_one("SELECT count(*) FROM boss.fetch('welcome', 1)").unwrap();
        assert_eq!(again, Some(0), "active job must not be re-fetched");

        let done: Option<bool> = Spi::get_one_with_args(
            "SELECT boss.complete('welcome', $1, '{\"ok\": true}')",
            &[id.unwrap().into()],
        )
        .unwrap();
        assert_eq!(done, Some(true));

        let completed: Option<i64> = Spi::get_one(
            "SELECT count(*) FROM boss.job WHERE state = 'completed'",
        )
        .unwrap();
        assert_eq!(completed, Some(1));
    }

    #[pg_test]
    fn test_fail_retries_then_fails() {
        Spi::run(
            "SELECT boss.create_queue('flaky', '{\"retryLimit\": 1, \"retryDelay\": 0}')",
        )
        .unwrap();
        Spi::run("SELECT boss.send('flaky', '{}')").unwrap();

        // First attempt fails -> goes to retry (1 attempt remaining).
        let id1: pgrx::Uuid =
            Spi::get_one("SELECT id FROM boss.fetch('flaky', 1)").unwrap().unwrap();
        Spi::run_with_args(
            "SELECT boss.fail('flaky', $1, '{\"err\": \"boom\"}')",
            &[id1.into()],
        )
        .unwrap();
        let state: Option<String> = Spi::get_one_with_args(
            "SELECT state::text FROM boss.job WHERE id = $1",
            &[id1.into()],
        )
        .unwrap();
        assert_eq!(state.as_deref(), Some("retry"));

        // Second attempt fails -> exhausted, moves to failed.
        let id2: pgrx::Uuid =
            Spi::get_one("SELECT id FROM boss.fetch('flaky', 1)").unwrap().unwrap();
        Spi::run_with_args("SELECT boss.fail('flaky', $1, '{}')", &[id2.into()]).unwrap();
        let state: Option<String> = Spi::get_one_with_args(
            "SELECT state::text FROM boss.job WHERE id = $1",
            &[id2.into()],
        )
        .unwrap();
        assert_eq!(state.as_deref(), Some("failed"));
    }

    #[pg_test]
    fn test_priority_ordering() {
        Spi::run("SELECT boss.create_queue('p')").unwrap();
        Spi::run("SELECT boss.send('p', '{}', '{\"priority\": 1}')").unwrap();
        let hi: pgrx::Uuid =
            Spi::get_one("SELECT boss.send('p', '{}', '{\"priority\": 10}')")
                .unwrap()
                .unwrap();

        // Higher priority job is fetched first.
        let first: pgrx::Uuid =
            Spi::get_one("SELECT id FROM boss.fetch('p', 1)").unwrap().unwrap();
        assert_eq!(first, hi);
    }

    #[pg_test]
    #[should_panic(expected = "does not exist")]
    fn test_send_to_missing_queue_errors() {
        Spi::run("SELECT boss.send('nope', '{}')").unwrap();
    }

    #[pg_test]
    fn test_delete_queue_cascades() {
        Spi::run("SELECT boss.create_queue('temp')").unwrap();
        Spi::run("SELECT boss.send('temp', '{}')").unwrap();
        Spi::run("SELECT boss.delete_queue('temp')").unwrap();

        let jobs: Option<i64> =
            Spi::get_one("SELECT count(*) FROM boss.job WHERE name = 'temp'").unwrap();
        assert_eq!(jobs, Some(0));
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // Load the library so _PG_init runs and the background worker starts.
        vec!["shared_preload_libraries = 'taskboss'"]
    }
}
