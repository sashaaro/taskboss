//! Abstract syntax tree for the test DSL.
//!
//! One [`Scenario`] per file; each [`Statement`] is one line, optionally
//! addressed to a numbered client (`#N`). Options are kept in typed structs so
//! only the options the extension actually supports can be expressed — unknown
//! keys simply fail to parse.

use std::time::Duration;

pub use serde_json::Value;

#[derive(Debug, Clone)]
pub struct Scenario {
    pub name: String,
    pub statements: Vec<Statement>,
}

#[derive(Debug, Clone)]
pub struct Statement {
    /// 1-based source line, for error reporting.
    pub line: usize,
    /// Client (DB session) this statement runs on; defaults to 1.
    pub client: u32,
    pub command: Command,
}

#[derive(Debug, Clone)]
pub enum Command {
    CreateQueue { name: String, options: QueueOptions },
    DeleteQueue { name: String },
    Maintain,
    Push { queue: String, data: Value, options: SendOptions },
    /// Foreground claim; binds the job to `var`. With `within`, waits via
    /// LISTEN/NOTIFY up to the duration before giving up.
    Consume { queue: String, var: String, within: Option<Duration> },
    /// Background claim on a dedicated session/thread; collected by `await`.
    SpawnConsume { queue: String, var: String, within: Option<Duration> },
    Await { var: String },
    Ack { var: String, output: Option<Value> },
    Fail { var: String, output: Option<Value> },
    AssertQueueEmpty { queue: String },
    AssertQueueSize { queue: String, size: i64 },
    CheckState { var: String, state: String },
    CheckAck { var: String, within: Option<Duration> },
    CheckEmpty { var: String },
    CheckData { var: String, expected: Value },
    CheckGone { var: String },
    AssertVarEq { left: String, right: String },
    AssertExactlyOneClaimed { vars: Vec<String> },
}

/// Per-queue defaults — maps to `boss.create_queue` options.
#[derive(Debug, Clone, Default)]
pub struct QueueOptions {
    pub retry_limit: Option<i64>,
    pub retry_delay: Option<i64>,
    pub expire_in_seconds: Option<i64>,
    pub retention_seconds: Option<i64>,
}

impl QueueOptions {
    /// Set a field by its DSL key. The caller only passes keys the parser has
    /// already accepted, so an unknown key here is a bug, not user input.
    pub fn set(&mut self, key: &str, value: i64) {
        match key {
            "retryLimit" => self.retry_limit = Some(value),
            "retryDelay" => self.retry_delay = Some(value),
            "expireInSeconds" => self.expire_in_seconds = Some(value),
            "retentionSeconds" => self.retention_seconds = Some(value),
            _ => unreachable!("unvalidated queue option key: {key}"),
        }
    }

    pub fn to_json(&self) -> Value {
        let mut m = serde_json::Map::new();
        put(&mut m, "retryLimit", self.retry_limit);
        put(&mut m, "retryDelay", self.retry_delay);
        put(&mut m, "expireInSeconds", self.expire_in_seconds);
        put(&mut m, "retentionSeconds", self.retention_seconds);
        Value::Object(m)
    }
}

/// Per-job overrides — maps to `boss.send` options.
#[derive(Debug, Clone, Default)]
pub struct SendOptions {
    pub priority: Option<i64>,
    pub start_after: Option<i64>,
    pub retry_limit: Option<i64>,
    pub retry_delay: Option<i64>,
    pub expire_in_seconds: Option<i64>,
}

impl SendOptions {
    pub fn set(&mut self, key: &str, value: i64) {
        match key {
            "priority" => self.priority = Some(value),
            "startAfter" => self.start_after = Some(value),
            "retryLimit" => self.retry_limit = Some(value),
            "retryDelay" => self.retry_delay = Some(value),
            "expireInSeconds" => self.expire_in_seconds = Some(value),
            _ => unreachable!("unvalidated send option key: {key}"),
        }
    }

    pub fn to_json(&self) -> Value {
        let mut m = serde_json::Map::new();
        put(&mut m, "priority", self.priority);
        put(&mut m, "startAfter", self.start_after);
        put(&mut m, "retryLimit", self.retry_limit);
        put(&mut m, "retryDelay", self.retry_delay);
        put(&mut m, "expireInSeconds", self.expire_in_seconds);
        Value::Object(m)
    }
}

fn put(m: &mut serde_json::Map<String, Value>, key: &str, value: Option<i64>) {
    if let Some(v) = value {
        m.insert(key.to_string(), Value::from(v));
    }
}
