//! Executor: runs a parsed [`Scenario`] against a live Postgres instance.
//!
//! Each numbered client (`#N`) gets its own [`postgres::Client`] — a distinct
//! session — so competing consumers exercise `FOR UPDATE SKIP LOCKED` for real
//! and `spawn consume` can block on LISTEN/NOTIFY while another client produces.

use std::collections::HashMap;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use fallible_iterator::FallibleIterator;
use postgres::{Client, NoTls};

use crate::ast::*;
use crate::error::DslError;

/// A claimed job: queue name plus its uuid (kept as text to avoid a uuid dep).
#[derive(Debug, Clone)]
pub struct JobHandle {
    pub queue: String,
    pub id: String,
}

/// What a scenario variable is bound to.
#[derive(Debug, Clone)]
enum VarValue {
    Job(JobHandle),
    /// A spawned consume that timed out without claiming anything.
    Empty,
}

pub struct Runner {
    dsn: String,
    clients: HashMap<u32, Client>,
    vars: HashMap<String, VarValue>,
    pending: HashMap<String, JoinHandle<Result<Option<JobHandle>, String>>>,
}

impl Runner {
    pub fn new(dsn: String) -> Self {
        Runner { dsn, clients: HashMap::new(), vars: HashMap::new(), pending: HashMap::new() }
    }

    /// Run one scenario start to finish, cleaning its queues before and after.
    pub fn run_scenario(&mut self, sc: &Scenario) -> Result<(), DslError> {
        // Ensure the base session exists (also runs CREATE EXTENSION once).
        self.client_for(1)?;
        let queues = collect_queues(sc);
        self.cleanup_queues(&queues);

        let mut result = Ok(());
        for st in &sc.statements {
            if let Err(mut e) = self.exec(st) {
                if e.line == 0 {
                    e.line = st.line;
                }
                result = Err(e);
                break;
            }
        }

        // Always join leftover background consumers and drop the queues.
        self.join_all_pending();
        self.cleanup_queues(&queues);
        result
    }

    fn exec(&mut self, st: &Statement) -> Result<(), DslError> {
        let line = st.line;
        let client = st.client;
        match &st.command {
            Command::CreateQueue { name, options } => {
                let opts = options.to_json();
                let c = self.client_for(client)?;
                c.execute("SELECT boss.create_queue($1, $2::jsonb)", &[name, &opts])
                    .map_err(db)?;
            }
            Command::DeleteQueue { name } => {
                let c = self.client_for(client)?;
                c.execute("SELECT boss.delete_queue($1)", &[name]).map_err(db)?;
            }
            Command::Maintain => {
                let c = self.client_for(client)?;
                c.execute("SELECT boss.maintain()", &[]).map_err(db)?;
            }
            Command::Push { queue, data, options } => {
                let opts = options.to_json();
                let c = self.client_for(client)?;
                c.query_one(
                    "SELECT boss.send($1, $2::jsonb, $3::jsonb)::text",
                    &[queue, data, &opts],
                )
                .map_err(db)?;
            }
            Command::Consume { queue, var, within } => {
                let job = {
                    let c = self.client_for(client)?;
                    consume_wait(c, queue, *within)?
                };
                match job {
                    Some(j) => {
                        self.vars.insert(var.clone(), VarValue::Job(j));
                    }
                    None => {
                        return Err(DslError::assert(
                            line,
                            format!("consume {queue} -> {var}: no job became available"),
                        ));
                    }
                }
            }
            Command::SpawnConsume { queue, var, within } => {
                let dsn = self.dsn.clone();
                let queue = queue.clone();
                let within = *within;
                let handle = std::thread::spawn(move || -> Result<Option<JobHandle>, String> {
                    let mut c = Client::connect(&dsn, NoTls).map_err(|e| e.to_string())?;
                    // Extension already created by the foreground session; ignore errors.
                    let _ = c.batch_execute("CREATE EXTENSION IF NOT EXISTS taskboss");
                    consume_wait(&mut c, &queue, within).map_err(|e| e.msg)
                });
                self.pending.insert(var.clone(), handle);
            }
            Command::Await { var } => {
                let handle = self
                    .pending
                    .remove(var)
                    .ok_or_else(|| DslError::runtime(line, format!("await {var}: no spawned consume")))?;
                let joined = handle
                    .join()
                    .map_err(|_| DslError::runtime(line, format!("await {var}: consumer thread panicked")))?;
                let value = joined.map_err(|e| DslError::runtime(line, format!("await {var}: {e}")))?;
                self.vars.insert(
                    var.clone(),
                    match value {
                        Some(j) => VarValue::Job(j),
                        None => VarValue::Empty,
                    },
                );
            }
            Command::Ack { var, output } => {
                let j = self.job(var, line)?;
                let out = output.clone().unwrap_or_else(empty_obj);
                let ok = {
                    let c = self.client_for(client)?;
                    let row = c
                        .query_one("SELECT boss.complete($1, $2::text::uuid, $3::jsonb)", &[&j.queue, &j.id, &out])
                        .map_err(db)?;
                    row.get::<_, bool>(0)
                };
                if !ok {
                    return Err(DslError::assert(line, format!("ack {var}: job was not active")));
                }
            }
            Command::Fail { var, output } => {
                let j = self.job(var, line)?;
                let out = output.clone().unwrap_or_else(empty_obj);
                let ok = {
                    let c = self.client_for(client)?;
                    let row = c
                        .query_one("SELECT boss.fail($1, $2::text::uuid, $3::jsonb)", &[&j.queue, &j.id, &out])
                        .map_err(db)?;
                    row.get::<_, bool>(0)
                };
                if !ok {
                    return Err(DslError::assert(line, format!("fail {var}: job was not active")));
                }
            }
            Command::AssertQueueEmpty { queue } => {
                let n = self.pending_count(client, queue)?;
                if n != 0 {
                    return Err(DslError::assert(
                        line,
                        format!("assert queue {queue} empty: {n} job(s) still pending"),
                    ));
                }
            }
            Command::AssertQueueSize { queue, size } => {
                let n = self.pending_count(client, queue)?;
                if n != *size {
                    return Err(DslError::assert(
                        line,
                        format!("assert queue {queue} size {size}: got {n}"),
                    ));
                }
            }
            Command::CheckState { var, state } => {
                let j = self.job(var, line)?;
                let actual = {
                    let c = self.client_for(client)?;
                    job_state(c, &j)?
                };
                match actual {
                    Some(s) if &s == state => {}
                    Some(s) => {
                        return Err(DslError::assert(
                            line,
                            format!("check {var} state {state}: got {s}"),
                        ))
                    }
                    None => {
                        return Err(DslError::assert(
                            line,
                            format!("check {var} state {state}: job not found"),
                        ))
                    }
                }
            }
            Command::CheckAck { var, within } => {
                let j = self.job(var, line)?;
                let deadline = within.map(|w| Instant::now() + w);
                loop {
                    let state = {
                        let c = self.client_for(client)?;
                        job_state(c, &j)?
                    };
                    if state.as_deref() == Some("completed") {
                        break;
                    }
                    match deadline {
                        Some(d) if Instant::now() < d => {
                            std::thread::sleep(Duration::from_millis(20));
                        }
                        _ => {
                            return Err(DslError::assert(
                                line,
                                format!("check {var} ack: state={state:?}, expected completed"),
                            ))
                        }
                    }
                }
            }
            Command::CheckEmpty { var } => match self.vars.get(var) {
                Some(VarValue::Empty) => {}
                Some(VarValue::Job(_)) => {
                    return Err(DslError::assert(line, format!("check {var} empty: a job was claimed")))
                }
                None => return Err(DslError::runtime(line, format!("check {var} empty: {var} is not bound"))),
            },
            Command::AssertVarEq { left, right } => {
                let a = self.job(left, line)?;
                let b = self.job(right, line)?;
                if a.id != b.id {
                    return Err(DslError::assert(
                        line,
                        format!("assert {left} == {right}: {} != {}", a.id, b.id),
                    ));
                }
            }
            Command::AssertExactlyOneClaimed { vars } => {
                let mut claimed = 0;
                for v in vars {
                    match self.vars.get(v) {
                        Some(VarValue::Job(_)) => claimed += 1,
                        Some(VarValue::Empty) => {}
                        None => {
                            return Err(DslError::runtime(line, format!("assert exactly_one_claimed: {v} is not bound")))
                        }
                    }
                }
                if claimed != 1 {
                    return Err(DslError::assert(
                        line,
                        format!("assert exactly_one_claimed {:?}: {claimed} claimed (expected 1)", vars),
                    ));
                }
            }
        }
        Ok(())
    }

    fn client_for(&mut self, n: u32) -> Result<&mut Client, DslError> {
        if !self.clients.contains_key(&n) {
            let c = connect(&self.dsn)?;
            self.clients.insert(n, c);
        }
        Ok(self.clients.get_mut(&n).unwrap())
    }

    fn job(&self, var: &str, line: usize) -> Result<JobHandle, DslError> {
        match self.vars.get(var) {
            Some(VarValue::Job(j)) => Ok(j.clone()),
            Some(VarValue::Empty) => {
                Err(DslError::runtime(line, format!("{var} is empty (no job was claimed)")))
            }
            None => Err(DslError::runtime(line, format!("{var} is not bound"))),
        }
    }

    fn pending_count(&mut self, client: u32, queue: &str) -> Result<i64, DslError> {
        let c = self.client_for(client)?;
        let row = c
            .query_one(
                "SELECT count(*) FROM boss.job WHERE name = $1 AND state IN ('created', 'retry')",
                &[&queue],
            )
            .map_err(db)?;
        Ok(row.get::<_, i64>(0))
    }

    fn cleanup_queues(&mut self, queues: &[String]) {
        for q in queues {
            if let Ok(c) = self.client_for(1) {
                let _ = c.execute("SELECT boss.delete_queue($1)", &[q]);
            }
        }
    }

    fn join_all_pending(&mut self) {
        for (_, handle) in self.pending.drain() {
            let _ = handle.join();
        }
    }
}

/// Try to claim one job; if none and `within` is set, LISTEN and wait for a
/// NOTIFY (re-fetching on each wake) until the deadline.
fn consume_wait(c: &mut Client, queue: &str, within: Option<Duration>) -> Result<Option<JobHandle>, DslError> {
    if let Some(j) = fetch_one(c, queue)? {
        return Ok(Some(j));
    }
    let within = match within {
        Some(w) => w,
        None => return Ok(None),
    };

    let channel = channel_name(c, queue)?;
    c.batch_execute(&format!("LISTEN {}", quote_ident(&channel))).map_err(db)?;
    // A producer may have raced in between the first fetch and the LISTEN.
    if let Some(j) = fetch_one(c, queue)? {
        return Ok(Some(j));
    }

    let deadline = Instant::now() + within;
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Ok(None);
        }
        let woke = {
            let mut notifications = c.notifications();
            let mut iter = notifications.timeout_iter(deadline - now);
            iter.next().map_err(db)?.is_some()
        };
        if !woke {
            return Ok(None); // timed out
        }
        if let Some(j) = fetch_one(c, queue)? {
            return Ok(Some(j));
        }
        // Lost the race to another consumer; keep waiting until the deadline.
    }
}

fn fetch_one(c: &mut Client, queue: &str) -> Result<Option<JobHandle>, DslError> {
    let rows = c
        .query("SELECT id::text FROM boss.fetch($1, 1)", &[&queue])
        .map_err(db)?;
    Ok(rows
        .first()
        .map(|r| JobHandle { queue: queue.to_string(), id: r.get::<_, String>(0) }))
}

fn job_state(c: &mut Client, j: &JobHandle) -> Result<Option<String>, DslError> {
    let row = c
        .query_opt(
            "SELECT state::text FROM boss.job WHERE name = $1 AND id = $2::text::uuid",
            &[&j.queue, &j.id],
        )
        .map_err(db)?;
    Ok(row.map(|r| r.get::<_, String>(0)))
}

fn channel_name(c: &mut Client, queue: &str) -> Result<String, DslError> {
    let row = c.query_one("SELECT boss.channel($1)", &[&queue]).map_err(db)?;
    Ok(row.get::<_, String>(0))
}

fn connect(dsn: &str) -> Result<Client, DslError> {
    let mut c = Client::connect(dsn, NoTls).map_err(|e| DslError::conn(format!("connect: {e}")))?;
    c.batch_execute("CREATE EXTENSION IF NOT EXISTS taskboss")
        .map_err(|e| DslError::conn(format!("create extension: {e}")))?;
    Ok(c)
}

/// Collect every queue name a scenario touches, for setup/teardown cleanup.
fn collect_queues(sc: &Scenario) -> Vec<String> {
    let mut seen = Vec::new();
    let mut push = |q: &String| {
        if !seen.iter().any(|x| x == q) {
            seen.push(q.clone());
        }
    };
    for st in &sc.statements {
        match &st.command {
            Command::CreateQueue { name, .. } | Command::DeleteQueue { name } => push(name),
            Command::Push { queue, .. }
            | Command::Consume { queue, .. }
            | Command::SpawnConsume { queue, .. }
            | Command::AssertQueueEmpty { queue }
            | Command::AssertQueueSize { queue, .. } => push(queue),
            _ => {}
        }
    }
    seen
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

fn empty_obj() -> Value {
    Value::Object(serde_json::Map::new())
}

fn db(e: postgres::Error) -> DslError {
    DslError::db(format!("db: {e}"))
}
