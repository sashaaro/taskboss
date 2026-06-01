//! Error type for the DSL: every failure carries the 1-based source line so the
//! runner can report `FAIL <file>:<line>: <msg>` and pick a meaningful exit code.

use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// The scenario file could not be parsed (bad syntax, unknown option, ...).
    Parse,
    /// An `assert`/`check` statement did not hold.
    Assert,
    /// A statement was well-formed but could not run (e.g. unbound variable).
    Runtime,
    /// Could not connect / set up the database session.
    Conn,
    /// A database call returned an error.
    Db,
}

#[derive(Debug, Clone)]
pub struct DslError {
    /// 1-based source line, or 0 when not yet known (filled in by the runner).
    pub line: usize,
    pub kind: Kind,
    pub msg: String,
}

impl DslError {
    pub fn new(kind: Kind, line: usize, msg: impl Into<String>) -> Self {
        DslError { line, kind, msg: msg.into() }
    }
    pub fn parse(line: usize, msg: impl Into<String>) -> Self {
        Self::new(Kind::Parse, line, msg)
    }
    pub fn assert(line: usize, msg: impl Into<String>) -> Self {
        Self::new(Kind::Assert, line, msg)
    }
    pub fn runtime(line: usize, msg: impl Into<String>) -> Self {
        Self::new(Kind::Runtime, line, msg)
    }
    pub fn conn(msg: impl Into<String>) -> Self {
        Self::new(Kind::Conn, 0, msg)
    }
    pub fn db(msg: impl Into<String>) -> Self {
        Self::new(Kind::Db, 0, msg)
    }

    /// Process exit code associated with this failure category.
    pub fn exit_code(&self) -> i32 {
        match self.kind {
            Kind::Parse => 2,
            Kind::Conn => 3,
            Kind::Assert | Kind::Runtime | Kind::Db => 1,
        }
    }
}

impl fmt::Display for DslError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl std::error::Error for DslError {}
