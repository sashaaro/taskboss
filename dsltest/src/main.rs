//! `dsltest` — a declarative integration-test runner for the taskboss queue.
//!
//! Parses `.scenario` files (winnow) and executes them against a running
//! Postgres instance, one numbered client per real session. Usage:
//!
//! ```text
//! cargo run -p dsltest -- scenarios                    # a directory
//! cargo run -p dsltest -- scenarios/basic.scenario     # specific files
//! TASKBOSS_DSN=postgres://user@host:5432/db cargo run -p dsltest
//! ```

mod ast;
mod error;
mod parser;
mod runner;

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use error::DslError;
use runner::Runner;

const DEFAULT_DSN: &str = "postgres://sasha@localhost:28818/taskboss";
const DEFAULT_DIR: &str = "scenarios";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let dsn = std::env::var("TASKBOSS_DSN").unwrap_or_else(|_| DEFAULT_DSN.to_string());

    let files = match collect_files(&args) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::from(3);
        }
    };
    if files.is_empty() {
        eprintln!("no .scenario files found");
        return ExitCode::from(3);
    }

    let mut worst: i32 = 0;
    let mut passed = 0;
    for file in &files {
        match run_file(file, &dsn) {
            Ok(name) => {
                println!("PASS {} ({name})", file.display());
                passed += 1;
            }
            Err(e) => {
                let where_ = if e.line > 0 {
                    format!("{}:{}", file.display(), e.line)
                } else {
                    file.display().to_string()
                };
                println!("FAIL {where_}: {e}");
                worst = worst.max(e.exit_code());
            }
        }
    }

    println!("\n{passed}/{} scenario(s) passed", files.len());
    ExitCode::from(worst as u8)
}

fn run_file(file: &Path, dsn: &str) -> Result<String, DslError> {
    let src = std::fs::read_to_string(file)
        .map_err(|e| DslError::conn(format!("read {}: {e}", file.display())))?;
    let scenario = parser::parse_scenario(&src)?;
    let mut runner = Runner::new(dsn.to_string());
    runner.run_scenario(&scenario)?;
    Ok(scenario.name)
}

/// Resolve CLI args (files and/or directories) into a sorted list of scenario
/// files. With no args, default to the `dsltest/scenarios` directory.
fn collect_files(args: &[String]) -> Result<Vec<PathBuf>, String> {
    let inputs: Vec<PathBuf> = if args.is_empty() {
        vec![PathBuf::from(DEFAULT_DIR)]
    } else {
        args.iter().map(PathBuf::from).collect()
    };

    let mut files = Vec::new();
    for input in inputs {
        if input.is_dir() {
            let entries =
                std::fs::read_dir(&input).map_err(|e| format!("read dir {}: {e}", input.display()))?;
            for entry in entries {
                let path = entry.map_err(|e| e.to_string())?.path();
                if path.extension().map(|e| e == "scenario").unwrap_or(false) {
                    files.push(path);
                }
            }
        } else if input.exists() {
            files.push(input);
        } else {
            return Err(format!("no such file or directory: {}", input.display()));
        }
    }
    files.sort();
    Ok(files)
}
