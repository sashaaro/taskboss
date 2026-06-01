//! winnow parser: scenario text -> [`Scenario`].
//!
//! The DSL is line-oriented (one statement per line), so [`parse_scenario`]
//! tracks line numbers itself, strips blank/comment lines, and hands each
//! statement line to a winnow parser. Options are matched against a fixed set
//! of allowed keys per command — an unknown key (e.g. `capacity`) is left
//! unconsumed and surfaces as an "unexpected trailing input" parse error that
//! names it, on the right line.

use std::time::Duration;

use winnow::ascii::{digit1, space1};
use winnow::combinator::{alt, delimited, opt, preceded, repeat};
use winnow::token::take_while;
use winnow::{ModalResult, Parser};

use crate::ast::*;
use crate::error::DslError;

/// Parse a whole scenario file.
pub fn parse_scenario(src: &str) -> Result<Scenario, DslError> {
    let mut name: Option<String> = None;
    let mut statements = Vec::new();

    for (i, raw) in src.lines().enumerate() {
        let line_no = i + 1;
        let text = raw.trim();
        if text.is_empty() || is_comment(text) {
            continue;
        }

        if name.is_none() {
            let mut input = text;
            let n = header
                .parse_next(&mut input)
                .map_err(|e| DslError::parse(line_no, format!("expected `scenario <name>:` ({e})")))?;
            if !input.trim().is_empty() {
                return Err(DslError::parse(
                    line_no,
                    format!("unexpected input after scenario header: {:?}", input.trim()),
                ));
            }
            name = Some(n);
            continue;
        }

        let (client, command) = parse_line(line_no, text)?;
        statements.push(Statement { line: line_no, client, command });
    }

    let name = name
        .ok_or_else(|| DslError::parse(0, "empty scenario file (missing `scenario` header)"))?;
    Ok(Scenario { name, statements })
}

/// A line is a comment when it starts with `#` followed by a non-digit (or
/// nothing). `#` directly followed by a digit is a client prefix, not a comment.
fn is_comment(text: &str) -> bool {
    let mut chars = text.chars();
    if chars.next() != Some('#') {
        return false;
    }
    match chars.next() {
        Some(c) => !c.is_ascii_digit(),
        None => true,
    }
}

fn parse_line(line_no: usize, text: &str) -> Result<(u32, Command), DslError> {
    let mut input = text;
    match statement.parse_next(&mut input) {
        Ok((client, command)) => {
            let rest = input.trim();
            if !rest.is_empty() {
                return Err(DslError::parse(line_no, format!("unexpected trailing input: {rest:?}")));
            }
            Ok((client, command))
        }
        Err(e) => Err(DslError::parse(line_no, format!("syntax error: {e}"))),
    }
}

// ---------------------------------------------------------------------------
// Top-level statement
// ---------------------------------------------------------------------------

fn statement(input: &mut &str) -> ModalResult<(u32, Command)> {
    let client = client_prefix(input)?;
    let command = command(input)?;
    Ok((client, command))
}

fn client_prefix(input: &mut &str) -> ModalResult<u32> {
    opt((preceded('#', digit1).try_map(|s: &str| s.parse::<u32>()), space1))
        .map(|o| o.map(|(n, _)| n).unwrap_or(1))
        .parse_next(input)
}

fn command(input: &mut &str) -> ModalResult<Command> {
    alt((
        create_queue,
        delete_queue,
        maintain,
        push_cmd,
        spawn_consume,
        consume_cmd,
        ack_cmd,
        fail_cmd,
        await_cmd,
        assert_cmd,
        check_cmd,
    ))
    .parse_next(input)
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

fn create_queue(input: &mut &str) -> ModalResult<Command> {
    ("create", space1, "queue", space1).parse_next(input)?;
    let name = ident(input)?;
    let raw: Vec<(String, i64)> = repeat(0.., preceded(space1, queue_opt)).parse_next(input)?;
    let mut options = QueueOptions::default();
    for (k, v) in raw {
        options.set(&k, v);
    }
    Ok(Command::CreateQueue { name, options })
}

fn delete_queue(input: &mut &str) -> ModalResult<Command> {
    ("delete", space1, "queue", space1).parse_next(input)?;
    let name = ident(input)?;
    Ok(Command::DeleteQueue { name })
}

fn maintain(input: &mut &str) -> ModalResult<Command> {
    "maintain".parse_next(input)?;
    Ok(Command::Maintain)
}

enum PushItem {
    Value(Value),
    Opt(String, i64),
}

fn push_cmd(input: &mut &str) -> ModalResult<Command> {
    (alt(("push", "send")), space1).parse_next(input)?;
    let queue = ident(input)?;
    let items: Vec<PushItem> = repeat(
        0..,
        preceded(
            space1,
            alt((
                message_clause.map(PushItem::Value),
                data_clause.map(PushItem::Value),
                send_opt.map(|(k, v)| PushItem::Opt(k, v)),
            )),
        ),
    )
    .parse_next(input)?;

    let mut data: Option<Value> = None;
    let mut options = SendOptions::default();
    for item in items {
        match item {
            PushItem::Value(v) => data = Some(v),
            PushItem::Opt(k, v) => options.set(&k, v),
        }
    }
    let data = data.unwrap_or_else(|| Value::Object(serde_json::Map::new()));
    Ok(Command::Push { queue, data, options })
}

fn consume_cmd(input: &mut &str) -> ModalResult<Command> {
    (alt(("consume", "fetch")), space1).parse_next(input)?;
    let queue = ident(input)?;
    (space1, "->", space1).parse_next(input)?;
    let var = ident(input)?;
    let within = opt(preceded((space1, "within", space1), duration)).parse_next(input)?;
    Ok(Command::Consume { queue, var, within })
}

fn spawn_consume(input: &mut &str) -> ModalResult<Command> {
    ("spawn", space1, "consume", space1).parse_next(input)?;
    let queue = ident(input)?;
    (space1, "->", space1).parse_next(input)?;
    let var = ident(input)?;
    let within = opt(preceded((space1, "within", space1), duration)).parse_next(input)?;
    Ok(Command::SpawnConsume { queue, var, within })
}

fn await_cmd(input: &mut &str) -> ModalResult<Command> {
    ("await", space1).parse_next(input)?;
    let var = ident(input)?;
    Ok(Command::Await { var })
}

fn ack_cmd(input: &mut &str) -> ModalResult<Command> {
    (alt(("ack", "complete")), space1).parse_next(input)?;
    let var = ident(input)?;
    let output = opt(preceded((space1, "output", space1), json_value)).parse_next(input)?;
    Ok(Command::Ack { var, output })
}

fn fail_cmd(input: &mut &str) -> ModalResult<Command> {
    ("fail", space1).parse_next(input)?;
    let var = ident(input)?;
    let output = opt(preceded((space1, "output", space1), json_value)).parse_next(input)?;
    Ok(Command::Fail { var, output })
}

fn assert_cmd(input: &mut &str) -> ModalResult<Command> {
    ("assert", space1).parse_next(input)?;
    alt((assert_queue, assert_exactly_one, assert_var_eq)).parse_next(input)
}

fn assert_queue(input: &mut &str) -> ModalResult<Command> {
    ("queue", space1).parse_next(input)?;
    let queue = ident(input)?;
    space1.parse_next(input)?;
    let size = alt((
        "empty".map(|_| None::<i64>),
        (("size", space1), integer).map(|(_, n)| Some(n)),
    ))
    .parse_next(input)?;
    Ok(match size {
        None => Command::AssertQueueEmpty { queue },
        Some(n) => Command::AssertQueueSize { queue, size: n },
    })
}

fn assert_exactly_one(input: &mut &str) -> ModalResult<Command> {
    "exactly_one_claimed".parse_next(input)?;
    let vars: Vec<String> = repeat(1.., preceded(space1, ident)).parse_next(input)?;
    Ok(Command::AssertExactlyOneClaimed { vars })
}

fn assert_var_eq(input: &mut &str) -> ModalResult<Command> {
    let left = ident(input)?;
    (space1, "==", space1).parse_next(input)?;
    let right = ident(input)?;
    Ok(Command::AssertVarEq { left, right })
}

enum CheckKind {
    State(String),
    Ack(Option<Duration>),
    Empty,
}

fn check_cmd(input: &mut &str) -> ModalResult<Command> {
    ("check", space1).parse_next(input)?;
    let var = ident(input)?;
    space1.parse_next(input)?;
    let kind = alt((
        (("state", space1), state_lit).map(|(_, s)| CheckKind::State(s.to_string())),
        ("ack", opt(preceded((space1, "within", space1), duration))).map(|(_, w)| CheckKind::Ack(w)),
        "empty".map(|_| CheckKind::Empty),
    ))
    .parse_next(input)?;
    Ok(match kind {
        CheckKind::State(state) => Command::CheckState { var, state },
        CheckKind::Ack(within) => Command::CheckAck { var, within },
        CheckKind::Empty => Command::CheckEmpty { var },
    })
}

// ---------------------------------------------------------------------------
// Leaf parsers
// ---------------------------------------------------------------------------

fn header(input: &mut &str) -> ModalResult<String> {
    ("scenario", space1).parse_next(input)?;
    let name = ident(input)?;
    opt(':').parse_next(input)?;
    Ok(name)
}

fn ident(input: &mut &str) -> ModalResult<String> {
    take_while(1.., |c: char| c.is_alphanumeric() || c == '_' || c == '-')
        .map(|s: &str| s.to_string())
        .parse_next(input)
}

fn state_lit<'s>(input: &mut &'s str) -> ModalResult<&'s str> {
    alt((
        "created",
        "retry",
        "active",
        "completed",
        "cancelled",
        "failed",
    ))
    .parse_next(input)
}

fn integer(input: &mut &str) -> ModalResult<i64> {
    (opt('-'), digit1)
        .take()
        .try_map(|s: &str| s.parse::<i64>())
        .parse_next(input)
}

fn duration(input: &mut &str) -> ModalResult<Duration> {
    let n = digit1.try_map(|s: &str| s.parse::<u64>()).parse_next(input)?;
    let unit = alt(("ms", "s", "m")).parse_next(input)?;
    Ok(match unit {
        "ms" => Duration::from_millis(n),
        "s" => Duration::from_secs(n),
        _ => Duration::from_secs(n * 60),
    })
}

fn quoted(input: &mut &str) -> ModalResult<String> {
    delimited('"', take_while(0.., |c: char| c != '"'), '"')
        .map(|s: &str| s.to_string())
        .parse_next(input)
}

/// Consume the remaining text on the line (used for inline JSON values).
fn rest_of_line<'s>(input: &mut &'s str) -> ModalResult<&'s str> {
    let s = *input;
    *input = "";
    Ok(s)
}

fn json_value(input: &mut &str) -> ModalResult<Value> {
    rest_of_line
        .try_map(|s: &str| serde_json::from_str::<Value>(s.trim()))
        .parse_next(input)
}

fn message_clause(input: &mut &str) -> ModalResult<Value> {
    ("message", space1).parse_next(input)?;
    let s = quoted(input)?;
    Ok(Value::String(s))
}

fn data_clause(input: &mut &str) -> ModalResult<Value> {
    ("data", space1).parse_next(input)?;
    json_value(input)
}

fn queue_opt(input: &mut &str) -> ModalResult<(String, i64)> {
    let key = alt((
        "retryLimit",
        "retryDelay",
        "expireInSeconds",
        "retentionSeconds",
    ))
    .parse_next(input)?;
    '='.parse_next(input)?;
    let value = integer(input)?;
    Ok((key.to_string(), value))
}

fn send_opt(input: &mut &str) -> ModalResult<(String, i64)> {
    let key = alt((
        "priority",
        "startAfter",
        "retryLimit",
        "retryDelay",
        "expireInSeconds",
    ))
    .parse_next(input)?;
    '='.parse_next(input)?;
    let value = integer(input)?;
    Ok((key.to_string(), value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_basic_scenario() {
        let src = "scenario demo:\n    create queue orders retryLimit=3\n    #2 push orders message \"hi\" priority=5\n    consume orders -> m1\n    ack m1\n    assert queue orders empty\n";
        let sc = parse_scenario(src).expect("parse");
        assert_eq!(sc.name, "demo");
        assert_eq!(sc.statements.len(), 5);
        assert_eq!(sc.statements[1].client, 2);
    }

    #[test]
    fn rejects_unknown_option() {
        let src = "scenario demo:\n    create queue orders capacity=100\n";
        let err = parse_scenario(src).unwrap_err();
        assert_eq!(err.line, 2);
        assert!(err.msg.contains("capacity"), "msg was: {}", err.msg);
    }

    #[test]
    fn hash_then_space_is_comment() {
        let src = "scenario demo:\n# a comment\n    maintain\n";
        let sc = parse_scenario(src).expect("parse");
        assert_eq!(sc.statements.len(), 1);
    }
}
