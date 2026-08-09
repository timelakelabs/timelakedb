//! SEC-2: Accumulo-style visibility label expressions.
//!
//! A row's `_visibility` column holds a label expression — `admin`,
//! `ops&audit`, `(ops&audit)|admin` — evaluated against the session's
//! authorization set at scan time. Semantics follow Accumulo's
//! `ColumnVisibility`:
//!
//! - an empty (or NULL) label means the row is visible to everyone;
//! - `&` and `|` may not be mixed at one nesting level without
//!   parentheses (`a&b|c` is malformed — write `(a&b)|c`);
//! - tokens are `[A-Za-z0-9_.:/-]+`, or arbitrary strings quoted with
//!   `"` (backslash escapes `"` and `\`);
//! - a malformed expression makes the row visible to NO ONE (fail
//!   closed): bad data must not become public data.
//!
//! One deviation, for line-protocol ergonomics: ASCII whitespace between
//! tokens is skipped rather than rejected.

use std::collections::HashSet;

/// Evaluate `expr` against `auths`. Empty expressions are visible to all;
/// malformed expressions are visible to none (Err is only for callers
/// that want to distinguish "denied" from "broken" — [`is_visible`]
/// collapses both to false).
pub fn evaluate(expr: &str, auths: &HashSet<&str>) -> Result<bool, String> {
    if expr.trim().is_empty() {
        return Ok(true);
    }
    let mut p = Parser {
        s: expr.as_bytes(),
        i: 0,
        auths,
    };
    let v = p.expression()?;
    p.skip_ws();
    if p.i != p.s.len() {
        return Err(format!(
            "trailing input at byte {} in visibility expression",
            p.i
        ));
    }
    Ok(v)
}

/// The scan-time question: can a session with `auths` see a row labeled
/// `expr`? Malformed labels answer no.
pub fn is_visible(expr: &str, auths: &HashSet<&str>) -> bool {
    evaluate(expr, auths).unwrap_or(false)
}

struct Parser<'a> {
    s: &'a [u8],
    i: usize,
    auths: &'a HashSet<&'a str>,
}

impl<'a> Parser<'a> {
    fn skip_ws(&mut self) {
        while self.i < self.s.len() && self.s[self.i].is_ascii_whitespace() {
            self.i += 1;
        }
    }

    fn peek(&mut self) -> Option<u8> {
        self.skip_ws();
        self.s.get(self.i).copied()
    }

    /// `term (op term)*` with one operator kind per level (Accumulo's
    /// no-precedence rule: mixing means parentheses were forgotten).
    fn expression(&mut self) -> Result<bool, String> {
        let mut acc = self.term()?;
        let mut level_op: Option<u8> = None;
        loop {
            match self.peek() {
                Some(op @ (b'&' | b'|')) => {
                    match level_op {
                        None => level_op = Some(op),
                        Some(prev) if prev != op => {
                            return Err("'&' and '|' mixed without parentheses".to_string());
                        }
                        Some(_) => {}
                    }
                    self.i += 1;
                    let rhs = self.term()?;
                    acc = if op == b'&' { acc && rhs } else { acc || rhs };
                }
                _ => return Ok(acc),
            }
        }
    }

    fn term(&mut self) -> Result<bool, String> {
        match self.peek() {
            Some(b'(') => {
                self.i += 1;
                let v = self.expression()?;
                if self.peek() != Some(b')') {
                    return Err("unclosed '('".to_string());
                }
                self.i += 1;
                Ok(v)
            }
            Some(b'"') => {
                self.i += 1;
                let mut tok = String::new();
                loop {
                    match self.s.get(self.i).copied() {
                        None => return Err("unclosed '\"'".to_string()),
                        Some(b'"') => {
                            self.i += 1;
                            break;
                        }
                        Some(b'\\') => match self.s.get(self.i + 1).copied() {
                            Some(c @ (b'"' | b'\\')) => {
                                tok.push(c as char);
                                self.i += 2;
                            }
                            _ => return Err("bad escape in quoted token".to_string()),
                        },
                        Some(c) => {
                            tok.push(c as char);
                            self.i += 1;
                        }
                    }
                }
                if tok.is_empty() {
                    return Err("empty quoted token".to_string());
                }
                Ok(self.auths.contains(tok.as_str()))
            }
            _ => {
                self.skip_ws();
                let start = self.i;
                while self.i < self.s.len() && is_token_byte(self.s[self.i]) {
                    self.i += 1;
                }
                if self.i == start {
                    return Err(format!("expected a token at byte {start}"));
                }
                // tokens are ASCII-classed bytes, so this slice is valid UTF-8
                let tok = std::str::from_utf8(&self.s[start..self.i])
                    .map_err(|_| "non-UTF-8 token".to_string())?;
                Ok(self.auths.contains(tok))
            }
        }
    }
}

fn is_token_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b':' | b'/')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn auths<'a>(list: &[&'a str]) -> HashSet<&'a str> {
        list.iter().copied().collect()
    }

    #[test]
    fn single_token_and_empty() {
        assert!(is_visible("", &auths(&[])));
        assert!(is_visible("  ", &auths(&["x"])));
        assert!(is_visible("admin", &auths(&["admin"])));
        assert!(!is_visible("admin", &auths(&["ops"])));
        assert!(!is_visible("admin", &auths(&[])));
    }

    #[test]
    fn and_or_and_parens() {
        let a = auths(&["ops", "audit"]);
        assert!(is_visible("ops&audit", &a));
        assert!(!is_visible("ops&admin", &a));
        assert!(is_visible("ops|admin", &a));
        assert!(is_visible("(ops&audit)|admin", &a));
        assert!(is_visible("admin|(ops&audit)", &a));
        assert!(!is_visible("(ops&admin)|hr", &a));
        assert!(is_visible("((ops))", &a));
        assert!(!is_visible("a & b | c", &auths(&["c"]))); // mixed: malformed
        assert!(is_visible("ops & audit", &a)); // whitespace tolerated
    }

    #[test]
    fn quoted_tokens() {
        let a = auths(&["needs quoting", "with\"quote"]);
        assert!(is_visible("\"needs quoting\"", &a));
        assert!(is_visible("\"with\\\"quote\"", &a));
        assert!(!is_visible("\"nope\"", &a));
    }

    #[test]
    fn malformed_fails_closed() {
        let a = auths(&["admin", "ops", "audit"]);
        for bad in [
            "admin&",
            "&admin",
            "admin|",
            "(admin",
            "admin)",
            "a&b|c",
            "\"unclosed",
            "\"\"",
            "admin ops",
            "a$b",
            "()",
        ] {
            assert!(!is_visible(bad, &a), "{bad:?} must be visible to no one");
            assert!(evaluate(bad, &a).is_err(), "{bad:?} must parse as an error");
        }
    }

    #[test]
    fn accumulo_shaped_expressions() {
        assert!(is_visible("(ops&audit)|admin", &auths(&["admin"])));
        assert!(is_visible("a|b|c|d", &auths(&["d"])));
        assert!(!is_visible("a&b&c&d", &auths(&["a", "b", "c"])));
        assert!(is_visible("system:internal", &auths(&["system:internal"])));
    }
}
