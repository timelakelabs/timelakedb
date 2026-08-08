//! Line-protocol parser (FR-1). Zero heavy dependencies; nothing here may
//! scale with historical distinct-key count (PR-2) — the parser sees one
//! request at a time and holds no cross-request state.
//!
//! Supports the full escape set (`\,` `\ ` `\=` in keys/tags, `\"` `\\` in
//! string fields), field types f64 / i64 (`i`) / u64 (`u`) / bool /
//! string, and the v1/v2 `precision` parameter (FR-9). Missing timestamps
//! take the server-provided default (Telegraf sends none by default).

use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    Float(f64),
    Int(i64),
    UInt(u64),
    Bool(bool),
    Str(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParsedLine {
    pub table: String,
    pub tags: Vec<(String, String)>,
    pub fields: Vec<(String, FieldValue)>,
    pub timestamp_ns: i64,
}

#[derive(Debug)]
pub struct ParseError {
    pub line_no: usize,
    pub msg: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {}: {}", self.line_no, self.msg)
    }
}

impl std::error::Error for ParseError {}

/// ns multiplier for a v1/v2 `precision` parameter (FR-9).
pub fn precision_multiplier(p: &str) -> Option<i64> {
    match p {
        "ns" | "n" => Some(1),
        "us" | "u" => Some(1_000),
        "ms" => Some(1_000_000),
        "s" => Some(1_000_000_000),
        _ => None,
    }
}

/// Parse a whole request body. `mult` scales integer timestamps to ns;
/// lines without a timestamp get `default_ts_ns`.
pub fn parse_lines(
    input: &str,
    mult: i64,
    default_ts_ns: i64,
) -> Result<Vec<ParsedLine>, ParseError> {
    let mut out = Vec::with_capacity(128);
    for (i, raw) in input.lines().enumerate() {
        let line = raw.trim_end_matches('\r');
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        out.push(parse_line(line, mult, default_ts_ns).map_err(|msg| ParseError {
            line_no: i + 1,
            msg: format!("{msg} in {line:?}"),
        })?);
    }
    Ok(out)
}

fn parse_line(line: &str, mult: i64, default_ts_ns: i64) -> Result<ParsedLine, String> {
    let bytes = line.as_bytes();
    let mut pos = 0usize;

    // measurement
    let table = take_escaped(bytes, &mut pos, &[b','])?;
    if table.is_empty() {
        return Err("empty measurement".into());
    }

    // tags
    let mut tags = Vec::new();
    while pos < bytes.len() && bytes[pos] == b',' {
        pos += 1;
        let key = take_escaped(bytes, &mut pos, &[b'='])?;
        if pos >= bytes.len() || bytes[pos] != b'=' {
            return Err(format!("tag '{key}' has no value"));
        }
        pos += 1;
        let val = take_escaped(bytes, &mut pos, &[b','])?;
        tags.push((key, val));
    }

    if pos >= bytes.len() || bytes[pos] != b' ' {
        return Err("no field set".into());
    }
    pos += 1;

    // fields
    let mut fields = Vec::new();
    loop {
        let key = take_escaped(bytes, &mut pos, &[b'='])?;
        if pos >= bytes.len() || bytes[pos] != b'=' {
            return Err(format!("field '{key}' has no value"));
        }
        pos += 1;
        let val = take_field_value(bytes, &mut pos)?;
        fields.push((key, val));
        if pos < bytes.len() && bytes[pos] == b',' {
            pos += 1;
            continue;
        }
        break;
    }
    if fields.is_empty() {
        return Err("no fields".into());
    }

    // optional timestamp
    let timestamp_ns = if pos < bytes.len() && bytes[pos] == b' ' {
        pos += 1;
        let ts = std::str::from_utf8(&bytes[pos..])
            .map_err(|_| "invalid utf8 in timestamp".to_string())?
            .trim();
        if ts.is_empty() {
            default_ts_ns
        } else {
            ts.parse::<i64>()
                .map_err(|_| format!("bad timestamp {ts:?}"))?
                .checked_mul(mult)
                .ok_or_else(|| "timestamp overflow".to_string())?
        }
    } else {
        default_ts_ns
    };

    Ok(ParsedLine {
        table,
        tags,
        fields,
        timestamp_ns,
    })
}

/// Read until an unescaped stop byte or space (exclusive); unescapes
/// `\,` `\ ` `\=` `\\`.
fn take_escaped(bytes: &[u8], pos: &mut usize, stops: &[u8]) -> Result<String, String> {
    let mut s = String::new();
    while *pos < bytes.len() {
        let b = bytes[*pos];
        if b == b'\\' && *pos + 1 < bytes.len() {
            let n = bytes[*pos + 1];
            if n == b',' || n == b' ' || n == b'=' || n == b'\\' {
                s.push(n as char);
                *pos += 2;
                continue;
            }
        }
        if stops.contains(&b) || b == b' ' {
            break;
        }
        s.push(b as char);
        *pos += 1;
    }
    Ok(s)
}

fn take_field_value(bytes: &[u8], pos: &mut usize) -> Result<FieldValue, String> {
    if *pos < bytes.len() && bytes[*pos] == b'"' {
        // quoted string with \" and \\ escapes
        *pos += 1;
        let mut s = String::new();
        while *pos < bytes.len() {
            match bytes[*pos] {
                b'\\' if *pos + 1 < bytes.len() => {
                    s.push(bytes[*pos + 1] as char);
                    *pos += 2;
                }
                b'"' => {
                    *pos += 1;
                    return Ok(FieldValue::Str(s));
                }
                b => {
                    s.push(b as char);
                    *pos += 1;
                }
            }
        }
        return Err("unterminated string field".into());
    }

    let start = *pos;
    while *pos < bytes.len() && bytes[*pos] != b',' && bytes[*pos] != b' ' {
        *pos += 1;
    }
    let tok = std::str::from_utf8(&bytes[start..*pos]).map_err(|_| "invalid utf8".to_string())?;
    if tok.is_empty() {
        return Err("empty field value".into());
    }
    match tok {
        "t" | "T" | "true" | "True" | "TRUE" => return Ok(FieldValue::Bool(true)),
        "f" | "F" | "false" | "False" | "FALSE" => return Ok(FieldValue::Bool(false)),
        _ => {}
    }
    if let Some(num) = tok.strip_suffix('i') {
        return num
            .parse::<i64>()
            .map(FieldValue::Int)
            .map_err(|_| format!("bad integer {tok:?}"));
    }
    if let Some(num) = tok.strip_suffix('u') {
        return num
            .parse::<u64>()
            .map(FieldValue::UInt)
            .map_err(|_| format!("bad uinteger {tok:?}"));
    }
    tok.parse::<f64>()
        .map(FieldValue::Float)
        .map_err(|_| format!("bad float {tok:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workload_shapes_parse_exactly() {
        let lp = "pipeline_events,product_id=p00001-00002,step=01-download,route=alpha,worker_ip=172.16.1.7,event=start value=1i 1700000000000000001\npipeline_events,product_id=p00001-00002,step=01-download,route=alpha,worker_ip=172.16.1.7,event=stop duration_s=144.7 1700000000000000002\ndisk_metrics,host=host0001,device=nvme0n1 capacity_gb=240i,used_gb=166.26,used_pct=69.28,read_bps=2919372i,write_bps=4151719i 1700000000000000003";
        let rows = parse_lines(lp, 1, 0).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].table, "pipeline_events");
        assert_eq!(rows[0].tags.len(), 5);
        assert_eq!(rows[0].fields, vec![("value".into(), FieldValue::Int(1))]);
        assert_eq!(rows[0].timestamp_ns, 1_700_000_000_000_000_001);
        assert_eq!(
            rows[1].fields,
            vec![("duration_s".into(), FieldValue::Float(144.7))]
        );
        assert_eq!(rows[2].fields.len(), 5);
    }

    #[test]
    fn escapes_strings_bools_and_precision() {
        let lp = "weird\\ table,k\\==v\\,x msg=\"say \\\"hi\\\"\",ok=true,n=7u 1700000000";
        let rows = parse_lines(lp, 1_000_000_000, 42).unwrap();
        assert_eq!(rows[0].table, "weird table");
        assert_eq!(rows[0].tags, vec![("k=".to_string(), "v,x".to_string())]);
        assert_eq!(rows[0].fields[0].1, FieldValue::Str("say \"hi\"".into()));
        assert_eq!(rows[0].fields[1].1, FieldValue::Bool(true));
        assert_eq!(rows[0].fields[2].1, FieldValue::UInt(7));
        assert_eq!(rows[0].timestamp_ns, 1_700_000_000_000_000_000);

        // missing timestamp -> default
        let rows = parse_lines("m f=1.5", 1, 42).unwrap();
        assert_eq!(rows[0].timestamp_ns, 42);
    }

    #[test]
    fn errors_carry_line_numbers() {
        let err = parse_lines("m f=1\nbroken", 1, 0).unwrap_err();
        assert_eq!(err.line_no, 2);
        let err = parse_lines("m, f=1", 1, 0).unwrap_err();
        assert_eq!(err.line_no, 1);
        assert!(parse_lines("m f=notanumber", 1, 0).is_err());
        assert!(parse_lines("m f=\"unterminated", 1, 0).is_err());
    }
}
