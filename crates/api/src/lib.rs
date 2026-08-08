//! HTTP surface: /write (FR-9 v1), /api/v2/write (FR-9 v2),
//! /api/v3/write_lp (FR-1), /api/sql (harness/debug), /health, /ping.
//! M0: routes live in timelord-server; they move here at M1 together
//! with the FR-9 contract tests (gzip, precisions, 204, line errors).
//!
//! See ARCHITECTURE.md SS3 for the crate map.
