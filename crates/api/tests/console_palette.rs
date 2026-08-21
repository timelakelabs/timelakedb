//! The console must stay on the brand palette.
//!
//! It did not, and nothing noticed. The site, the docs and the Grafana
//! dashboard moved to navy/blue/sky/teal; `admin_ui.html` kept
//! `--gold: #D4AF37` and a navy one digit out from the real one, and
//! shipped that way for months. It is the only one of those surfaces a
//! customer actually opens.
//!
//! Nobody spotted it because nobody opens the admin UI unless something is
//! on fire, and `#0B1320` against `#0B1220` does not announce itself. So
//! this asserts the tokens instead of trusting the next person to look.
//!
//! What this does NOT check is whether the result is any good — that a
//! warning still reads as a warning, that contrast holds. Render it and
//! look:
//!
//!     docker run --rm -v "$PWD":/w -w /w zenika/alpine-chrome \
//!       --no-sandbox --headless --screenshot=/w/out.png \
//!       /w/crates/api/src/admin_ui.html

const CONSOLE: &str = include_str!("../src/admin_ui.html");

/// The palette of record, from `CLAUDE.md`. Brand tokens only.
///
/// `--gray` is shared with the site and unchanged. `--red`, `--amber` and
/// `--green` are deliberately absent: they are semantic status colours,
/// not brand, and they are allowed to move independently of a rebrand.
/// That distinction is the whole reason `--gold` could not simply be
/// renamed — it was doing both jobs at once.
const BRAND: &[(&str, &str)] = &[
    ("--navy", "#0B1220"),
    ("--blue", "#3B82F6"),
    ("--sky", "#7DD3FC"),
    ("--teal", "#14B8A6"),
    ("--mist", "#E6EDF5"),
];

/// Values from the pre-rebrand palette. Any of these reappearing means the
/// console has drifted back, or a copy-paste brought an old block with it.
const RETIRED: &[(&str, &str)] = &[
    ("#D4AF37", "gold, dropped from the brand entirely"),
    ("#0B1320", "the old navy, one digit from the current one"),
    ("#2563EB", "the old blue"),
    ("#E6E8EC", "the old mist"),
    ("212,175,55", "gold as rgb(), in the warning background"),
    ("230,232,236", "old mist as rgb(), in borders"),
    ("#93b4fd", "a hand-mixed light blue, superseded by --sky"),
];

#[test]
fn console_uses_the_current_brand_palette() {
    for (token, hex) in BRAND {
        let decl = format!("{token}:{hex}");
        assert!(
            CONSOLE.contains(&decl),
            "admin_ui.html is missing `{decl}`.\n\
             The console palette must match CLAUDE.md. If the brand moved, \
             move this test with it — deliberately, in the same commit."
        );
    }
}

#[test]
fn no_pre_rebrand_colours_survive() {
    for (value, what) in RETIRED {
        assert!(
            !CONSOLE.contains(value),
            "admin_ui.html still contains `{value}` ({what}).\n\
             Retired colours must not come back. Check derived rgba() values \
             too — they are hand-mixed from the tokens and drift silently \
             when a token changes."
        );
    }
}

#[test]
fn gold_is_gone_by_name_as_well_as_by_value() {
    assert!(
        !CONSOLE.contains("--gold"),
        "the `--gold` token is still declared or referenced. It has no \
         counterpart in the new palette: it was doing brand-accent duty in \
         one place and warning duty in another, so it was replaced by \
         --teal and --amber respectively rather than renamed."
    );
}

#[test]
fn warnings_do_not_use_the_brand_accent() {
    // A warning stripe painted with the accent makes every caution box look
    // like a heading. Semantic colours stay separate from brand ones, which
    // is the mistake --gold embodied.
    let warn = CONSOLE
        .lines()
        .find(|l| l.trim_start().starts_with(".warn{"))
        .expect("no .warn rule in admin_ui.html");
    assert!(
        warn.contains("var(--amber)"),
        ".warn should use the semantic --amber, got: {warn}"
    );
    assert!(
        !warn.contains("var(--teal)") && !warn.contains("var(--blue)"),
        ".warn must not use a brand colour: {warn}"
    );
}
