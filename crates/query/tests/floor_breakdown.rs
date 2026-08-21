//! Where does the per-query fixed cost actually go?
//!
//! Freshet measured a ~43.8 ms floor on a query returning almost nothing,
//! and the identical floor on InfluxDB 3 — the other DataFusion engine in
//! the set. That looked like a planning cost the two shared, and issue #17
//! was opened to find it.
//!
//! It isn't one. This test is what settled it: the entire in-process query
//! path costs about 2.4 ms, of which session construction is 0.2 ms and
//! logical planning 0.34 ms. There is no 40 ms of planning to find, and a
//! plan cache would have bought under a millisecond.
//!
//! The 43.8 ms turned out to be Docker Desktop's Windows port-forwarding
//! proxy penalising POST-with-body. Same client, same server, same query:
//! 44 ms through the published port, 0.65 ms from inside the container's
//! own network namespace. The two "DataFusion engines" matched because
//! they were the two the harness reached by POST; the three fast ones were
//! reached by GET.
//!
//! Keep this test. It is cheap, and it is the thing that stops the next
//! person spending a week building a plan cache to save 0.34 ms.
//!
//! Run it with `--nocapture` or you get nothing:
//!
//!     cargo test -p timelake-query --test floor_breakdown -- --nocapture

use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Float64Array, StringArray, TimestampNanosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionConfig, SessionContext};
use timelake_query::{run_sql_env, QueryEnv, QuerySession};

const ROUNDS: usize = 30;

/// A table small enough that execution cost is noise next to setup cost.
/// The floor under test is the part that does not depend on row count, so
/// a big table would only bury it.
fn provider() -> Arc<dyn datafusion::datasource::TableProvider> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("host", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![1_i64, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
        ],
    )
    .unwrap();
    Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap())
}

fn ms(d: std::time::Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

fn report(label: &str, samples: &mut Vec<f64>) -> f64 {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median = samples[samples.len() / 2];
    println!(
        "  {label:<34} median {median:8.3} ms   min {:8.3}   max {:8.3}",
        samples[0],
        samples[samples.len() - 1]
    );
    median
}

#[tokio::test(flavor = "multi_thread")]
async fn where_the_fixed_cost_goes() {
    let env = QueryEnv::new(64 * 1024 * 1024, 4, 30);
    let sql = "SELECT COUNT(*) FROM m";

    // Warm everything once. The first call in a process pays for lazy
    // statics and allocator growth that no later query will, and reporting
    // that as the steady-state floor would be its own measurement bug.
    let _ = run_sql_env(
        &env,
        &QuerySession::default(),
        "poc",
        vec![("m".into(), provider())],
        sql,
    )
    .await
    .unwrap();

    println!("\n  phases of one query, {ROUNDS} rounds\n");

    // 1. Building the SessionContext. This is the suspect: it registers
    //    every built-in scalar, aggregate and window function plus the
    //    analyzer and optimizer rule sets, and `run_sql_env` does it fresh
    //    for every single query at crates/query/src/lib.rs:179.
    let mut build = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let t = Instant::now();
        let mut config = SessionConfig::new()
            .with_information_schema(true)
            .with_default_catalog_and_schema("poc", "public");
        config
            .options_mut()
            .execution
            .skip_partial_aggregation_probe_rows_threshold = 8192;
        let ctx = SessionContext::new_with_config_rt(config, env.runtime.clone());
        std::hint::black_box(&ctx);
        build.push(ms(t.elapsed()));
    }

    // 2. register_table, on a context built outside the timer.
    let mut register = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let ctx = SessionContext::new_with_config_rt(
            SessionConfig::new()
                .with_information_schema(true)
                .with_default_catalog_and_schema("poc", "public"),
            env.runtime.clone(),
        );
        let p = provider();
        let t = Instant::now();
        ctx.register_table("m", p).unwrap();
        register.push(ms(t.elapsed()));
    }

    // 3. Planning only: parse plus logical planning, no execution.
    let mut plan = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let ctx = SessionContext::new_with_config_rt(
            SessionConfig::new()
                .with_information_schema(true)
                .with_default_catalog_and_schema("poc", "public"),
            env.runtime.clone(),
        );
        ctx.register_table("m", provider()).unwrap();
        let t = Instant::now();
        let lp = ctx.state().create_logical_plan(sql).await.unwrap();
        plan.push(ms(t.elapsed()));
        std::hint::black_box(lp);
    }

    // 4. Execution only, from an already-built logical plan.
    let mut exec = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let ctx = SessionContext::new_with_config_rt(
            SessionConfig::new()
                .with_information_schema(true)
                .with_default_catalog_and_schema("poc", "public"),
            env.runtime.clone(),
        );
        ctx.register_table("m", provider()).unwrap();
        let lp = ctx.state().create_logical_plan(sql).await.unwrap();
        let t = Instant::now();
        let df = ctx.execute_logical_plan(lp).await.unwrap();
        let _ = df.collect().await.unwrap();
        exec.push(ms(t.elapsed()));
    }

    // 5. The whole thing through the production call site, as a control.
    //    If the phases do not roughly add up to this, one of them is
    //    measuring the wrong thing.
    let mut whole = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let t = Instant::now();
        let _ = run_sql_env(
            &env,
            &QuerySession::default(),
            "poc",
            vec![("m".into(), provider())],
            sql,
        )
        .await
        .unwrap();
        whole.push(ms(t.elapsed()));
    }

    let b = report("1. SessionContext::new", &mut build);
    let r = report("2. register_table", &mut register);
    let p = report("3. create_logical_plan", &mut plan);
    let e = report("4. execute + collect", &mut exec);
    let w = report("5. run_sql_env (whole)", &mut whole);

    println!("\n  parts sum to {:.3} ms, whole is {:.3} ms", b + r + p + e, w);
    println!("  session build is {:.0}% of the whole\n", 100.0 * b / w);
}
