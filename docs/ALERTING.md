# Grafana alerting over TimeLakeDB

Grafana alerting works against TimeLakeDB. It is also easy to configure a
rule that can never fire, in a way Grafana reports as healthy and no
dashboard will ever reveal. This document covers both, in that order,
because the second one is what will actually bite you.

Verified end to end on 2026-08-21 against Grafana 13.1.3 and a stock node:
data written → rule evaluates → threshold breached → alert fires →
notification delivered to a receiver → data returns to normal → alert
clears. The drill that proves it is
`deploy/compose/alert-drill/alert_drill.sh`; the transcript is
`docs/evidence/grafana-alerting-drill.log`.

---

## 1. How a Grafana alert reads TimeLakeDB

Grafana's InfluxDB datasource in **SQL** mode speaks Arrow Flight SQL on
port **1964** — the same surface the operator console reads. There is no
alerting-specific plumbing on the server side. From TimeLakeDB's point of
view an alert rule is just another Flight SQL client issuing a `SELECT`
every evaluation interval.

An alert rule is a three-stage pipeline, and the stages matter:

| stage | what it does | typical config |
|---|---|---|
| **A** — query | runs SQL, returns a *frame* (columns of rows) | `rawSql`, `relativeTimeRange` |
| **B** — reduce | collapses that frame to **one number** | `reducer: last` |
| **C** — threshold | compares that number | `gt 50` |

A dashboard panel only ever exercises stage A. Everything below is about
stage B, which is the stage a dashboard cannot test for you.

---

## 2. The field is `rawSql`

The datasource query model field is **`rawSql`**. Not `query`, not `expr`.

Use either of the other two and the SQL never leaves Grafana. The server
receives an empty statement and answers:

```
Error during planning: No SQL statements were provided in the query string
```

which reads like an engine fault and is not one. If you see that message,
the field name is wrong.

---

## 3. `ORDER BY time DESC` silently disables the alert

This is the one to internalise.

**`reduce: last` means the positionally last row of the frame. It does not
sort, and it does not know which of your columns is time.**

So this rule — phrased the way almost everyone writes a "recent data"
query — thresholds the **oldest** row in the window:

```sql
SELECT time, value FROM alertprobe ORDER BY time DESC   -- BROKEN
```

Measured directly, with the window holding older rows at value 10 and
newer rows at value 100:

```
  order returned by the alert query: [100, 100, 100, 100, 100, 100, 10, 10, 10, 10]
  Grafana reduce(last) takes ->  10        <-- thresholded
  the newest row is           -> 100       <-- what you meant
```

Against a threshold of `gt 50`, the correct answer is *fire*. The rule
stayed in Normal for two solid minutes of polling, and reported:

```
  rule: alertprobe value high | state: inactive | health: ok
```

`health: ok` is the trap. It is true — the query ran, the expressions ran,
nothing errored. It says nothing whatsoever about whether the rule is
capable of firing. A rule that can never fire is healthy by every signal
Grafana exposes, and it stays that way until an incident it was supposed
to catch goes unreported.

Nothing else in the stack surfaces this. A dashboard panel using the same
SQL renders perfectly, because a panel plots the whole frame and never
reduces it. Rule history shows an unbroken green line, which is what a
never-firing rule and a genuinely-quiet system look like — identically.

**Write `ORDER BY time ASC`.** After flipping exactly that one word and
changing nothing else, the same rule on the same data went to `firing` on
the next evaluation tick.

Two other ways to get this right, if ASC does not suit the query:

- `reducer: max` (or `min`) — order-independent, and often what a
  threshold rule actually means. Prefer this when "at any point in the
  window" is the real question.
- Keep DESC and use `LIMIT 1` — the frame is one row, so position and
  recency coincide. This is what our own fixture dashboards do
  (`fixtures/grafana/dashboards/host_detail.json`, six panels), and it is
  correct. It is also the one form that rots quietly: widen the limit
  later to make a panel look better and any alert built from that query
  breaks, with no error and no visible change to the panel.

Audited 2026-08-22, because an earlier draft of this document asserted the
opposite and was wrong. Every `ORDER BY` in the repo's dashboards is
already safe: thirteen console panels use `ORDER BY 1` over a
`DATE_BIN(...)` first column (ascending, SQL default), six host-detail
panels use `DESC LIMIT 1`, one uses a bare `ORDER BY time`, and the
remaining three order by a metric for top-N tables and are not
alert-shaped. Nothing here teaches the broken phrasing — the trap is real,
but it is a trap for new rules, not a defect we have shipped.

---

## 4. A rule that works

Provisioned, from `deploy/compose/alert-drill/provisioning/alerting/rules.yml`:

```yaml
groups:
  - orgId: 1
    name: tldb
    folder: TimeLakeDB
    interval: 1m                     # 10s in the drill, to keep it short
    rules:
      - uid: tldb-value-high
        title: alertprobe value high
        condition: C
        for: 0s
        noDataState: NoData          # keep distinct from Alerting
        execErrState: Error
        data:
          - refId: A
            relativeTimeRange: {from: 900, to: 0}
            datasourceUid: tldb-alerts
            model:
              refId: A
              rawSql: SELECT time, value FROM alertprobe ORDER BY time ASC
              intervalMs: 1000
              maxDataPoints: 100
          - refId: B
            datasourceUid: __expr__
            model: {refId: B, type: reduce, expression: A, reducer: last,
                    datasource: {type: __expr__, uid: __expr__}}
          - refId: C
            datasourceUid: __expr__
            model:
              refId: C
              type: threshold
              expression: B
              datasource: {type: __expr__, uid: __expr__}
              conditions:
                - evaluator: {type: gt, params: [50]}
```

Notes that are not obvious from the YAML:

- **`noDataState: NoData`.** Mapping NoData onto Alerting is tempting and
  wrong: it makes an unreachable database indistinguishable from a
  breached threshold. Keep them separate and alert on both, differently.
- **The datasource is bound to one database.** A Flight SQL connection
  carries `dbName`, so a rule cannot join across databases and a second
  database needs a second datasource.
- **`relativeTimeRange.from` is seconds**, and it must comfortably exceed
  your write-to-visible latency. Too narrow and the rule flaps into NoData
  every time a flush runs a little late.

---

## 5. Running the drill

```sh
docker compose -f deploy/compose/timelakedb-alerting.yml up -d --build

docker run --rm --network timelake-alerting_default -v "$PWD:/repo:ro" alpine \
  sh -c 'apk add --no-cache curl python3 >/dev/null &&
         sh /repo/deploy/compose/alert-drill/alert_drill.sh'
```

Grafana is on <http://localhost:3005> (admin/admin) if you want to watch.

The drill asserts, in order: the stack is up and the table is empty; the
provisioned datasource returns a frame with a time column and a numeric
column (i.e. one alerting can reduce); both rules load and evaluate;
low data holds Normal; **high data fires**; the notification actually
reaches a receiver; and low data clears it again.

### Why there is a rule that must never fire

The group contains a second rule, `ordering guard (must not fire)`,
identical to the first except it says `DESC`. It is deliberately broken
and the drill asserts it **stays Normal while the real rule fires**.

That is what makes a green run mean anything. Read the outcomes as:

| result | meaning |
|---|---|
| value-high fires, guard holds | pass — alerting works *and* the trap is real |
| both fire | Grafana changed `last`; good news, but this document is now wrong — fix it |
| neither fires | the drill is broken, not the alerting; a rule that cannot fire passes a "did not fire" check for free |

The drill was itself verified by mutation: flipping `ASC` to `DESC` in the
provisioned rules makes it fail at phase E with exit code 1 and the
message `the rule did NOT fire on data above its threshold … note health
is 'ok'`. Phases F and G then report **skipped**, not passed, because
delivery and recovery are meaningless for an alert that never fired.

### Rule state is not delivery

The stack includes `alert-sink`, a small recorder (`sink.py`) standing in
for PagerDuty or Slack, and the drill asserts the notification arrived and
named the firing rule.

This is a separate claim from the rule turning red, and it fails
separately: a contact point pointing at a host that does not resolve, a
notification policy whose matchers exclude the rule, or a `group_wait`
longer than the operator's patience all leave the rule showing `firing`
and the on-call phone silent.

### The drill needs a fresh stack

It refuses to run against a populated `alertprobe` table, and says so with
the command to fix it.

This is not fussiness. `/api/sql` is read-only — no `DROP TABLE` — and
there is no delete-database route, so the drill cannot clean up after
itself. Rows from an earlier run stay inside the 15-minute window the rules
read. The discriminator depends on the window holding *older low* and
*newer high* rows; let a previous run's high rows age past this run's low
seed and they become the oldest row in the window, the guard fires, and the
drill announces that the ordering trap no longer exists. That is both wrong
and the most misleading thing it could say, so the precondition is asserted
rather than hoped for.

```sh
docker compose -f deploy/compose/timelakedb-alerting.yml down -v
docker compose -f deploy/compose/timelakedb-alerting.yml up -d
```

---

## 6. What to alert on where

Two surfaces, and they fail independently — which is the point of having
both.

**`/metrics` + Alertmanager — the node's own health.** Liveness, ingest
rate, WAL and flush lag, query timeouts and refusals, catalog commit
conflicts, TLS certificate expiry, `timelake_admin_default_credential_active`,
`timelake_querier_refusals_total`. This surface answers from atomics with
no query path, so it still works when the query path is exactly what is
broken. Alert on node health here, not in Grafana.

**Grafana alerting over Flight SQL — your stored data.** Thresholds on
what was actually written: a sensor out of range, a queue depth, a missing
feed. This is what this document is about.

Alerting on TimeLakeDB's health *through* TimeLakeDB's query path is a
loop with an obvious failure mode. Keep it on `/metrics`.

---

## 7. Known limits

- **Grafana 13.1.3 is what was tested.** The reducer semantics in §3 are
  long-standing Grafana behaviour, not version-specific, but the guard rule
  exists so a change gets caught rather than assumed.
- **No `DoPut` and no prepared statements over Flight** (see
  `site/docs/reference.html`). Neither affects alerting, which only reads.
- **The drill's 10s group interval is a drill setting.** Production rules
  should use Grafana's 1m default or slower; evaluating a threshold every
  ten seconds buys nothing and costs a query each time.
- **Recording rules are untested here.** The drill covers alert rules only.
