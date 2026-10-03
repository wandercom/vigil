# ADR-001: vigil extraction architecture

**Status:** Accepted (2026-05-06; Claude/Codex collaboration)
**Source spec:** `vigil/SPEC.md`

## Context

vigil is the sub-threshold anomaly detector: it watches event
streams + trace samples + contract violations for pattern divergence
that doesn't trip a single alarm but represents real systemic
weirdness. ("47 LLM timeouts in an hour for tenant X on the same
prompt pattern" — no single one trips an alarm; the cluster is.)

Reeve has slice 6.5 (sub-threshold trace sampling) at
`reeve/src/tracing/` that's ONE input source. vigil itself doesn't
exist yet.

## Decision

**vigil is a standalone Python service. Off-path; consumes events
asynchronously. V1 detection is rolling z-score per (component, op,
tenant). anomalies table + forensic query API. No Reeve-side
runtime code beyond emitting trace samples (which already exists).**

### Why Python

- Baton is Python; vigil's primary input stream is Baton's event
  channel. Co-locating reduces serialization overhead.
- Numerical work (z-score, rolling baselines, future
  CUSUM/EWMA/clustering) has better ergonomics in Python (numpy,
  scipy).
- Off-path → no hot-path latency requirements that would force a
  twin-language implementation.

### Why off-path

- Anomaly detection is forensic. It tells you "something is weird"
  AFTER the fact, so an operator can investigate. It is NOT a
  real-time gate.
- On-path detection would require sub-millisecond evaluation per
  request — a different problem (slice-5 canary thresholds + slice-6
  alarms cover that).
- Off-path lets vigil consume from a queue/topic (Baton's event
  channel) at vigil's own pace, with retries and backpressure.

### Input streams (V1)

1. **Baton event channel** — published by `baton/src/baton/`
   on every adapter health-check, canary state change, and
   service-event. vigil subscribes.
2. **Reeve trace_samples** — read via Reeve's
   `querySampledTraces({...})` API (slice 6.5). vigil pulls every
   N minutes.
3. **covenant contract_violations** — once covenant_main DB exists,
   vigil queries it. V1 (Reeve's table is the substrate) → vigil
   queries Reeve's read replica or via a Reeve API.

### Detection model (V1) — sim-corrected

**Quantile-based, NOT z-score.** A first draft proposed rolling
z-score; sim's review rejected it: request rates are log-normal,
multimodal, or bursty. Z-score on log-normal data produces
systematic false positives (every Monday batch job crosses z=2 and
screams), trains alert fatigue within a week, makes vigil
counter-productive.

V1 detection: per-tenant **multiplicative threshold against the
rolling 7-day 95th percentile**.

```
For each (component, op, tenant):
  baseline_p95 = 95th percentile of metric over last 7 days
  observed = metric over last 5 minutes
  if observed > 3 * baseline_p95:
    emit Anomaly(...)
```

Why quantile + multiplicative:
- No distributional assumption (works for log-normal, multimodal,
  bursty rates).
- Adapts per tenant: high-volume tenant baseline is high; low-volume
  tenant baseline is low; a 3x burst is genuinely anomalous in
  context.
- Tunable via the multiplier — operators adjust without understanding
  statistics.

Output: an `Anomaly` record with patternId, baseline_p95, observed,
multiplier (observed / baseline_p95), sample of N matching events
for forensic context.

Future work (ADR-002+): per-component-class baselines (latency uses
different math than rate), CUSUM, EWMA, ML clustering. V1 ships ONE
detection method that works.

### Repo layout

```
vigil/
├── SPEC.md
├── ADR-001-extraction.md  # this file
├── pyproject.toml
├── src/vigil/
│   ├── __init__.py
│   ├── types.py             # Anomaly, ExecutionEvent shapes
│   ├── ingest.py            # event-stream subscriber
│   ├── baseline.py          # rolling baseline maintenance
│   ├── detect.py            # z-score evaluation
│   ├── persist.py           # anomalies table
│   ├── api.py               # FastAPI for forensic queries
│   └── cli.py               # 'vigil run', 'vigil query'
├── tests/
│   ├── test_baseline.py
│   ├── test_detect.py
│   ├── test_ingest.py
│   └── test_api.py
└── docs/
    └── detection-math.md    # formal description of z-score model
```

### Schema (single table)

```sql
CREATE TABLE anomalies (
  id            uuid PRIMARY KEY,
  pattern_id    text NOT NULL,
  component     text NOT NULL,
  op            text NOT NULL,
  tenant_id     uuid,
  baseline_window text NOT NULL,  -- '24h' / '7d'
  baseline_value numeric NOT NULL,
  observed_window text NOT NULL,  -- '5m'
  observed_value numeric NOT NULL,
  divergence_sigmas numeric NOT NULL,
  sample_event_ids text[] NOT NULL,
  detected_at   timestamptz NOT NULL DEFAULT now(),
  dismissed_at  timestamptz,
  dismiss_reason text
);
CREATE INDEX anomalies_recent_idx
  ON anomalies (component, op, detected_at DESC)
  WHERE dismissed_at IS NULL;
```

### Forensic query API (V1)

```
GET /v1/anomalies?component=...&op=...&tenant=...&since=24h
GET /v1/anomalies/{id}
POST /v1/anomalies/{id}/dismiss { reason: "operator dismissal text" }
```

Reeve's operator dashboard (or a future stack-wide ops console)
consumes these to display a "weirdness" tab next to the alarm view.

### Hosting

Single Fly.io app (or whichever the user prefers); single Postgres
DB (could be `vigil_main` Neon project or a shared
stack-observability cluster). vigil is stateless aside from the
anomalies table.

### Coordination with slice 6 alarms

vigil and slice 6 alarms are complementary:
- **alarm**: "is THIS metric over THIS threshold right now?" Fast,
  predicate-based, response handler dispatches.
- **vigil**: "is the distribution of recent activity unusual
  compared to history?" Rolling, statistical, forensic.

Operator workflow:
1. Slice 6 alarm fires → page → operator investigates the immediate
   issue.
2. Slice 6 also feeds vigil (alarm fires are events too) → vigil
   sees a cluster of fires → detects systematic anomaly →
   operator investigates the broader pattern.

vigil never AUTO-rolls back. It surfaces; humans decide.

## Consequences

**Positive**
- Catches the "sub-threshold pattern" cases that alarms miss by
  design.
- Off-path; no production-risk impact.
- Single language (Python) keeps maintenance contained.

**Negative**
- Requires a running service (one more component to deploy +
  monitor).
- z-score V1 will produce noisy results until baselines stabilize
  (24h+ of data).
- Adoption depends on operators actually checking the forensic UI;
  V1 has no automatic surfacing beyond slice-6-alarm composition.

## Migration plan

vigil has no migration — it's net-new. First milestone:

1. Init `vigil/` per layout.
2. Author the schema migration.
3. Implement the rolling baseline + z-score detector.
4. Wire the FastAPI for forensic queries.
5. Connect to ONE input stream first (Reeve trace_samples; pull-mode,
   no Baton subscription yet).
6. Deploy to staging.
7. Add Baton event-channel subscription as a follow-up.
8. Build the Reeve operator UI tab for vigil queries.

## Open questions for next ADR

- Sample-event storage: does vigil store the actual matching events
  (potentially large) or just IDs that operators can dereference into
  the source-of-truth? Lean ID-only; Reeve's trace_samples and
  Baton's event log are the canonical stores.
- Anomaly suppression heuristics: noisy z-score detections will need
  a "this happens regularly, ignore" override. ADR-002.
- Cross-component anomaly correlation (the same anomaly seen from
  Reeve's view AND Baton's view) — V2 work; V1 reports per-source.
