# etl-resume-spec

A checkpoint declaration schema and a resume-consistency analyzer for batch
ETL pipelines. Declare how your pipeline checkpoints; feed a batch journal
in JSONL; the tool finds the resume point, detects gaps, overlaps, batch restarts and lost partial
records, and proves idempotent re-consumption by replaying the tail twice.
Pure standard library. *No direct equivalent found as of 2026-10-13.*

<p align="center"><img src="docs/demo.gif" alt="demo" width="720"></p>

## Problem

"Resume from where we left off" is where ETL pipelines lose or duplicate
data. Resume logic is usually implicit in code: which batch was last
committed, what happens to the partial batch, what if a record shows up in
two batches. Bugs surface as silent duplicates or silent holes weeks later.
A declarative checkpoint contract plus a journal analyzer makes resume
behavior inspectable and testable before the 3 a.m. rerun.

## How it works

**Declaration** (`validate`): a JSON spec with `source` (cursor/offset/
watermark), `batch_size`, `checkpoint` (format v1, must carry `cursor` and
`records_done`), `idempotency.key_fields`, and `ordering` (rules R1–R7).

**Journal analysis** (`analyze`): entries are
`{"seq", "batch", "record_id", "status": done|partial}`. The analyzer:

1. computes the **resume point** — the largest B such that all batches ≤ B
   are fully done;
2. flags **overlaps** (same record in multiple batches), **gaps** (missing
   batch numbers), **restarts** (batch numbers decreasing in seq order),
   **partial_committed** (a partial record skipped by a later fully
   committed batch), and malformed lines;
3. runs the **replay test**: done records in the uncommitted tail are
   delivered twice through a dedupe set; consistency requires each consumed
   exactly once.

## Quickstart

```console
$ python etl_resume_spec.py validate spec.json
OK
$ python etl_resume_spec.py analyze journal.jsonl
resume_point=41 tail=37 consumed_once=37 redeliveries_suppressed=37 CONSISTENT
```

10,000 journal lines analyze in ~0.05s [Measured]; a full validate+replay
round-trip stays under 0.1s on typical CI hardware.

## Why not X

No tool with the same scope surfaced during research (checked 2026-10-13),
but adjacent solutions exist:

- **Airflow / Dagster checkpointing** — scheduler-level state for *tasks*;
  they cannot see record-level duplicates inside a batch or validate a
  pipeline's own resume contract.
- **Kafka consumer offsets** — transport-level exactly-once *iff* the
  consumer is idempotent; this tool is precisely the test harness for that
  "iff", storage-agnostic.
- **Great Expectations / dbt tests** — validate data *content*, not journal
  topology (gaps/overlaps/restarts) or replay semantics.
- **JSON Schema** — could check the declaration's shape, but says nothing
  about resume consistency of an actual journal.

## Performance

All numbers [Measured] locally (Python 3.13, Windows 10, single core):

| Workload | Result | Command |
|---|---|---|
| 10,000 journal lines analyzed | 0.054s (184k lines/s) | `python etl_resume_spec.py bench --lines 10000` |
| Declaration validation | <1ms | `python etl_resume_spec.py validate spec.json` |

## Minimal Fallback

Without the tool, write one SQL query per invariant against your ledger:
`GROUP BY record_id HAVING COUNT(DISTINCT batch) > 1` for overlaps and a
`batch_number - ROW_NUMBER()` window for gaps. You lose the resume point,
the malformed-line triage, and the replay proof.

## Design Trade-offs

- **Journal as truth.** The analyzer trusts an append-only journal rather
  than pipeline internals — auditable, but the journal must actually be
  written before commit.
- **Batch-level resume.** Resume granularity is the batch, not the record;
  finer grain belongs to the idempotency keys, which the spec requires you
  to declare.
- **done/partial status only.** A two-state model keeps the invariants
  decidable; richer state machines are the pipeline's business.
- **Replay = deliver tail twice.** A crude but honest duplicate-delivery
  simulation; it catches the common "no dedupe key on re-consume" bug.

## Limits & Second-order Effects

- Gaps report one violation per missing batch — a 500-batch gap is 500
  lines of report (deliberately verbose for triage).
- The tool proves the *journal* is consistent; a journal written after
  commit defeats it (write-ahead is your responsibility).
- Watermark sources with out-of-order writes need `ordering: best_effort`,
  which weakens restart detection.
- Teams may treat "CONSISTENT" as a guarantee; it is a snapshot of the
  journal, not of the sink.

## FAQ

**When NOT to use this?** Streaming jobs with true exactly-once sinks
(dedup happens downstream), pipelines whose checkpoint state lives entirely
in the database with transactional guarantees, and single-shot jobs where
"rerun from scratch" is cheaper than any checkpointing.

**Why JSONL for journals?** Append-only, line-diffable, and each line parses
independently — a corrupted line becomes one `malformed` finding, not a
lost file.

**What if my batches aren't numbered from 0?** Numbering may start anywhere;
only contiguity *within* the observed range is enforced.

**Can two records share an id?** Within one batch, yes (idempotent
re-delivery); across batches it is an overlap violation.

## [Core Assumptions]

1. The journal is append-only and written before the corresponding batch is
   committed.
2. Batch numbers are integers and monotone in `seq` order (restarts are
   anomalies, not features).
3. Exactly-once means "consumed once per dedupe key", keyed by the declared
   `idempotency.key_fields`.

Confidence: high in the analyzer invariants (covered by self_test.py, 60
cases); medium that two record statuses suffice for real pipelines. As of
2026-10-13.

---

Part of the [zhengming-toolbox](https://github.com/chenhz01/zhengming-toolbox) — 105 purpose-built AI-engineering tools across neglected niches.

Related tools: [queue-idempotent-consume](https://github.com/chenhz01/queue-idempotent-consume) · [pagination-integrity-check](https://github.com/chenhz01/pagination-integrity-check) · [uniqueness-precheck](https://github.com/chenhz01/uniqueness-precheck)
