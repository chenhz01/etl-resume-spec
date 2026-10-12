# -*- coding: utf-8 -*-
"""etl-resume-spec: checkpoint declaration syntax + idempotent consume semantics
+ resume consistency analysis for batch journals.

Pure standard library. CLI via argparse.
Exit codes: 0 consistent, 1 violations found / validation failure,
2 usage error (argparse).
"""
import argparse
import json
import re
import sys
import time

SOURCE_TYPES = ("cursor", "offset", "watermark")
ORDERINGS = ("strict", "best_effort")
STATUSES = ("done", "partial")
IDENT_RE = re.compile(r"^[^\W\d][\w-]*$", re.UNICODE)  # unicode-aware identifier
REQUIRED_CKPT_FIELDS = ("cursor", "records_done")


def validate_spec(spec):
    """Validate a checkpoint declaration dict. Returns list of
    {rule, path, message}; empty = valid."""
    errs = []

    def err(rule, path, msg):
        errs.append({"rule": rule, "path": path, "message": msg})

    if not isinstance(spec, dict):
        err("R1", "$", "spec must be a JSON object")
        return errs
    if spec.get("version") != 1:
        err("R1", "$.version", "version must be 1")
    pipeline = spec.get("pipeline")
    if not isinstance(pipeline, str) or not pipeline.strip():
        err("R2", "$.pipeline", "pipeline must be a non-empty string")
    src = spec.get("source")
    if not isinstance(src, dict):
        err("R3", "$.source", "source must be an object")
    else:
        if src.get("type") not in SOURCE_TYPES:
            err("R3", "$.source.type", "must be one of %s" % (", ".join(SOURCE_TYPES),))
        if src.get("type") in ("cursor", "watermark"):
            key = src.get("key")
            if not isinstance(key, str) or not key.strip():
                err("R3", "$.source.key", "cursor/watermark sources require a key")
    bs = spec.get("batch_size")
    if isinstance(bs, bool) or not isinstance(bs, int) or bs < 1:
        err("R4", "$.batch_size", "batch_size must be an integer >= 1")
    ck = spec.get("checkpoint")
    if not isinstance(ck, dict):
        err("R5", "$.checkpoint", "checkpoint must be an object")
    else:
        if ck.get("format") != "v1":
            err("R5", "$.checkpoint.format", "checkpoint format must be 'v1'")
        fields = ck.get("fields")
        if not isinstance(fields, list) or not all(isinstance(x, str) for x in fields):
            err("R5", "$.checkpoint.fields", "fields must be a list of strings")
        else:
            if len(set(fields)) != len(fields):
                err("R5", "$.checkpoint.fields", "fields must be unique")
            missing = [f for f in REQUIRED_CKPT_FIELDS if f not in fields]
            if missing:
                err("R5", "$.checkpoint.fields",
                    "missing required field(s): %s" % ", ".join(missing))
    idem = spec.get("idempotency")
    if not isinstance(idem, dict):
        err("R6", "$.idempotency", "idempotency must be an object")
    else:
        kf = idem.get("key_fields")
        if not isinstance(kf, list) or not kf:
            err("R6", "$.idempotency.key_fields", "key_fields must be a non-empty list")
        else:
            for k in kf:
                if not isinstance(k, str) or not IDENT_RE.match(k):
                    err("R6", "$.idempotency.key_fields",
                        "key field %r must be an identifier" % (k,))
    if spec.get("ordering") not in ORDERINGS:
        err("R7", "$.ordering", "must be one of %s" % (", ".join(ORDERINGS),))
    return errs


def _malformed(entry, line_no, why):
    return {"type": "malformed", "line": line_no, "detail": why, "record_id": None,
            "batch": None}


def analyze(entries):
    """Analyze a batch journal (list of dicts or raw JSONL string).

    Returns report {resume_point, violations, tail_records, consumed_once,
    redeliveries_suppressed, consistent}.
    """
    if isinstance(entries, str):
        parsed, line_no = [], 0
        for raw in entries.splitlines():
            line_no += 1
            if not raw.strip():
                continue
            try:
                parsed.append(json.loads(raw))
            except ValueError:
                parsed.append(None)
        recs = []
        for i, e in enumerate(parsed):
            recs.append(_parse_entry(e, i + 1))
    else:
        recs = [_parse_entry(e, i + 1) for i, e in enumerate(entries)]

    violations = []
    for r in recs:
        if r is not None and r.get("type") == "malformed":
            violations.append(r)

    good = [r for r in recs if r is not None and r.get("type") != "malformed"]
    batches = {}
    for r in good:
        batches.setdefault(r["batch"], []).append(r)

    # batch restart: batch number decreases in seq order
    prev = None
    for r in sorted(good, key=lambda x: x["seq"]):
        if prev is not None and r["batch"] < prev:
            violations.append({"type": "restart", "line": r["line"],
                               "detail": "batch %d after batch %d" % (r["batch"], prev),
                               "record_id": r["record_id"], "batch": r["batch"]})
        prev = r["batch"]

    # gaps: every integer between min and max batch must exist
    if batches:
        lo, hi = min(batches), max(batches)
        for b in range(lo, hi + 1):
            if b not in batches:
                violations.append({"type": "gap", "line": None, "detail": "missing batch %d" % b,
                                   "record_id": None, "batch": b})

    # overlap: record_id in more than one batch
    seen = {}
    for r in good:
        seen.setdefault(r["record_id"], set()).add(r["batch"])
    for rid, bs in seen.items():
        if len(bs) > 1:
            violations.append({"type": "overlap", "line": None,
                               "detail": "record in batches %s" % sorted(bs),
                               "record_id": rid, "batch": None})

    # resume point: largest B such that all batches <= B are fully done
    resume_point = -1
    done_batches = {b for b, rs in batches.items() if all(r["status"] == "done"
                                                          for r in rs)}
    if batches:
        for b in sorted(batches):
            if b in done_batches:
                resume_point = b
            else:
                break
        # a partial record in batch b is lost if any LATER batch fully committed
        for r in good:
            if r["status"] == "partial" and \
                    any(d > r["batch"] for d in done_batches):
                violations.append({"type": "partial_committed", "line": r["line"],
                                   "detail": "partial record skipped by later "
                                             "committed batch",
                                   "record_id": r["record_id"], "batch": r["batch"]})

    # idempotent re-consume of the uncommitted tail, with one simulated redelivery
    tail = sorted({r["record_id"] for r in good
                   if r["status"] == "done" and r["batch"] > resume_point})
    dedupe, consumed, suppressed = set(), 0, 0
    for rid in tail + tail:  # deliver the tail twice
        if rid in dedupe:
            suppressed += 1
        else:
            dedupe.add(rid)
            consumed += 1
    bad_types = {"overlap", "gap", "restart", "partial_committed", "malformed"}
    consistent = not violations and consumed == len(tail)
    return {"resume_point": resume_point, "violations": violations,
            "tail_records": len(tail), "consumed_once": consumed,
            "redeliveries_suppressed": suppressed, "consistent": consistent}


def _parse_entry(e, line_no):
    if not isinstance(e, dict):
        return _malformed(e, line_no, "entry is not an object")
    seq, batch = e.get("seq"), e.get("batch")
    rid, status = e.get("record_id"), e.get("status")
    if isinstance(seq, bool) or not isinstance(seq, int):
        return _malformed(e, line_no, "seq must be an integer")
    if isinstance(batch, bool) or not isinstance(batch, int):
        return _malformed(e, line_no, "batch must be an integer")
    if not isinstance(rid, str) or not rid:
        return _malformed(e, line_no, "record_id must be a non-empty string")
    if status not in STATUSES:
        return _malformed(e, line_no, "status must be done|partial")
    return {"seq": seq, "batch": batch, "record_id": rid, "status": status, "line": line_no}


def read_journal(path):
    """Read a JSONL journal file; returns raw text ('' if unreadable lines kept)."""
    with open(path, "r", encoding="utf-8", newline="") as f:
        return f.read()


def _load(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def cmd_validate(args):
    errs = validate_spec(_load(args.spec))
    if args.json:
        print(json.dumps({"errors": errs}, ensure_ascii=False, indent=2))
    else:
        for e in errs:
            print("%s %s: %s" % (e["rule"], e["path"], e["message"]))
        print("OK" if not errs else "%d error(s)" % len(errs))
    return 1 if errs else 0


def _print_report(rep, as_json):
    if as_json:
        print(json.dumps(rep, ensure_ascii=False, indent=2))
        return
    for v in rep["violations"]:
        print("%s line=%s batch=%s record=%s: %s"
              % (v["type"], v.get("line"), v.get("batch"), v.get("record_id"), v["detail"]))
    print("resume_point=%s tail=%d consumed_once=%d redeliveries_suppressed=%d %s"
          % (rep["resume_point"], rep["tail_records"], rep["consumed_once"],
             rep["redeliveries_suppressed"],
             "CONSISTENT" if rep["consistent"] else "INCONSISTENT"))


def cmd_analyze(args):
    rep = analyze(read_journal(args.journal))
    _print_report(rep, args.json)
    return 0 if rep["consistent"] else 1


def cmd_replay(args):
    errs = validate_spec(_load(args.spec))
    if errs:
        for e in errs:
            print("%s %s: %s" % (e["rule"], e["path"], e["message"]), file=sys.stderr)
        return 1
    rep = analyze(read_journal(args.journal))
    _print_report(rep, args.json)
    return 0 if rep["consistent"] else 1


def cmd_bench(args):
    lines = []
    seq = 0
    batch = 0
    while len(lines) < args.lines:
        status = "done" if batch < args.lines // 2000 else "partial"
        for i in range(20):
            lines.append(json.dumps({"seq": seq, "batch": batch,
                                     "record_id": "r%07d" % seq, "status": status}))
            seq += 1
        batch += 1
    journal = "\n".join(lines[:args.lines]) + "\n"
    t0 = time.perf_counter()
    rep = analyze(journal)
    dt = time.perf_counter() - t0
    print("bench: %d journal lines analyzed in %.3fs (%.0f lines/s, consistent=%s)"
          % (args.lines, dt, args.lines / dt if dt > 0 else 0, rep["consistent"]))
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(
        prog="etl-resume-spec",
        description="ETL checkpoint declaration syntax + idempotent consume semantics "
                    "+ resume consistency tests.")
    sub = ap.add_subparsers(dest="cmd", required=True)
    v = sub.add_parser("validate", help="validate a checkpoint declaration JSON file")
    v.add_argument("spec")
    v.add_argument("--json", action="store_true")
    v.set_defaults(fn=cmd_validate)
    a = sub.add_parser("analyze", help="analyze a JSONL batch journal for resume consistency")
    a.add_argument("journal")
    a.add_argument("--json", action="store_true")
    a.set_defaults(fn=cmd_analyze)
    r = sub.add_parser("replay", help="validate spec, then run the idempotent replay test")
    r.add_argument("journal")
    r.add_argument("--spec", required=True)
    r.add_argument("--json", action="store_true")
    r.set_defaults(fn=cmd_replay)
    b = sub.add_parser("bench", help="synthetic benchmark")
    b.add_argument("--lines", type=int, default=10000)
    b.set_defaults(fn=cmd_bench)
    args = ap.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
