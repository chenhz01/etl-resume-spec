# -*- coding: utf-8 -*-
"""self_test for etl-resume-spec (scaffold v2, four defenses inherited).

D1: SystemExit caught in run_cli. D2: unicode ids/keys exercised.
D3: fixtures written with newline="". D4: last line "SUMMARY: PASS n/n".
"""
import contextlib
import importlib.util
import io
import json
import os
import re
import sys
import tempfile
import time
import unittest

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", newline="\n")  # D3

_HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location(
    "core", os.path.join(_HERE, "etl_resume_spec.py"))
core = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(core)
main = core.main

CASES = []


def case(category, name):
    def deco(fn):
        CASES.append((category, name, fn))
        return fn
    return deco


def run_cli(argv):
    buf_out, buf_err = io.StringIO(), io.StringIO()
    code = 0
    try:
        with contextlib.redirect_stdout(buf_out), contextlib.redirect_stderr(buf_err):
            ret = main(argv)
            code = 0 if ret is None else int(ret)
    except SystemExit as e:
        code = e.code if isinstance(e.code, int) else (0 if e.code is None else 1)
    except Exception as e:
        code = 1
        buf_err.write(repr(e))
    return code, buf_out.getvalue(), buf_err.getvalue()


_TMP = tempfile.mkdtemp(prefix="ers_selftest_")


def write_fixture(name, text):
    p = os.path.join(_TMP, name)
    with open(p, "w", encoding="utf-8", newline="") as f:  # D3
        f.write(text)
    return p


GOOD_SPEC = {
    "version": 1, "pipeline": "nightly-sync",
    "source": {"type": "cursor", "key": "updated_at"},
    "batch_size": 500,
    "checkpoint": {"format": "v1", "fields": ["cursor", "records_done", "sha256"]},
    "idempotency": {"key_fields": ["id"]},
    "ordering": "strict"}


def jline(seq, batch, rid, status="done"):
    return json.dumps({"seq": seq, "batch": batch, "record_id": rid, "status": status})


def good_journal():
    out = []
    seq = 0
    for b in range(2):
        for i in range(3):
            out.append(jline(seq, b, "r%d_%d" % (b, i)))
            seq += 1
    return "\n".join(out) + "\n"


def vtypes(rep):
    return sorted({v["type"] for v in rep["violations"]})


# ---------- [A] functional ----------
def _t_good_spec():
    assert core.validate_spec(GOOD_SPEC) == []
case("A", "validate good spec")(_t_good_spec)

def _t_offset_no_key():
    s = dict(GOOD_SPEC, source={"type": "offset"})
    assert core.validate_spec(s) == []
case("A", "offset source needs no key")(_t_offset_no_key)

def _t_best_effort():
    s = dict(GOOD_SPEC, ordering="best_effort")
    assert core.validate_spec(s) == []
case("A", "best_effort ordering valid")(_t_best_effort)

def _t_unicode_keyfields():
    s = dict(GOOD_SPEC, idempotency={"key_fields": ["订单号"]})
    assert core.validate_spec(s) == []
case("A", "unicode key_fields valid (D2)")(_t_unicode_keyfields)

def _t_good_journal():
    rep = core.analyze(good_journal())
    assert rep["consistent"] and rep["resume_point"] == 1 and rep["tail_records"] == 0
case("A", "fully-committed journal consistent")(_t_good_journal)

def _t_tail_partial():
    lines = good_journal().strip().splitlines()
    lines.append(jline(6, 2, "r2_0", "partial"))
    rep = core.analyze("\n".join(lines) + "\n")
    assert rep["consistent"] and rep["resume_point"] == 1 and rep["tail_records"] == 0
case("A", "partial in uncommitted tail is fine")(_t_tail_partial)

def _t_tail_done_redelivery():
    lines = good_journal().strip().splitlines()
    lines.append(jline(6, 2, "r2_0"))
    lines.append(jline(7, 2, "r2_1", "partial"))
    rep = core.analyze("\n".join(lines) + "\n")
    assert rep["consistent"] and rep["resume_point"] == 1 and rep["tail_records"] == 1
    assert rep["consumed_once"] == 1 and rep["redeliveries_suppressed"] == 1
case("A", "done record in tail replayed exactly once")(_t_tail_done_redelivery)

def _t_overlap():
    lines = [jline(0, 0, "dup"), jline(1, 1, "dup")]
    rep = core.analyze("\n".join(lines))
    assert "overlap" in vtypes(rep) and not rep["consistent"]
case("A", "overlap across batches detected")(_t_overlap)

def _t_gap():
    lines = [jline(0, 0, "a"), jline(1, 2, "b")]
    rep = core.analyze("\n".join(lines))
    assert "gap" in vtypes(rep)
case("A", "missing batch number detected as gap")(_t_gap)

def _t_restart():
    lines = [jline(0, 1, "a"), jline(1, 0, "b")]
    rep = core.analyze("\n".join(lines))
    assert "restart" in vtypes(rep)
case("A", "batch number restart detected")(_t_restart)

def _t_partial_committed():
    lines = [jline(0, 0, "a"), jline(1, 0, "b", "partial"), jline(2, 1, "c")]
    rep = core.analyze("\n".join(lines))
    assert "partial_committed" in vtypes(rep)
case("A", "partial record inside committed batch")(_t_partial_committed)

def _t_malformed_entry():
    rep = core.analyze('{"seq": 0, "batch": 0}\n')
    assert "malformed" in vtypes(rep)
case("A", "malformed journal entry detected")(_t_malformed_entry)

def _t_resume_neg1():
    rep = core.analyze(jline(0, 0, "a", "partial"))
    assert rep["resume_point"] == -1
case("A", "resume_point -1 when batch 0 not committed")(_t_resume_neg1)

def _t_empty_journal():
    rep = core.analyze("")
    assert rep["consistent"] and rep["resume_point"] == -1
case("A", "empty journal is consistent")(_t_empty_journal)

def _t_cli_validate_ok():
    p = write_fixture("spec.json", json.dumps(GOOD_SPEC))
    code, out, _ = run_cli(["validate", p])
    assert code == 0 and "OK" in out
case("A", "CLI validate good -> 0")(_t_cli_validate_ok)

def _t_cli_validate_json():
    p = write_fixture("spec.json", json.dumps(GOOD_SPEC))
    code, out, _ = run_cli(["validate", p, "--json"])
    assert code == 0 and json.loads(out)["errors"] == []
case("A", "CLI validate --json round-trip")(_t_cli_validate_json)

def _t_cli_validate_bad():
    p = write_fixture("spec.json", json.dumps({"version": 2}))
    code, out, _ = run_cli(["validate", p])
    assert code == 1
case("A", "CLI validate bad -> 1")(_t_cli_validate_bad)

def _t_cli_analyze_ok():
    p = write_fixture("j.jsonl", good_journal())
    code, out, _ = run_cli(["analyze", p])
    assert code == 0 and "CONSISTENT" in out
case("A", "CLI analyze consistent -> 0")(_t_cli_analyze_ok)

def _t_cli_analyze_json():
    p = write_fixture("j.jsonl", good_journal())
    code, out, _ = run_cli(["analyze", p, "--json"])
    assert code == 0 and json.loads(out)["consistent"] is True
case("A", "CLI analyze --json round-trip")(_t_cli_analyze_json)

def _t_cli_analyze_bad():
    p = write_fixture("j.jsonl", jline(0, 0, "a") + "\n" + jline(1, 1, "a"))
    code, _, _ = run_cli(["analyze", p])
    assert code == 1
case("A", "CLI analyze inconsistent -> 1")(_t_cli_analyze_bad)

def _t_cli_replay_ok():
    sp = write_fixture("spec.json", json.dumps(GOOD_SPEC))
    jp = write_fixture("j.jsonl", good_journal())
    code, _, _ = run_cli(["replay", jp, "--spec", sp])
    assert code == 0
case("A", "CLI replay good -> 0")(_t_cli_replay_ok)

def _t_cli_replay_bad_spec():
    sp = write_fixture("spec.json", json.dumps({"version": 9}))
    jp = write_fixture("j.jsonl", good_journal())
    code, _, _ = run_cli(["replay", jp, "--spec", sp])
    assert code == 1
case("A", "CLI replay with bad spec -> 1")(_t_cli_replay_bad_spec)

def _t_dup_same_batch_ok():
    lines = [jline(0, 0, "a"), jline(0, 0, "a")]
    rep = core.analyze("\n".join(lines))
    assert rep["consistent"]
case("A", "duplicate lines in same batch allowed")(_t_dup_same_batch_ok)

# ---------- [B] exception paths ----------
def _t_non_dict():
    assert core.validate_spec([1])[0]["rule"] == "R1"
case("B", "validate non-dict -> R1")(_t_non_dict)

def _t_bad_version():
    assert core.validate_spec(dict(GOOD_SPEC, version=2))[0]["rule"] == "R1"
case("B", "version 2 -> R1")(_t_bad_version)

def _t_empty_pipeline():
    assert any(e["rule"] == "R2" for e in core.validate_spec(dict(GOOD_SPEC, pipeline="  ")))
case("B", "blank pipeline -> R2")(_t_empty_pipeline)

def _t_source_not_dict():
    assert any(e["rule"] == "R3" for e in core.validate_spec(dict(GOOD_SPEC, source=7)))
case("B", "source not an object -> R3")(_t_source_not_dict)

def _t_bad_source_type():
    s = dict(GOOD_SPEC, source={"type": "queue"})
    assert any(e["rule"] == "R3" for e in core.validate_spec(s))
case("B", "unknown source.type -> R3")(_t_bad_source_type)

def _t_cursor_no_key():
    s = dict(GOOD_SPEC, source={"type": "cursor"})
    assert any(e["rule"] == "R3" for e in core.validate_spec(s))
case("B", "cursor without key -> R3")(_t_cursor_no_key)

def _t_bs_zero():
    assert any(e["rule"] == "R4" for e in core.validate_spec(dict(GOOD_SPEC, batch_size=0)))
case("B", "batch_size 0 -> R4")(_t_bs_zero)

def _t_bs_bool():
    assert any(e["rule"] == "R4" for e in core.validate_spec(dict(GOOD_SPEC, batch_size=True)))
case("B", "batch_size bool -> R4")(_t_bs_bool)

def _t_bs_str():
    assert any(e["rule"] == "R4" for e in core.validate_spec(dict(GOOD_SPEC, batch_size="5")))
case("B", "batch_size string -> R4")(_t_bs_str)

def _t_ckpt_not_dict():
    assert any(e["rule"] == "R5" for e in core.validate_spec(dict(GOOD_SPEC, checkpoint=[])))
case("B", "checkpoint not an object -> R5")(_t_ckpt_not_dict)

def _t_ckpt_bad_format():
    s = dict(GOOD_SPEC, checkpoint={"format": "v2", "fields": ["cursor", "records_done"]})
    assert any(e["rule"] == "R5" for e in core.validate_spec(s))
case("B", "checkpoint format v2 -> R5")(_t_ckpt_bad_format)

def _t_ckpt_missing_fields():
    s = dict(GOOD_SPEC, checkpoint={"format": "v1", "fields": ["cursor"]})
    assert any("records_done" in e["message"] for e in core.validate_spec(s))
case("B", "missing required checkpoint field -> R5")(_t_ckpt_missing_fields)

def _t_ckpt_dup_fields():
    s = dict(GOOD_SPEC, checkpoint={"format": "v1", "fields": ["cursor", "cursor"]})
    assert any(e["rule"] == "R5" for e in core.validate_spec(s))
case("B", "duplicate checkpoint fields -> R5")(_t_ckpt_dup_fields)

def _t_idem_not_dict():
    assert any(e["rule"] == "R6" for e in
               core.validate_spec(dict(GOOD_SPEC, idempotency="dedupe")))
case("B", "idempotency not an object -> R6")(_t_idem_not_dict)

def _t_idem_empty():
    s = dict(GOOD_SPEC, idempotency={"key_fields": []})
    assert any(e["rule"] == "R6" for e in core.validate_spec(s))
case("B", "empty key_fields -> R6")(_t_idem_empty)

def _t_idem_bad_ident():
    s = dict(GOOD_SPEC, idempotency={"key_fields": ["1abc"]})
    assert any(e["rule"] == "R6" for e in core.validate_spec(s))
case("B", "key_field not identifier -> R6")(_t_idem_bad_ident)

def _t_bad_ordering():
    assert any(e["rule"] == "R7" for e in core.validate_spec(dict(GOOD_SPEC, ordering="loose")))
case("B", "unknown ordering -> R7")(_t_bad_ordering)

def _t_no_seq():
    rep = core.analyze('{"batch": 0, "record_id": "a", "status": "done"}')
    assert "malformed" in vtypes(rep)
case("B", "entry without seq -> malformed")(_t_no_seq)

def _t_batch_str():
    rep = core.analyze('{"seq": 0, "batch": "0", "record_id": "a", "status": "done"}')
    assert "malformed" in vtypes(rep)
case("B", "batch as string -> malformed")(_t_batch_str)

def _t_bad_status():
    rep = core.analyze('{"seq": 0, "batch": 0, "record_id": "a", "status": "maybe"}')
    assert "malformed" in vtypes(rep)
case("B", "unknown status -> malformed")(_t_bad_status)

def _t_empty_record_id():
    rep = core.analyze('{"seq": 0, "batch": 0, "record_id": "", "status": "done"}')
    assert "malformed" in vtypes(rep)
case("B", "empty record_id -> malformed")(_t_empty_record_id)

def _t_broken_jsonl():
    rep = core.analyze("this is not json\n")
    assert "malformed" in vtypes(rep)
case("B", "broken JSONL line -> malformed")(_t_broken_jsonl)

def _t_no_args():
    code, _, _ = run_cli([])
    assert code == 2
case("B", "CLI no args -> SystemExit 2 (D1)")(_t_no_args)

def _t_unknown_cmd():
    code, _, _ = run_cli(["sync"])
    assert code == 2
case("B", "CLI unknown subcommand -> 2")(_t_unknown_cmd)

def _t_missing_journal():
    code, _, err = run_cli(["analyze", os.path.join(_TMP, "nope.jsonl")])
    assert code == 1
case("B", "CLI missing journal file -> 1")(_t_missing_journal)

# ---------- [C] extreme edges ----------
def _t_crlf_journal():
    p = write_fixture("crlf.jsonl", good_journal().replace("\n", "\r\n"))
    code, _, _ = run_cli(["analyze", p])
    assert code == 0
case("C", "CRLF journal parses (D3)")(_t_crlf_journal)

def _t_unicode_ids():
    lines = [jline(0, 0, "记忆-α"), jline(1, 1, "记忆-α")]
    rep = core.analyze("\n".join(lines))
    ov = [v for v in rep["violations"] if v["type"] == "overlap"]
    assert len(ov) == 1 and ov[0]["record_id"] == "记忆-α"
case("C", "unicode record ids in overlap report")(_t_unicode_ids)

def _t_long_id():
    rid = "x" * 10000
    rep = core.analyze(jline(0, 0, rid))
    assert rep["consistent"]
case("C", "10k-char record id works")(_t_long_id)

def _t_blank_lines():
    rep = core.analyze("\n\n" + jline(0, 0, "a") + "\n\n" + jline(1, 1, "b") + "\n")
    assert rep["consistent"]
case("C", "blank lines interleaved are skipped")(_t_blank_lines)

def _t_only_malformed():
    rep = core.analyze("garbage\nmore garbage\n")
    assert not rep["consistent"] and len(rep["violations"]) == 2
case("C", "journal of only malformed lines -> inconsistent")(_t_only_malformed)

def _t_big_gap():
    lines = [jline(0, 0, "a"), jline(1, 500, "b")]
    rep = core.analyze("\n".join(lines))
    gaps = [v for v in rep["violations"] if v["type"] == "gap"]
    assert len(gaps) == 499
case("C", "large gap yields one violation per missing batch")(_t_big_gap)

def _t_start_at_5():
    lines = [jline(0, 5, "a"), jline(1, 6, "b")]
    rep = core.analyze("\n".join(lines))
    assert rep["consistent"] and rep["resume_point"] == 6
case("C", "batch numbering may start above 0")(_t_start_at_5)

def _t_extra_entry_fields():
    rep = core.analyze(json.dumps({"seq": 0, "batch": 0, "record_id": "a",
                                   "status": "done", "note": "extra"}) )
    assert rep["consistent"]
case("C", "unknown entry fields tolerated")(_t_extra_entry_fields)

def _t_extra_spec_keys():
    s = dict(GOOD_SPEC, future="reserved")
    assert core.validate_spec(s) == []
case("C", "unknown spec keys tolerated")(_t_extra_spec_keys)

def _t_perf_sane():
    lines = []
    seq = 0
    for b in range(200):
        for i in range(50):
            lines.append(jline(seq, b, "r%06d" % seq))
            seq += 1
    t0 = time.perf_counter()
    rep = core.analyze("\n".join(lines))
    assert rep["consistent"] and time.perf_counter() - t0 < 5.0
case("C", "10k-line journal analyzed under 5s")(_t_perf_sane)

def _t_json_report_shape():
    rep = core.analyze(good_journal())
    assert set(rep) == {"resume_point", "violations", "tail_records", "consumed_once",
                        "redeliveries_suppressed", "consistent"}
case("C", "report shape is stable")(_t_json_report_shape)

def _t_meta_count():
    assert len(CASES) >= 60
case("C", "meta: at least 60 cases defined")(_t_meta_count)


class T(unittest.TestCase):
    pass


def _add(i, cat, name, fn):
    def t(self, _f=fn):
        _f()
    t.__doc__ = "%s: %s" % (cat, name)
    setattr(T, "test_%s_%03d_%s" % (cat, i, re.sub(r"[^0-9A-Za-z_]", "_", name)), t)


for _i, (_c, _n, _f) in enumerate(CASES):
    _add(_i + 1, _c, _n, _f)

if __name__ == "__main__":
    r = unittest.TextTestRunner(verbosity=0).run(
        unittest.defaultTestLoader.loadTestsFromModule(sys.modules["__main__"]))
    total, fails = r.testsRun, len(r.failures) + len(r.errors)
    print("SUMMARY: PASS %d/%d" % (total - fails, total))  # D4
    sys.exit(0 if fails == 0 else 1)
