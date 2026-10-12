# Changelog — etl-resume-spec

All releases are local drafts; no remote operations are performed.

## v0.1.1 — 2026-10-13

Fixed:
- partial_committed redefined as 'partial record skipped by a later fully-committed batch'.

Changed:
- self_test expanded to >=60 cases across functional / exception / extreme-edge
  categories (scaffold v2 four defenses: SystemExit capture, Unicode-safe
  matching, Windows newline-safe fixtures, SUMMARY last line).
- README: added Performance section with [Measured] numbers and in-repo
  reproduction commands.

## v0.1.0 — 2026-10-13

Added:
- Initial local draft: ETL checkpoint declaration syntax + idempotent resume consistency tests.
- argparse CLI (pure standard library), self_test.py, MIT LICENSE.
