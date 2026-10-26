# Stage 4 implementation validation

Date: 2026-10-26. Branch: ICC2027. Base: 62af3828702d42b9b78ef868213b3552f862c75c.

## Completed scope

Instrumented generator timing with exclusive phases and separately documented
legacy envelopes; isolated sequential benchmark executor; seeded/block schedule;
fresh-store repetition and explicit warm-up records; hardware/toolchain/source/
configuration hashes; disk budget checks; interruption/failure/recovery handling;
raw CSV/JSON export; per-repetition post-generation input/rule identity checks; inclusive quartiles/IQR/CV and opt-in justified Student-t CI;
probability-stratified reuse of Stage 3 query/structural/retention analysis; full
parameter grid and dry-run; immutable execution IDs and local audit checkpoint.

Python/Jena and the standard library remain the stack. No new dependency or public
API contract is introduced. Existing manifest schema/fields stay readable; valid
selection algorithms and R04 decisions preserve RDF bytes. Empty/foreign policy
parameters now fail early instead of silently changing selection.

## Tests executed

- Unit tests: 80; failures/errors/skips: 0/0/0.
- Integration tests: 6; failures/errors/skips: 0/0/0.
- Line coverage of the ten new/changed production modules: 90.55%
  using Python stdlib trace (not branch coverage; subprocess lines excluded).
- All 30 policy/seed configurations generated tiny R04 manifests successfully.
- Six-policy artifact SHA-256 values match pre-instrumentation Stage 3 fixtures.
- Existing ambiguous-row R04 tests and 39 archived SPARQL answer regressions pass.
- Lifecycle tests cover five measured repetitions, separate warm-ups, scheduling,
  failure/retry, Ctrl+C child cleanup, orphan blocking, OS locking, corruption,
  report failure, sealed completion/root-state crash recovery and unsealed retries.
- Real Jena: two fresh tiny stores from 24 synthetic flows per policy (Baseline
  and Aggregation); generation, RIOT, transactional load, count validation,
  query warm-up/measurement, reports, no-op resume and invalid query handling.
- Existing in-memory Jena flow/aggregation query equivalence and negative entity
  control pass. Tests use temporary stores, not existing experimental databases.

Evidence: reports/stage4-validation/tests.txt, validation.json, plan-summary.json.
The Git source/evidence export and isolated worktree receive additional portable
checks before delivery; their logs/checkpoint are registered outside the commit
when they require the final HEAD identity.

## Dry plan and capacity

- Configurations: 30.
- Total tasks: 180 (one warm-up plus five measured per configuration).
- Measured tasks: 150.
- Budgeted requirement: 1,556,925,644,800 bytes.
- Free bytes observed: 137,746,132,992.
- Sufficient for this declared budget: False.

This is a conservative operator-supplied budget (8 GiB per repetition plus 10 GiB
reserve), not an observed measurement of full-campaign consumption. The runner
refuses the complete preset under this capacity. Provision storage or select a
reviewed smaller pilot/budget. No full CICIDS2017 generation, benchmark matrix or
new real-data TDB2 load was executed during Stage 4. No optimum or statistical
sufficiency of five repetitions is inferred.

## Remaining limitations/debt

The implementation is validated on Windows/Python 3.12/Jena 5.6.0. Other platforms
were not exercised. Query timings are external CLI wall times with a new JVM per
request; normalized Jena engine-internal times are unavailable. OS page-cache,
JIT steady state, background load and physical disk allocation are not claimed.
Confidence assumptions are operator declarations, not established by n=5.

R04 hypotheses remain, along with legacy preflight counter defects, unknown-label
existence checking and the old Fuseki HTTP timing script. New Sampling eligibility
uses the actual generation pass. Source/manifest hashes detect drift; completed
TDB2 validation uses file lengths rather than exhaustive database cryptographic
hashing. Campaign relocation is not supported for resume. Live unverified orphan
PIDs block safely and require operator review.

Specific PPGC experimental-design materials were absent from the repository and
not consulted. See BENCHMARKING.md and STAGE4_AUDIT_HANDOFF.md for definitions,
scientific limits, references, reproduction commands and independent audit scope.

No push of Stage 4 changes is performed. Code, tests, documentation and small
validation evidence form one organized local implementation commit.
