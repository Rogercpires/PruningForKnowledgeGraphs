# Stage 4 independent audit handoff

Implementation base: `62af3828702d42b9b78ef868213b3552f862c75c` (Stage 3).
The review checkpoint is the final local commit containing this document;
obtain its exact identity with `git rev-parse HEAD` in the isolated worktree.
The final worktree path and full HEAD are recorded in the external Stage 4
checkpoint file and supplied to the user. No Stage 4 push is performed.

## Review scope

- `icc2027_pipeline.py` and `phase_timing.py`: exclusive monotonic phase timers,
  documented envelopes, legacy manifest compatibility, early invalid-parameter
  rejection, actual Sampling eligibility/probability, and unchanged valid-policy
  RDF bytes. R04 source/rules and the original investigation queries are unchanged.
- `benchmark_plan.py`: four Sampling probabilities × five seeds, per-source
  K=10/20/50, four temporal windows, configurable aggregation buckets, fixed vs
  reproducibly randomized order and configuration/repetition block designs.
- `benchmark_environment.py`: hardware, free-space budgets and OS-released lock.
- `benchmark_backend.py`: frozen-source generation, RIOT validation, fresh basic/
  sequential TDB2 load, entity checks, warm-up/measured query trials and storage.
  Generated policy/rules/input identities must match the sealed campaign, and
  source CSVs are rehashed after generation before any load is allowed.
- `benchmark_runner.py`: immutable campaign/attempt IDs, execution state,
  evidence seals, drift checks, timeouts, interruption and fresh-path retries.
- `benchmark_reports.py` and `performance_statistics.py`: raw attempts, explicit
  warm-up/failure exclusions, grouped performance/CI and probability-stratified
  reuse of Stage 3 quality analysis. `generation/manifest.json` uses a unique
  attempt run ID; deterministic task IDs identify the repetition separately.

## Checks for the reviewer

```powershell
git rev-parse HEAD
git status --porcelain=v2 --branch --untracked-files=all
python -B -m unittest discover -s .\icc2027-experiments\tests -v
# Optional installed-Jena tests use only tiny synthetic inputs and new TEMP stores:
$env:ICC2027_JENA_HOME = 'C:\Apache\apache-jena-5.6.0'
python -B -m unittest discover -s .\icc2027-experiments\tests\integration -v
```

Without Java, four portable integration regressions still run; two Jena-specific
integration methods explicitly skip. The primary workspace validation configures
Jena and exercises all methods. No complete CICIDS CSVs or pre-existing databases
are needed by the unit/fixture tests. Do not run the full default campaign during
audit. See BENCHMARKING.md for the dry-plan command and all future-run controls.

Inspect the sealed-completion/root-state crash window, interrupted/unsealed
attempt preservation, failed-report recovery without new loads, live orphan
blocking and evidence-tampering tests. Invalid execution results must never become
zero-valued successful observations. Query trials and performance repetitions
must not be pooled; Sampling probabilities/seeds remain distinct.

Check that only `exclusive_process_stages_seconds` is summed into attempt total,
and only generator exclusive phases into generator total. The internal generator
durations and external process envelopes overlap. Query CLI wall time includes
JVM startup; OS/JIT warming is not inferred. Verify raw outliers remain and t CIs
require operator justification rather than just n=5.

## Evidence and empirical limits

`reports/stage4-validation` contains the test transcript, trace-based executable
line coverage and the default dry-plan capacity check. Frozen pre-instrumentation
RDF hashes for six policies and all 30 configurations on tiny fixtures are tested.
Stage 3 archived results remain unchanged and replayable.

The full sensitivity campaign has **not** been executed. Its initial 180-task
budget is intentionally conservative and currently exceeds available capacity.
It is an operator-specified budget, not a measured claim about actual campaign
space. Provision storage, narrow a pilot or revise the estimate based on evidence
before real execution. No p, K or window has been established as optimal.

R04 remains a documented interpretation hypothesis. Legacy preflight eligible/
invalid counters and unknown-label existence checks remain debt; new Sampling
eligibility does not use those counters. This stage does not fix the old HTTP
Fuseki timing script or normalize historical results. No assertion of general
semantic preservation, cold caches, sufficient replication, idle-host control or
validated real-data confidence intervals is made. CPU/RAM/OS are identified,
not resource-reserved; peak RSS and physical SSD allocation are not measured.

The `publication_ready` campaign field describes only the clean-source-checkpoint
gate; it does not certify scientific or submission readiness. Resume requires
identical frozen bytes/environment, rejects relocated campaigns and checks TDB2
file lengths rather than cryptographically hashing every database file. Specific
PPGC design materials were not located or consulted and are not cited as a source.
