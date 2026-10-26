# Reproducible benchmark and sensitivity protocol — Stage 4

This runner executes only when explicitly invoked with `run` or `resume`.
`plan` is a dry-run: it validates the design, checks canonical input names and
lengths, estimates disk requirements and writes a new JSON plan. It does not
hash the dataset, generate RDF, start Java, load TDB2 or clear caches.

## Design and experimental unit

The initial specification, `configs/benchmark-sensitivity-r04.json`, contains:

- Sampling probabilities 0.005/0.01/0.05/0.10, each with seeds 1001–1005.
- Top-K values 10/20/50, **destinations per source IP**, with lexical tie-breaking.
- Temporal windows 6/12/24/48 h, applying R04 and excluding ambiguous rows.
- Aggregation with a reference bucket of 60 s; other positive integer divisors
  of 3600 can be configured for this hourly query workload.
- Baseline and the existing all-attacks Label-Based policy.

There are 30 parameter/seed configurations. One whole-pipeline warm-up repetition
and five measured repetitions initially yield **180 fresh generation/ingestion
tasks**, including 150 measured tasks. This is a plan, not a campaign executed
during implementation. Five repetitions are an initial operational choice,
not a demonstration of adequate power, normality or universal sample sufficiency.

The performance experimental unit is a new generation and a **new TDB2 store**
with fixed configuration and seed. Sampling seed variability and repeated
performance of one seed are separate axes. Never pool those axes. A retry has
a new attempt directory and preserves the failed attempt. It is not an extra
successful independent repetition or a new Sampling seed.

The initial design is randomized complete repetition blocks: a seeded PRNG
shuffles configurations separately within each warm-up/measured block. The
schedule is persisted before execution with order index, block ID, kind,
configuration, seed and repetition. `blocking=configuration` instead keeps all
repetitions of one configuration together; `order=fixed` retains declared order.
Query ordering is also reproducible, seeded by order seed plus task order index.

Each store initially receives one per-query warm-up round and five measured
rounds. These are **within-store trials**, not independent ingestion repetitions.
For each query, performance summaries compare the per-store measured-trial mean
across independent repetitions. All trial values and orders remain available.

Every query trial launches a fresh Java process. Thus the query measurements are
**external CLI request wall times including JVM startup**, not isolated query
engine latency. Warm-up requests may affect filesystem/page caches; they do not
retain JVM/JIT state across requests. No process restart is reported as a cold
OS cache. Cache state defaults to `os_cache_uncontrolled`; `unknown` is also
accepted. An `externally_controlled` declaration requires operator-supplied
evidence. The runner does not claim to verify that evidence or evict OS caches.

## Exclusive timings and envelopes

All durations use `perf_counter`, a monotonic wall clock. They are not CPU time.
UTC timestamps identify events and are never used to calculate durations.

Generator manifest schema version 2 and existing field names are preserved;
new fields and `timing.schema_version=1` are additive. Old manifests remain
readable by the Stage 3 evaluator. Previously saved manifests are never edited.

| Generator duration | Exact definition |
|---|---|
| input_verification_seconds | CSV SHA-256 scan and input metadata collection |
| temporal_preflight_seconds | Existing temporal preflight pass for all policies |
| temporal_reference_seconds | Temporal maximum-reference scan and cutoff calculation, zero for other policies |
| selection_seconds | Policy membership/seeded draw checks during the generation pass, including Baseline's acceptance path |
| topk_selection_seconds | Envelope: Top-K counting pass plus per-source ranking; do not add its child phases again |
| generation_seconds | Legacy envelope from entry into generation through artifact writing/hashing and aggregate invariant checks; includes decoding, selection, grouping and emission |
| total_seconds | Generator entry through end of generation/invariants; includes prepasses; excludes subsequent manifest metadata construction/serialization and process shutdown |

`timing.exclusive_seconds` partitions generator work into input verification,
preflight, temporal reference, Top-K counting, Top-K ranking, input decoding,
selection, aggregation grouping, RDF entity emission/writing, artifact
verification and an explicit unattributed remainder. These phases do not nest.
The partition sums to `timing.partition_total_seconds`. Grouping and entity
emission remain separate. Gzip finalization and miscellaneous bookkeeping may
be in the unattributed remainder. Fine-grained per-row timer overhead is real
and included; compare only runs using the same instrumentation revision/profile.

The generator's `rdf_validation_seconds`, `tdb2_load_seconds` and
`sparql_query_seconds` are `null`: the generator does not perform those stages.
Zero for an inapplicable generator prepass means no such work was performed,
not a measurement of a separate phase. The pre-existing preflight population
counter defects remain documented debt; Sampling's new effective probability
uses **actual selection-eligible rows from the generation pass**, not the biased
legacy `input_counts.eligible_flows`. Seed consumption order is unchanged.

The benchmark attempt has a different, external partition:

| External stage | Definition |
|---|---|
| generation_process_seconds | Complete generator subprocess invocation, including process startup, metadata and manifest completion |
| input_identity_validation_seconds | Compare generated policy/rules/input identities to frozen campaign and rehash CSVs after generation, before any load |
| rdf_validation_seconds | Artifact checksum/length checks plus Jena RIOT validation of all generated gzip N-Triples |
| tdb2_load_seconds | One fresh, serial transactional loader invocation and its evidence handling |
| tdb2_validation_seconds | Stored distinct-triple and RDF entity count queries, validated against the manifest |
| sparql_warmup_seconds | All warm-up query requests and evidence handling for that repetition |
| sparql_query_seconds | All measured query requests and evidence handling for that repetition |
| storage_inventory_seconds | Logical TDB2 file-length inventory |
| total_seconds | Attempt execution, validation and bookkeeping through completion result preparation; excludes writing final completion seal |

`exclusive_process_stages_seconds`, including its unattributed remainder, sums
to the attempt's total. Raw process metadata separately contains the subprocess
wall time, which excludes hashing/serializing its raw-output evidence. Generator
internal durations, prefixed detailed `generator_*` phases and process envelopes
are exposed for analysis but **must not be summed with the external partition**.
`generator_total_seconds` is the internal generator envelope. Normalized Jena
engine-internal times are unavailable (`null`); native stderr is retained rather
than mislabeling CLI wall times as engine-only timings.

The generator makes three complete CSV traversals for Baseline/Labels/Sampling/
Aggregation (hash, preflight, generation), and four for Temporal/Top-K (additional
reference/counting pass). Artifact hashing scans RDF, not CSV. Campaign setup and
resume verification add control-plane hash passes, outside repetition timing.
Each repetition also performs a post-generation identity/hash pass in the
external input-identity validation phase; this extra pass is outside the
generator's three/four-pass count. Keep input files immutable throughout reads.
Creation has `campaign_setup_seconds`; each session records preflight verification,
active execution and reporting durations. Active session time includes report
consolidation; reporting is a child envelope. Downtime between sessions is not
presented as measured pipeline time.

## Policy interpretation and compatibility

- R04 decisions, civil timestamps, ambiguous-row exclusion and rule version are
  unchanged. The runner freezes and hashes the exact R04 file.
- Temporal times reference scanning and window membership separately.
- Top-K separates its counting traversal, per-source ranking and final filtering.
- Aggregation retains per-label counts and per-file grouping. Grouping time and
  aggregate RDF emission time are separate; individual flow identities are not
  restored. A reduction may be negative for singleton groups.
- Label-Based measures a supervised selection using true labels; it is an oracle
  upper bound rather than an unlabeled deployable detector.
- Sampling records nominal probability, seed, eligible population and effective
  retained fraction. Its existing PRNG stream is unchanged. Seeds are reproducible
  only for the same ordered files and eligibility/temporal interpretation.
- Baseline follows the same timed decoding/selection/emission path.

Policy configurations are validated before large input passes: nonfinite or
nonpositive parameters, empty labels and foreign/residual policy keys are
rejected. Valid existing configurations retain identical generated RDF bytes;
six-policy artifact hashes captured before instrumentation form a regression.
Unknown nonempty labels are still a separate generator validation limitation.

## Safe orchestration and provenance

An execution owns a new directory, a unique campaign ID, frozen specification,
configuration expansion, source modules, queries, rules, input hashes, hardware
description, Git commit/dirty state, Python executable/version, JVM arguments,
Jena library hashes, Java/Jena version probes, schedule, events and state.
CPU model/logical cores, total RAM, OS, architecture and host are recorded; an
unavailable hardware field is explicitly identified. Inherited JVM option values
are represented by hashes, not a dump of all environment variables.

The default protocol requires a clean identified Git commit. Synthetic tests
explicitly disable this gate and mark development evidence non-publication-ready.
No service, dependency, public API or new selection technology is introduced.

Storage budgets are operator-specified estimates, not measured predictions.
Free space is checked for the planned remaining tasks and before each attempt
and load. Initial budgets reserve 10 GiB plus 8 GiB per repetition, which is over
1 TiB for the full default design. Choose capacity and estimates deliberately;
the runner never deletes prior output to make room. Logical TDB2 file lengths are
reported separately from physical filesystem allocation/SSD occupancy.

Only one runner can own a campaign via an OS file lock. Stages are sequential,
each loader targets an exclusive fresh attempt store, and Jena's normal database
locking remains active. No parallel-loading mode is selected. This does not
reserve a machine against unrelated applications or other campaigns. Use an
otherwise idle dedicated host, fixed power/software settings and sufficient
space; document remaining background load. Never disable locks for performance.

Each task/attempt has configuration, generator manifest, state, logs, original
stdout/stderr, command, PID, timestamps, timeout, return code, raw query trials,
metrics, storage inventory and completion checksums. State updates are atomic
and events append-only. Result/data files are created exclusively. A timeout or
Ctrl+C kills/reaps the active child and preserves logs before marking failure or
interruption. A crash can leave a partial attempt; a missing final process record
with a still-live PID blocks resume. PID reuse is possible, so the operator must
verify the process rather than blindly killing a PID.

Resume verifies sealed configuration, rules, queries, source revision, input
hashes, Jena libraries, CPU/RAM/OS/Python and Java/Jena CLI identity. Completed
evidence and compressed RDF hashes are verified, as are TDB2 file lengths except
transient locks. This is not a full cryptographic rehash of TDB2 contents. A
campaign is not relocatable for resume because stored paths are absolute.
An interrupted/failed attempt is never continued inside a partial database;
retry uses a new attempt directory. Successful repetitions are verified/skipped.

## Summaries and sensitivity comparison

Reports preserve every attempt, failed/interrupted status, warm-up value and
successful measured value. Predeclared warm-ups and invalid/failed attempts have
explicit exclusion reasons. No valid outlier is removed. Summaries are grouped
by exact policy parameters and Sampling seed: mean, median, sample SD, min/max,
inclusive-method Q1/Q3, IQR and coefficient of variation (SD/mean). Empty samples
have undefined summaries; SD/CV need at least two values, and CV also needs a
nonzero mean. Singleton quartiles equal the observation and IQR is zero.

Confidence intervals are off by default. An opt-in two-sided Student-t interval
for a mean requires `method=student_t`, a supported level (0.5–0.999), declared
`assumptions=independent_normal_repetitions` and a nonempty justification. It is
computed only with at least two defined repetitions and complete planned
replication. The declaration is not a test establishing independence/normality.
Five observations alone do not justify it. The symmetric lower limit is not
artificially clipped to zero. No bootstrap is applied.

The formula is mean ± t critical × sample SD / sqrt(n), as described by the
[NIST confidence-limit reference](https://www.itl.nist.gov/div898/handbook/eda/section3/eda352.htm).
The t critical value is obtained by adaptive integration of its normalized
density and bisection, with regression tests against known critical values.

Stage 3 quality modules are reused per matched measured repetition. Deterministic
policies are compared with that repetition's Baseline. Sampling quality groups
are stratified by probability, with seeds within each group; different p values
are never pooled in a five-seed summary. Outputs provide structural reduction,
attack counts/classes, exact SPARQL preservation, TVD, HT estimates and logical
storage beside per-configuration performance summaries. Repeated copies of the
same seed are not treated as new independent quality samples. No p, K or temporal
window is labeled optimal by this implementation.

Each consolidation writes a new report directory; earlier reports remain.
`raw-attempts.json/.csv` contains all attempts. `performance-statistics.json/.csv`
contains grouped performance and exclusions. `quality-index.json` identifies
complete/incomplete paired groups, each complete group having replayable Stage 3
bundles and CSV/JSON metric reports. An incomplete campaign is not successful.

## PowerShell commands (from repository root)

Choose a new directory name every time. First inspect the default design:

```powershell
$bench = 'C:\UFRGS\ICC2027_Benchmarks'
New-Item -ItemType Directory -Force -Path $bench | Out-Null
python -B .\icc2027-experiments\src\benchmark_runner.py plan `
  --spec .\icc2027-experiments\configs\benchmark-sensitivity-r04.json `
  --csv-root C:\UFRGS\ICC2027_CSV --output "$bench\sensitivity-plan-r01.json"
```

The plan may show insufficient space and still be a valid dry-run; `run` refuses
to start with insufficient capacity. To run **later**, after review and provision
of the required capacity (this command starts the full design):

```powershell
python -B .\icc2027-experiments\src\benchmark_runner.py run `
  --spec .\icc2027-experiments\configs\benchmark-sensitivity-r04.json `
  --csv-root C:\UFRGS\ICC2027_CSV --jena-home C:\Apache\apache-jena-5.6.0 `
  --jvm-arg=-Xmx4g --output "$bench\sensitivity-r01"
```

For an initial pilot, copy the JSON spec and replace the grid with an explicit
`configurations` list containing Baseline and the desired candidates; preserve
the protocol controls and record a separate campaign. No campaign was launched
on the full CICIDS2017 dataset during Stage 4.

```powershell
python -B .\icc2027-experiments\src\benchmark_runner.py resume `
  --campaign "$bench\sensitivity-r01"
# Failed (rather than merely interrupted) attempts need an explicit retry:
python -B .\icc2027-experiments\src\benchmark_runner.py resume `
  --campaign "$bench\sensitivity-r01" --retry-failed
python -B .\icc2027-experiments\src\benchmark_runner.py summarize `
  --campaign "$bench\sensitivity-r01"
```

Exit codes: 0 successful/validated dry-plan; 1 failed attempt/campaign; 2 invalid
inputs, drift, lock, capacity or other control errors; 130 interrupted execution.
Inside the repository, large campaign outputs are accepted only under ignored
`icc2027-experiments/benchmark-runs/`. Prefer storage outside OneDrive. No RDF,
datasets or TDB2 files from campaigns are added to Git.

## Validation and limits

```powershell
python -B -m unittest discover -s .\icc2027-experiments\tests -v
$env:ICC2027_JENA_HOME = 'C:\Apache\apache-jena-5.6.0'
python -B -m unittest discover -s .\icc2027-experiments\tests\integration -v
```

Unit tests use synthetic counts/files and an explicitly labeled fake backend for
failure/resume/ordering tests. Integration tests separately use real Jena on a
24-flow fixture with two fresh tiny stores, validating RDF, ingestion, entity
counts, warm-ups, measured queries, quality reports and no-op resume. Invalid
SPARQL returns failure. Previous R04 and Stage 3 regressions remain mandatory.
Synthetic test timings are infrastructure checks, not scientific measurements.

No specific PPGC experimental-design materials were located in the repository or
consulted in this stage. Recommendations are not attributed to those materials.
The statistical reference above and the explicit local design definitions are
the consulted basis. Full-campaign capacity, machine/cache control, adequacy of
replication, normality/independence, R04 sensitivity assumptions and general
semantic preservation remain empirical questions, not validated conclusions.
