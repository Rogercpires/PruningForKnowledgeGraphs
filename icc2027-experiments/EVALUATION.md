# Automatic query preservation and storage evaluation

This package evaluates **existing** runs and databases. It never invokes the
CSV-to-RDF generator, a loader, compaction, updates or a performance campaign.
The selection algorithms, their PRNG streams and R04 rules are unchanged.
Python 3.12 and the standard library suffice. RDFLib remains optional for the
older generator tests; the new evaluator does not require it.

## Inputs and comparison scope

`configs/evaluation-r04.json` maps the 13 policies to their exact run-manifest
and TDB2 directory names. Roots are parameters. No newest-directory guessing is
performed. Adjust the registry explicitly for another campaign. IDs are unique;
manifests must agree on ordered input CSV names/hashes/lengths, temporal mode,
ambiguous-row policy and rule identity. Manifest label sums must agree with
selected flows. Aggregation must have consistent represented-flow counts and
an integer bucket size dividing 3600 for the hourly comparison.

The current registry declares `graph_scope=default` and
`ontology_scope=not_verified`: legacy ingestion metadata does not establish the
ontology scope. A stored count is therefore explicitly **distinct triples in
the default graph**, not an asserted ontology-free count of every graph. If the
ingestion scope has been independently established, declare `excluded` or
`included` in a new registry. The evaluator neither enables union-default-graph
nor silently switches to named graphs. All results record the declared scope.

Flow representations use the three `*-flow.rq` files. Aggregation uses the three
`*-aggregate.rq` files. An additional `stored-triples-default.rq` measures
distinct stored triples (52 SELECTs for 13 bases; `--skip-stored-count` selects
only the 39 preservation queries). `count-triples.rq`, which counts named graphs,
cannot substitute for this default-graph query.

The [Jena TDB2 command documentation](https://jena.apache.org/documentation/tdb2/tdb2_cmds.html)
describes the installed utilities. Before querying, the evaluator captures the
installed `tdb2.tdbquery --help`, `--version`, and Java version and requires JSON
to be advertised by `--results=`. This was verified on Jena 5.6.0. On Windows the
wrapper is `bat/tdb2_tdbquery.bat`. The executor launches its Java main class
directly with an argument list, equivalent classpath and logging configuration;
this avoids shell quoting and lets a timeout terminate Java itself.

## Commands from repository root

Validate inputs, run each query once, save raw answers and calculate metrics:

```powershell
python -B .\icc2027-experiments\src\evaluate_queries.py run `
  --registry .\icc2027-experiments\configs\evaluation-r04.json `
  --runs-root C:\UFRGS\ICC2027_Runs `
  --tdb-root C:\UFRGS\ICC2027_TDB2 `
  --jena-home C:\Apache\apache-jena-5.6.0 `
  --jvm-arg=-Xmx4g --timeout 300 `
  --output .\icc2027-experiments\evaluations\quality-r04-r01
```

Use a new output directory each time. `--java` can select a specific installed
Java executable; repeat `--jvm-arg=...` for additional explicit JVM options.
There are no repetitions, cache resets or claims of benchmark latency. Recorded
wall time includes process startup and is operational trace information.
Jena's normal database lock remains enabled. Stop any Fuseki process using a
store before accessing it directly; a lock failure is recorded, never bypassed.
Opening existing stores for SELECT may update transient lock/journal metadata;
the evaluator performs no RDF update. Storage inventory is not an atomic
snapshot of a database being modified by another process.

Recalculate reports offline from a saved evaluation, without Java or databases:

```powershell
python -B .\icc2027-experiments\src\evaluate_queries.py report `
  --input .\icc2027-experiments\reports\stage3-reference-r04 `
  --output .\icc2027-experiments\evaluations\reference-replay-r01
```

Explicitly import the original exploratory text tables:

```powershell
python -B .\icc2027-experiments\src\evaluate_queries.py import-legacy `
  --registry .\icc2027-experiments\configs\evaluation-r04.json `
  --runs-root C:\UFRGS\ICC2027_Runs `
  --tdb-root C:\UFRGS\ICC2027_TDB2 `
  --legacy-dir C:\UFRGS\ICC2027_TDB2\query-preservation-r04 `
  --output .\icc2027-experiments\evaluations\legacy-import-r01
```

`--tdb-root` is optional for historical import; without it storage is unavailable.
Import accepts only the complete two-column Jena tables archived by policy ID
and query kind (`baseline-top.txt`, etc.). It marks execution provenance as
**unverified**: historical commands, durations, exit codes and original query
hashes cannot be reconstructed. Current query files are saved as references,
not represented as proof of what was historically executed. Stored triple
counts absent from historical answers remain `null`, even when manifests have
emitted-statement counts. JSON is preferred for all new execution.

Compare all 39 required answers exactly, with per-category divergences in JSON:

```powershell
python -B .\icc2027-experiments\src\evaluate_queries.py compare `
  --reference .\icc2027-experiments\reports\stage3-reference-r04 `
  --candidate .\icc2027-experiments\evaluations\quality-r04-r01 `
  --output .\icc2027-experiments\evaluations\comparison-r01
```

Comparisons require identical policy IDs and source-manifest hashes. Exit code 1
indicates differences or unavailable answers; a failed query cannot compare equal
to an empty result. The additional stored-count query is not compared when it is
absent from the historical evidence.

## Evidence, validation and failures

Each evaluation saves immutable manifest copies, query copies, SHA-256 values,
raw stdout/stderr, per-process command/parameters/start/duration/return code,
validation status/error and `bundle.json`. Evaluator provenance includes Git
commit/dirty state, source hashes, argv, Python and OS. Existing source CSVs,
RDF artifacts and manifests are read or referenced, never rewritten. Evaluation
does not rehash multi-gigabyte RDF or CSV files; their identities are inherited
from the manifests, and missing historical ingestion provenance remains a limit.

Successful JSON must have the exact selected variables, bound terms of the
expected type, integral nonnegative counts, unique groups, valid local civil
hours and deterministic top-20 order. A valid empty SELECT is distinct from an
error. Hourly attack sums must match selected manifest label counts; top results
cannot be empty for a positive represented population. These checks catch a
wrong default graph or the wrong RDF representation. Protocol totals may be
smaller than selected flows when protocol attributes are absent.

Timeouts, nonzero exits, malformed results and population mismatches retain raw
evidence, mark the query failed and yield unavailable comparative metrics. A
failed Baseline also invalidates comparisons requiring that answer. Process
metadata records execution/parsing; bundle/report status additionally records
population validation. One failed query does not convert to `{}` or stop later
queries. Required-query omissions and checksum changes are rejected on replay.

Exit codes: 0 = all required preservation results available (including explicitly
imported historical results); 1 = recorded query/result failures; 2 = invalid
inputs, unsupported CLI, missing paths or filesystem errors. An interrupted run
can leave partial raw evidence; it is not a completed evaluation. Output creation
is exclusive and existing directories are refused. Replay writes a new directory
and preserves its source-bundle hash. No global Git/Jena configuration is changed.

## Metric definitions

Let `B` be the Baseline count and `C` the observed candidate count. Percentages
have `_pct` suffixes. Other ratios are fractions. Counts absent from a **valid**
grouped answer are zero on the union support; an unavailable answer is never zero.
JSON uses `null` for undefined metrics, and CSV uses an empty cell. No NaN/Infinity
is emitted. Negative reductions are valid growth, not clipped to zero.

| Metric | Definition and denominator | Undefined case |
|---|---|---|
| Flow reduction (%) | `100 * (1 - selected_candidate / selected_baseline)` | Baseline zero |
| Emitted RDF reduction (%) | Same expression using manifest `emitted_triples`: emitted N-Triples **statements/lines**, including duplicates | Baseline zero |
| Distinct stored reduction (%) | Same expression using SELECT counts in the declared default graph | Baseline zero or count unavailable |
| Attack count retention | Sum observed attack label counts / sum Baseline attack label counts | No Baseline attacks |
| Attack instance retention | Candidate individual `NetworkFlow` attack entities / Baseline attack entities; Aggregation has zero such entities | No Baseline attacks |
| Attack class coverage | Number of Baseline attack classes with positive observed count / number of positive Baseline attack classes | No Baseline attack classes |
| Per-class preservation | Observed represented count of class / Baseline count of class | Class absent from Baseline |
| Absolute error | `abs(C - B)` per category | Only when an answer is unavailable |
| Relative error | `abs(C - B) / B` per category | `B = 0`, including `B = C = 0` |
| Count L1 error | `sum_union abs(C_i - B_i) / sum B_i` | Baseline total zero or unavailable answer |
| TVD | `0.5 * sum_union abs(C_i/sum C - B_i/sum B)` | Either total zero or unavailable answer |
| Top-N overlap | `size(intersection(topN(B), topN(C))) / N` | Empty Baseline ranking |
| Common rank displacement | Mean `abs(rank_B - rank_C)` over common top-N destinations | No common destinations |
| Union rank displacement | Same mean over union; absent destination receives position `N+1` | Empty Baseline ranking |

Labels are taken from `metrics.label_counts`, not the known-biased historical
`temporal_interpretation.input_counts` preflight. BENIGN is excluded using the
generator's existing label normalizer. Attack **counts**, **classes** and
individual **instances** are reported separately. Aggregation represents input
flow/label counts and stores `AggregatedFlow` groups; it does not preserve
individual flow identity. It can increase RDF size for singleton groups.

Rank order is descending count with ascending lexical destination IRI for ties,
matching the versioned queries. N=20 in reports. If either ranking has fewer
than N items, the overlap denominator remains N; `same_ranking` compares the
available ordered lists. Common displacement is conditional on survival and
must be read beside union displacement and overlap. No Spearman/Kendall
coefficient on only surviving items is substituted. `top_with_ties` supports a
full input distribution; a LIMIT-20 answer cannot reveal all ties beyond position
20, so tie-expanded overlap is explicitly unavailable in these reports.

Count L1 and TVD answer different questions: raw Sampling counts lose about 99%
of the population, while TVD compares normalized composition. If L1 is instead
computed on normalized proportions it equals `2*TVD`; that is **not** the count
L1 column. TVD alone cannot establish population retention or semantic fidelity.

### Bernoulli Sampling estimates

Only `technique=sample`, with known uniform positive inclusion probability
`p <= 1`, permits Horvitz–Thompson count estimates:

- Estimated category total: `estimated_i = observed_i / p`.
- Plug-in design variance: `observed_i * (1-p) / p^2`.
- Standard error: square root of that variance.
- Rescaled error: `sum_union abs(estimated_i - B_i) / sum B_i`.

The API rejects HT for Baseline, Temporal, Label-Based, Top-K and Aggregation.
Observed integer counts, estimates, Baseline values and probabilities have
distinct fields/columns. A zero observed category yields an estimate of zero,
but variance/SE are `null` for `p < 1` to avoid a false claim of certainty. For
`p=1` the complete population is observed and variance is zero. These are design
estimates for counts, not confidence intervals, HT estimates of ratios, or
estimates of unseen ranked destinations. No bootstrap or CI is supplied for five
seeds. Rare-class coverage remains probabilistic.

### Descriptive statistics

For each Sampling metric, retain all five policy values and report arithmetic
mean, median, sample standard deviation `sqrt(sum((x-mean)^2)/(n-1))`, minimum
and maximum. Standard deviation is undefined for `n < 2`; all other summaries
are undefined for `n=0`. Undefined observations are not imputed: `n_total`,
`n_defined` and `n_undefined` disclose exclusion. No ingestion/query repetitions
are pooled as independent seed replicates.

## Reports and compatibility

- `metrics.json`: complete precision, per-policy statuses, population metrics,
  class preservation, rankings, category errors, HT and Sampling statistics.
- `query-preservation-summary.csv` and `query-preservation-extended-r04.csv`:
  same semicolon-separated columns, policy order and rounding as the exploratory
  reports (6 decimal places for count errors; 4 for TVD/rank shift/HT errors).
  Numeric formatting such as `0` versus `0.0` is semantically equivalent.
- `population-metrics.csv`, `class-preservation.csv`, `category-errors.csv`,
  `sampling-statistics.csv`: explicit observed/Baseline/estimated quantities.
- `storage-logical.json` and `.csv`: sum of `stat().st_size` for **all files**
  recursively, plus `SPO.dat`, `POS.dat`, `OSP.dat`, `nodes.dat`,
  `nodes-data.obj`, other bytes and detailed inventory. Components are summed
  across generations if several Data-* directories exist. Missing components
  are identified. MiB = bytes / 1024^2. Linked paths are refused.

Logical file length is not allocated filesystem blocks, compressed/sparse usage,
or physical occupied SSD space. Transient `tdb.lock` lengths may change with PID
or lock metadata; inventories explain small differences between observations.

`reports/stage3-reference-r04` preserves the 39 historical tables, 13 manifest
copies and the reproduced reports. They are small evidence artifacts, not RDF,
datasets or TDB2 files. The two original exploratory CSVs are retained separately
in `tests/fixtures` as regression expectations; expected values occur in tests,
not in metric implementation. See STAGE3_RESULTS.md for the local Jena run.
The root `.gitattributes` sets `-text` only for reports and test-fixture evidence:
Git must preserve their raw bytes rather than convert line endings and invalidate
recorded checksums in another checkout. It does not change global Git settings or
the generator's existing files.

## Tests

```powershell
python -B -m unittest discover -s .\icc2027-experiments\tests -v
$env:ICC2027_JENA_HOME = 'C:\Apache\apache-jena-5.6.0'
python -B -m unittest discover -s .\icc2027-experiments\tests\integration -v
```

Unit tests use synthetic counts, tiny synthetic manifests/tables, mock failed
programs and temporary files. They do not require complete CICIDS inputs or Java.
Integration tests separately reproduce archived reports and, when Jena is
configured, run the six queries on four synthetic flows **in memory**. They check
minute/hour boundaries and use individual-flow count as a negative control for
general semantic equivalence. No new TDB2 database or load is used by tests.
