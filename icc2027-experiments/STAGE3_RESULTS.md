# Stage 3 validation and reproduced results

Local validation date: 2026-10-25 (UTC-3).
Source before implementation: 8630fe683e5589f5effb101b655348b43fcd0aff, ICC2027.

## Reproduction

All 13 pre-existing databases were accessible. One execution of each of the three
preservation queries plus one default-graph distinct-triple count per database
completed successfully: **52 SELECTs, zero execution/validation failures**. The
installed CLI advertised JSON and reported Apache Jena 5.6.0. Java/JVM options,
stdout/stderr and command metadata are archived in reports/stage3-jena-r04.
No full RDF generation, new TDB2 load, compaction or benchmark campaign was run.

Exact comparison against the 39 original exploratory answer tables: **39/39
identical**. Both exploratory CSV report schemas and all numeric values were
reproduced. The original tables were not modified. See
reports/stage3-comparison-r04/comparison.json and the integration regressions.

## Counts observed in this validation

Stored counts are distinct triples in the **default graph**. Ontology scope is
not established by the historical ingestion metadata and remains not_verified.
Emitted counts are statement lines, including duplicates; they are deliberately
reported separately. Represented attacks in Aggregation are counts rather than
individual NetworkFlow identities. Logical bytes are file lengths, not occupied
SSD space.

| Policy | Represented flows | Emitted statements | Stored distinct triples | Represented attacks | Attack classes | Logical bytes |
|---|---:|---:|---:|---:|---:|---:|
| Baseline | 2,830,741 | 31,138,151 | 25,495,798 | 557,646 | 14/14 | 4,004,798,446 |
| Aggregation 60s | 2,830,741 | 11,941,365 | 10,368,312 | 557,646 | 14/14 | 1,884,709,460 |
| Temporal 6h | 579,815 | 6,377,965 | 5,225,001 | 287,451 | 3/14 | 982,554,549 |
| Temporal 12h | 703,245 | 7,735,695 | 6,337,535 | 288,923 | 3/14 | 1,150,036,402 |
| Temporal 24h | 704,316 | 7,747,476 | 6,347,194 | 288,923 | 3/14 | 1,150,127,110 |
| Temporal 48h | 1,164,388 | 12,808,268 | 10,491,874 | 291,139 | 7/14 | 1,766,615,562 |
| Label-Based | 557,646 | 6,134,106 | 5,018,827 | 557,646 | 14/14 | 943,969,107 |
| Top-K 20 | 2,203,670 | 24,240,370 | 19,850,070 | 557,642 | 14/14 | 3,150,394,601 |
| Sampling r01 | 28,224 | 310,464 | 258,427 | 5,528 | 12/14 | 228,525,735 |
| Sampling r02 | 28,269 | 310,959 | 258,831 | 5,596 | 11/14 | 228,530,519 |
| Sampling r03 | 28,290 | 311,190 | 259,105 | 5,544 | 12/14 | 228,535,907 |
| Sampling r04 | 28,372 | 312,092 | 259,855 | 5,551 | 11/14 | 228,540,463 |
| Sampling r05 | 28,169 | 309,859 | 257,907 | 5,631 | 12/14 | 228,522,128 |

## Regression reference comparison

- Aggregation: exact equality in all three answers, protocol TVD 0%, hourly
  attack TVD 0%, same Top-20 and ranking. This is limited workload preservation,
  not general semantic equivalence; the in-memory negative control has zero
  NetworkFlow entities in Aggregation versus four in the flow representation.
- Top-K 20: overlap 20/20, same ordered ranking, four fewer attacks;
  count L1 error for hourly attacks = 0.0007173009%.
- Sampling, five pre-existing seeds: protocol TVD mean
  0.34351831%, hourly attack TVD mean
  1.05518408%, HT-rescaled count L1 mean for protocols
  0.74577646% and for hourly attacks
  2.30196218%. Attack class coverage:
  11 to 12 of 14, mean 11.6 classes.

These agree with the supplied rounded regression references. No constants are
used by metric implementation to force these results. Full precision, all five
seed values, median, sample standard deviation, minimum and maximum are saved.

## Logical storage comparison

All five principal component sizes match the original six-base storage CSV at
its published MiB precision. Total byte lengths match for four entries; Sampling
r02 and r03 are two bytes lower in this observation. Transient lock files contain
process-dependent data and have lengths of 10 or 12 bytes in the combined
inventory. That is consistent with the two-byte differences; the old CSV lacks
per-file inventory, so an exclusive historical attribution cannot be proved.
The initial read of Baseline also differed by two bytes before the query run,
then matched the historical total after the normal Jena lock cycle. No total
was adjusted to match a reference. See storage-comparison.json for exact deltas.

## Automated verification

- 54 unit/generator tests, independent of complete CICIDS CSVs or live databases.
- Five separate integration tests: archival CSV compatibility, reference metric
  regressions, exact real-Jena answer agreement, replay stability across Python
  hash seeds, and installed-Jena in-memory query validation.
- Portable validation discovery ran 59 tests with zero failures/errors and one
  optional installed-Jena skip; that optional test is separately executed with
  ICC2027_JENA_HOME configured (see final integration validation log).
- Standard-library trace coverage across the five new production modules:
  617/740 executable lines,
  83.38%. This measurement includes unit and portable
  integration tests; subprocess execution is not counted. It is line coverage,
  not branch coverage or a SonarQube/ZAP scan. No new dependency was installed.
- Git evidence attributes preserve raw answer/query/manifest bytes through Git
  staging and fresh archive extraction; staged artifact replay is verified before
  the final local commit.

## Scope and remaining limitations

The six original queries, existing selection/normalization modules, R04 rules,
configuration files and experimental manifests/outputs were preserved. New files
are additive except for evaluation documentation, the evidence-only Git
attributes and the exclusion of local scratch evaluations.

Historical generation/ingestion provenance cannot be reconstructed. R04
sensitivity, generator preflight/dispatch defects, the old Fuseki script and
benchmark methodology remain debt documented in STAGE3_AUDIT_DISPOSITION.md.
No new dependencies, services or public API contracts were introduced. The
implementation stays with the project's existing Python/Jena stack.

No push of Stage 3 changes is authorized or performed. A single dedicated local
commit contains code, tests, documentation and small reproducibility artifacts.
