# Stage 3 audit disposition

Scope: quality evaluation of existing runs. Baseline before implementation:
`8630fe683e5589f5effb101b655348b43fcd0aff`, branch ICC2027, clean and tracking
origin/ICC2027. The Antigravity report supplied by the user audits this commit;
its synthetic probes used RDFLib rather than Jena and could not inspect the full
experimental databases. This stage independently checks the local databases.

## Existing components reused

- All six versioned investigation queries are used without modification.
- Existing manifests provide selected counts, label counts, representation,
  policy parameters, input hashes and temporal identity.
- `icc2027_pipeline.sha256` and `normalized_label` are reused.
- Existing generator/temporal tests remain in the validation suite.
- The archived exploratory stdout and CSVs provide regression expectations.
- `run-fuseki-measurement.ps1` measures an HTTP workload and discards answers;
  its known `$error` assignment defect makes it unsuitable as this evaluator.
  No new evaluator depends on it or on legacy root scripts.

## Addressed findings

| Audit finding | Stage 3 action |
|---|---|
| D7 / C9 absent quality metrics | Pure metric module, synthetic unit tests, all five-seed descriptive summaries |
| D7 / C10 absent answer comparator | Strict JSON validation, exact result comparison, replay, raw evidence and regression tests |
| D1 emitted vs distinct triples | Separate emitted statement and stored distinct reduction fields; METHODS corrected; generator unchanged |
| D2 aggregation can grow RDF | Reductions allow negative values; represented flows, groups and individual entities separated |
| D3 preflight bias | Evaluation uses selected manifest metrics and cross-checks query totals; does not use eligible_flows as denominator |
| D8 evaluator provenance | Git commit/dirty state, source/query/manifest hashes, argv and installed Jena/Java evidence; historical missing provenance is not fabricated |
| C13 hourly aggregation | Evaluator refuses noninteger/nonpositive bucket sizes and sizes not dividing 3600 |
| D12 aggregation scope documentation | README corrected to describe the actual per-file aggregation map |
| C12 semantic overclaim | In-memory Jena tests verify three-query agreement and a negative individual-entity control |
| Missing logical storage report | Full recursive file-length inventory with principal components and explicit units/scope |

## Remaining technical and methodological debt

No generator, selection algorithm, PRNG stream, R04 rule or existing manifest is
changed in this stage. Consequently D3 preflight implementation defects,
D4 configuration dispatch/label validation, D5 cross-mode Sampling instability,
D6 Fuseki script, D8 historical generator provenance, D9 timing decomposition,
D10 parameter-validation timing, D11 legacy tooling and R04 sensitivity analysis
remain separate work. They must not be presented as repaired by this evaluator.
Changing Sampling now would invalidate the pre-existing five-seed reference.

Aggregation equality is limited to queries aligned with its key and label-count
representation. Label-Based is a supervised oracle; Top-K is per source rather
than a global backbone; five seeds give limited rare-class uncertainty. Neither
TVD nor these answer comparisons establish causal/semantic equivalence.

The original ingestion metadata does not prove ontology scope or a source Git
commit for each historical database. Registry scope remains `not_verified`;
distinct counts are explicitly for the default graph. No new ingestion,
compaction, benchmark campaign or new seed campaign is part of this stage.
