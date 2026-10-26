# Methodological protocol and audit disposition

## Non-negotiable comparison unit

A comparable result is a tuple of `(run_id, baseline_run_id, generator manifest, ingestion repetition, query workload revision)`. Results from a temporal workload and the general comparison workload must remain separate unless all elements of this tuple match.

Every table must name the baseline run ID and the metric denominator. Report generated RDF triples from `manifest.json`, not an estimated generator counter or a TDB/Fuseki UI total.

## Resolution of the audit items

| Audit item | Resolution in this protocol | Evidence required before publication |
|---|---|---|
| Label coverage 100% vs. 51.46% | Report global retention as retained labelled flows divided by baseline labelled flows, plus per-label retention. Do not call retention of selected labels “global coverage”. | Baseline and candidate manifests with `label_counts`. |
| Sample 0.01 ambiguous | Treat `sample_probability=0.01` as an input policy only. Report selected flows/baseline flows and emitted triples/baseline triples separately. | Same seed, input hashes, flow counts, triple counts. |
| Constant 45-triple difference | Do not attribute it to the ontology without a graph-level count. Count data graphs and ontology graph separately, then document which graphs are included. | SPARQL `count-triples.rq` output per named graph. |
| Conflicting temporal 24h values | Historical values are separate observations, not repetitions. Re-run temporal configurations with this generator and new run IDs. | New manifests and raw timing observations. |
| Conflicting baseline timings | Ingestion and latency are different response variables. Never average them together, and never compare values from different hardware or Jena versions. | Environment metadata and per-trial raw CSV. |
| “Optimal” 24h claim | Describe 24h only as the selected configuration under a predeclared criterion. Do not claim universal optimum. | Full results for 6/12/24/48 h and stated selection rule. |
| End-to-end time | Report `T_generate`, `T_ingest`, `T_compact`, and `T_query` independently. Calculate `T_total` only when all phases belong to the same run. | One timing record per phase and run ID. |
| Aggregation label preservation | An aggregate explicitly records a count for every input-flow label in its key. Compare the sum of those counts with the baseline label totals. | `aggregate-label-distribution.rq`, aggregate manifest counters, and baseline manifest. |
| Semantic preservation | Aggregated label counts do not preserve individual flow entities, identifiers, causal chains, or all per-flow attributes. Causal reconstruction is a future validation task. | Fixed query suite and result-count checks. |

## Minimum reporting protocol

- Freeze the CSV file hashes and the source-code Git commit before executing runs.
- For stochastic sampling, execute at least five predeclared seeds; do not select the best result after observing it.
- Use at least five post-warm-up query repetitions per `(dataset, query)` under a fixed Fuseki/Jena version and machine state.
- Keep raw observations. Report median and IQR (or mean and 95% CI if its assumptions are justified), along with all repetition counts.
- Import the ontology in a named graph and report whether counts include it.
- Start every ingestion repetition with a new TDB2 directory. Do not compare a warm cache with a cold cache without saying so.
- For Aggregation Pruning, report `aggregate_input_flows` (valid source-flow rows passed to aggregation), `aggregated_groups`, `aggregate_label_count_sum`, and per-label totals from the manifest. The last two must equal `aggregate_input_flows` and `label_counts`, respectively.

## Scope limits

The generator validates structural reproducibility. It does not validate the correctness of the CIC-IDS labels, nor does it establish a new pruning algorithm. The final manuscript should describe the contribution as an empirical evaluation of predeclared pruning policies.

## Temporal normalization R04

New runs must declare `raw` or `normalized` temporal mode in the manifest. The recommended normalized mode uses R04 rules and excludes the two timestamps classified as ambiguous; this is a fixed treatment policy, not a label-dependent correction. Temporal Pruning windows use the manifest's `reference_local` and `cutoff_local` civil timestamps. Because the CICIDS2017 CSVs do not establish a timezone, values must not be reported as UTC or compared to absolute epochs. See `TEMPORAL_PIPELINE_INTEGRATION.md` for the exact RDF-model migration and commands.



