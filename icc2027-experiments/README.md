# ICC 2027 — experimental pipeline

This directory is a new, reproducible experiment package. It does not reuse or overwrite the historical outputs under `D:\UFRGS\Extras\Gerencia de Redes\Dataset\CIC-IDS-2017`.

## What one run proves

A run transforms the configured raw CSV files into compressed N-Triples and writes a `manifest.json`. The manifest identifies the input CSVs by SHA-256, records every selection parameter, records exact emitted flow and triple counts, and hashes each generated artifact. It is the source for RDF-volume and retained-label metrics.

It does **not** by itself prove Fuseki ingestion time, query latency, semantic preservation, causal reconstruction, or an end-to-end speedup. Those measurements are deliberately collected in separate stages.

## Quick start

```powershell
Set-Location 'C:\Users\roger.pires\OneDrive\Documentos\Meus Documentos\Documentos Roger\Meus Cursos\Documentos UFRGS\ICC 2027\ICC2027_Experiments'
.\scripts\run-experiment.ps1 -Config .\configs\baseline.json -CsvRoot 'D:\UFRGS\Extras\Gerencia de Redes\Dataset\CIC-IDS-2017\CSVs\GeneratedLabelledFlows\TrafficLabelling' -RunId baseline-r01
```

Run each configuration in fresh output directories (`baseline-r01`, `baseline-r02`, ...). Keep the generated manifests and measurement CSVs as the traceable evidence for the paper.

## Experiment stages

1. **RDF generation** — `run-experiment.ps1`; exact output counts and input/output checksums.
2. **TDB2/Fuseki ingestion** — run from a fresh TDB2 directory per repetition; record wall-clock time, loader command, Jena version, and resulting manifest in `ingestion.csv`.
3. **SPARQL measurement** — `run-fuseki-measurement.ps1`; one warm-up and configurable repeated trials per fixed query, saved as individual observations.
4. **Analysis** — derive means and confidence intervals from raw observations; do not paste values from different run IDs into one comparison table.

## Design decisions

- Dataset discovery is deterministic (the eight canonical CIC-IDS-2017 filenames, in fixed order).
- Sampling uses a local seeded PRNG and records the seed; it reports selected **flows** separately from emitted triples.
- Aggregated node URIs use SHA-256, never Python's randomized `hash()`.
- Aggregation is held in one map for the complete run. This prevents a time bucket from becoming several RDF resources when an in-memory flush happens.
- Aggregation retains a SPARQL-queryable label distribution: each `AggregatedFlow` has one `ex:LabelCount` resource per source-flow label, linked through `ex:hasLabelCount`, with `ex:label` and exact `ex:count` values. The extension is defined in `rdf-model.ttl` and queried by `queries/aggregate-label-distribution.rq`.
- Top-K ties use lexical destination ordering, which makes selection deterministic.
- Label retention is reported by label and relative to the baseline manifest. “All selected labels retained” is not reported as global label coverage.

### Aggregation semantic scope

The `aggregate-60s.json` policy retains the number of source flows per label inside each aggregation key `(bucket_start, source_ip, destination_ip, protocol, destination_port)`. It supports comparison of retained label counts with the Baseline. It does **not** preserve or reconstruct individual flow RDF entities, their identifiers, or their full per-flow attributes. Generation tests validate structural counts only; their execution times are not benchmark measurements.

For an aggregation run, `manifest.json` reports `aggregate_input_flows` (valid source-flow rows passed to aggregation), `aggregated_groups`, `aggregate_label_counts`, `aggregate_label_count_sum`, and exact `emitted_triples`. The generator rejects the run if the sum of label counts differs from the number of input flows represented by the aggregates.

See [METHODS.md](METHODS.md) for the audit-resolution matrix and reporting rules.


