# Static audit evidence: legacy generator

This is source-code evidence, not a rerun of the historical experiment.

| Question in the audit | What the legacy code establishes | Conclusion |
|---|---|---|
| Was `--sample 0.01` a 1% sample? | The condition is `random.random() >= sample_p`; therefore every eligible input flow has probability 0.01 of selection. The seed defaults to 42. | It is a 1% **flow-selection policy**. A fraction of emitted triples is not, by itself, its observed sampling rate because triples per flow can vary. |
| Does label pruning preserve 100% coverage? | The code filters to `labels_keep` before emitting RDF. | It can establish retention of the *chosen* labels only. It cannot establish 100% global retention of all baseline labels. |
| Do generator console totals establish RDF totals? | The legacy counter adds `12` per flow and `10` per aggregate, explicitly described as an estimate. Optional properties make actual triples variable. | Console totals must not be used as exact RDF counts. Use the new manifest plus graph-level SPARQL counts. |
| Is aggregation reproducible? | Aggregate resource identifiers use Python `hash((src,dst,proto,dstPort))`. Python hash randomization changes values between processes. | Historical aggregate resource IRIs are not guaranteed stable across executions. The replacement uses SHA-256. |
| Can aggregation split an event bucket? | The legacy process clears `agg_map` after reaching 200,000 groups. A later occurrence of a prior key creates another aggregate state. | This may fragment an aggregate and must be replaced before evaluating aggregate results. |
| Can old output files be traced to a command and inputs? | Output paths are fixed under `out_nt`; the generator writes no input checksums, run identifier, source revision, or manifest. | Historical artifacts have incomplete provenance. Preserve them as legacy evidence, but do not combine them with new repetitions. |

The source evidence resolves the meaning of the legacy procedures. It does not resolve the conflicting historical numerical values. Only controlled reruns can produce a single defensible results table.



