"""Small, canonical eight-file fixture shared by timing and orchestration tests."""
from pathlib import Path
from icc2027_pipeline import FILES


def make_csvs(root: Path):
    root.mkdir()
    header = "Flow ID,Source IP,Destination IP,Timestamp,Label,Source Port,Destination Port,Protocol,Flow Duration\n"
    for index, name in enumerate(FILES):
        day = (3, 4, 5, 6, 6, 7, 7, 7)[index]
        rows = [f"a,10.0.0.1,10.0.0.2,{day}/7/2017 09:59:59,BENIGN,1,80,6,2",
                f"b,10.0.0.1,10.0.0.2,{day}/7/2017 10:00:00,DDoS,2,80,6,3",
                f"c,10.0.0.1,10.0.0.3,{day}/7/2017 10:00:30,PortScan,3,443,17,4"]
        (root / name).write_text(header + "\n".join(rows) + "\n", encoding="utf-8")
    return root


def make_spec(configs=None):
    return {"schema_version": 1, "temporal_mode": "normalized", "ambiguous_policy": "exclude",
            "configurations": configs or [{"id": "baseline", "config": {"technique": "baseline"}},
                                            {"id": "aggregate", "config": {"technique": "aggregate", "bucket_seconds": 60}}],
            "protocol": {"warmup_repetitions": 1, "measured_repetitions": 5, "query_warmups": 0,
                         "query_measurements": 1, "order": "randomized", "order_seed": 2027,
                         "blocking": "repetition", "loader": "basic", "cache_state": "unknown",
                         "cache_evidence": "Synthetic tests; cache not controlled", "require_clean_git": False,
                         "subprocess_timeout_seconds": 60},
            "storage": {"reserve_free_bytes": 1024, "estimated_bytes_per_repetition": 1024},
            "confidence_interval": {"method": "none"}}
