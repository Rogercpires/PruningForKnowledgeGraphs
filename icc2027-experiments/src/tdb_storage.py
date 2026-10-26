"""Logical file lengths, never allocated SSD space; no database is opened."""
from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone

COMPONENTS = ("SPO.dat", "POS.dat", "OSP.dat", "nodes.dat", "nodes-data.obj")


def logical_storage(root: Path) -> dict:
    root = Path(root)
    if not root.is_dir() or root.is_symlink() or root.is_junction():
        raise ValueError(f"Missing TDB2 directory: {root}")
    files = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink() or path.is_junction():
            raise ValueError(f"Linked storage path is not supported: {path}")
        if path.is_file():
            files.append({"path": path.relative_to(root).as_posix(), "bytes": path.stat().st_size})
    components = {name: sum(f["bytes"] for f in files if Path(f["path"]).name == name) for name in COMPONENTS}
    total = sum(f["bytes"] for f in files)
    return {"kind": "logical_file_lengths", "root": str(root.resolve()), "total_bytes": total,
            "total_mib": total / 1024 ** 2, "components_bytes": components,
            "observed_utc": datetime.now(timezone.utc).isoformat(),
            "missing_components": [name for name in COMPONENTS if not any(Path(f["path"]).name == name for f in files)],
            "other_bytes": total - sum(components.values()), "files": files}
