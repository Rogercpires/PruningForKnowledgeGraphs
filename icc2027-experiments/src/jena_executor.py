"""One Java process per query: shell-free arguments and bounded execution.

Launching the class used by the installed wrapper directly also ensures that a
timeout terminates Java itself, rather than leaving a child of cmd.exe running.
"""
from __future__ import annotations

import json
import math
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter

from icc2027_pipeline import sha256


def write_json(path: Path, value: object) -> None:
    with path.open("x", encoding="utf-8", newline="\n") as sink:
        json.dump(value, sink, indent=2, ensure_ascii=False, allow_nan=False)
        sink.write("\n")


def run_recorded(command: list[str], prefix: Path, timeout: float, validator=None) -> dict:
    if timeout <= 0 or not math.isfinite(timeout):
        raise ValueError("Timeout must be finite and positive")
    stdout, stderr = prefix.with_suffix(".stdout"), prefix.with_suffix(".stderr")
    meta = {"command": command, "started_utc": datetime.now(timezone.utc).isoformat(),
            "timeout_seconds": timeout, "returncode": None, "status": "execution_error",
            "error": None, "result": None}
    started = perf_counter()
    interrupted = False
    with stdout.open("xb") as out, stderr.open("xb") as err:
        try:
            with subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=out, stderr=err) as process:
                meta["pid"] = process.pid
                try:
                    write_json(prefix.with_suffix(".process.json"), {"pid": process.pid, "command": command,
                                                                   "started_utc": meta["started_utc"]})
                    process.wait(timeout=timeout)
                except BaseException:
                    process.kill()
                    process.wait()
                    raise
                meta["returncode"] = process.returncode
                meta["status"] = "success" if process.returncode == 0 else "execution_error"
                if process.returncode:
                    meta["error"] = f"Process exited with status {process.returncode}; see stderr"
        except subprocess.TimeoutExpired:
            meta.update(status="timeout", error="Execution exceeded timeout; Java process terminated")
        except OSError as exc:
            meta["error"] = str(exc)
        except KeyboardInterrupt:
            interrupted = True
            meta.update(status="interrupted", error="Interrupted; child process terminated and reaped")
    meta["duration_seconds"] = perf_counter() - started
    meta["stdout_sha256"], meta["stderr_sha256"] = sha256(stdout), sha256(stderr)
    if validator is not None and meta["status"] == "success":
        try:
            meta["result"] = validator(stdout.read_bytes())
        except (ValueError, TypeError, KeyError, AttributeError) as exc:
            meta.update(status="invalid_result", error=str(exc))
    write_json(prefix.with_suffix(".meta.json"), meta)
    if interrupted:
        raise KeyboardInterrupt
    return meta


class JenaExecutor:
    def __init__(self, home: Path, java: str = "java", jvm_args: tuple[str, ...] = ()):
        self.home = Path(home).resolve()
        executable = shutil.which(java)
        if executable is None or not list((self.home / "lib").glob("jena-tdb2-*.jar")):
            raise ValueError("Java executable or Jena TDB2 jars not found")
        self.java = executable
        self.command = [executable, *jvm_args,
                        "-Dlog4j.configurationFile=" + str(self.home / "log4j2.properties"),
                        "-cp", str(self.home / "lib" / "*"), "tdb2.tdbquery"]

    def probe(self, folder: Path, timeout: float) -> dict:
        folder.mkdir()
        results = {}
        for name, command in (("help", [*self.command, "--help"]),
                              ("version", [*self.command, "--version"]),
                              ("java", [self.java, "-version"])):
            record = run_recorded(command, folder / name, timeout)
            results[name] = record
            if record["status"] != "success":
                raise ValueError(f"CLI probe failed: {name}; evidence in {folder}")
        help_text = (folder / "help.stdout").read_text(encoding="utf-8", errors="replace")
        help_text += (folder / "help.stderr").read_text(encoding="utf-8", errors="replace")
        if "--results=" not in help_text or "JSON" not in help_text:
            raise ValueError("Installed Jena CLI did not advertise JSON SELECT results")
        return results

    def query(self, tdb: Path, query: Path, prefix: Path, timeout: float, validator) -> dict:
        # Never let Jena silently create a store at a misspelled location.
        data = list(tdb.glob("Data-*")) if tdb.is_dir() else []
        if not any((p / "SPO.dat").is_file() for p in data) or not query.is_file():
            raise ValueError(f"Missing existing TDB2 store or query: {tdb}, {query}")
        return run_recorded([*self.command, "--loc=" + str(tdb), "--query=" + str(query),
                             "--results=JSON"], prefix, timeout, validator)
