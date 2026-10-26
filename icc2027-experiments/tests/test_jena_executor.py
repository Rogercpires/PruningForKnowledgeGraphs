import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
from jena_executor import JenaExecutor, run_recorded


class ExecutionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_success_preserves_raw_bytes_command_and_metadata(self):
        command = [sys.executable, "-c", "print('42')"]
        record = run_recorded(command, self.root / "ok", 10, lambda data: int(data))
        self.assertEqual("success", record["status"])
        self.assertEqual(42, record["result"])
        self.assertEqual(command, record["command"])
        self.assertIn(b"42", (self.root / "ok.stdout").read_bytes())
        self.assertEqual(record, json.loads((self.root / "ok.meta.json").read_text()))

    def test_nonzero_exit_is_never_empty_success(self):
        record = run_recorded([sys.executable, "-c", "import sys; print('failure',file=sys.stderr); sys.exit(7)"], self.root / "bad", 10)
        self.assertEqual(("execution_error", 7, None), (record["status"], record["returncode"], record["result"]))
        self.assertIn(b"failure", (self.root / "bad.stderr").read_bytes())

    def test_invalid_result_remains_failure_with_raw_output(self):
        record = run_recorded([sys.executable, "-c", "print('not json')"], self.root / "parse", 10, json.loads)
        self.assertEqual("invalid_result", record["status"])
        self.assertIsNone(record["result"])
        self.assertTrue((self.root / "parse.stdout").is_file())

    def test_timeout_is_recorded_and_process_terminated(self):
        record = run_recorded([sys.executable, "-u", "-c", "import time; print('partial'); time.sleep(20)"], self.root / "timeout", 0.3)
        self.assertEqual("timeout", record["status"])
        self.assertIsNone(record["returncode"])
        self.assertIsNone(record["result"])
        self.assertLess(record["duration_seconds"], 10)

    def test_missing_program_and_output_collision(self):
        record = run_recorded([str(self.root / "does-not-exist")], self.root / "missing", 1)
        self.assertEqual("execution_error", record["status"])
        with self.assertRaises(FileExistsError):
            run_recorded([sys.executable, "-V"], self.root / "missing", 1)

    def test_typo_in_store_path_never_creates_database(self):
        executor = object.__new__(JenaExecutor)
        executor.command = [sys.executable]
        with self.assertRaises(ValueError):
            executor.query(self.root / "not-a-store", self.root / "query.rq", self.root / "result", 1, None)
        self.assertFalse((self.root / "not-a-store").exists())


if __name__ == "__main__":
    unittest.main()
