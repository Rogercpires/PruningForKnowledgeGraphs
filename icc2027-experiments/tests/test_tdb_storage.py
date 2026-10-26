import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))
from tdb_storage import logical_storage


class StorageTests(unittest.TestCase):
    def test_logical_lengths_include_other_files_and_all_generations(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for name, size in (("Data-0001/SPO.dat", 3), ("Data-0002/SPO.dat", 2),
                               ("Data-0002/nodes-data.obj", 1), ("tdb.lock", 7)):
                path = root / name
                path.parent.mkdir(exist_ok=True)
                path.write_bytes(b"x" * size)
            result = logical_storage(root)
            self.assertEqual("logical_file_lengths", result["kind"])
            self.assertEqual(13, result["total_bytes"])
            self.assertEqual(5, result["components_bytes"]["SPO.dat"])
            self.assertEqual(7, result["other_bytes"])
            self.assertEqual(0, result["components_bytes"]["POS.dat"])

    def test_missing_root_is_not_zero_size(self):
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(ValueError):
                logical_storage(Path(temp) / "missing")


if __name__ == "__main__":
    unittest.main()
