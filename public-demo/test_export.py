"""Check public recordings preserve outcomes rather than only favorable totals."""
import importlib.util
import json
from pathlib import Path
import unittest

SPEC = importlib.util.spec_from_file_location("demo_export", Path(__file__).with_name("export.py"))
exporter = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(exporter)


class RecordingTest(unittest.TestCase):
    def setUp(self):
        self.raw = json.loads(Path(__file__).with_name("recorded-simulation.json").read_text())
        self.data = exporter.export(exporter.extract(self.raw))

    def test_committed_export_keeps_all_native_results(self):
        actual = json.loads(Path(__file__).with_name("site").joinpath("data.json").read_text())
        self.assertEqual(self.data, actual)
        self.assertEqual(self.raw["results"], actual["results"])
        self.assertEqual(32, len(actual["results"]))

    def test_carbon_label_does_not_turn_a_loss_into_savings(self):
        rows = {row["policy"]: row for row in self.data["results"] if row["workload"] == "carbon-flex"}
        self.assertGreater(rows["lowest-carbon"]["carbon_kg_estimate"], rows["lowest-cost"]["carbon_kg_estimate"])
        self.assertGreater(rows["lowest-carbon"]["cost_usd"], rows["lowest-cost"]["cost_usd"])

    def test_incomplete_work_is_not_removed(self):
        for row in self.data["results"]:
            self.assertEqual(row["jobs"], row["completed"] + row["expired"] + row["unfinished_at_horizon"])
            self.assertLessEqual(row["deadline_misses"], row["deadline_jobs"])
            self.assertEqual("simulator", row["evidence_class"])
        self.assertTrue(any(row["expired"] > 0 for row in self.data["results"]))
        self.assertNotIn("decide_latency", self.data)


if __name__ == "__main__":
    unittest.main()
