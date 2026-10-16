"""Check public recordings preserve outcomes rather than only favorable totals."""
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("demo_export", Path(__file__).with_name("export.py"))
exporter = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(exporter)


class RecordingTest(unittest.TestCase):
    def setUp(self):
        self.raw = json.loads(Path(__file__).with_name("recorded-simulation.json").read_text())
        self.data = exporter.export(exporter.extract(self.raw))

    def test_committed_export_keeps_all_native_results(self):
        actual = json.loads(Path(__file__).with_name("site").joinpath("data.json").read_text())
        exporter.verify_export(actual, exporter.extract(self.raw))
        self.assertEqual(self.raw["results"], actual["results"])
        self.assertEqual(32, len(actual["results"]))

    def test_source_hash_and_result_tampering_are_rejected(self):
        for field in ["sources", "results"]:
            changed = json.loads(json.dumps(self.data))
            changed[field] = []
            with self.assertRaises(ValueError):
                exporter.verify_export(changed, exporter.extract(self.raw))

    def test_rebase_keeps_content_checks_without_a_historical_object(self):
        changed = dict(self.data, source_commit="0" * 40)
        with patch.object(exporter.subprocess, "run") as run:
            run.return_value.returncode = 1
            self.assertIn("unavailable", exporter.verify_export(changed, exporter.extract(self.raw)))

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
