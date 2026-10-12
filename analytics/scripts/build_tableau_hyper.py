"""Build a Tableau Hyper extract from the analytics CSV."""

from __future__ import annotations

import argparse
import csv
from datetime import date
from pathlib import Path

from tableauhyperapi import (
    Connection,
    CreateMode,
    HyperProcess,
    Inserter,
    SqlType,
    TableDefinition,
    TableName,
    Telemetry,
)


def build(input_csv: Path, output_hyper: Path) -> tuple[int, int]:
    rows = list(csv.DictReader(input_csv.open(newline="", encoding="utf-8")))
    table = TableDefinition(
        TableName("Extract", "Extract"),
        [
            TableDefinition.Column("date", SqlType.date()),
            TableDefinition.Column("tenant", SqlType.text()),
            TableDefinition.Column("instance_type", SqlType.text()),
            TableDefinition.Column("jobs", SqlType.big_int()),
            TableDefinition.Column("mean_queue_wait_ms", SqlType.double()),
            TableDefinition.Column("cost_usd", SqlType.double()),
            TableDefinition.Column("utilization", SqlType.double()),
            TableDefinition.Column("slo_miss_rate", SqlType.double()),
        ],
    )
    output_hyper.parent.mkdir(parents=True, exist_ok=True)
    with HyperProcess(Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as process:
        with Connection(process.endpoint, str(output_hyper), CreateMode.CREATE_AND_REPLACE) as connection:
            connection.catalog.create_schema("Extract")
            connection.catalog.create_table(table)
            with Inserter(connection, table) as inserter:
                for row in rows:
                    inserter.add_row(
                        [
                            date.fromisoformat(row["date"]),
                            row["tenant"],
                            row["instance_type"],
                            int(row["jobs"]),
                            float(row["mean_queue_wait_ms"]),
                            float(row["cost_usd"]),
                            float(row["utilization"]),
                            float(row["slo_miss_rate"]),
                        ]
                    )
                inserter.execute()
            count = int(connection.execute_scalar_query('SELECT COUNT(*) FROM "Extract"."Extract"'))
            return count, output_hyper.stat().st_size


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("input_csv", type=Path)
    parser.add_argument("output_hyper", type=Path)
    args = parser.parse_args()
    rows, size = build(args.input_csv, args.output_hyper)
    print(f"rows={rows} hyper_bytes={size}")


if __name__ == "__main__":
    main()
