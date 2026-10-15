# GPU Broker Hadoop and Tableau analytics

This module converts broker event JSONL into two reviewable outputs:

1. Snappy-compressed Parquet under Hive-style `date=.../tenant=...` partitions,
   written through Hadoop `FileSystem` and Parquet Avro APIs.
2. A flat CSV under `tableau/gpu_broker_metrics.csv` for Tableau extracts.

The input contract is intentionally small: `timestamp_ms`, `tenant`,
`instance_type`, `queue_wait_ms`, `cost_usd`, `utilization`, and `slo_met`.
Production adapters will read the broker audit or outbox export into this
contract. They are not allowed to infer missing tenant or instance dimensions.

## Build and test

```sh
cd analytics
gradle test --no-daemon --max-workers=1
gradle run --args='fixtures/events.jsonl /tmp/gpu-broker-analytics'
```

The local fixture proves aggregation, partitioning, and schema validation. The
`StorageTarget` contract accepts only `file`, `hdfs`, and bucket-qualified
`s3a` roots before Hadoop is contacted, which makes cloud configuration errors
explicit. With `hadoop-aws` installed, the same aggregator can write to an
S3A bucket using the active AWS credential chain:

```sh
gradle run --args='fixtures/events.jsonl s3a://BUCKET/gpu-broker/'
```

CSV dimensions use standard comma, quote and newline escaping. Tenant and
instance values are percent-encoded when used as filesystem segments; their
original values remain in CSV and Parquet. Bare output paths resolve to local
file URIs, and the writer validates the storage scheme before contacting Hadoop.

The S3A command requires an authorized account and credentials; local tests do
not establish a live object-store round trip. The test suite starts Hadoop `MiniDFSCluster`
fixtures and reads the generated CSV and Parquet back through `FileSystem`.
The multi-node fixture uses two DataNodes with replication two, stops one
DataNode, and verifies both outputs remain readable. This is a local
replication and failover check, not a production HDFS capacity or throughput
result:

```sh
gradle test --no-daemon --max-workers=1
```

To create a Tableau Hyper extract from the CSV:

```sh
uv run --with-requirements requirements-hyper.txt python scripts/build_tableau_hyper.py \
  /tmp/gpu-broker-analytics/tableau/gpu_broker_metrics.csv \
  /tmp/gpu-broker-analytics/tableau/gpu_broker_metrics.hyper
```

The script reads every typed cell back from Hyper and compares it with the CSV.
CI runs the Java fixture export and this Hyper round trip with pinned dependencies.
Desktop or Cloud workbook rendering remains a separate access gate.
