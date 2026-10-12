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

This proves a live S3A object-store round trip, not HDFS throughput or a
rendered Tableau workbook. The test suite also starts a one-node Hadoop
`MiniDFSCluster` and reads the generated CSV and Parquet back through
`FileSystem`; this verifies the HDFS adapter path without claiming a
multi-node cluster or throughput result:

```sh
gradle test --no-daemon --max-workers=1
```

To create a Tableau Hyper extract from the CSV:

```sh
uv run --with tableauhyperapi python scripts/build_tableau_hyper.py \
  /tmp/gpu-broker-analytics/tableau/gpu_broker_metrics.csv \
  /tmp/gpu-broker-analytics/tableau/gpu_broker_metrics.hyper
```

The Hyper file is Tableau-readable and its row count is checked by the script.
Desktop or Cloud workbook rendering remains a separate access gate.
