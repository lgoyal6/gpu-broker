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
rendered Tableau workbook. Tableau consumes the generated CSV or a Hyper
extract; workbook rendering remains a separate access gate.
