package io.lgoyal.gpubroker.analytics;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.parquet.avro.AvroParquetReader;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetricsAggregatorTest {
    @Test
    void keepsTenantPathSegmentsInsideOutputRoot() throws Exception {
        assertBoundedDimensions("lab/../../../escaped", "g4dn.xlarge");
    }

    @Test
    void keepsInstancePathSegmentsInsideOutputRoot() throws Exception {
        assertBoundedDimensions("lab", "../../../escaped");
    }

    private void assertBoundedDimensions(String tenant, String instanceType) throws Exception {
        java.nio.file.Path workspace = Files.createTempDirectory("gpu-broker-bounded-dimensions-");
        java.nio.file.Path root = workspace.resolve("bounded/deep/output");
        String event = new ObjectMapper().writeValueAsString(Map.of(
                "timestamp_ms", 1791763200000L, "tenant", tenant, "instance_type", instanceType));
        var aggregates = MetricsAggregator.aggregate(new BufferedReader(new StringReader(event)));
        MetricsAggregator.writeOutputs(aggregates, new Path(root.toUri()));
        java.util.List<java.nio.file.Path> files;
        try (var paths = Files.walk(workspace)) {
            files = paths.filter(path -> path.toString().endsWith(".parquet")).toList();
        }
        assertEquals(1, files.size());
        assertTrue(files.get(0).startsWith(root), "dimension escaped output root: " + files.get(0));
        try (var reader = AvroParquetReader.<GenericRecord>builder(new Path(files.get(0).toUri())).build()) {
            GenericRecord record = reader.read();
            assertEquals(tenant, record.get("tenant").toString());
            assertEquals(instanceType, record.get("instance_type").toString());
        }
    }

    @Test
    void acceptsLocalHdfsAndS3aOutputRoots() {
        assertEquals("s3a", StorageTarget.parse("s3a://metrics-bucket/run").uri().getScheme());
        assertEquals("hdfs", StorageTarget.parse("hdfs://namenode:8020/metrics").uri().getScheme());
        assertEquals("file", StorageTarget.parse("file:///tmp/metrics").uri().getScheme());
    }

    @Test
    void rejectsUnscopedOrUnknownOutputRoots() {
        assertThrows(IllegalArgumentException.class, () -> StorageTarget.parse("s3a:///missing-bucket"));
        assertThrows(IllegalArgumentException.class, () -> StorageTarget.parse("http://example.com/metrics"));
        assertThrows(IllegalArgumentException.class, () -> MetricsAggregator.writeOutputs(Map.of(), new Path("http://example.com/metrics")));
        assertThrows(IllegalArgumentException.class, () -> MetricsAggregator.writeOutputs(Map.of(), new Path("s3a:///missing-bucket")));
    }

    @Test
    void aggregatesByUtcDateTenantAndInstanceType() throws Exception {
        String events = """
            {"timestamp_ms":1791763200000,"tenant":"alpha","instance_type":"g4dn.xlarge","queue_wait_ms":10,"cost_usd":0.01,"utilization":0.8,"slo_met":true}
            {"timestamp_ms":1791763201000,"tenant":"alpha","instance_type":"g4dn.xlarge","queue_wait_ms":30,"cost_usd":0.02,"utilization":0.6,"slo_met":false}
            """;
        Map<MetricsAggregator.Key, MetricsAggregator.Aggregate> result =
                MetricsAggregator.aggregate(new BufferedReader(new StringReader(events)));
        MetricsAggregator.Aggregate aggregate = result.values().iterator().next();
        assertEquals(1, result.size());
        assertEquals(2, aggregate.jobs());
        assertEquals(20.0, aggregate.meanQueueWaitMs());
        assertEquals(0.03, aggregate.costUsd(), 0.00001);
        assertEquals(0.5, aggregate.sloMissRate());
    }

    @Test
    void rejectsEventsWithoutRequiredDimensions() {
        String event = "{\"timestamp_ms\":1791763200000,\"tenant\":\"\",\"instance_type\":\"g4dn.xlarge\"}";
        assertThrows(IllegalArgumentException.class, () -> MetricsAggregator.aggregate(
                new BufferedReader(new StringReader(event))));
    }

    @Test
    void writesCsvAndParquetThroughMiniDfs() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "1");
        configuration.setBoolean("dfs.permissions", false);
        configuration.setBoolean("dfs.client.use.datanode.hostname", false);
        configuration.set("hadoop.security.authentication", "simple");
        configuration.set("hadoop.tmp.dir", Files.createTempDirectory("gpu-broker-minidfs-").toString());

        try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(configuration)
                .numDataNodes(1)
                .format(true)
                .build()) {
            cluster.waitClusterUp();
            Configuration clientConfiguration = cluster.getConfiguration(0);
            Path outputRoot = new Path(cluster.getFileSystem().getUri().toString() + "/analytics");
            Map<MetricsAggregator.Key, MetricsAggregator.Aggregate> aggregates =
                    MetricsAggregator.aggregate(new BufferedReader(new StringReader("""
                        {"timestamp_ms":1791763200000,"tenant":"alpha","instance_type":"g4dn.xlarge","queue_wait_ms":10,"cost_usd":0.01,"utilization":0.8,"slo_met":true}
                        {"timestamp_ms":1791763201000,"tenant":"alpha","instance_type":"g4dn.xlarge","queue_wait_ms":30,"cost_usd":0.02,"utilization":0.6,"slo_met":false}
                        """)));

            MetricsAggregator.writeOutputs(aggregates, outputRoot);

            try (FileSystem fileSystem = outputRoot.getFileSystem(clientConfiguration)) {
                Path csv = new Path(outputRoot, "tableau/gpu_broker_metrics.csv");
                Path parquet = new Path(outputRoot, "date=2026-10-12/tenant=alpha/g4dn.xlarge.parquet");
                assertTrue(fileSystem.exists(csv));
                assertTrue(fileSystem.exists(parquet));
                try (FSDataInputStream stream = fileSystem.open(csv);
                     BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                    assertEquals("date,tenant,instance_type,jobs,mean_queue_wait_ms,cost_usd,utilization,slo_miss_rate", reader.readLine());
                    assertEquals("2026-10-12,alpha,g4dn.xlarge,2,20.0,0.03,0.7,0.5", reader.readLine());
                }
                try (var reader = AvroParquetReader.<GenericRecord>builder(parquet)
                        .withConf(clientConfiguration)
                        .build()) {
                    GenericRecord record = reader.read();
                    assertEquals(2L, record.get("jobs"));
                    assertEquals(0.5, (double) record.get("slo_miss_rate"), 0.00001);
                    assertEquals(null, reader.read());
                }
            }
        }
    }

    @Test
    void keepsReplicatedOutputsReadableAfterDataNodeFailure() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        configuration.setBoolean("dfs.permissions", false);
        configuration.setBoolean("dfs.client.use.datanode.hostname", false);
        configuration.set("hadoop.security.authentication", "simple");
        configuration.set("hadoop.tmp.dir", Files.createTempDirectory("gpu-broker-minidfs-ha-").toString());

        try (MiniDFSCluster cluster = new MiniDFSCluster.Builder(configuration)
                .numDataNodes(2)
                .format(true)
                .build()) {
            cluster.waitClusterUp();
            Configuration clientConfiguration = cluster.getConfiguration(0);
            Path outputRoot = new Path(cluster.getFileSystem().getUri().toString() + "/analytics-ha");
            Map<MetricsAggregator.Key, MetricsAggregator.Aggregate> aggregates =
                    MetricsAggregator.aggregate(new BufferedReader(new StringReader("""
                        {"timestamp_ms":1791763200000,"tenant":"alpha","instance_type":"g4dn.xlarge","queue_wait_ms":10,"cost_usd":0.01,"utilization":0.8,"slo_met":true}
                        {"timestamp_ms":1791763201000,"tenant":"alpha","instance_type":"g4dn.xlarge","queue_wait_ms":30,"cost_usd":0.02,"utilization":0.6,"slo_met":false}
                        """)));

            MetricsAggregator.writeOutputs(aggregates, outputRoot);
            Path csv = new Path(outputRoot, "tableau/gpu_broker_metrics.csv");
            Path parquet = new Path(outputRoot, "date=2026-10-12/tenant=alpha/g4dn.xlarge.parquet");
            try (FileSystem fileSystem = outputRoot.getFileSystem(clientConfiguration)) {
                assertEquals((short) 2, fileSystem.getFileStatus(csv).getReplication());
                assertEquals((short) 2, fileSystem.getFileStatus(parquet).getReplication());
            }

            cluster.stopDataNode(0);

            try (FileSystem fileSystem = outputRoot.getFileSystem(clientConfiguration);
                 FSDataInputStream stream = fileSystem.open(csv);
                 BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                assertEquals("date,tenant,instance_type,jobs,mean_queue_wait_ms,cost_usd,utilization,slo_miss_rate", reader.readLine());
                assertEquals("2026-10-12,alpha,g4dn.xlarge,2,20.0,0.03,0.7,0.5", reader.readLine());
            }
            try (var reader = AvroParquetReader.<GenericRecord>builder(parquet)
                    .withConf(clientConfiguration)
                    .build()) {
                GenericRecord record = reader.read();
                assertEquals(2L, record.get("jobs"));
                assertEquals(null, reader.read());
            }
        }
    }
}
