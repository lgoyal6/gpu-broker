package io.lgoyal.gpubroker.analytics;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MetricsAggregatorTest {
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
}
