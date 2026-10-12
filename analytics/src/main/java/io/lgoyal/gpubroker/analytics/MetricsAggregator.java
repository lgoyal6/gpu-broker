package io.lgoyal.gpubroker.analytics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Aggregates broker events into Hadoop-partitioned Parquet and Tableau CSV. */
public final class MetricsAggregator {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Schema SCHEMA = new Schema.Parser().parse("""
        {"type":"record","name":"GpuBrokerMetric","fields":[
          {"name":"date","type":"string"},
          {"name":"tenant","type":"string"},
          {"name":"instance_type","type":"string"},
          {"name":"jobs","type":"long"},
          {"name":"mean_queue_wait_ms","type":"double"},
          {"name":"cost_usd","type":"double"},
          {"name":"utilization","type":"double"},
          {"name":"slo_miss_rate","type":"double"}
        ]}
        """);

    private MetricsAggregator() {}

    public record Key(String date, String tenant, String instanceType) {}

    public static final class Aggregate {
        private final Key key;
        private long jobs;
        private long queueWaitTotal;
        private double cost;
        private double utilizationTotal;
        private long sloMisses;

        private Aggregate(Key key) { this.key = key; }

        private void add(JsonNode event) {
            jobs++;
            queueWaitTotal += event.path("queue_wait_ms").asLong();
            cost += event.path("cost_usd").asDouble();
            utilizationTotal += event.path("utilization").asDouble();
            if (!event.path("slo_met").asBoolean(true)) sloMisses++;
        }

        public Key key() { return key; }
        public long jobs() { return jobs; }
        public double meanQueueWaitMs() { return jobs == 0 ? 0.0 : (double) queueWaitTotal / jobs; }
        public double costUsd() { return cost; }
        public double utilization() { return jobs == 0 ? 0.0 : utilizationTotal / jobs; }
        public double sloMissRate() { return jobs == 0 ? 0.0 : (double) sloMisses / jobs; }
    }

    public static Map<Key, Aggregate> aggregate(BufferedReader input) throws IOException {
        Map<Key, Aggregate> result = new LinkedHashMap<>();
        String line;
        while ((line = input.readLine()) != null) {
            if (line.isBlank()) continue;
            JsonNode event = JSON.readTree(line);
            String date = Instant.ofEpochMilli(event.path("timestamp_ms").asLong())
                    .atZone(ZoneOffset.UTC).toLocalDate().toString();
            Key key = new Key(date, required(event, "tenant"), required(event, "instance_type"));
            result.computeIfAbsent(key, Aggregate::new).add(event);
        }
        return result;
    }

    public static void writeOutputs(Map<Key, Aggregate> aggregates, Path outputRoot) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = outputRoot.getFileSystem(configuration);
        fileSystem.mkdirs(outputRoot);
        Path csv = new Path(outputRoot, "tableau/gpu_broker_metrics.csv");
        fileSystem.mkdirs(csv.getParent());
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileSystem.create(csv, true), StandardCharsets.UTF_8))) {
            writer.write("date,tenant,instance_type,jobs,mean_queue_wait_ms,cost_usd,utilization,slo_miss_rate\n");
            for (Aggregate aggregate : sorted(aggregates)) {
                writer.write(csvRow(aggregate));
                writer.write('\n');
            }
        }
        for (Aggregate aggregate : sorted(aggregates)) {
            Path partition = new Path(outputRoot, "date=" + aggregate.key().date() + "/tenant=" + aggregate.key().tenant());
            fileSystem.mkdirs(partition);
            Path parquet = new Path(partition, aggregate.key().instanceType() + ".parquet");
            try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(parquet)
                    .withSchema(SCHEMA)
                    .withConf(configuration)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .build()) {
                writer.write(record(aggregate));
            }
        }
    }

    private static String required(JsonNode event, String field) {
        String value = event.path(field).asText();
        if (value.isBlank()) throw new IllegalArgumentException("missing event field: " + field);
        return value;
    }

    private static List<Aggregate> sorted(Map<Key, Aggregate> aggregates) {
        List<Aggregate> values = new ArrayList<>(aggregates.values());
        values.sort(Comparator.comparing(item -> item.key().toString()));
        return values;
    }

    private static String csvRow(Aggregate item) {
        return String.join(",", item.key().date(), item.key().tenant(), item.key().instanceType(),
                Long.toString(item.jobs()), Double.toString(item.meanQueueWaitMs()), Double.toString(item.costUsd()),
                Double.toString(item.utilization()), Double.toString(item.sloMissRate()));
    }

    private static GenericRecord record(Aggregate item) {
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("date", item.key().date());
        record.put("tenant", item.key().tenant());
        record.put("instance_type", item.key().instanceType());
        record.put("jobs", item.jobs());
        record.put("mean_queue_wait_ms", item.meanQueueWaitMs());
        record.put("cost_usd", item.costUsd());
        record.put("utilization", item.utilization());
        record.put("slo_miss_rate", item.sloMissRate());
        return record;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) throw new IllegalArgumentException("usage: MetricsAggregator INPUT_JSONL OUTPUT_ROOT");
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(args[0]), StandardCharsets.UTF_8)) {
            writeOutputs(aggregate(reader), new Path(args[1]));
        }
    }
}
