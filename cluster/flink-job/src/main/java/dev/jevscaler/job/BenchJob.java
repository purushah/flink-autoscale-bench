package dev.jevscaler.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ParameterTool;

import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.locks.LockSupport;

/**
 * Stateful benchmark job with three separately scalable vertices:
 *
 * <pre>
 *   Kafka source  --keyBy-->  Aggregate (keyed state, expensive)  --rebalance-->  Measure (cheap) + Kafka sink
 * </pre>
 *
 * <p>Input records are CSV: {@code seq,key,eventTimeMillis,aggregateCostMicros,measureCostMicros}.
 * The per-record cost is simulated by parking the task thread (see {@link Pacer}), so a subtask's
 * capacity is {@code 1 / cost} records per second without burning CPU. The load generator sets the
 * cost, which lets a benchmark scenario change how much work one worker can do while the job runs.
 *
 * <p>The Measure vertex emits one summary per second and subtask to an output topic:
 * {@code windowEndMillis,subtask,count,p50,p95,p99,max} (latencies in milliseconds from the moment
 * the broker appended the record to the moment it left the pipeline).
 *
 * <p>This copy (for the cluster test) is written against the Flink 2 API and also reports the latency as Flink metrics of the Measure
 * operator, so that a metrics backend sees what the output topic carries: gauges {@code
 * latencyP50Ms}, {@code latencyP95Ms} and {@code latencyMaxMs} of the last full second (-1 when the
 * subtask has emitted no summary for three seconds), and counters {@code secondsMeasured} and
 * {@code secondsOverLimit} (seconds whose 95th percentile exceeded {@code --latency-limit-ms}).
 */
public final class BenchJob {

    private BenchJob() {}

    /** One parsed input record. Public fields and a no-arg constructor make it a Flink POJO. */
    public static final class Event {
        public long seq;
        public String key;
        public long eventTime;
        public int aggCostMicros;
        public int measureCostMicros;

        /** Running count and sum for this key, filled in by the aggregate vertex. */
        public long keyCount;

        public long keySum;

        public Event() {}

        static Event parse(String line) {
            var e = new Event();
            String[] f = line.split(",", 6);
            e.seq = Long.parseLong(f[0]);
            e.key = f[1];
            e.eventTime = Long.parseLong(f[2]);
            e.aggCostMicros = Integer.parseInt(f[3]);
            e.measureCostMicros = Integer.parseInt(f[4]);
            return e;
        }
    }

    public static void main(String[] args) throws Exception {
        var params = ParameterTool.fromArgs(args);
        String brokers = params.get("brokers", "kafka.kafka.svc:9092");
        String inTopic = params.getRequired("in-topic");
        String outTopic = params.getRequired("out-topic");
        String group = params.get("group", "bench-" + inTopic);

        // Client settings shared by the source and the sink. With --msk-iam true the job logs in to
        // Amazon MSK with the pod's IAM role (port 9098).
        var kafkaProps = new Properties();
        if (params.getBoolean("msk-iam", false)) {
            kafkaProps.setProperty("security.protocol", "SASL_SSL");
            kafkaProps.setProperty("sasl.mechanism", "AWS_MSK_IAM");
            kafkaProps.setProperty("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
            kafkaProps.setProperty(
                    "sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        }

        var env = StreamExecutionEnvironment.getExecutionEnvironment();

        var sourceBuilder =
                KafkaSource.<String>builder()
                        .setBootstrapServers(brokers)
                        .setTopics(inTopic)
                        .setGroupId(group)
                        .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .setProperty("partition.discovery.interval.ms", "30000");
        sourceBuilder.setProperties(kafkaProps);
        // The consumer only learns a partition's end offset when it fetches from it, and it does not
        // fetch again until the previous response has been consumed. With the default 1 MiB per
        // partition, a slow job therefore reports a stale, far too small lag (pendingRecords). A small
        // fetch size keeps the reported lag close to the real one.
        String fetchBytes = params.get("max-partition-fetch-bytes");
        if (fetchBytes != null && !fetchBytes.isBlank()) {
            sourceBuilder.setProperty("max.partition.fetch.bytes", fetchBytes.trim());
        }
        var source = sourceBuilder.build();

        // --sink-guarantee none: the latency summaries are sent without waiting for them at a
        // checkpoint, and may wait ten minutes for a broker. With the default (at-least-once) the
        // sink flushes at every checkpoint, so a broker that does not answer blocks the Measure
        // subtasks and the checkpoint for as long as it lasts (seen on a managed Kafka cluster).
        boolean sinkWaits = !"none".equals(params.get("sink-guarantee", "at-least-once"));
        var sinkProps = new Properties();
        sinkProps.putAll(kafkaProps);
        if (!sinkWaits) {
            sinkProps.setProperty("delivery.timeout.ms", "600000");
        }
        var sink =
                KafkaSink.<String>builder()
                        .setBootstrapServers(brokers)
                        .setKafkaProducerConfig(sinkProps)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(outTopic)
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build())
                        .setDeliveryGuarantee(sinkWaits ? DeliveryGuarantee.AT_LEAST_ONCE : DeliveryGuarantee.NONE)
                        .build();

        // Optional fixed parallelism per vertex (0 = the job default, which the autoscaler may change).
        int sourcePar = params.getInt("source-parallelism", 0);
        int aggPar = params.getInt("agg-parallelism", 0);
        int measurePar = params.getInt("measure-parallelism", 0);
        long restoreDelayMs = params.getLong("restore-delay-ms", 0);
        // false: the delay applies only when the job restores from a checkpoint, not on its first start.
        boolean restoreDelayFirstStart = params.getBoolean("restore-delay-first-start", true);
        // Real state: every key holds this many bytes, written with the key's first records (0: none).
        long ballastBytesPerKey = params.getLong("ballast-bytes-per-key", 0);
        int ballastEntryBytes = params.getInt("ballast-entry-bytes", 4096);
        int ballastEntriesPerRecord = params.getInt("ballast-entries-per-record", 16);
        long latencyLimitMs = params.getLong("latency-limit-ms", 2000);

        var raw = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Source").uid("source");
        if (sourcePar > 0) {
            raw.setParallelism(sourcePar);
        }
        var parsed = raw.map(Event::parse).name("Parse").uid("parse");
        if (sourcePar > 0) {
            parsed.setParallelism(sourcePar);
        }
        var aggregated =
                parsed.keyBy(e -> e.key)
                        .process(new Aggregate(restoreDelayMs, restoreDelayFirstStart, ballastBytesPerKey, ballastEntryBytes, ballastEntriesPerRecord))
                        .name("Aggregate")
                        .uid("aggregate");
        if (aggPar > 0) {
            aggregated.setParallelism(aggPar);
        }
        var measured = aggregated.rebalance().process(new Measure(latencyLimitMs)).name("Measure").uid("measure");
        if (measurePar > 0) {
            measured.setParallelism(measurePar);
        }
        var summaries = measured.sinkTo(sink).name("Summaries").uid("summaries");
        if (measurePar > 0) {
            summaries.setParallelism(measurePar);
        }

        env.execute("bench-" + inTopic);
    }

    /**
     * How long Aggregate waits before it processes its first record after a (re)start. The delay stands
     * in for loading a large state, so it always applies to a restore. Whether the first start waits too
     * is a choice: a first start that waits builds a backlog before the run begins.
     */
    static long delayFor(boolean restored, boolean firstStartToo, long delayMs) {
        return (restored || firstStartToo) && delayMs > 0 ? delayMs : 0;
    }

    /**
     * Real state of a key: a fixed number of entries of a fixed size. Aggregate writes them with the
     * key's first records, a few with each, and reads one of the first few on every record. Most of the state is
     * cold, as in a job with a large state, but a restore has to bring all of it back. The bytes do
     * not compress, so the size on disk and in a checkpoint is the size asked for.
     */
    static final class Ballast {
        private Ballast() {}

        /** How many entries of {@code entryBytes} hold at least {@code bytesPerKey}; 0 when no state is asked for. */
        static int entries(long bytesPerKey, int entryBytes) {
            return bytesPerKey <= 0 ? 0 : (int) ((bytesPerKey + entryBytes - 1) / entryBytes);
        }

        /** The content of one entry: the same for the same key and index, different otherwise. */
        static byte[] entry(String key, int index, int entryBytes) {
            byte[] bytes = new byte[entryBytes];
            new java.util.Random(key.hashCode() * 1_000_003L + index).nextBytes(bytes);
            return bytes;
        }

        /** A key's records read this many of its entries; the rest is state that is kept and restored but cold. */
        static final int HOT = 8;

        /**
         * The entries the n-th record of a key writes, as [from, to): {@code perRecord} with each of the
         * key's first records until all are written, and at least the ones its records read with the
         * first. Writing a key's state at once stalled RocksDB for minutes (second smoke run, 23 Nov).
         */
        static int[] fillRange(long recordOfKey, int entries, int perRecord) {
            long step = Math.max(perRecord, HOT);
            long from = Math.min((recordOfKey - 1) * step, entries);
            return new int[] {(int) from, (int) Math.min(from + step, entries)};
        }

        /** Which entry the n-th record of a key reads: its first {@link #HOT} entries in turn. */
        static int slot(long recordOfKey, int entries) {
            return (int) Math.floorMod(recordOfKey, (long) Math.min(entries, HOT));
        }
    }

    /** Keeps a running count and sum per key and spends the record's simulated cost. */
    static final class Aggregate extends KeyedProcessFunction<String, Event, Event> implements CheckpointedFunction {
        private final long restoreDelayMs;
        private final boolean restoreDelayFirstStart;
        private final long ballastBytesPerKey;
        private final int ballastEntryBytes;
        private final int ballastEntriesPerRecord;
        private transient boolean restored;
        private transient ValueState<long[]> totals;
        private transient MapState<Integer, byte[]> ballast;
        private transient int ballastEntries;
        private transient Pacer pacer;

        Aggregate(
                long restoreDelayMs, boolean restoreDelayFirstStart, long ballastBytesPerKey, int ballastEntryBytes, int ballastEntriesPerRecord) {
            this.restoreDelayMs = restoreDelayMs;
            this.restoreDelayFirstStart = restoreDelayFirstStart;
            this.ballastBytesPerKey = ballastBytesPerKey;
            this.ballastEntryBytes = ballastEntryBytes;
            this.ballastEntriesPerRecord = ballastEntriesPerRecord;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {
            restored = context.isRestored();
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {}

        @Override
        public void open(OpenContext openContext) throws Exception {
            totals = getRuntimeContext().getState(new ValueStateDescriptor<>("totals", long[].class));
            ballastEntries = Ballast.entries(ballastBytesPerKey, ballastEntryBytes);
            if (ballastEntries > 0) {
                ballast = getRuntimeContext().getMapState(new MapStateDescriptor<>("ballast", Integer.class, byte[].class));
            }
            pacer = new Pacer();
            long delay = delayFor(restored, restoreDelayFirstStart, restoreDelayMs);
            if (delay > 0) {
                // Stands in for restoring a large state: the subtask processes nothing for this long
                // when the job restores (and, unless told otherwise, on its first start), as it would
                // while downloading and loading its state.
                Thread.sleep(delay);
            }
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<Event> out) throws Exception {
            long[] t = totals.value();
            if (t == null) {
                t = new long[2];
            }
            t[0]++;
            t[1] += e.seq;
            totals.update(t);
            if (ballastEntries > 0) {
                // The key's first records write its state, a few entries each; every record reads one.
                int[] write = Ballast.fillRange(t[0], ballastEntries, ballastEntriesPerRecord);
                for (int i = write[0]; i < write[1]; i++) {
                    ballast.put(i, Ballast.entry(ctx.getCurrentKey(), i, ballastEntryBytes));
                }
                if (ballast.get(Ballast.slot(t[0], ballastEntries)) == null) {
                    throw new IllegalStateException("state of key " + ctx.getCurrentKey() + " is incomplete");
                }
            }
            // Latency is measured from the broker's append time (the topic uses LogAppendTime), so
            // the load generator's clock does not matter.
            Long appendTime = ctx.timestamp();
            if (appendTime != null) {
                e.eventTime = appendTime;
            }
            pacer.work(e.aggCostMicros);
            e.keyCount = t[0];
            e.keySum = t[1];
            out.collect(e);
        }
    }

    /** Spends the record's simulated cost and emits per-second latency summaries. */
    static final class Measure extends ProcessFunction<Event, String> {
        private transient long[] latencies;
        private transient int n;
        private transient long windowEnd;
        private transient int subtask;
        private transient Pacer pacer;
        private final long limitMs;
        // Read by the metric reporter's thread.
        private transient volatile long lastP50;
        private transient volatile long lastP95;
        private transient volatile long lastMax;
        private transient volatile long lastFlushMillis;
        private transient Counter secondsMeasured;
        private transient Counter secondsOverLimit;

        Measure(long limitMs) {
            this.limitMs = limitMs;
        }

        @Override
        public void open(OpenContext openContext) {
            pacer = new Pacer();
            latencies = new long[4096];
            n = 0;
            windowEnd = 0;
            subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            lastFlushMillis = 0;
            var metrics = getRuntimeContext().getMetricGroup();
            metrics.gauge("latencyP50Ms", (Gauge<Long>) () -> fresh() ? lastP50 : -1L);
            metrics.gauge("latencyP95Ms", (Gauge<Long>) () -> fresh() ? lastP95 : -1L);
            metrics.gauge("latencyMaxMs", (Gauge<Long>) () -> fresh() ? lastMax : -1L);
            secondsMeasured = metrics.counter("secondsMeasured");
            secondsOverLimit = metrics.counter("secondsOverLimit");
        }

        /** True if this subtask emitted a summary in the last three seconds. */
        private boolean fresh() {
            return System.currentTimeMillis() - lastFlushMillis <= 3000;
        }

        @Override
        public void processElement(Event e, Context ctx, Collector<String> out) {
            pacer.work(e.measureCostMicros);
            long now = System.currentTimeMillis();
            if (windowEnd == 0) {
                windowEnd = (now / 1000 + 1) * 1000;
            }
            if (now >= windowEnd) {
                flush(out);
                windowEnd = (now / 1000 + 1) * 1000;
            }
            if (n == latencies.length) {
                latencies = Arrays.copyOf(latencies, n * 2);
            }
            latencies[n++] = Math.max(0, now - e.eventTime);
        }

        private void flush(Collector<String> out) {
            if (n == 0) {
                return;
            }
            Arrays.sort(latencies, 0, n);
            lastP50 = latencies[(int) ((n - 1) * 0.50)];
            lastP95 = latencies[(int) ((n - 1) * 0.95)];
            lastMax = latencies[n - 1];
            lastFlushMillis = System.currentTimeMillis();
            secondsMeasured.inc();
            if (lastP95 > limitMs) {
                secondsOverLimit.inc();
            }
            out.collect(
                    windowEnd
                            + ","
                            + subtask
                            + ","
                            + n
                            + ","
                            + latencies[(int) ((n - 1) * 0.50)]
                            + ","
                            + latencies[(int) ((n - 1) * 0.95)]
                            + ","
                            + latencies[(int) ((n - 1) * 0.99)]
                            + ","
                            + latencies[n - 1]);
            n = 0;
        }
    }

    /**
     * Simulated per-record work: blocks the task thread without using CPU.
     *
     * <p>Parking for each record separately is inaccurate: on the benchmark VM a 2 ms park takes 2.3
     * to 2.8 ms and a 0.3 ms park about 0.45 ms, and the overshoot depends on how busy the machine
     * is, so a subtask's capacity would be 15 to 40% below nominal and vary with load. The pacer
     * therefore accumulates the time owed, parks in slices of about 5 ms, and subtracts what was
     * actually slept, so an overshoot is paid back by the next slice. Over any stretch longer than a
     * few slices a subtask spends exactly the nominal cost per record.
     */
    static final class Pacer {
        private static final long SLICE_NANOS = 5_000_000L;
        private long owedNanos;

        void work(int micros) {
            if (micros <= 0) {
                return;
            }
            owedNanos += micros * 1000L;
            if (owedNanos < SLICE_NANOS) {
                return;
            }
            long start = System.nanoTime();
            long end = start + owedNanos;
            long remaining;
            while ((remaining = end - System.nanoTime()) > 0) {
                LockSupport.parkNanos(remaining);
            }
            owedNanos = Math.max(-SLICE_NANOS, owedNanos - (System.nanoTime() - start));
        }
    }
}
