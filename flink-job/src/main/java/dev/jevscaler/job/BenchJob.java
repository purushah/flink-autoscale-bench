package dev.jevscaler.job;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.Arrays;
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

        var env = StreamExecutionEnvironment.getExecutionEnvironment();

        var sourceBuilder =
                KafkaSource.<String>builder()
                        .setBootstrapServers(brokers)
                        .setTopics(inTopic)
                        .setGroupId(group)
                        .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .setProperty("partition.discovery.interval.ms", "30000");
        // The consumer only learns a partition's end offset when it fetches from it, and it does not
        // fetch again until the previous response has been consumed. With the default 1 MiB per
        // partition, a slow job therefore reports a stale, far too small lag (pendingRecords). A small
        // fetch size keeps the reported lag close to the real one.
        String fetchBytes = params.get("max-partition-fetch-bytes");
        if (fetchBytes != null && !fetchBytes.isBlank()) {
            sourceBuilder.setProperty("max.partition.fetch.bytes", fetchBytes.trim());
        }
        var source = sourceBuilder.build();

        var sink =
                KafkaSink.<String>builder()
                        .setBootstrapServers(brokers)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(outTopic)
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build())
                        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                        .build();

        // Optional fixed parallelism per vertex (0 = the job default, which the autoscaler may change).
        int sourcePar = params.getInt("source-parallelism", 0);
        int aggPar = params.getInt("agg-parallelism", 0);
        int measurePar = params.getInt("measure-parallelism", 0);
        long restoreDelayMs = params.getLong("restore-delay-ms", 0);
        // false: the delay applies only when the job restores from a checkpoint, not on its first start.
        boolean restoreDelayFirstStart = params.getBoolean("restore-delay-first-start", true);

        var raw = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Source").uid("source");
        if (sourcePar > 0) {
            raw.setParallelism(sourcePar);
        }
        var parsed = raw.map(Event::parse).name("Parse").uid("parse");
        if (sourcePar > 0) {
            parsed.setParallelism(sourcePar);
        }
        var aggregated =
                parsed.keyBy(e -> e.key).process(new Aggregate(restoreDelayMs, restoreDelayFirstStart)).name("Aggregate").uid("aggregate");
        if (aggPar > 0) {
            aggregated.setParallelism(aggPar);
        }
        var measured = aggregated.rebalance().process(new Measure()).name("Measure").uid("measure");
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

    /** Keeps a running count and sum per key and spends the record's simulated cost. */
    static final class Aggregate extends KeyedProcessFunction<String, Event, Event> implements CheckpointedFunction {
        private final long restoreDelayMs;
        private final boolean restoreDelayFirstStart;
        private transient boolean restored;
        private transient ValueState<long[]> totals;
        private transient Pacer pacer;

        Aggregate(long restoreDelayMs, boolean restoreDelayFirstStart) {
            this.restoreDelayMs = restoreDelayMs;
            this.restoreDelayFirstStart = restoreDelayFirstStart;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {
            restored = context.isRestored();
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {}

        @Override
        public void open(Configuration parameters) throws Exception {
            totals = getRuntimeContext().getState(new ValueStateDescriptor<>("totals", long[].class));
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

        @Override
        public void open(Configuration parameters) {
            pacer = new Pacer();
            latencies = new long[4096];
            n = 0;
            windowEnd = 0;
            subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
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
