package dev.jevscaler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class BrokerLagTest {

    private static final Instant NOW = Instant.parse("2026-11-17T10:00:00Z");

    private static Configuration conf() {
        var conf = new Configuration();
        conf.set(AutoScalerOptions.METRICS_WINDOW, Duration.ofMinutes(1));
        conf.set(AutoScalerOptions.CATCH_UP_DURATION, Duration.ofMinutes(1));
        conf.set(AutoScalerOptions.RESTART_TIME, Duration.ofSeconds(30));
        conf.set(AutoScalerOptions.UTILIZATION_TARGET, 0.7);
        conf.set(AutoScalerOptions.UTILIZATION_MAX, 0.7);
        return conf;
    }

    /** What the built-in evaluation produced for a throttled source: it believes 900 rec/s and no backlog. */
    private static Map<ScalingMetric, EvaluatedScalingMetric> believed() {
        var m = new HashMap<ScalingMetric, EvaluatedScalingMetric>();
        m.put(ScalingMetric.TARGET_DATA_RATE, new EvaluatedScalingMetric(900, 900));
        m.put(ScalingMetric.CATCH_UP_DATA_RATE, EvaluatedScalingMetric.of(0));
        m.put(ScalingMetric.LAG, EvaluatedScalingMetric.of(0));
        m.put(ScalingMetric.TRUE_PROCESSING_RATE, EvaluatedScalingMetric.avg(10000));
        return m;
    }

    @Test
    void firstCycleCorrectsTheBacklogOnly() {
        var job = new JevRuntime.JobState();
        var out = BrokerLag.overrides(job, believed(), conf(), 60000, 1_000_000, NOW);
        assertEquals(60000, out.get(ScalingMetric.LAG).getCurrent());
        assertEquals(1000, out.get(ScalingMetric.CATCH_UP_DATA_RATE).getCurrent());
        // No arrival rate yet: one sample of the end offsets is not a rate.
        assertFalse(out.containsKey(ScalingMetric.TARGET_DATA_RATE));
        // Scale-up threshold with the believed 900 rec/s: 1000 + 900 / 0.7.
        assertEquals(Math.round(1000 + 900 / 0.7), out.get(ScalingMetric.SCALE_UP_RATE_THRESHOLD).getCurrent());
    }

    @Test
    void arrivalRateComesFromTheGrowthOfTheEndOffsets() {
        var job = new JevRuntime.JobState();
        BrokerLag.overrides(job, believed(), conf(), 0, 1_000_000, NOW);
        BrokerLag.overrides(job, believed(), conf(), 30000, 1_060_000, NOW.plusSeconds(30));
        var out = BrokerLag.overrides(job, believed(), conf(), 60000, 1_120_000, NOW.plusSeconds(60));
        // 120,000 records arrived in 60 s although the throttled source only read 900 rec/s.
        assertEquals(2000, out.get(ScalingMetric.TARGET_DATA_RATE).getAverage());
        assertEquals(60000, out.get(ScalingMetric.LAG).getCurrent());
        // Thresholds follow the operator's formula with the corrected inputs:
        // up: catch-up + rate / upper bound; down: catch-up + rate * restart / catch-up + rate / lower bound.
        assertEquals(Math.round(1000 + 2000 / 0.7), out.get(ScalingMetric.SCALE_UP_RATE_THRESHOLD).getCurrent());
        assertEquals(Math.round(1000 + 2000 * 0.5 + 2000 / 0.4), out.get(ScalingMetric.SCALE_DOWN_RATE_THRESHOLD).getCurrent(), 1.0);

        // Samples older than the window are dropped: 6 minutes later at a steady 500 rec/s.
        for (int i = 1; i <= 24; i++) {
            out = BrokerLag.overrides(job, believed(), conf(), 0, 1_120_000 + i * 7500, NOW.plusSeconds(60 + i * 15));
        }
        assertEquals(500, out.get(ScalingMetric.TARGET_DATA_RATE).getAverage(), 1e-6);
    }

    @Test
    void gaugeNamesAreSplitIntoShortQueries() {
        var ids = new ArrayList<String>();
        for (int p = 0; p < 64; p++) {
            ids.add("Source__Kafka_source.KafkaSourceReader.topic.in-some-long-topic-name-1234.partition." + p + ".currentOffset");
        }
        List<String> queries = BrokerLag.queries(ids, 2000);
        assertEquals(4, queries.size());
        var seen = new ArrayList<String>();
        for (String q : queries) {
            assertEquals(true, q.length() <= 2000);
            seen.addAll(List.of(q.split(",")));
        }
        // Every name is asked for exactly once, in order.
        assertEquals(ids, seen);
        // A short list stays one request.
        assertEquals(1, BrokerLag.queries(ids.subList(0, 8), 2000).size());
        assertEquals(List.of(), BrokerLag.queries(List.of(), 2000));
    }
}
