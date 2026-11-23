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

    /** Four cycles at 400 rec/s, then one that falls 6 s after the load has jumped to 4800 rec/s. */
    private static Map<ScalingMetric, EvaluatedScalingMetric> jump(boolean stepAware, double backlogAfter) {
        var job = new JevRuntime.JobState();
        for (int i = 0; i <= 4; i++) {
            BrokerLag.overrides(job, believed(), conf(), 200, 1_000_000 + i * 6000, NOW.plusSeconds(i * 15), stepAware, 0.2, 400);
        }
        // 9 s at 400 rec/s and 6 s at 4800 rec/s: 32,400 records in the newest interval.
        return BrokerLag.overrides(job, believed(), conf(), backlogAfter, 1_024_000 + 32_400, NOW.plusSeconds(75), stepAware, 0.2, Double.NaN);
    }

    @Test
    void aJumpInTheLoadIsSizedAtTheNewestRateWhileTheBacklogGrows() {
        // Without the option the demand is the window's average: 50,400 records in 60 s (ISSUES.md #123).
        var plain = jump(false, 26_600);
        assertEquals(840, plain.get(ScalingMetric.TARGET_DATA_RATE).getAverage(), 1e-6);

        // With it the newest interval counts: 32,400 records in 15 s, while the backlog grew by 1760 rec/s.
        var aware = jump(true, 26_600);
        assertEquals(2160, aware.get(ScalingMetric.TARGET_DATA_RATE).getAverage(), 1e-6);
        assertEquals(26_600, aware.get(ScalingMetric.LAG).getCurrent());
        assertEquals(
                Math.round(26_600 / 60.0 + 2160 / 0.7), aware.get(ScalingMetric.SCALE_UP_RATE_THRESHOLD).getCurrent(), 1.0);

        // A job that keeps up with the jump has no growing backlog: the window's average stays.
        var keepsUp = jump(true, 300);
        assertEquals(840, keepsUp.get(ScalingMetric.TARGET_DATA_RATE).getAverage(), 1e-6);
    }

    @Test
    void aJumpTwoSecondsAgoIsSizedAtTheRateOfTheLastSecond() {
        // 13 s at 400 rec/s and 2 s at 4800 rec/s: 14,800 records in the newest interval, 8,800 of them unread.
        double[] demand = new double[3];
        double[] instant = {4800, Double.NaN, 500};
        for (int k = 0; k < 3; k++) {
            var job = new JevRuntime.JobState();
            for (int i = 0; i <= 4; i++) {
                BrokerLag.overrides(job, believed(), conf(), 200, 1_000_000 + i * 6000, NOW.plusSeconds(i * 15), true, 0.2, 400);
            }
            var out = BrokerLag.overrides(job, believed(), conf(), 9000, 1_024_000 + 14_800, NOW.plusSeconds(75), true, 0.2, instant[k]);
            demand[k] = out.get(ScalingMetric.TARGET_DATA_RATE).getAverage();
        }
        // The newest interval holds mostly old load (987 rec/s); the two readings of this cycle show the load now.
        assertEquals(4800, demand[0], 1e-6);
        // Without a second reading, or with one below the interval's rate, the interval's rate is the demand.
        assertEquals(14_800 / 15.0, demand[1], 1e-3);
        assertEquals(14_800 / 15.0, demand[2], 1e-3);
    }

    @Test
    void theStepAwareDemandIsNeverBelowTheWindowsAverage() {
        var job = new JevRuntime.JobState();
        // 4800 rec/s for a minute, then the load falls to 400 rec/s while a backlog is still there.
        for (int i = 0; i <= 4; i++) {
            BrokerLag.overrides(job, believed(), conf(), 100_000 + i * 20_000, 1_000_000 + i * 72_000, NOW.plusSeconds(i * 15), true, 0.2, 4800);
        }
        var out = BrokerLag.overrides(job, believed(), conf(), 150_000, 1_288_000 + 6000, NOW.plusSeconds(75), true, 0.2, 400);
        // 222,000 records in the last 60 s: the average, not the 400 rec/s of the newest interval.
        assertEquals(3700, out.get(ScalingMetric.TARGET_DATA_RATE).getAverage(), 1e-6);
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
