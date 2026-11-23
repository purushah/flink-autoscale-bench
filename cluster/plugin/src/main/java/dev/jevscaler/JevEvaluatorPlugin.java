package dev.jevscaler;

import dev.jevscaler.JevRuntime.JobState;
import dev.jevscaler.JevRuntime.Sample;
import dev.jevscaler.Model.Plan;
import dev.jevscaler.Model.PlanStatus;
import dev.jevscaler.Model.ScalingEvent;
import dev.jevscaler.Model.VertexSnapshot;

import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetricsEvaluatorPlugin;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.ShipStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Asks Jev for per-vertex parallelism once per autoscaler cycle.
 *
 * <p>The operator calls this plugin for every vertex in topological order. All work happens on the
 * last vertex, when the built-in metrics of every vertex are known. If Jev wants a change, the
 * plugin overrides the last vertex's metrics so that the built-in logic raises a scaling action;
 * {@link JevExecutorPlugin} then replaces that action with Jev's actual per-vertex decision. Without
 * this step the executor plugin would only run when the built-in autoscaler itself wants to scale.
 *
 * <p>On any error the plugin returns no overrides, which leaves the built-in autoscaler in charge.
 */
public class JevEvaluatorPlugin implements ScalingMetricsEvaluatorPlugin {

    private static final Logger LOG = LoggerFactory.getLogger(JevEvaluatorPlugin.class);

    @Override
    public Map<ScalingMetric, EvaluatedScalingMetric> evaluateVertexMetrics(
            JobVertexID vertex,
            Map<ScalingMetric, EvaluatedScalingMetric> evaluatedMetrics,
            Context<?> ctx) {
        try {
            return evaluate(vertex, evaluatedMetrics, ctx);
        } catch (Throwable t) {
            LOG.warn("Jev evaluator failed; the built-in autoscaler decides this cycle.", t);
            try {
                var plan = new Plan();
                plan.status = PlanStatus.FALLBACK;
                plan.reason = "evaluator_error: " + t;
                plan.cycleTime = ctx.getScalingCycleState().getNow();
                JevRuntime.job(ctx.getJobKey()).plan = plan;
                JevRuntime.log("decision", ctx.getJobKey(), plan, null);
            } catch (Throwable ignored) {
                // Nothing more can be done; the executor passes built-in decisions through.
            }
            return Map.of();
        }
    }

    private Map<ScalingMetric, EvaluatedScalingMetric> evaluate(
            JobVertexID vertex,
            Map<ScalingMetric, EvaluatedScalingMetric> evaluatedMetrics,
            Context<?> ctx) {
        var cycle = ctx.getScalingCycleState();
        var collected = cycle.getCollectedMetrics();
        if (collected == null) {
            return Map.of();
        }
        JobTopology topology = collected.getJobTopology();
        List<JobVertexID> order = topology.getVerticesInTopologicalOrder();
        Configuration conf = ctx.getConfiguration();
        JevConfig cfg = config(conf);
        JobState job = JevRuntime.job(ctx.getJobKey());
        Instant now = cycle.getNow() != null ? cycle.getNow() : Instant.now();

        // A source's backlog and demand can be replaced by what the broker reports. The operator
        // merges these overrides before it evaluates the vertices downstream.
        Map<ScalingMetric, EvaluatedScalingMetric> lagFix = Map.of();
        if (cfg.lagFromBroker() && topology.isSource(vertex)) {
            String brokers =
                    cfg.lagBootstrapServers.isEmpty() ? System.getenv("ARBITER_KAFKA_BOOTSTRAP") : cfg.lagBootstrapServers;
            lagFix =
                    BrokerLag.overrides(
                            job, ctx, vertex, evaluatedMetrics, conf, brokers, cfg.lagAuth, now, cfg.lagStepAware, cfg.lagStepMargin,
                            cfg.lagStepConfirmSeconds);
            if (!lagFix.isEmpty()) {
                evaluatedMetrics = new HashMap<>(evaluatedMetrics);
                evaluatedMetrics.putAll(lagFix);
            }
        }
        if (order.isEmpty() || !vertex.equals(order.get(order.size() - 1))) {
            return lagFix;
        }

        job.plan = null;
        job.lastVertexRealMetrics = null;
        job.triggeredCycle = null;
        if (!collected.isFullyCollected()) {
            // The metric window is not full yet (start-up or just rescaled); nobody may scale.
            return lagFix;
        }

        var all = new HashMap<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>>(ctx.getEvaluatedVertexMetrics());
        all.put(vertex, evaluatedMetrics);
        resolveNames(job, ctx, order);

        List<VertexSnapshot> snapshots = snapshots(order, topology, all, job);
        applyTrends(job, snapshots, now, cfg.trendWindowSeconds, historySeconds(cfg));

        var history = new ArrayList<ScalingEvent>();
        Instant lastScaling = null;
        if (cycle.getScalingHistory() != null) {
            for (var e : cycle.getScalingHistory().entrySet()) {
                for (var s : e.getValue().entrySet()) {
                    history.add(
                            new ScalingEvent(
                                    s.getKey(),
                                    e.getKey().toHexString(),
                                    s.getValue().getCurrentParallelism(),
                                    s.getValue().getNewParallelism()));
                    if (lastScaling == null || s.getKey().isAfter(lastScaling)) {
                        lastScaling = s.getKey();
                    }
                }
            }
        }

        // What the job's own past adds to this cycle's measurements, for the settings that ask for it.
        var extra = new Model.Extra();
        synchronized (job.samples) {
            if (cfg.scaleDownWindowSeconds > 0) {
                History.applyPeaks(
                        job.samples, snapshots, now, cfg.scaleDownWindowSeconds, Math.max(90, cfg.trendWindowSeconds * 3 / 2));
            }
            if (cfg.isLlm() && cfg.llmHistory) {
                extra.demandHistory = History.demandHistory(job.samples, snapshots, now);
                History.applyOutcomes(job.samples, snapshots, history);
            }
        }

        var engine = new DecisionEngine(cfg, JevRuntime.model(cfg));
        Plan plan = engine.decide(snapshots, history, lastScaling, now, job.breaker, extra);
        plan.cycleTime = cycle.getNow();
        job.plan = plan;
        job.lastVertexId = vertex.toHexString();
        job.lastVertexRealMetrics = new HashMap<>(evaluatedMetrics);
        JevRuntime.log("decision", ctx.getJobKey(), plan, null);

        if (plan.hasChanges()) {
            // The trigger only makes sense if the executor plugin will replace the decision it causes.
            var executors = conf.toMap().get("job.autoscaler.scaling.custom-executors");
            if (executors == null || executors.isBlank()) {
                LOG.warn("The plan changes a vertex but no custom scaling executor is configured for this job; not triggering.");
                return lagFix;
            }
            job.triggeredCycle = cycle.getNow();
            return trigger(evaluatedMetrics, conf);
        }
        return lagFix;
    }

    /**
     * Overrides that make the built-in logic compute a parallelism change for this vertex, so the
     * scaling executor (and with it {@link JevExecutorPlugin}) runs in this cycle. The values are
     * never applied: the executor plugin rebuilds every summary from Jev's plan and the real metrics.
     */
    static Map<ScalingMetric, EvaluatedScalingMetric> trigger(
            Map<ScalingMetric, EvaluatedScalingMetric> real, Configuration conf) {
        int current = (int) real.get(ScalingMetric.PARALLELISM).getCurrent();
        int upper = Math.max(current, conf.get(AutoScalerOptions.VERTEX_MAX_PARALLELISM));
        var maxPar = real.get(ScalingMetric.MAX_PARALLELISM);
        if (maxPar != null && !Double.isNaN(maxPar.getCurrent())) {
            upper = Math.min(upper, (int) maxPar.getCurrent());
        }
        double tpr = real.get(ScalingMetric.TRUE_PROCESSING_RATE).getAverage();
        if (Double.isNaN(tpr) || Double.isInfinite(tpr) || tpr <= 0) {
            tpr = 1000;
        }
        var out = new HashMap<ScalingMetric, EvaluatedScalingMetric>();
        out.put(ScalingMetric.TRUE_PROCESSING_RATE, EvaluatedScalingMetric.avg(tpr));
        out.put(ScalingMetric.CATCH_UP_DATA_RATE, EvaluatedScalingMetric.of(0));
        if (current < upper) {
            // Looks overloaded: the built-in logic proposes a scale-up.
            out.put(ScalingMetric.TARGET_DATA_RATE, new EvaluatedScalingMetric(tpr * 3, tpr * 3));
            out.put(ScalingMetric.SCALE_UP_RATE_THRESHOLD, EvaluatedScalingMetric.of(Double.MAX_VALUE));
            out.put(ScalingMetric.SCALE_DOWN_RATE_THRESHOLD, EvaluatedScalingMetric.of(Double.MAX_VALUE));
        } else {
            // Already at the built-in upper limit, so the only possible trigger is a scale-down.
            // The built-in scale-down delay applies to this path.
            out.put(ScalingMetric.TARGET_DATA_RATE, new EvaluatedScalingMetric(tpr * 0.05, tpr * 0.05));
            out.put(ScalingMetric.SCALE_UP_RATE_THRESHOLD, EvaluatedScalingMetric.of(0));
            out.put(ScalingMetric.SCALE_DOWN_RATE_THRESHOLD, EvaluatedScalingMetric.of(0));
        }
        return out;
    }

    /** Jev settings, with limits falling back to the built-in autoscaler's own options. */
    static JevConfig config(Configuration conf) {
        Map<String, String> raw = conf.toMap();
        // The endpoint and the name of the key variable decide where the operator's secret is sent.
        // A job's own configuration may only set them when the operator explicitly allows it.
        boolean jobEndpoints = "true".equalsIgnoreCase(System.getenv("ARBITER_ALLOW_JOB_ENDPOINTS"));
        Function<String, String> lookup =
                key -> {
                    boolean endpoint =
                            key.equals(JevConfig.PREFIX + "api-url")
                                    || key.equals(JevConfig.PREFIX + "api-key-env")
                                    || key.equals(JevConfig.PREFIX + "llm.api-key-env");
                    var v = endpoint && !jobEndpoints ? null : raw.get(key);
                    if (v != null) {
                        return v;
                    }
                    if (key.equals(JevConfig.PREFIX + "catch-up-seconds")) {
                        return String.valueOf(conf.get(AutoScalerOptions.CATCH_UP_DURATION).toSeconds());
                    }
                    if (key.equals(JevConfig.PREFIX + "trend-window-seconds")) {
                        return String.valueOf(conf.get(AutoScalerOptions.METRICS_WINDOW).toSeconds());
                    }
                    if (key.equals(JevConfig.PREFIX + "decision-gap-seconds")) {
                        return String.valueOf(
                                conf.get(AutoScalerOptions.STABILIZATION_INTERVAL).toSeconds()
                                        + conf.get(AutoScalerOptions.METRICS_WINDOW).toSeconds());
                    }
                    if (key.equals(JevConfig.PREFIX + "restart-seconds")) {
                        return String.valueOf(conf.get(AutoScalerOptions.RESTART_TIME).toSeconds());
                    }
                    if (key.equals(JevConfig.PREFIX + "excluded-vertices")) {
                        return String.join(",", conf.get(AutoScalerOptions.VERTEX_EXCLUDE_IDS));
                    }
                    if (key.equals(JevConfig.PREFIX + "max-parallelism")) {
                        return String.valueOf(Math.min(conf.get(AutoScalerOptions.VERTEX_MAX_PARALLELISM), 1024));
                    }
                    if (key.equals(JevConfig.PREFIX + "min-parallelism")) {
                        return String.valueOf(conf.get(AutoScalerOptions.VERTEX_MIN_PARALLELISM));
                    }
                    if (key.equals(JevConfig.PREFIX + "target-utilization")) {
                        return String.valueOf(conf.get(AutoScalerOptions.UTILIZATION_TARGET));
                    }
                    return null;
                };
        return JevConfig.from(lookup);
    }

    private static List<VertexSnapshot> snapshots(
            List<JobVertexID> order,
            JobTopology topology,
            Map<JobVertexID, Map<ScalingMetric, EvaluatedScalingMetric>> all,
            JobState job) {
        var out = new ArrayList<VertexSnapshot>();
        double totalLag = 0;
        double sourceDemand = 0;
        for (var id : order) {
            var m = all.get(id);
            var info = topology.get(id);
            var v = new VertexSnapshot();
            v.id = id.toHexString();
            boolean source = topology.isSource(id);
            boolean sink = info.getOutputs() == null || info.getOutputs().isEmpty();
            v.role = source ? "source" : sink ? "sink" : "operator";
            v.name = job.vertexNames.getOrDefault(v.id, v.role + "-" + (out.size() + 1));
            info.getInputs().keySet().forEach(i -> v.inputs.add(i.toHexString()));
            v.parallelism = info.getParallelism();
            if (m != null) {
                v.parallelism = (int) current(m, ScalingMetric.PARALLELISM, v.parallelism);
                v.maxParallelism = (int) current(m, ScalingMetric.MAX_PARALLELISM, Integer.MAX_VALUE);
                v.numSourcePartitions = (int) current(m, ScalingMetric.NUM_SOURCE_PARTITIONS, 0);
                // Like the built-in autoscaler: sources divide their partitions evenly, vertices with a
                // keyed input their key groups.
                if (v.numSourcePartitions > 0) {
                    v.alignTo = v.numSourcePartitions;
                } else if (info.getInputs().containsValue(ShipStrategy.HASH) && v.maxParallelism < Integer.MAX_VALUE) {
                    v.alignTo = v.maxParallelism;
                }
                v.busy = average(m, ScalingMetric.LOAD);
                v.trueProcessingRate = average(m, ScalingMetric.TRUE_PROCESSING_RATE);
                v.targetRate = average(m, ScalingMetric.TARGET_DATA_RATE);
                v.catchUpRate = current(m, ScalingMetric.CATCH_UP_DATA_RATE, 0);
                if (source) {
                    v.backlog = Math.max(0, current(m, ScalingMetric.LAG, 0));
                    totalLag += v.backlog;
                    if (!Double.isNaN(v.targetRate)) {
                        sourceDemand += v.targetRate;
                    }
                }
            }
            out.add(v);
        }
        // Downstream vertices have no lag metric of their own: attribute the source backlog to them
        // in proportion to how many records they must process per source record.
        for (var v : out) {
            if (!"source".equals(v.role) && sourceDemand > 0 && !Double.isNaN(v.targetRate)) {
                v.backlog = totalLag * (v.targetRate / sourceDemand);
            }
        }
        return out;
    }

    /** Sets each vertex's demand trend (relative change per minute) and backlog growth (records/s),
     * measured against the newest sample that is between half a window and two windows old. */
    private static void applyTrends(
            JobState job, List<VertexSnapshot> snapshots, Instant now, int windowSeconds, int historySeconds) {
        Duration retention = Duration.ofSeconds(Math.max(Math.max(300, 5L * windowSeconds), historySeconds));
        synchronized (job.samples) {
            while (!job.samples.isEmpty()
                    && Duration.between(job.samples.peekFirst().time, now).compareTo(retention) > 0) {
                job.samples.pollFirst();
            }
            // Reference: the newest sample that is at least half a trend window old.
            Sample ref = null;
            for (var s : job.samples) {
                long age = Duration.between(s.time, now).toSeconds();
                if (age >= windowSeconds / 2 && age <= windowSeconds * 2L) {
                    ref = s;
                }
            }
            var sample = new Sample(now);
            for (var v : snapshots) {
                sample.perVertex.put(v.id, new double[] {v.targetRate, v.backlog});
                if (ref == null) {
                    continue;
                }
                var old = ref.perVertex.get(v.id);
                double seconds = Math.max(1, Duration.between(ref.time, now).toMillis() / 1000.0);
                if (old != null && !Double.isNaN(old[0]) && old[0] > 0 && !Double.isNaN(v.targetRate)) {
                    // Normalise to change per trend window.
                    v.demandTrend = (v.targetRate - old[0]) / old[0] * (60.0 / seconds);
                }
                if (old != null) {
                    v.backlogGrowth = (v.backlog - old[1]) / seconds;
                }
            }
            job.samples.addLast(sample);
        }
    }

    /** How far back the job's samples must reach for the settings that look at its past. */
    static int historySeconds(JevConfig cfg) {
        int ages = History.AGES[History.AGES.length - 1] + History.AGE_TOLERANCE_SECONDS;
        return Math.max(cfg.scaleDownWindowSeconds, cfg.isLlm() && cfg.llmHistory ? ages : 0);
    }

    /** Looks up human-readable vertex names once per job; falls back to role-based names. */
    private static void resolveNames(JobState job, Context<?> ctx, List<JobVertexID> order) {
        boolean missing = order.stream().anyMatch(id -> !job.vertexNames.containsKey(id.toHexString()));
        if (!missing || ctx.getJobID() == null) {
            return;
        }
        try (var client = ctx.getRestClusterClient()) {
            var details = client.getJobDetails(ctx.getJobID()).get(5, TimeUnit.SECONDS);
            details.getJobVertexInfos()
                    .forEach(
                            info ->
                                    job.vertexNames.put(
                                            info.getJobVertexID().toHexString(), shortName(info.getName())));
        } catch (Exception e) {
            LOG.debug("Could not resolve vertex names; using role-based names.", e);
        }
    }

    static String shortName(String name) {
        if (name == null) {
            return "vertex";
        }
        var s = name.replaceAll("\\s+", " ").trim();
        return s.length() > 60 ? s.substring(0, 60) : s;
    }

    private static double current(Map<ScalingMetric, EvaluatedScalingMetric> m, ScalingMetric metric, double def) {
        var e = m.get(metric);
        return e == null || Double.isNaN(e.getCurrent()) ? def : e.getCurrent();
    }

    private static double average(Map<ScalingMetric, EvaluatedScalingMetric> m, ScalingMetric metric) {
        var e = m.get(metric);
        return e == null ? Double.NaN : e.getAverage();
    }
}
