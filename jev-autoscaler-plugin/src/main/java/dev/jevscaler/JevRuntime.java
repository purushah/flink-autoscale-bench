package dev.jevscaler;

import dev.jevscaler.Model.DecisionModel;
import dev.jevscaler.Model.Plan;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.autoscaler.metrics.EvaluatedScalingMetric;
import org.apache.flink.autoscaler.metrics.ScalingMetric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * State shared between the evaluator plugin (which asks Jev) and the executor plugin (which applies
 * the answer). Both are loaded from the same plugin jar, so static state is visible to both.
 */
public final class JevRuntime {

    private static final Logger LOG = LoggerFactory.getLogger(JevRuntime.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Marker that the benchmark tooling greps for in the operator log. */
    public static final String LOG_MARKER = "JEV_DECISION ";

    /** One demand/backlog observation, used to compute trends. */
    static final class Sample {
        final Instant time;
        final Map<String, double[]> perVertex = new HashMap<>();

        Sample(Instant time) {
            this.time = time;
        }
    }

    /** Everything remembered about one job between autoscaler cycles. */
    static final class JobState {
        volatile Plan plan;

        /** The last vertex's metrics before the evaluator overrode them to raise a trigger. */
        volatile Map<ScalingMetric, EvaluatedScalingMetric> lastVertexRealMetrics;

        volatile String lastVertexId;

        /** The cycle in which the evaluator last raised a trigger; its built-in decision is not real. */
        volatile Instant triggeredCycle;

        /** State of {@link BrokerLag}: the job's REST address, the names of the source's per-partition
         * offset gauges, and recent (time, sum of end offsets) samples for the arrival rate. */
        volatile String restUrl;

        final LinkedList<double[]> endOffsets = new LinkedList<>();
        final DecisionEngine.Breaker breaker = new DecisionEngine.Breaker();
        final Deque<Sample> samples = new ArrayDeque<>();
        final Map<String, String> vertexNames = new ConcurrentHashMap<>();
    }

    private static final Map<String, JobState> JOBS = new ConcurrentHashMap<>();
    private static final Map<String, DecisionModel> CLIENTS = new ConcurrentHashMap<>();

    /** Replaced in tests to avoid real HTTP calls. */
    static volatile Function<JevConfig, DecisionModel> modelFactory = JevRuntime::defaultModel;

    private JevRuntime() {}

    static JobState job(Object jobKey) {
        return JOBS.computeIfAbsent(String.valueOf(jobKey), k -> new JobState());
    }

    static JobState jobIfPresent(Object jobKey) {
        return JOBS.get(String.valueOf(jobKey));
    }

    static DecisionModel model(JevConfig cfg) {
        DecisionModel inner = modelFactory.apply(cfg);
        String faultFile = System.getenv("ARBITER_FAULT_FILE");
        if (faultFile == null || faultFile.isBlank() || cfg.isRule() || cfg.isObserve()) {
            return inner;
        }
        // Test hook, only active when the operator sets ARBITER_FAULT_FILE: while that file holds
        // "error" every model call fails at once; while it holds "hang" every call blocks for as long
        // as all its attempts would take to time out and then fails. This simulates an outage of the
        // model provider for all jobs without touching the network.
        return request -> {
            String fault = "";
            try {
                fault = java.nio.file.Files.readString(java.nio.file.Path.of(faultFile)).trim();
            } catch (java.io.IOException e) {
                // no file: no fault
            }
            if (fault.equals("error")) {
                throw new Model.ModelException("injected fault: HTTP 503", 503, null);
            }
            if (fault.equals("hang")) {
                var perAttempt = cfg.isLlm() ? cfg.llmRequestTimeout : cfg.requestTimeout;
                int attempts = 1 + (cfg.isLlm() ? cfg.llmMaxRetries : cfg.maxRetries);
                try {
                    Thread.sleep(perAttempt.toMillis() * attempts);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                throw new Model.ModelException("injected fault: timeout", 0, null);
            }
            return inner.decide(request);
        };
    }

    private static DecisionModel defaultModel(JevConfig cfg) {
        if (cfg.isRule() || cfg.isObserve()) {
            // The engine applies the policy in code and never calls a model for this provider.
            return request -> {
                throw new Model.ModelException("the rule provider does not call a model", 0, null);
            };
        }
        boolean llm = cfg.isLlm();
        var keyEnv = llm ? cfg.llmApiKeyEnv : cfg.apiKeyEnv;
        var key =
                llm
                        ? "anthropic|" + cfg.llmModel + "|" + keyEnv + "|" + cfg.llmRequestTimeout + "|"
                                + cfg.llmMaxRetries + "|" + cfg.llmEffort + "|" + cfg.llmMaxTokens
                        : cfg.apiUrl + "|" + cfg.model + "|" + keyEnv + "|" + cfg.requestTimeout + "|"
                                + cfg.maxRetries;
        return CLIENTS.computeIfAbsent(
                key,
                k -> {
                    var apiKey = System.getenv(keyEnv);
                    if (apiKey == null || apiKey.isBlank()) {
                        LOG.warn(
                                "Environment variable {} is not set; model decisions are disabled and the built-in autoscaler will be used.",
                                keyEnv);
                    }
                    return llm
                            ? new LlmClient(
                                    apiKey,
                                    cfg.llmModel,
                                    cfg.llmRequestTimeout,
                                    cfg.llmMaxRetries,
                                    cfg.llmEffort,
                                    cfg.llmMaxTokens)
                            : new JevClient(cfg.apiUrl, cfg.model, apiKey, cfg.requestTimeout, cfg.maxRetries);
                });
    }

    /** Writes one machine-readable line describing a decision or how it was applied. */
    static void log(String event, Object jobKey, Plan plan, Map<String, Object> extra) {
        try {
            var m = new LinkedHashMap<String, Object>();
            m.put("event", event);
            m.put("ts", Instant.now().toString());
            m.put("job", String.valueOf(jobKey));
            if (plan != null) {
                m.put("status", String.valueOf(plan.status));
                m.put("reason", plan.reason);
                if (plan.response != null) {
                    m.put("model", plan.response.model);
                    m.put("latency_ms", plan.response.latencyMs);
                    m.put("attempts", plan.response.attempts);
                    m.put("input_tokens", plan.response.inputTokens);
                    m.put("output_tokens", plan.response.outputTokens);
                }
                var vs = new LinkedHashMap<String, Object>();
                plan.decisions.forEach(
                        (id, d) -> {
                            var v = new LinkedHashMap<String, Object>();
                            v.put("name", d.name);
                            v.put("current", d.current);
                            v.put("target", d.target);
                            v.put("reason", d.reason);
                            if (d.offered != null) {
                                v.put("offered", d.offered);
                            }
                            if (d.ruleChoice > 0) {
                                v.put("rule_choice", d.ruleChoice);
                            }
                            if (!Double.isNaN(d.demand)) {
                                v.put("rule_trigger", d.ruleTrigger);
                                v.put("rule_size", d.ruleSize);
                                v.put("demand", round(d.demand));
                                v.put("projected_demand", round(d.projectedDemand));
                                v.put("capacity_per_subtask", round(d.capacityPerSubtask));
                                v.put("busy", round(d.busy));
                                v.put("backlog", round(d.backlog));
                                v.put("backlog_growth", round(d.backlogGrowth));
                            }
                            if (d.modelChoice != null) {
                                v.put("choice", d.modelChoice);
                                v.put("confidence", round(d.confidence));
                                v.put("p_up", round(d.upProbability));
                                v.put("p_down", round(d.downProbability));
                            }
                            vs.put(id, v);
                        });
                m.put("vertices", vs);
            }
            if (extra != null) {
                m.putAll(extra);
            }
            LOG.info("{}{}", LOG_MARKER, MAPPER.writeValueAsString(m));
        } catch (Exception e) {
            LOG.warn("Could not write Jev decision log line", e);
        }
    }

    private static Object round(double v) {
        return Double.isNaN(v) || Double.isInfinite(v) ? null : Math.round(v * 1000) / 1000.0;
    }
}
