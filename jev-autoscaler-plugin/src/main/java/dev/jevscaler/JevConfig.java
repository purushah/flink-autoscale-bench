package dev.jevscaler;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * Settings for the Jev autoscaler. All keys live under {@code job.autoscaler.jev.} in the job's
 * Flink configuration, so they can differ per FlinkDeployment.
 */
public final class JevConfig {

    public static final String PREFIX = "job.autoscaler.jev.";

    public String apiUrl = "https://api.typesafe.ai/v1/systemone";
    public String model = "jev-1.14.0";

    /** Name of the environment variable (in the operator pod) that holds the API key. */
    public String apiKeyEnv = "TYPESAFE_API_KEY";

    /**
     * Which model makes the decisions: "jev" (TypeSafe's decision model, picks among options the
     * engine describes), "anthropic" (a Claude model, sizes the vertices itself) or "rule" (no model:
     * the policy that the decision model is asked to follow, applied in code; used as a control).
     */
    public String provider = "jev";

    /** Settings used when {@link #provider} is "anthropic". */
    public String llmModel = "claude-haiku-5-5";

    public String llmApiKeyEnv = "ANTHROPIC_API_KEY";
    public Duration llmRequestTimeout = Duration.ofSeconds(8);
    public int llmMaxRetries = 1;
    public String llmEffort = "medium";
    public long llmMaxTokens = 8000;

    public boolean isLlm() {
        return "anthropic".equalsIgnoreCase(provider);
    }

    public boolean isRule() {
        return "rule".equalsIgnoreCase(provider);
    }

    /** "observe": only log what the built-in autoscaler sees each cycle; never change a decision. */
    public boolean isObserve() {
        return "observe".equalsIgnoreCase(provider);
    }

    public Duration requestTimeout = Duration.ofSeconds(4);
    public int maxRetries = 2;

    /** Hard per-vertex limits. */
    public int minParallelism = 1;

    public int maxParallelism = 8;

    /** A single decision may multiply a vertex's parallelism by at most this factor. */
    public double maxScaleUpFactor = 4.0;

    /** A single decision may remove at most this fraction of a vertex's parallelism. */
    public double maxScaleDownFactor = 0.5;

    /** Upper limit for the sum of all vertex parallelisms; 0 disables the check. */
    public int maxTotalParallelism = 0;

    /** No Jev-driven scaling at all for this long after any scaling action. */
    public Duration scaleUpCooldown = Duration.ofSeconds(45);

    /** No Jev-driven scale-down for this long after any scaling action. */
    public Duration scaleDownCooldown = Duration.ofSeconds(150);

    /** Probability mass Jev must put on "more than current" before a scale-up is applied. */
    public double minUpProbability = 0.5;

    /** Probability mass Jev must put on "less than current" before a scale-down is applied. */
    public double minDownProbability = 0.7;

    /**
     * A change is only applied when the model's most likely option itself lies in that direction.
     * Without this, probability spread over several higher options can add up to a scale-up although
     * the most likely single option is "keep the current setting".
     */
    public boolean requireTopChoice = true;

    /** Utilization the decision aims for, shown to Jev in the option descriptions. */
    public double targetUtilization = 0.7;

    /** A scale-down is never allowed to push projected utilization above this. */
    public double maxUtilizationAfterScaleDown = 0.85;

    /**
     * Time in which a backlog should be worked off, and the expected duration of a restart. Both are
     * the built-in autoscaler's settings (catch-up.duration, restart.time), so that the engine sizes
     * a vertex with the same formula and the same parameters as the built-in rule.
     */
    public int catchUpSeconds = 60;

    public int restartSeconds = 30;

    /**
     * How far back the demand and backlog trends look (the autoscaler's metric window), and how long
     * after a change the operator allows no further decision (stabilization interval plus one metric
     * window). Both follow the job's autoscaler settings.
     */
    public int trendWindowSeconds = 60;

    public int decisionGapSeconds = 90;

    /**
     * Where a Kafka source's backlog and demand come from: "metric" (the source's pendingRecords, as
     * the built-in autoscaler does) or "broker" (end offsets from the broker minus the reader's
     * position, see {@link BrokerLag}). The broker address is an operator-level setting.
     */
    public String lagSource = "metric";

    public boolean lagFromBroker() {
        return "broker".equalsIgnoreCase(lagSource);
    }

    /** Vertex ids that must not be scaled (the built-in autoscaler's vertex.exclude.ids). */
    public Set<String> excludedVertices = new HashSet<>();

    /**
     * When demand is rising, options are also judged at the demand expected this many seconds
     * ahead, because the next chance to scale is at least one metric window away. 0 disables it.
     */
    public int trendHorizonSeconds = 120;

    /** After this many consecutive failures, stop calling Jev for {@link #circuitOpen}. */
    public int circuitFailures = 3;

    public Duration circuitOpen = Duration.ofSeconds(120);

    private JevConfig() {}

    public static JevConfig defaults() {
        return new JevConfig();
    }

    /** Builds the settings from a key lookup that returns {@code null} for unset keys. */
    public static JevConfig from(Function<String, String> lookup) {
        var c = new JevConfig();
        c.apiUrl = str(lookup, "api-url", c.apiUrl);
        c.model = str(lookup, "model", c.model);
        c.apiKeyEnv = str(lookup, "api-key-env", c.apiKeyEnv);
        c.provider = str(lookup, "provider", c.provider);
        c.llmModel = str(lookup, "llm.model", c.llmModel);
        c.llmApiKeyEnv = str(lookup, "llm.api-key-env", c.llmApiKeyEnv);
        c.llmRequestTimeout = dur(lookup, "llm.request-timeout", c.llmRequestTimeout);
        c.llmMaxRetries = (int) num(lookup, "llm.max-retries", c.llmMaxRetries);
        c.llmEffort = str(lookup, "llm.effort", c.llmEffort).toLowerCase();
        c.llmMaxTokens = (long) num(lookup, "llm.max-tokens", c.llmMaxTokens);
        c.requestTimeout = dur(lookup, "request-timeout", c.requestTimeout);
        c.maxRetries = (int) num(lookup, "max-retries", c.maxRetries);
        c.minParallelism = Math.max(1, (int) num(lookup, "min-parallelism", c.minParallelism));
        c.maxParallelism =
                Math.max(c.minParallelism, (int) num(lookup, "max-parallelism", c.maxParallelism));
        c.maxScaleUpFactor = Math.max(1.0, num(lookup, "max-scale-up-factor", c.maxScaleUpFactor));
        c.maxScaleDownFactor =
                clamp01(num(lookup, "max-scale-down-factor", c.maxScaleDownFactor));
        c.maxTotalParallelism = (int) num(lookup, "max-total-parallelism", c.maxTotalParallelism);
        c.scaleUpCooldown = dur(lookup, "cooldown.scale-up", c.scaleUpCooldown);
        c.scaleDownCooldown = dur(lookup, "cooldown.scale-down", c.scaleDownCooldown);
        c.minUpProbability = clamp01(num(lookup, "min-up-probability", c.minUpProbability));
        c.minDownProbability = clamp01(num(lookup, "min-down-probability", c.minDownProbability));
        c.requireTopChoice =
                !"false".equalsIgnoreCase(str(lookup, "require-top-choice", String.valueOf(c.requireTopChoice)));
        c.targetUtilization = clamp01(num(lookup, "target-utilization", c.targetUtilization));
        c.maxUtilizationAfterScaleDown =
                clamp01(
                        num(
                                lookup,
                                "max-utilization-after-scale-down",
                                c.maxUtilizationAfterScaleDown));
        c.lagSource = str(lookup, "lag.source", c.lagSource);
        c.catchUpSeconds = (int) num(lookup, "catch-up-seconds", c.catchUpSeconds);
        c.restartSeconds = (int) num(lookup, "restart-seconds", c.restartSeconds);
        c.trendWindowSeconds = Math.max(30, (int) num(lookup, "trend-window-seconds", c.trendWindowSeconds));
        c.decisionGapSeconds = (int) num(lookup, "decision-gap-seconds", c.decisionGapSeconds);
        for (var id : str(lookup, "excluded-vertices", "").split("[,;\\s]+")) {
            if (!id.isBlank()) {
                c.excludedVertices.add(id.trim());
            }
        }
        c.trendHorizonSeconds = (int) num(lookup, "trend-horizon-seconds", c.trendHorizonSeconds);
        c.circuitFailures = (int) num(lookup, "circuit-breaker.failures", c.circuitFailures);
        c.circuitOpen = dur(lookup, "circuit-breaker.open", c.circuitOpen);
        return c;
    }

    private static String str(Function<String, String> lookup, String key, String def) {
        var v = lookup.apply(PREFIX + key);
        return v == null || v.isBlank() ? def : v.trim();
    }

    private static double num(Function<String, String> lookup, String key, double def) {
        var v = lookup.apply(PREFIX + key);
        if (v == null || v.isBlank()) {
            return def;
        }
        try {
            return Double.parseDouble(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }

    /** Accepts plain seconds ("90"), or a number with an s/m/ms suffix ("90s", "2m", "500ms"). */
    static Duration dur(Function<String, String> lookup, String key, Duration def) {
        var v = lookup.apply(PREFIX + key);
        if (v == null || v.isBlank()) {
            return def;
        }
        var s = v.trim().toLowerCase().replace(" ", "");
        try {
            if (s.endsWith("ms")) {
                return Duration.ofMillis(Long.parseLong(s.substring(0, s.length() - 2)));
            }
            if (s.endsWith("min")) {
                return Duration.ofMinutes(Long.parseLong(s.substring(0, s.length() - 3)));
            }
            if (s.endsWith("m")) {
                return Duration.ofMinutes(Long.parseLong(s.substring(0, s.length() - 1)));
            }
            if (s.endsWith("s")) {
                return Duration.ofSeconds(Long.parseLong(s.substring(0, s.length() - 1)));
            }
            return Duration.ofSeconds(Long.parseLong(s));
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static double clamp01(double v) {
        return Math.max(0.0, Math.min(1.0, v));
    }
}
