package dev.jevscaler;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
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
    /**
     * Per attempt. 8 s were enough for the plain prompt (answers of about 400 tokens in 2 to 4 s) and
     * not for a prompt with the job's history or a note, whose answers took up to 12.9 s (ISSUES.md #106).
     */
    public Duration llmRequestTimeout = Duration.ofSeconds(20);
    public int llmMaxRetries = 1;
    public String llmEffort = "medium";
    public long llmMaxTokens = 8000;

    /**
     * Free text from the job's owner about load that is planned or expected, for example "the push
     * notification goes out at 20:15 UTC, last time the rate went up fivefold for two minutes". A
     * generative model gets it with every request, together with the current time. Key
     * {@code llm.notes}, or {@code llm.notes-base64} when the text has characters that a job's
     * configuration does not carry well.
     */
    public String llmNotes = "";

    /**
     * Whether a generative model also gets the source's rate over the last half hour and what
     * earlier changes of the job cost (key {@code llm.history}).
     */
    public boolean llmHistory = false;

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

    /**
     * The brokers to ask for end offsets when {@link #lagSource} is "broker" (key
     * {@code lag.bootstrap-servers}). Empty: the operator's environment variable
     * ARBITER_KAFKA_BOOTSTRAP, as before.
     */
    public String lagBootstrapServers = "";

    /** How to log in to those brokers (key {@code lag.auth}): "" for none, "msk-iam" for Amazon MSK with IAM. */
    public String lagAuth = "";

    /**
     * With the broker-side signal: when the arrival rate of the newest interval is above the
     * window's average by more than {@link #lagStepMargin} and the backlog grows by more than that
     * share of the average, the newest interval's rate is the source's demand (key
     * {@code lag.step-aware}). A true backlog makes the rule act within seconds of a jump in the
     * load, while its demand is still an average over a window of mostly old load; the step it
     * then takes is small, and after it the rule cannot decide again for a stabilization interval
     * and a window (ISSUES.md #123).
     */
    public boolean lagStepAware = false;

    /** The share by which the newest interval must exceed the window (key {@code lag.step-margin}). */
    public double lagStepMargin = 0.2;

    /**
     * At the first step of a rise the rate between two readings of the end offsets half a second
     * apart was up to 2.6 times the load on a cluster (ISSUES.md #141). When that rate would be the
     * demand, the end offsets are read once more this many seconds after the first reading, and the
     * rate over that longer span is used (key {@code lag.step-confirm-seconds}; 0 keeps the short
     * reading). The wait is on the reconcile thread, once per rise.
     */
    public double lagStepConfirmSeconds = 2.0;

    public boolean lagFromBroker() {
        return "broker".equalsIgnoreCase(lagSource);
    }

    /**
     * Load that is known in advance, for the policy in code: entries
     * {@code startEpochSeconds,endEpochSeconds,recordsPerSecond} separated by ";" (key
     * {@code schedule}). From {@link #scheduleLeadSeconds} before an entry starts until it ends, the
     * source is taken to need at least that rate, and every other vertex its share of it. This is
     * what a calendar integration would give the policy; a generative model gets the same
     * information as free text in {@link #llmNotes}.
     */
    public List<double[]> schedule = new ArrayList<>();

    /** How long before a planned load starts the policy sizes for it; 0: restart plus decision gap. */
    public int scheduleLeadSeconds = 0;

    /**
     * When positive, the policy in code sizes a scale-down for the highest demand of the last this
     * many seconds instead of the current demand (key {@code scale-down.window-seconds}), as the
     * built-in autoscaler's delayed scale-down does with its recommendations.
     */
    public int scaleDownWindowSeconds = 0;

    /**
     * With a scale-down window: while the demand is not above the highest demand that the window
     * saw before the current rise, plus {@link #windowCapMargin}, the trend projection does not go
     * above that level, or above the demand itself when that is the higher of the two (key {@code
     * scale-down.window-caps-trend}). A load that returns to a level the job has just carried is
     * expected to reach that level, not to pass it. Without it the first decision after a jump can
     * double the demand and undo what the window kept (ISSUES.md #107). The level is taken from
     * before the rise because the reading at the jump is itself the window's highest by a few
     * percent (ISSUES.md #119).
     */
    public boolean windowCapsTrend = false;

    /**
     * Readings of one load level differ by a few percent: a demand up to this share above the
     * earlier level still counts as that level (key {@code scale-down.window-cap-margin}). It is
     * not added to the capped projection.
     */
    public double windowCapMargin = 0.1;

    /**
     * With a scale-down window: a scale-down keeps what the highest demand of the window plus
     * {@link #windowCapMargin} needs, not what that demand alone needs (key {@code
     * scale-down.window-keeps-margin}). A demand up to the margin above the level still counts as
     * the level, and one reading of a subtask's capacity moves by a few percent. A size that just
     * fits the level at one reading is too small at the next, and the scale-down is undone when
     * the load returns (ISSUES.md #119, second cause).
     */
    public boolean windowKeepsMargin = false;

    /**
     * When positive, a plan that only scales down is dropped unless it frees at least this many
     * slots (key {@code scale-down.min-slots}). Every change restarts the whole job, and with slot
     * sharing a job holds as many slots as its largest vertex has subtasks, so a smaller sink next
     * to a large vertex frees nothing. The change is made when another change is due.
     */
    public int scaleDownMinSlots = 0;

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
        c.llmNotes = str(lookup, "llm.notes", c.llmNotes);
        var encoded = str(lookup, "llm.notes-base64", "");
        if (!encoded.isEmpty()) {
            try {
                c.llmNotes = new String(Base64.getUrlDecoder().decode(encoded), StandardCharsets.UTF_8).trim();
            } catch (IllegalArgumentException e) {
                // Not base64: keep the plain notes, if any.
            }
        }
        c.llmHistory = "true".equalsIgnoreCase(str(lookup, "llm.history", String.valueOf(c.llmHistory)));
        c.schedule = schedule(str(lookup, "schedule", ""));
        c.scheduleLeadSeconds = Math.max(0, (int) num(lookup, "schedule.lead-seconds", c.scheduleLeadSeconds));
        c.scaleDownWindowSeconds =
                Math.max(0, (int) num(lookup, "scale-down.window-seconds", c.scaleDownWindowSeconds));
        c.windowCapsTrend =
                "true".equalsIgnoreCase(str(lookup, "scale-down.window-caps-trend", String.valueOf(c.windowCapsTrend)));
        c.windowCapMargin = Math.max(0, num(lookup, "scale-down.window-cap-margin", c.windowCapMargin));
        c.windowKeepsMargin =
                "true"
                        .equalsIgnoreCase(
                                str(lookup, "scale-down.window-keeps-margin", String.valueOf(c.windowKeepsMargin)));
        c.scaleDownMinSlots = Math.max(0, (int) num(lookup, "scale-down.min-slots", c.scaleDownMinSlots));
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
        c.lagBootstrapServers = str(lookup, "lag.bootstrap-servers", c.lagBootstrapServers).trim();
        c.lagAuth = str(lookup, "lag.auth", c.lagAuth).trim();
        c.lagStepAware = "true".equalsIgnoreCase(str(lookup, "lag.step-aware", String.valueOf(c.lagStepAware)));
        c.lagStepMargin = Math.max(0, num(lookup, "lag.step-margin", c.lagStepMargin));
        c.lagStepConfirmSeconds =
                Math.max(0, Math.min(10, num(lookup, "lag.step-confirm-seconds", c.lagStepConfirmSeconds)));
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

    /** Parses {@link #schedule}; entries that are not three numbers with start before end are dropped. */
    static List<double[]> schedule(String text) {
        var out = new ArrayList<double[]>();
        for (var entry : text.split(";")) {
            var f = entry.trim().split(",");
            if (f.length != 3) {
                continue;
            }
            try {
                double start = Double.parseDouble(f[0].trim());
                double end = Double.parseDouble(f[1].trim());
                double rate = Double.parseDouble(f[2].trim());
                if (end > start && rate >= 0) {
                    out.add(new double[] {start, end, rate});
                }
            } catch (NumberFormatException e) {
                // dropped
            }
        }
        return out;
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
