package dev.jevscaler;

import dev.jevscaler.Model.ChoiceAnswer;
import dev.jevscaler.Model.DecisionModel;
import dev.jevscaler.Model.Extra;
import dev.jevscaler.Model.ModelException;
import dev.jevscaler.Model.ModelRequest;
import dev.jevscaler.Model.ModelResponse;
import dev.jevscaler.Model.Plan;
import dev.jevscaler.Model.PlanStatus;
import dev.jevscaler.Model.ScalingEvent;
import dev.jevscaler.Model.VertexDecision;
import dev.jevscaler.Model.VertexSnapshot;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns one cycle's vertex snapshots into a guarded per-vertex parallelism plan.
 *
 * <p>All sizing arithmetic is done here, with the formula of the built-in autoscaler and its
 * parameters (target utilization, restart time, catch-up time):
 *
 * <ul>
 *   <li>{@link #trigger}: the fewest subtasks that keep a vertex at or below the target and catch
 *       up on its backlog if nothing is restarted. Below it, the vertex must be scaled up.
 *   <li>{@link #size}: the size after a restart, which also absorbs what arrives meanwhile.
 * </ul>
 *
 * <p>The policy built on these two numbers ({@link #ruleChoice}) is what every mode of the engine
 * follows: applied in code ("rule"), read back by a decision model from a menu whose options carry
 * the policy's verdicts in words ("jev"), or stated in words to a generative model that sizes the
 * vertices itself ("anthropic"). The limits are enforced twice: by only offering allowed options,
 * and by re-checking the answer.
 */
public final class DecisionEngine {

    /** Mutable per-job failure tracking for the circuit breaker. */
    public static final class Breaker {
        public int consecutiveFailures;
        public Instant openUntil;
    }

    /** The allowed parallelism range of one vertex in this cycle. */
    static final class Range {
        final int lo;
        final int hi;

        Range(int lo, int hi) {
            this.lo = lo;
            this.hi = hi;
        }
    }

    private final JevConfig cfg;
    private final DecisionModel model;

    public DecisionEngine(JevConfig cfg, DecisionModel model) {
        this.cfg = cfg;
        this.model = model;
    }

    public Plan decide(
            List<VertexSnapshot> vertices,
            List<ScalingEvent> history,
            Instant lastScaling,
            Instant now,
            Breaker breaker) {
        return decide(vertices, history, lastScaling, now, breaker, new Extra());
    }

    public Plan decide(
            List<VertexSnapshot> vertices,
            List<ScalingEvent> history,
            Instant lastScaling,
            Instant now,
            Breaker breaker,
            Extra extra) {
        var plan = new Plan();
        plan.cycleTime = now;
        applySchedule(vertices, now);
        for (var v : vertices) {
            var d = new VertexDecision();
            d.vertexId = v.id;
            d.name = v.name;
            d.current = v.parallelism;
            d.target = v.parallelism;
            d.reason = "hold";
            if (v.hasUsableMetrics()) {
                // What the autoscaler believes this cycle; logged even when nothing may change.
                d.demand = v.targetRate;
                d.projectedDemand = projectedDemand(v);
                d.capacityPerSubtask = capacityPerSubtask(v);
                d.busy = v.busy;
                d.backlog = v.backlog;
                d.backlogGrowth = v.backlogGrowth;
                d.plannedRate = v.plannedRate;
                d.peakRate = v.peakRate;
                d.priorPeakRate = v.priorPeakRate;
                d.ruleTrigger = trigger(v);
                d.ruleSize = size(v);
            }
            plan.decisions.put(v.id, d);
        }

        if (cfg.isObserve()) {
            plan.status = PlanStatus.DECIDED;
            plan.reason = "observe";
            return plan;
        }

        if (breaker.openUntil != null && now.isBefore(breaker.openUntil)) {
            plan.status = PlanStatus.FALLBACK;
            plan.reason = "circuit_open";
            return plan;
        }

        Duration sinceLast = lastScaling == null ? null : Duration.between(lastScaling, now);
        if (sinceLast != null && sinceLast.compareTo(cfg.scaleUpCooldown) < 0) {
            plan.status = PlanStatus.COOLDOWN;
            plan.reason = "cooldown_" + sinceLast.toSeconds() + "s_of_" + cfg.scaleUpCooldown.toSeconds() + "s";
            return plan;
        }
        boolean allowDown = sinceLast == null || sinceLast.compareTo(cfg.scaleDownCooldown) >= 0;

        var offered = new LinkedHashMap<String, List<Integer>>();
        var request = new ModelRequest();
        for (var v : vertices) {
            var d = plan.decisions.get(v.id);
            if (cfg.excludedVertices.contains(v.id)) {
                d.reason = "excluded";
                continue;
            }
            if (!v.hasUsableMetrics()) {
                d.reason = "no_metrics";
                continue;
            }
            var options = options(v, allowedRange(v, allowDown));
            d.offered = "p" + options.get(0) + "..p" + options.get(options.size() - 1);
            d.ruleChoice = ruleChoice(v, options);
            if (options.size() == 1) {
                d.reason = allowDown ? "no_options" : "scale_down_cooldown";
                continue;
            }
            offered.put(v.id, options);
            request.questions.put(v.id, cfg.isLlm() ? llmQuestion(v, options) : question(v, options));
        }

        if (request.questions.isEmpty()) {
            plan.status = PlanStatus.DECIDED;
            plan.reason = "nothing_to_ask";
            return plan;
        }

        request.state = state(vertices, history, sinceLast, now, extra);
        if (cfg.isLlm()) {
            request.instructions = llmInstructions();
        }
        try {
            plan.response = cfg.isRule() ? ruleResponse(plan, offered) : model.decide(request);
            breaker.consecutiveFailures = 0;
            breaker.openUntil = null;
        } catch (ModelException | RuntimeException e) {
            breaker.consecutiveFailures++;
            if (breaker.consecutiveFailures >= cfg.circuitFailures) {
                breaker.openUntil = now.plus(cfg.circuitOpen);
            }
            plan.status = PlanStatus.FALLBACK;
            plan.reason = "model_error: " + e.getMessage();
            return plan;
        }

        for (var e : offered.entrySet()) {
            interpret(plan.decisions.get(e.getKey()), plan.response.answers.get(e.getKey()), e.getValue());
        }
        capTotal(plan);
        holdSmallScaleDown(plan);
        plan.status = PlanStatus.DECIDED;
        plan.reason = plan.hasChanges() ? "scale" : "hold";
        return plan;
    }

    /** Records per second one subtask of this vertex can process. */
    static double capacityPerSubtask(VertexSnapshot v) {
        if (Double.isInfinite(v.trueProcessingRate)) {
            // The vertex was never busy, so its capacity is unknown but clearly ample.
            return Math.max(100, v.targetRate * 10);
        }
        return v.capacityPerSubtask();
    }

    /**
     * Demand expected one trend horizon ahead if a rising trend continues, capped at twice the
     * current demand. Equal to the current demand when demand is flat, falling or unknown.
     */
    double projectedDemand(VertexSnapshot v) {
        double projected = v.targetRate;
        if (cfg.trendHorizonSeconds > 0 && !Double.isNaN(v.demandTrend) && v.demandTrend >= 0.1) {
            double growth = v.demandTrend * cfg.trendHorizonSeconds / 60.0;
            projected = v.targetRate * (1 + Math.min(1.0, growth));
        }
        // A load on its way back to a level the scale-down window saw before this rise is expected
        // to reach that level, not to pass it. Readings of one level differ by a few percent, so a
        // demand up to the margin above the level still counts as that level, and the projection
        // stops at the higher of the two. A demand above the margin is a new high and is projected
        // in full. The margin widens the comparison only: a projection capped at the level plus the
        // margin asked for more than the window had kept, and scaled up again (ISSUES.md #119).
        if (cfg.windowCapsTrend && !Double.isNaN(v.priorPeakRate)) {
            double level = v.priorPeakRate * (1 + cfg.windowCapMargin);
            if (v.targetRate <= level) {
                projected = Math.min(projected, Math.max(v.priorPeakRate, v.targetRate));
            }
        }
        // A planned load that is under way or about to start counts as demand already.
        return Double.isNaN(v.plannedRate) ? projected : Math.max(projected, v.plannedRate);
    }

    /**
     * Sets {@link VertexSnapshot#plannedRate} from the schedule: from the lead time before a planned
     * load starts until it ends, the sources together need at least its rate, and every other
     * vertex its share of that (its own demand relative to the sources' demand now).
     */
    void applySchedule(List<VertexSnapshot> vertices, Instant now) {
        if (cfg.schedule.isEmpty()) {
            return;
        }
        int lead = cfg.scheduleLeadSeconds > 0 ? cfg.scheduleLeadSeconds : cfg.restartSeconds + cfg.decisionGapSeconds;
        double t = now.toEpochMilli() / 1000.0;
        double planned = Double.NaN;
        for (var e : cfg.schedule) {
            if (t >= e[0] - lead && t < e[1]) {
                planned = Double.isNaN(planned) ? e[2] : Math.max(planned, e[2]);
            }
        }
        if (Double.isNaN(planned)) {
            return;
        }
        double sourceDemand = 0;
        for (var v : vertices) {
            if ("source".equals(v.role) && v.hasUsableMetrics()) {
                sourceDemand += v.targetRate;
            }
        }
        for (var v : vertices) {
            if (!v.hasUsableMetrics()) {
                continue;
            }
            // Without a usable source rate every vertex is taken to see the planned rate itself.
            v.plannedRate = sourceDemand > 0 ? planned * v.targetRate / sourceDemand : planned;
        }
    }

    /** The subtasks a vertex needs to stay at the target utilization at this rate; 0 when unknown. */
    int sizeAt(VertexSnapshot v, double rate) {
        if (Double.isNaN(rate) || rate <= 0) {
            return 0;
        }
        return Math.max(1, (int) Math.ceil(rate / cfg.targetUtilization / capacityPerSubtask(v) - 1e-9));
    }

    /** Records per second needed on top of the demand to work off the backlog in the catch-up time. */
    double catchUp(VertexSnapshot v) {
        return cfg.catchUpSeconds > 0 ? Math.max(0, v.backlog) / cfg.catchUpSeconds : 0;
    }

    /**
     * The fewest subtasks that keep the vertex at or below the target utilization and work off its
     * backlog in the catch-up time, if nothing is restarted. A vertex with fewer subtasks must be
     * scaled up. This is the built-in autoscaler's scale-up threshold with the upper utilization
     * bound set to the target.
     */
    int trigger(VertexSnapshot v) {
        double needed = projectedDemand(v) / cfg.targetUtilization + catchUp(v);
        return Math.max(1, (int) Math.ceil(needed / capacityPerSubtask(v) - 1e-9));
    }

    /**
     * The size the built-in autoscaler's formula gives a vertex that is restarted: the trigger's
     * requirement plus the records that arrive while the job restarts, to be worked off in the
     * catch-up time.
     */
    int size(VertexSnapshot v) {
        double restart = cfg.catchUpSeconds > 0 ? v.targetRate * cfg.restartSeconds / cfg.catchUpSeconds : 0;
        double needed = projectedDemand(v) / cfg.targetUtilization + catchUp(v) + restart;
        return Math.max(1, (int) Math.ceil(needed / capacityPerSubtask(v) - 1e-9));
    }

    /** The parallelisms this vertex may be set to in this cycle. Always contains the current one. */
    Range allowedRange(VertexSnapshot v, boolean allowDown) {
        int cur = v.parallelism;
        double cap = capacityPerSubtask(v);

        int hi = Math.max(cur + 1, (int) Math.floor(cur * cfg.maxScaleUpFactor));
        hi = Math.min(hi, cfg.maxParallelism);
        hi = Math.min(hi, v.maxParallelism);
        if (v.numSourcePartitions > 0) {
            hi = Math.min(hi, v.numSourcePartitions);
        }

        int lo = cur;
        // An unknown backlog trend counts as growing: no scale-down until the trend is known.
        boolean backlogGrowing = v.backlog > 0 && !(v.backlogGrowth <= 0);
        if (allowDown && !backlogGrowing) {
            lo = (int) Math.ceil(cur * (1.0 - cfg.maxScaleDownFactor));
            lo = Math.max(lo, cfg.minParallelism);
            // Never offer a scale-down that would leave the vertex overloaded.
            int needed = (int) Math.ceil(projectedDemand(v) / (cap * cfg.maxUtilizationAfterScaleDown));
            lo = Math.max(lo, needed);
        }
        return new Range(Math.min(lo, cur), Math.max(hi, cur));
    }

    /**
     * The values inside the range that the built-in autoscaler would also use: for a source, and
     * for a vertex whose input is partitioned by key, only parallelisms that divide the number of
     * partitions or key groups evenly. The current parallelism is always included.
     */
    static List<Integer> options(VertexSnapshot v, Range range) {
        var out = new ArrayList<Integer>();
        for (int p = range.lo; p <= range.hi; p++) {
            if (p == v.parallelism || v.alignTo <= 0 || v.alignTo % p == 0) {
                out.add(p);
            }
        }
        return out;
    }

    /** The smallest option that is at least {@code wanted}, or the largest option if none is. */
    static int fit(List<Integer> options, int wanted) {
        for (int p : options) {
            if (p >= wanted) {
                return p;
            }
        }
        return options.get(options.size() - 1);
    }

    /**
     * The policy, applied in code. Scale up to {@link #size} when the current parallelism is below
     * {@link #trigger}. Scale down to {@link #size} when the vertex has at least two subtasks more
     * than that. Otherwise keep the current parallelism, because every change restarts the job.
     * With {@link JevConfig#scaleDownWindowSeconds} a scale-down is sized for the highest demand of
     * that window, so a load that comes back within it does not cost two restarts.
     */
    int ruleChoice(VertexSnapshot v, List<Integer> options) {
        int cur = v.parallelism;
        int size = size(v);
        if (cur < trigger(v)) {
            return fit(options, size);
        }
        // With a scale-down window the vertex keeps what the highest demand of that window needs.
        int keep = Math.max(size, sizeAt(v, v.peakRate));
        int down = fit(options, keep);
        return cur - keep >= 2 && down < cur ? down : cur;
    }

    /** The answers of the "rule" provider, in the same shape a model returns them. */
    private static ModelResponse ruleResponse(Plan plan, Map<String, List<Integer>> offered) {
        var r = new ModelResponse();
        r.model = "rule";
        for (var id : offered.keySet()) {
            var a = new ChoiceAnswer();
            a.choice = "p" + plan.decisions.get(id).ruleChoice;
            a.confidence = 1.0;
            a.probabilities.put(a.choice, 1.0);
            r.answers.put(id, a);
        }
        return r;
    }

    /**
     * One choice question for a vertex. A decision model matches descriptions rather than doing
     * arithmetic, so every option states the policy's verdict in words next to the numbers the
     * verdict was computed from. The verdicts are those of {@link #ruleChoice}: an option that
     * needs a restart is judged against {@link #size}, the current setting against {@link #trigger}.
     */
    private Map<String, Object> question(VertexSnapshot v, List<Integer> options) {
        double cap = capacityPerSubtask(v);
        int targetPct = (int) Math.round(cfg.targetUtilization * 100);
        double projected = projectedDemand(v);
        boolean rising = projected > v.targetRate;
        long horizonMin = Math.max(1, Math.round(cfg.trendHorizonSeconds / 60.0));
        int trigger = trigger(v);
        int size = size(v);
        int best = fit(options, size);
        var criteria = new LinkedHashMap<String, Object>();
        for (int p : options) {
            double total = p * cap;
            long util = Math.round(100.0 * v.targetRate / total);
            long projectedUtil = Math.round(100.0 * projected / total);
            double spare = total - v.targetRate;
            boolean current = p == v.parallelism;
            String kind =
                    current
                            ? "current setting, no restart"
                            : p > v.parallelism ? "scale up, needs restart" : "scale down, needs restart";
            String verdict;
            String load =
                    Math.max(util, projectedUtil) > 100
                            ? "overloaded, backlog grows"
                            : Math.max(util, projectedUtil) > targetPct
                                    ? "above the " + targetPct + "% target, too little headroom"
                                    : null;
            if (current) {
                int extra = p - size;
                verdict =
                        p < trigger
                                ? (load != null ? load : "too small to clear the backlog in " + cfg.catchUpSeconds + " s")
                                : extra < 1 ? "meets the target" : "meets the target but oversized by " + subtasks(extra);
            } else if (p == best) {
                // Exactly one option that needs a restart is marked as the one to take.
                verdict =
                        p >= size
                                ? "right size after a restart"
                                : "best option allowed now, although still too small after a restart";
            } else if (p < size) {
                verdict = (load != null ? load + "; " : "") + "not enough after a restart";
            } else {
                verdict = "oversized by " + subtasks(p - size) + " after a restart";
            }
            String drain;
            if (v.backlog <= 0) {
                drain = "no backlog";
            } else if (spare <= 0) {
                drain = "backlog keeps growing";
            } else {
                drain =
                        "backlog of "
                                + Math.round(v.backlog)
                                + " records drains in about "
                                + Math.round(v.backlog / spare)
                                + " s";
            }
            criteria.put(
                    "p" + p,
                    p
                            + " subtasks ("
                            + kind
                            + "): "
                            + verdict
                            + "; capacity "
                            + Math.round(total)
                            + " rec/s, utilization "
                            + util
                            + "%"
                            + (rising
                                    ? " now and " + projectedUtil + "% in " + horizonMin + " minutes if demand keeps rising"
                                    : "")
                            + "; "
                            + drain);
        }
        var q = new LinkedHashMap<String, Object>();
        q.put("type", "choice");
        q.put(
                "instructions",
                "Choose the parallelism for Flink vertex '"
                        + v.name
                        + "'. The aim is to stay at or below "
                        + targetPct
                        + "% utilization"
                        + (v.backlog > 0
                                ? " and to clear the backlog within about " + cfg.catchUpSeconds + " seconds"
                                : "")
                        + " with as few subtasks as possible. "
                        + (rising
                                ? "Demand is rising, so judge by the utilization in " + horizonMin + " minutes. "
                                : "")
                        + "Every change restarts the job, so keep the current setting if it meets the target and is "
                        + "oversized by at most 1 subtask. Otherwise choose the one option that is marked as the right "
                        + "size after a restart or as the best option allowed now. Options marked as not enough after "
                        + "a restart must not be chosen.");
        q.put("criteria", criteria);
        return q;
    }

    private static String subtasks(int n) {
        return n + (n == 1 ? " subtask" : " subtasks");
    }

    /** For a generative model: the vertex and the values it may answer in this cycle. */
    private static Map<String, Object> llmQuestion(VertexSnapshot v, List<Integer> options) {
        var q = new LinkedHashMap<String, Object>();
        q.put("name", v.name);
        q.put("current_parallelism", v.parallelism);
        q.put("allowed_parallelisms", options);
        q.put("min_allowed", options.get(0));
        q.put("max_allowed", options.get(options.size() - 1));
        return q;
    }

    /** The policy of {@link #ruleChoice} in words, for a generative model. */
    String llmInstructions() {
        double u = cfg.targetUtilization;
        long target = Math.round(u * 100);
        int catchUp = cfg.catchUpSeconds;
        int restart = cfg.restartSeconds;
        return "You set the parallelism of each vertex of a running Apache Flink streaming job. You are called"
                + " once per autoscaler cycle with the current measurements and answer with one parallelism per"
                + " vertex.\n\n"
                + "Goal: keep every vertex at or below " + target + "% utilization and work off any backlog within"
                + " about " + catchUp + " seconds, with as few subtasks and as few changes as possible.\n\n"
                + "Rule for one vertex (rates in records per second):\n"
                + "- capacity = capacity_records_per_sec_per_subtask, what one subtask can process when fully busy.\n"
                + "- demand = demand_expected_records_per_sec if it is given (demand is rising), otherwise"
                + " demand_records_per_sec.\n"
                + "- catch-up = backlog_records / " + catchUp + ".\n"
                + "- The vertex must be scaled up if current_parallelism x capacity is less than demand / " + u
                + " + catch-up.\n"
                + "- If a vertex is changed, its new parallelism is (demand / " + u + " + catch-up +"
                + " demand_records_per_sec x " + restart + " / " + catchUp + ") / capacity, rounded up. The last"
                + " term is what arrives while the job restarts.\n"
                + "- Scale a vertex down to that parallelism only if it currently has 2 or more subtasks more"
                + " than that.\n"
                + "- Otherwise keep the current parallelism.\n"
                + "- If the parallelism the rule gives is not among a vertex's allowed_parallelisms, answer the"
                + " smallest allowed value that is not below it, or the largest allowed value if there is none.\n\n"
                + "Cost of a change: any change restarts the whole job, and no further change is possible for"
                + " about " + cfg.decisionGapSeconds + " seconds. If you change anything, set every vertex to what"
                + " it needs in the same"
                + " answer.\n\n"
                + "The measurements can be wrong while a vertex is overloaded: backpressure can hide part of the"
                + " demand, and the reported backlog can lag behind the real one. You may choose more than the"
                + " rule gives when the measurements contradict each other, for example busy time near 100%"
                + " while the reported demand is below the capacity.\n\n"
                + (cfg.llmNotes.isBlank() ? "" : notesInstructions())
                + (cfg.llmHistory ? historyInstructions() : "")
                + "Limits: for every vertex in vertices_to_decide answer one of its allowed_parallelisms, using"
                + " its name exactly as given. The limits already include cooldowns and safety rules.";
    }

    /** How to use the owner's notes; only part of the instructions when the job has notes. */
    private String notesInstructions() {
        int lead = cfg.scheduleLeadSeconds > 0 ? cfg.scheduleLeadSeconds : cfg.restartSeconds + cfg.decisionGapSeconds;
        return "Notes from the job's owner: operator_notes is free text about load that is planned or expected,"
                + " for example an event at a given time; now_utc is the current time. Use it as information"
                + " about demand that the measurements cannot show yet. If a note announces a higher load, work"
                + " out what each vertex needs for it with the rule above: take the announced rate as the demand"
                + " of the source, and for every other vertex scale it by that vertex's share of the current"
                + " source demand. Make that change when the announced start is less than " + lead + " seconds"
                + " away, so that the restart is over before the load arrives, and not earlier. From then until"
                + " the announced load is over, do not put a vertex below what the announced load needs, even"
                + " while the measured demand is still low. After that follow the rule again. A note that says"
                + " nothing about load changes nothing.\n\n";
    }

    /** How to use the job's own past; only part of the instructions when the job asks for it. */
    private String historyInstructions() {
        return "History: demand_history is the source's rate over the last half hour, newest first. Each entry"
                + " of recent_scaling_actions also says what that change cost, as far as it is known:"
                + " measured_again_after_seconds (how long the job gave no measurements), backlog_before,"
                + " backlog_peak_after, backlog_cleared_after_seconds, and followed_by (the next change of the"
                + " same vertex). Look at both before you scale a vertex down. If the demand has been going up"
                + " and down, or an earlier scale-down was followed by a scale-up within a few minutes, a"
                + " scale-down now is likely to be undone soon and to cost two restarts. In that case size the"
                + " vertex for the highest demand of the last 10 minutes instead of the current demand (scaled"
                + " to the vertex by its share of the current source demand), and keep the current parallelism"
                + " unless it has 2 or more subtasks more than that. Scale-ups are decided as before.\n\n";
    }

    private Map<String, Object> state(
            List<VertexSnapshot> vertices, List<ScalingEvent> history, Duration sinceLast, Instant now, Extra extra) {
        var names = new LinkedHashMap<String, String>();
        vertices.forEach(v -> names.put(v.id, v.name));
        boolean notes = cfg.isLlm() && !cfg.llmNotes.isBlank();
        boolean past = cfg.isLlm() && cfg.llmHistory;

        var job = new LinkedHashMap<String, Object>();
        if (notes) {
            job.put("now_utc", now.truncatedTo(ChronoUnit.SECONDS).toString());
        }
        job.put("seconds_since_last_scaling", sinceLast == null ? "never scaled" : sinceLast.toSeconds());
        job.put("target_utilization_pct", Math.round(cfg.targetUtilization * 100));
        job.put("backlog_catch_up_seconds", cfg.catchUpSeconds);
        job.put("restart_seconds", cfg.restartSeconds);
        job.put("max_parallelism_per_vertex", cfg.maxParallelism);
        if (cfg.maxTotalParallelism > 0) {
            job.put("max_total_parallelism", cfg.maxTotalParallelism);
        }
        job.put("total_parallelism_now", vertices.stream().mapToInt(v -> v.parallelism).sum());

        var vs = new ArrayList<Map<String, Object>>();
        for (var v : vertices) {
            var m = new LinkedHashMap<String, Object>();
            m.put("name", v.name);
            m.put("role", v.role);
            var upstream = new ArrayList<String>();
            v.inputs.forEach(i -> upstream.add(names.getOrDefault(i, i)));
            m.put("upstream", upstream);
            m.put("current_parallelism", v.parallelism);
            if (!Double.isNaN(v.busy)) {
                m.put("busy_time_pct", Math.round(v.busy * 100));
            }
            if (v.hasUsableMetrics()) {
                double cap = capacityPerSubtask(v);
                m.put("demand_records_per_sec", Math.round(v.targetRate));
                double projected = projectedDemand(v);
                if (projected > v.targetRate) {
                    m.put("demand_expected_records_per_sec", Math.round(projected));
                }
                m.put("capacity_records_per_sec_per_subtask", Math.round(cap));
                m.put("current_utilization_pct", Math.round(100.0 * v.targetRate / (cap * v.parallelism)));
            }
            m.put("demand_trend", trendText(v.demandTrend));
            m.put("backlog_records", Math.round(v.backlog));
            m.put("backlog_trend", backlogText(v));
            vs.add(m);
        }

        var recent = new ArrayList<Map<String, Object>>();
        history.stream()
                .sorted(Comparator.comparing((ScalingEvent e) -> e.time).reversed())
                .limit(8)
                .forEach(
                        e -> {
                            var m = new LinkedHashMap<String, Object>();
                            m.put("seconds_ago", Duration.between(e.time, now).toSeconds());
                            m.put("vertex", names.getOrDefault(e.vertexId, e.vertexId));
                            m.put("from", e.from);
                            m.put("to", e.to);
                            if (past) {
                                describeOutcome(m, e, history);
                            }
                            recent.add(m);
                        });

        var state = new LinkedHashMap<String, Object>();
        state.put("job", job);
        if (notes) {
            state.put("operator_notes", cfg.llmNotes);
        }
        state.put("vertices", vs);
        state.put("recent_scaling_actions", recent);
        if (past) {
            var demand = new ArrayList<Map<String, Object>>();
            for (var h : extra.demandHistory) {
                var m = new LinkedHashMap<String, Object>();
                m.put("minutes_ago", Math.round(h[0] / 6.0) / 10.0);
                m.put("source_records_per_sec", Math.round(h[1]));
                demand.add(m);
            }
            state.put("demand_history", demand);
        }
        return state;
    }

    /** Adds what a past change cost, as far as it is known, and what happened to the vertex next. */
    private static void describeOutcome(Map<String, Object> m, ScalingEvent e, List<ScalingEvent> history) {
        if (e.measuredAgainAfterSeconds >= 0) {
            m.put("measured_again_after_seconds", e.measuredAgainAfterSeconds);
        }
        if (!Double.isNaN(e.backlogBefore)) {
            m.put("backlog_before", Math.round(e.backlogBefore));
        }
        if (!Double.isNaN(e.backlogPeakAfter)) {
            m.put("backlog_peak_after", Math.round(e.backlogPeakAfter));
            m.put(
                    "backlog_cleared_after_seconds",
                    e.backlogClearedAfterSeconds >= 0 ? (Object) e.backlogClearedAfterSeconds : "not yet");
        }
        ScalingEvent next = null;
        for (var o : history) {
            if (o.vertexId.equals(e.vertexId) && o.time.isAfter(e.time) && (next == null || o.time.isBefore(next.time))) {
                next = o;
            }
        }
        if (next != null) {
            m.put(
                    "followed_by",
                    "scaled " + (next.to > next.from ? "up" : "down") + " to " + next.to + " after "
                            + Duration.between(e.time, next.time).toSeconds() + " seconds");
        }
    }

    private static String trendText(double trend) {
        if (Double.isNaN(trend)) {
            return "unknown";
        }
        long pct = Math.round(trend * 100);
        if (pct >= 10) {
            return "rising " + pct + "% per minute";
        }
        if (pct <= -10) {
            return "falling " + (-pct) + "% per minute";
        }
        return "flat";
    }

    private static String backlogText(VertexSnapshot v) {
        if (Double.isNaN(v.backlogGrowth)) {
            return "unknown";
        }
        if (v.backlogGrowth > 1) {
            return "growing by " + Math.round(v.backlogGrowth) + " records/s";
        }
        if (v.backlogGrowth < -1) {
            return "shrinking by " + Math.round(-v.backlogGrowth) + " records/s";
        }
        return "flat";
    }

    /**
     * Applies an answer only when the model's most likely option lies in the direction of the change
     * and enough probability is on that direction; scale-downs need more agreement than scale-ups.
     * A generative model and the rule put all probability on one value, so for them this only
     * checks that the value was offered.
     */
    void interpret(VertexDecision d, ChoiceAnswer answer, List<Integer> options) {
        if (answer == null || answer.probabilities.isEmpty()) {
            d.reason = "no_answer";
            return;
        }
        d.modelChoice = answer.choice;
        d.confidence = answer.confidence;
        double up = 0;
        double down = 0;
        int bestUp = -1;
        int bestDown = -1;
        double bestUpP = -1;
        double bestDownP = -1;
        int top = -1;
        double topP = -1;
        for (var e : answer.probabilities.entrySet()) {
            int p = parseOption(e.getKey());
            if (!options.contains(p)) {
                continue;
            }
            double prob = e.getValue();
            if (prob > topP) {
                topP = prob;
                top = p;
            }
            if (p > d.current) {
                up += prob;
                if (prob > bestUpP) {
                    bestUpP = prob;
                    bestUp = p;
                }
            } else if (p < d.current) {
                down += prob;
                if (prob > bestDownP) {
                    bestDownP = prob;
                    bestDown = p;
                }
            }
        }
        d.upProbability = up;
        d.downProbability = down;
        boolean topUp = !cfg.requireTopChoice || top > d.current;
        boolean topDown = !cfg.requireTopChoice || (top > 0 && top < d.current);
        if (bestUp > 0 && topUp && up >= cfg.minUpProbability && up > down) {
            d.target = bestUp;
            d.reason = "model_scale_up";
        } else if (bestDown > 0 && topDown && down >= cfg.minDownProbability && down > up) {
            d.target = bestDown;
            d.reason = "model_scale_down";
        } else if (top == d.current || parseOption(answer.choice) == d.current) {
            d.reason = "model_hold";
        } else {
            d.reason = "low_confidence_hold";
        }
    }

    static int parseOption(String key) {
        if (key == null || key.length() < 2 || key.charAt(0) != 'p') {
            return -1;
        }
        try {
            return Integer.parseInt(key.substring(1));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /**
     * With {@link JevConfig#scaleDownMinSlots}: drops a plan that only scales down and frees fewer
     * slots than that. The slots of a job are the subtasks of its largest vertex (slot sharing).
     */
    private void holdSmallScaleDown(Plan plan) {
        if (cfg.scaleDownMinSlots <= 0) {
            return;
        }
        int before = 0;
        int after = 0;
        boolean up = false;
        boolean down = false;
        for (var d : plan.decisions.values()) {
            before = Math.max(before, d.current);
            after = Math.max(after, d.target);
            up |= d.target > d.current;
            down |= d.target < d.current;
        }
        if (!down || up || before - after >= cfg.scaleDownMinSlots) {
            return;
        }
        for (var d : plan.decisions.values()) {
            if (d.target < d.current) {
                d.target = d.current;
                d.reason = "held_frees_" + (before - after) + "_slots";
            }
        }
    }

    /** Trims scale-ups, largest first, until the job fits the total parallelism limit. */
    private void capTotal(Plan plan) {
        if (cfg.maxTotalParallelism <= 0) {
            return;
        }
        while (plan.decisions.values().stream().mapToInt(d -> d.target).sum() > cfg.maxTotalParallelism) {
            var victim =
                    plan.decisions.values().stream()
                            .filter(d -> d.target > d.current)
                            .max(Comparator.comparingInt(d -> d.target))
                            .orElse(null);
            if (victim == null) {
                return;
            }
            victim.target--;
            victim.reason = victim.target == victim.current ? "capped_by_total_limit" : "model_scale_up_capped";
        }
    }
}
