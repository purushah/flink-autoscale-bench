package dev.jevscaler;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Plain data types shared by the decision engine. None of them depend on Flink classes. */
public final class Model {

    private Model() {}

    /** What the autoscaler knows about one job vertex in the current cycle. */
    public static final class VertexSnapshot {
        public String id;
        public String name;

        /** "source", "operator" or "sink". */
        public String role;

        public List<String> inputs = new ArrayList<>();
        public int parallelism;

        /** Upper limit from key groups (Flink max parallelism). */
        public int maxParallelism = Integer.MAX_VALUE;

        /** Number of source partitions, or 0 when the vertex is not a partitioned source. */
        public int numSourcePartitions;

        /**
         * Number of partitions (source) or key groups (keyed input) that the parallelism should divide
         * evenly, as the built-in autoscaler does; 0 when any parallelism is fine.
         */
        public int alignTo;

        /** Fraction of time busy, 0..1. NaN when unknown. */
        public double busy = Double.NaN;

        /** Records per second the whole vertex can process when 100% busy. */
        public double trueProcessingRate = Double.NaN;

        /** Records per second the vertex must process to keep up with its inputs. */
        public double targetRate = Double.NaN;

        /** Extra records per second needed to drain the backlog in the catch-up window. */
        public double catchUpRate = 0;

        /** Backlog in records attributed to this vertex. */
        public double backlog = 0;

        /** Change in demand per minute, as a fraction (0.2 = +20% per minute). NaN when unknown. */
        public double demandTrend = Double.NaN;

        /** Backlog growth in records per second. NaN when unknown. */
        public double backlogGrowth = Double.NaN;

        /**
         * Demand this vertex is expected to have because of a planned load that is under way or
         * about to start ({@link JevConfig#schedule}). NaN when nothing is planned.
         */
        public double plannedRate = Double.NaN;

        /** The highest demand seen within the scale-down window. NaN when the window is off. */
        public double peakRate = Double.NaN;

        /**
         * The highest demand of the scale-down window before the current rise: of its samples that
         * are older than one and a half trend windows. NaN when the window is off or too young.
         */
        public double priorPeakRate = Double.NaN;

        public double capacityPerSubtask() {
            if (Double.isNaN(trueProcessingRate) || trueProcessingRate <= 0 || parallelism <= 0) {
                return Double.NaN;
            }
            if (Double.isInfinite(trueProcessingRate)) {
                return 1e9;
            }
            return trueProcessingRate / parallelism;
        }

        public boolean hasUsableMetrics() {
            return !Double.isNaN(capacityPerSubtask())
                    && !Double.isNaN(targetRate)
                    && !Double.isInfinite(targetRate)
                    && targetRate >= 0;
        }
    }

    /** One past scaling action. */
    public static final class ScalingEvent {
        public Instant time;
        public String vertexId;
        public int from;
        public int to;

        /**
         * What the change cost, read from the job's own measurements when they cover it: how long
         * the job gave no measurements, the source backlog just before, its peak in the five
         * minutes after, and how long it took to come back down. Negative or NaN: not known.
         */
        public long measuredAgainAfterSeconds = -1;

        public double backlogBefore = Double.NaN;
        public double backlogPeakAfter = Double.NaN;
        public long backlogClearedAfterSeconds = -1;

        public ScalingEvent(Instant time, String vertexId, int from, int to) {
            this.time = time;
            this.vertexId = vertexId;
            this.from = from;
            this.to = to;
        }
    }

    /** What a decision may use besides this cycle's measurements. */
    public static final class Extra {
        /** The source's rate at a few ages, newest first: {seconds ago, records per second}. */
        public List<double[]> demandHistory = new ArrayList<>();
    }

    /** Jev's typed answer to one choice question. */
    public static final class ChoiceAnswer {
        public String choice;
        public double confidence;
        public Map<String, Double> probabilities = new LinkedHashMap<>();
    }

    /** A complete Jev response. */
    public static final class ModelResponse {
        public String model;
        public Map<String, ChoiceAnswer> answers = new LinkedHashMap<>();
        public long inputTokens;
        public long outputTokens;
        public long latencyMs;
        public int attempts = 1;
    }

    /** A request to the decision model. */
    public static final class ModelRequest {
        public Object state;
        public Map<String, Object> questions = new LinkedHashMap<>();

        /** The policy in words; only used by generative models. */
        public String instructions;
    }

    /** The model call failed in a way the caller should treat as "use the fallback". */
    public static final class ModelException extends Exception {
        public final int status;

        public ModelException(String message, int status, Throwable cause) {
            super(message, cause);
            this.status = status;
        }
    }

    /** Anything that can answer a {@link ModelRequest}. */
    public interface DecisionModel {
        ModelResponse decide(ModelRequest request) throws ModelException;
    }

    public enum PlanStatus {
        /** Jev answered; {@link Plan#targets} holds the guarded per-vertex parallelism. */
        DECIDED,
        /** A cooldown is active, so nothing may change this cycle. */
        COOLDOWN,
        /** Jev could not be used; let the built-in autoscaler decide. */
        FALLBACK
    }

    /** The decision for one vertex after guardrails. */
    public static final class VertexDecision {
        public String vertexId;
        public String name;
        public int current;
        public int target;
        public String modelChoice;
        public double confidence = Double.NaN;
        public double upProbability = Double.NaN;
        public double downProbability = Double.NaN;

        /** Why the target is what it is, for the decision log. */
        public String reason;

        /** What the policy applied in code would choose, and the two numbers it is based on. */
        public int ruleChoice = -1;

        public int ruleTrigger = -1;
        public int ruleSize = -1;

        /** The inputs the decision was based on, for the decision log. */
        public double demand = Double.NaN;

        public double projectedDemand = Double.NaN;
        public double capacityPerSubtask = Double.NaN;
        public double busy = Double.NaN;
        public double backlog = Double.NaN;
        public double backlogGrowth = Double.NaN;
        public double plannedRate = Double.NaN;
        public double peakRate = Double.NaN;
        public double priorPeakRate = Double.NaN;
        public String offered;
    }

    /** The decision for one job in one autoscaler cycle. */
    public static final class Plan {
        public PlanStatus status;
        public String reason;
        public Instant cycleTime;
        public Map<String, VertexDecision> decisions = new LinkedHashMap<>();
        public ModelResponse response;

        public Map<String, Integer> targets() {
            var out = new LinkedHashMap<String, Integer>();
            decisions.forEach((id, d) -> out.put(id, d.target));
            return out;
        }

        public boolean hasChanges() {
            return status == PlanStatus.DECIDED
                    && decisions.values().stream().anyMatch(d -> d.target != d.current);
        }
    }
}
