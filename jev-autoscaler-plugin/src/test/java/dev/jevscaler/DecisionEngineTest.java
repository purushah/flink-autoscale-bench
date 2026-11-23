package dev.jevscaler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.jevscaler.Model.ChoiceAnswer;
import dev.jevscaler.Model.DecisionModel;
import dev.jevscaler.Model.ModelException;
import dev.jevscaler.Model.ModelRequest;
import dev.jevscaler.Model.ModelResponse;
import dev.jevscaler.Model.PlanStatus;
import dev.jevscaler.Model.VertexSnapshot;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class DecisionEngineTest {

    private static final Instant NOW = Instant.parse("2026-11-15T10:00:00Z");

    /** A model that answers each question with fixed probabilities and records what it was asked. */
    static final class FakeModel implements DecisionModel {
        final Map<String, Map<String, Double>> answers;
        final List<ModelRequest> requests = new ArrayList<>();
        ModelException failure;

        FakeModel(Map<String, Map<String, Double>> answers) {
            this.answers = answers;
        }

        @Override
        public ModelResponse decide(ModelRequest request) throws ModelException {
            requests.add(request);
            if (failure != null) {
                throw failure;
            }
            var r = new ModelResponse();
            r.model = "fake";
            answers.forEach(
                    (id, probs) -> {
                        var a = new ChoiceAnswer();
                        a.probabilities.putAll(probs);
                        a.choice = probs.entrySet().stream().max(Map.Entry.comparingByValue()).get().getKey();
                        a.confidence = probs.get(a.choice);
                        r.answers.put(id, a);
                    });
            return r;
        }
    }

    private static VertexSnapshot vertex(String id, int parallelism, double perSubtask, double demand) {
        var v = new VertexSnapshot();
        v.id = id;
        v.name = id;
        v.role = "operator";
        v.parallelism = parallelism;
        v.trueProcessingRate = perSubtask * parallelism;
        v.targetRate = demand;
        v.busy = Math.min(1, demand / (perSubtask * parallelism));
        return v;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> criteria(ModelRequest request, String id) {
        return (Map<String, Object>) ((Map<String, Object>) request.questions.get(id)).get("criteria");
    }

    @Test
    void appliesConfidentScaleUp() {
        var model = new FakeModel(Map.of("agg", Map.of("p6", 0.9, "p5", 0.05, "p2", 0.05)));
        var plan =
                new DecisionEngine(JevConfig.defaults(), model)
                        .decide(List.of(vertex("agg", 2, 500, 2000)), List.of(), null, NOW, new DecisionEngine.Breaker());
        assertEquals(PlanStatus.DECIDED, plan.status);
        assertEquals(6, plan.decisions.get("agg").target);
        assertTrue(plan.hasChanges());
    }

    @Test
    void offersOnlyParallelismsInsideTheLimits() {
        var cfg = JevConfig.defaults();
        cfg.maxParallelism = 6;
        cfg.maxScaleUpFactor = 2.0;
        var model = new FakeModel(Map.of("agg", Map.of("p3", 1.0)));
        new DecisionEngine(cfg, model)
                .decide(List.of(vertex("agg", 4, 500, 600)), List.of(), null, NOW, new DecisionEngine.Breaker());
        // Down: at most half (2), but 2 subtasks would run at 60% which is allowed. Up: x2 = 8, capped at 6.
        assertEquals(List.of("p2", "p3", "p4", "p5", "p6"), new ArrayList<>(criteria(model.requests.get(0), "agg").keySet()));
    }

    @Test
    void neverOffersAScaleDownThatWouldOverload() {
        var model = new FakeModel(Map.of("agg", Map.of("p4", 1.0)));
        new DecisionEngine(JevConfig.defaults(), model)
                .decide(List.of(vertex("agg", 4, 500, 1500)), List.of(), null, NOW, new DecisionEngine.Breaker());
        // 3 subtasks would be at 100% utilization, above the 85% limit, so 4 is the lowest option.
        assertEquals("p4", criteria(model.requests.get(0), "agg").keySet().iterator().next());
    }

    @Test
    void ignoresAnAnswerOutsideTheOfferedOptions() {
        var model = new FakeModel(Map.of("agg", Map.of("p32", 1.0)));
        var plan =
                new DecisionEngine(JevConfig.defaults(), model)
                        .decide(List.of(vertex("agg", 2, 500, 2000)), List.of(), null, NOW, new DecisionEngine.Breaker());
        assertEquals(2, plan.decisions.get("agg").target);
        assertFalse(plan.hasChanges());
    }

    @Test
    void holdsWhenScaleDownIsNotConfidentEnough() {
        var model = new FakeModel(Map.of("agg", Map.of("p3", 0.6, "p6", 0.4)));
        var plan =
                new DecisionEngine(JevConfig.defaults(), model)
                        .decide(List.of(vertex("agg", 6, 500, 900)), List.of(), null, NOW, new DecisionEngine.Breaker());
        assertEquals(6, plan.decisions.get("agg").target);
        assertEquals("low_confidence_hold", plan.decisions.get("agg").reason);
    }

    @Test
    void scaleUpCooldownBlocksEverythingWithoutCallingTheModel() {
        var model = new FakeModel(Map.of());
        var plan =
                new DecisionEngine(JevConfig.defaults(), model)
                        .decide(
                                List.of(vertex("agg", 2, 500, 2000)),
                                List.of(),
                                NOW.minusSeconds(20),
                                NOW,
                                new DecisionEngine.Breaker());
        assertEquals(PlanStatus.COOLDOWN, plan.status);
        assertTrue(model.requests.isEmpty());
        assertFalse(plan.hasChanges());
    }

    @Test
    void scaleDownCooldownOnlyOffersCurrentAndHigher() {
        var model = new FakeModel(Map.of("agg", Map.of("p6", 1.0)));
        new DecisionEngine(JevConfig.defaults(), model)
                .decide(
                        List.of(vertex("agg", 6, 500, 300)),
                        List.of(),
                        NOW.minusSeconds(100),
                        NOW,
                        new DecisionEngine.Breaker());
        assertEquals("p6", criteria(model.requests.get(0), "agg").keySet().iterator().next());
    }

    @Test
    void doesNotScaleDownWhileBacklogGrows() {
        var v = vertex("agg", 6, 500, 300);
        v.backlog = 10_000;
        v.backlogGrowth = 50;
        var model = new FakeModel(Map.of("agg", Map.of("p6", 1.0)));
        new DecisionEngine(JevConfig.defaults(), model)
                .decide(List.of(v), List.of(), null, NOW, new DecisionEngine.Breaker());
        assertEquals("p6", criteria(model.requests.get(0), "agg").keySet().iterator().next());
    }

    @Test
    void risingDemandIsProjectedAheadInTheOptions() {
        var v = vertex("agg", 2, 500, 600);
        v.demandTrend = 0.25; // +25% per minute -> +50% over the 2 minute horizon
        var model = new FakeModel(Map.of("agg", Map.of("p3", 1.0)));
        new DecisionEngine(JevConfig.defaults(), model)
                .decide(List.of(v), List.of(), null, NOW, new DecisionEngine.Breaker());
        var text = String.valueOf(criteria(model.requests.get(0), "agg").get("p2"));
        assertTrue(text.contains("60% now and 90% in 2 minutes"), text);
    }

    @Test
    void modelErrorFallsBackAndOpensTheCircuitAfterRepeatedFailures() {
        var model = new FakeModel(Map.of());
        model.failure = new ModelException("HTTP 529", 529, null);
        var engine = new DecisionEngine(JevConfig.defaults(), model);
        var breaker = new DecisionEngine.Breaker();
        var vertices = List.of(vertex("agg", 2, 500, 2000));
        for (int i = 0; i < 3; i++) {
            assertEquals(PlanStatus.FALLBACK, engine.decide(vertices, List.of(), null, NOW, breaker).status);
        }
        assertNotNull(breaker.openUntil);
        // While the circuit is open the model is not called at all.
        var plan = engine.decide(vertices, List.of(), null, NOW.plusSeconds(30), breaker);
        assertEquals("circuit_open", plan.reason);
        assertEquals(3, model.requests.size());
        // After the open period a successful call closes it again.
        model.failure = null;
        engine.decide(vertices, List.of(), null, NOW.plusSeconds(200), breaker);
        assertNull(breaker.openUntil);
        assertEquals(0, breaker.consecutiveFailures);
    }

    @Test
    void totalParallelismLimitTrimsScaleUps() {
        var cfg = JevConfig.defaults();
        cfg.maxTotalParallelism = 9;
        var model = new FakeModel(Map.of("a", Map.of("p6", 1.0), "b", Map.of("p6", 1.0)));
        var plan =
                new DecisionEngine(cfg, model)
                        .decide(
                                List.of(vertex("a", 2, 500, 2000), vertex("b", 2, 500, 2000)),
                                List.of(),
                                null,
                                NOW,
                                new DecisionEngine.Breaker());
        assertTrue(plan.targets().values().stream().mapToInt(Integer::intValue).sum() <= 9);
    }

    @Test
    void verticesWithoutMetricsAreLeftAlone() {
        var v = vertex("agg", 2, 500, 2000);
        v.trueProcessingRate = Double.NaN;
        var model = new FakeModel(Map.of());
        var plan =
                new DecisionEngine(JevConfig.defaults(), model)
                        .decide(List.of(v), List.of(), null, NOW, new DecisionEngine.Breaker());
        assertEquals("no_metrics", plan.decisions.get("agg").reason);
        assertTrue(model.requests.isEmpty());
    }

    @Test
    void holdsWhenTheMostLikelyOptionIsTheCurrentSetting() {
        // 0.55 of the probability is on higher options, but the single most likely option is "keep 3".
        var answers = Map.of("agg", Map.of("p3", 0.45, "p4", 0.30, "p5", 0.25));
        var plan =
                new DecisionEngine(JevConfig.defaults(), new FakeModel(answers))
                        .decide(List.of(vertex("agg", 3, 500, 600)), List.of(), null, NOW, new DecisionEngine.Breaker());
        assertEquals(3, plan.decisions.get("agg").target);
        assertEquals("model_hold", plan.decisions.get("agg").reason);

        var summed = JevConfig.defaults();
        summed.requireTopChoice = false;
        var old =
                new DecisionEngine(summed, new FakeModel(answers))
                        .decide(List.of(vertex("agg", 3, 500, 600)), List.of(), null, NOW, new DecisionEngine.Breaker());
        assertEquals(4, old.decisions.get("agg").target);
    }

    @Test
    void ruleProviderSizesInCodeWithoutCallingAModel() {
        var cfg = JevConfig.defaults(); // 70% target, 30 s restart, 60 s catch-up
        cfg.provider = "rule";
        var model = new FakeModel(Map.of());
        var engine = new DecisionEngine(cfg, model);

        // 2000 rec/s at 500 rec/s per subtask: 2000/0.7 = 2857 must fit, so 2 subtasks are too few.
        // After a restart another 2000 * 30/60 = 1000 rec/s is needed: 3857 / 500 -> 8 subtasks.
        var up = engine.decide(List.of(vertex("agg", 2, 500, 2000)), List.of(), null, NOW, new DecisionEngine.Breaker());
        assertEquals(8, up.decisions.get("agg").target);
        assertEquals("rule", up.response.model);
        assertEquals(6, up.decisions.get("agg").ruleTrigger);
        assertEquals(8, up.decisions.get("agg").ruleSize);

        // 5 subtasks at 1300 rec/s: 1857 fits in 2500, so the vertex meets the target and is kept,
        // although a restart would be sized at 6.
        var hold = engine.decide(List.of(vertex("agg", 5, 500, 1300)), List.of(), null, NOW, new DecisionEngine.Breaker());
        assertEquals(5, hold.decisions.get("agg").target);

        // 8 subtasks where a restart would be sized at 3: scale down, but by at most half in one step.
        var down = engine.decide(List.of(vertex("agg", 8, 500, 600)), List.of(), null, NOW, new DecisionEngine.Breaker());
        assertEquals(4, down.decisions.get("agg").target);

        // 4 subtasks where a restart would be sized at 3: oversized by one, so nothing changes.
        var keep = engine.decide(List.of(vertex("agg", 4, 500, 600)), List.of(), null, NOW, new DecisionEngine.Breaker());
        assertEquals(4, keep.decisions.get("agg").target);

        // A backlog of 60,000 records adds 1000 rec/s: (1429 + 1000) / 500 -> 5 are needed now, and
        // (1429 + 1000 + 500) / 500 -> 6 after a restart.
        var lagging = vertex("agg", 2, 500, 1000);
        lagging.backlog = 60000;
        lagging.backlogGrowth = 0;
        var drain = engine.decide(List.of(lagging), List.of(), null, NOW, new DecisionEngine.Breaker());
        assertEquals(6, drain.decisions.get("agg").target);

        assertTrue(model.requests.isEmpty());
    }

    @Test
    void onlyOffersParallelismsThatDivideTheKeyGroupsEvenly() {
        var cfg = JevConfig.defaults();
        cfg.provider = "rule";
        var keyed = vertex("agg", 4, 500, 2400); // sized at ceil((3429 + 1200) / 500) = 10 -> capped at 8
        keyed.alignTo = 120;
        keyed.maxParallelism = 120;
        var model = new FakeModel(Map.of("agg", Map.of("p7", 1.0)));
        new DecisionEngine(JevConfig.defaults(), model)
                .decide(List.of(keyed), List.of(), null, NOW, new DecisionEngine.Breaker());
        // 7 does not divide 120, so it is not offered; a model that names it is ignored.
        assertFalse(criteria(model.requests.get(0), "agg").containsKey("p7"));
        assertTrue(criteria(model.requests.get(0), "agg").containsKey("p8"));

        var source = vertex("src", 2, 5000, 20000);
        source.alignTo = 8;
        source.numSourcePartitions = 8;
        var plan = new DecisionEngine(cfg, model).decide(List.of(source), List.of(), null, NOW, new DecisionEngine.Breaker());
        // (28571 + 10000) / 5000 -> 8; the options are 2, 4 and 8.
        assertEquals("p2..p8", plan.decisions.get("src").offered);
        assertEquals(8, plan.decisions.get("src").target);
    }

    @Test
    void menuVerdictsMatchThePolicy() {
        // 3 subtasks at 1200 rec/s (80% busy): above the target. A restart is sized at
        // ceil((1714 + 600) / 500) = 5.
        var model = new FakeModel(Map.of("agg", Map.of("p5", 1.0)));
        var plan =
                new DecisionEngine(JevConfig.defaults(), model)
                        .decide(List.of(vertex("agg", 3, 500, 1200)), List.of(), null, NOW, new DecisionEngine.Breaker());
        var options = criteria(model.requests.get(0), "agg");
        assertTrue(String.valueOf(options.get("p3")).contains("above the 70% target"));
        assertTrue(String.valueOf(options.get("p4")).contains("not enough after a restart"));
        assertTrue(String.valueOf(options.get("p5")).contains("right size after a restart"));
        assertTrue(String.valueOf(options.get("p6")).contains("oversized by 1 subtask after a restart"));
        assertEquals(5, plan.decisions.get("agg").ruleChoice);
        assertEquals(5, plan.decisions.get("agg").target);
    }

    @Test
    void excludedVerticesAreNeverAsked() {
        var cfg = JevConfig.defaults();
        cfg.excludedVertices.add("src");
        var model = new FakeModel(Map.of("src", Map.of("p8", 1.0), "agg", Map.of("p8", 1.0)));
        var plan =
                new DecisionEngine(cfg, model)
                        .decide(
                                List.of(vertex("src", 2, 500, 2000), vertex("agg", 2, 500, 2000)),
                                List.of(),
                                null,
                                NOW,
                                new DecisionEngine.Breaker());
        assertEquals(2, plan.decisions.get("src").target);
        assertEquals("excluded", plan.decisions.get("src").reason);
        assertEquals(8, plan.decisions.get("agg").target);
        assertFalse(model.requests.get(0).questions.containsKey("src"));
    }

    @Test
    void unknownBacklogTrendBlocksScaleDown() {
        var model = new FakeModel(Map.of("agg", Map.of("p4", 1.0)));
        var v = vertex("agg", 8, 500, 600);
        v.backlog = 100; // backlogGrowth stays NaN: the trend is not known yet
        var plan =
                new DecisionEngine(JevConfig.defaults(), model)
                        .decide(List.of(v), List.of(), null, NOW, new DecisionEngine.Breaker());
        assertEquals(8, plan.decisions.get("agg").target);
    }
}
