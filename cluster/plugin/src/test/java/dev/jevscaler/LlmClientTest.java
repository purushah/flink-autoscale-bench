package dev.jevscaler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.jevscaler.Model.ModelException;
import dev.jevscaler.Model.ModelRequest;
import dev.jevscaler.Model.PlanStatus;
import dev.jevscaler.Model.VertexSnapshot;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class LlmClientTest {

    private static final Instant NOW = Instant.parse("2026-11-16T10:00:00Z");

    private static ModelRequest request() {
        var r = new ModelRequest();
        r.state = Map.of("job", Map.of());
        r.questions.put("v1", question("Aggregate", 2, 2, 8));
        r.questions.put("v2", question("Measure -> Sink", 2, 1, 8));
        return r;
    }

    private static Map<String, Object> question(String name, int current, int lo, int hi) {
        var q = new LinkedHashMap<String, Object>();
        q.put("name", name);
        q.put("current_parallelism", current);
        q.put("min_allowed", lo);
        q.put("max_allowed", hi);
        return q;
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

    @Test
    void mapsDecisionsOntoTheQuestionsByVertexName() throws Exception {
        var out =
                LlmClient.parse(
                        "{\"decisions\":[{\"vertex\":\"Aggregate\",\"parallelism\":5},"
                                + "{\"vertex\":\"Measure -> Sink\",\"parallelism\":2}]}",
                        request());
        assertEquals("p5", out.answers.get("v1").choice);
        assertEquals(1.0, out.answers.get("v1").probabilities.get("p5"));
        assertEquals("p2", out.answers.get("v2").choice);
    }

    @Test
    void movesAnAnswerOutsideTheAllowedRangeToTheNearestLimit() throws Exception {
        var out =
                LlmClient.parse(
                        "{\"decisions\":[{\"vertex\":\"Aggregate\",\"parallelism\":40},"
                                + "{\"vertex\":\"Measure -> Sink\",\"parallelism\":0}]}",
                        request());
        assertEquals("p8", out.answers.get("v1").choice);
        assertEquals("p1", out.answers.get("v2").choice);
    }

    @Test
    void aVertexTheModelSkippedOrMisnamedGetsNoAnswer() throws Exception {
        var out = LlmClient.parse("{\"decisions\":[{\"vertex\":\"Agregate\",\"parallelism\":5}]}", request());
        assertTrue(out.answers.isEmpty());
    }

    @Test
    void rejectsAResponseWithoutDecisions() {
        assertThrows(Exception.class, () -> LlmClient.parse("{\"answer\":5}", request()));
        assertThrows(Exception.class, () -> LlmClient.parse("not json", request()));
    }

    @Test
    void aMissingApiKeyIsAModelErrorNotACrash() {
        var client = new LlmClient(null, "claude-haiku-5-5", Duration.ofSeconds(1), 0, "medium", 1000);
        assertThrows(ModelException.class, () -> client.decide(request()));
    }

    @Test
    void engineSendsRawRangesAndPolicyAndAppliesTheAnswerWithinLimits() {
        var cfg = JevConfig.defaults();
        cfg.provider = "anthropic";
        var seen = new ModelRequest[1];
        Model.DecisionModel model =
                req -> {
                    seen[0] = req;
                    try {
                        return LlmClient.parse("{\"decisions\":[{\"vertex\":\"agg\",\"parallelism\":6}]}", req);
                    } catch (Exception e) {
                        throw new ModelException(e.getMessage(), -1, e);
                    }
                };
        var plan =
                new DecisionEngine(cfg, model)
                        .decide(List.of(vertex("agg", 2, 500, 2000)), List.of(), null, NOW, new DecisionEngine.Breaker());
        assertEquals(PlanStatus.DECIDED, plan.status);
        assertEquals(6, plan.decisions.get("agg").target);
        @SuppressWarnings("unchecked")
        var q = (Map<String, Object>) seen[0].questions.get("agg");
        assertEquals(2, q.get("min_allowed"));
        assertEquals(8, q.get("max_allowed"));
        assertFalse(q.containsKey("criteria"));
        assertTrue(seen[0].instructions.contains("70% utilization"));
    }

    @Test
    void jevRequestsCarryNoInstructions() {
        var seen = new ModelRequest[1];
        Model.DecisionModel model =
                req -> {
                    seen[0] = req;
                    return new Model.ModelResponse();
                };
        new DecisionEngine(JevConfig.defaults(), model)
                .decide(List.of(vertex("agg", 2, 500, 2000)), List.of(), null, NOW, new DecisionEngine.Breaker());
        assertNull(seen[0].instructions);
    }
}
