package dev.jevscaler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.jevscaler.JevRuntime.Sample;
import dev.jevscaler.Model.ModelException;
import dev.jevscaler.Model.ModelRequest;
import dev.jevscaler.Model.ScalingEvent;
import dev.jevscaler.Model.VertexSnapshot;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/** What a decision may use besides this cycle's measurements: notes, a schedule, the job's past. */
class ContextTest {

    private static final Instant NOW = Instant.parse("2026-11-21T20:13:00Z");

    private static VertexSnapshot vertex(String id, String role, int parallelism, double perSubtask, double demand) {
        var v = new VertexSnapshot();
        v.id = id;
        v.name = id;
        v.role = role;
        v.parallelism = parallelism;
        v.trueProcessingRate = perSubtask * parallelism;
        v.targetRate = demand;
        v.busy = Math.min(1, demand / (perSubtask * parallelism));
        return v;
    }

    private static int ruleTarget(JevConfig cfg, Instant now, VertexSnapshot... vertices) {
        cfg.provider = "rule";
        var plan =
                new DecisionEngine(cfg, request -> null)
                        .decide(List.of(vertices), List.of(), null, now, new DecisionEngine.Breaker());
        return plan.decisions.get("agg").target;
    }

    @Test
    void theScheduleMakesThePolicySizeForAPlannedLoadShortlyBeforeItStarts() {
        long start = NOW.getEpochSecond() + 100;
        var cfg = JevConfig.defaults(); // lead = 30 s restart + 90 s decision gap
        cfg.schedule = JevConfig.schedule(start + "," + (start + 120) + ",2000");

        // 100 s before the start: 2000/0.7 = 2857 must fit, plus 400 * 30/60 for the restart -> 7.
        assertEquals(7, ruleTarget(cfg, NOW, vertex("agg", "operator", 2, 500, 400)));
        // 200 s before the start the load is not planned for yet.
        assertEquals(2, ruleTarget(cfg, NOW.minusSeconds(100), vertex("agg", "operator", 2, 500, 400)));
        // During the planned load a low measurement does not bring the vertex down.
        assertEquals(7, ruleTarget(cfg, NOW.plusSeconds(150), vertex("agg", "operator", 7, 500, 400)));
        // After it the usual rule applies: 400 rec/s needs 2, so 7 is scaled down by at most half.
        assertEquals(4, ruleTarget(cfg, NOW.plusSeconds(230), vertex("agg", "operator", 7, 500, 400)));
    }

    @Test
    void aPlannedLoadIsSharedOutByEachVertexsPartOfTheSourceDemand() {
        long start = NOW.getEpochSecond() + 60;
        var cfg = JevConfig.defaults();
        cfg.schedule = JevConfig.schedule(start + "," + (start + 120) + ",2000");
        // The vertex sees half of what the source reads, so it is planned for 1000 rec/s:
        // 1000/0.7 = 1429 plus 200 * 30/60 -> 1529 / 500 -> 4.
        assertEquals(
                4,
                ruleTarget(cfg, NOW, vertex("src", "source", 2, 5000, 400), vertex("agg", "operator", 2, 500, 200)));
    }

    @Test
    void theScaleDownWindowKeepsWhatTheHighestDemandOfTheWindowNeeds() {
        var cfg = JevConfig.defaults();
        cfg.scaleDownWindowSeconds = 600;

        // Without a peak: 600 rec/s is sized at 3, and 8 is scaled down by at most half.
        assertEquals(4, ruleTarget(cfg, NOW, vertex("agg", "operator", 8, 500, 600)));
        // A peak of 1900 rec/s in the window needs 1900/0.7/500 -> 6.
        var some = vertex("agg", "operator", 8, 500, 600);
        some.peakRate = 1900;
        assertEquals(6, ruleTarget(cfg, NOW, some));
        // A peak of 2500 rec/s needs 8: nothing changes.
        var all = vertex("agg", "operator", 8, 500, 600);
        all.peakRate = 2500;
        assertEquals(8, ruleTarget(cfg, NOW, all));
    }

    @Test
    void aScaleDownKeepsWhatTheLevelPlusItsMarginNeeds() {
        var cfg = JevConfig.defaults(); // 70% target, a scale-down has to save two subtasks
        cfg.maxParallelism = 16;
        cfg.scaleDownWindowSeconds = 600;
        cfg.windowCapsTrend = true;

        // A recorded evaluation (on-off run of the cluster, low phase): 12 subtasks, one reading of
        // 461 rec/s per subtask, demand 797, the window's highest demand 3216. 3216/0.7/461 = 9.97:
        // 10 subtasks just fit, and the job scaled down. In the next high phase the reading was 443,
        // the level needed 10.4, and the job scaled up again (ISSUES.md #119, second cause).
        var low = vertex("agg", "operator", 12, 461, 797);
        low.peakRate = 3216;
        low.priorPeakRate = 3216;
        assertEquals(10, ruleTarget(cfg, NOW, low));

        // Kept for the level plus the margin: 3216 * 1.1 / 0.7 / 461 = 10.97 needs 11, one subtask
        // fewer than 12, and that does not pay for a restart.
        cfg.windowKeepsMargin = true;
        var held = vertex("agg", "operator", 12, 461, 797);
        held.peakRate = 3216;
        held.priorPeakRate = 3216;
        assertEquals(12, ruleTarget(cfg, NOW, held));

        // From 16 subtasks the same level still allows a scale-down, to 11.
        var first = vertex("agg", "operator", 16, 461, 797);
        first.peakRate = 3216;
        first.priorPeakRate = 3216;
        assertEquals(11, ruleTarget(cfg, NOW, first));

        // Without a window nothing is kept and the option changes nothing: 797 rec/s is sized at
        // (797/0.7 + 797 * 30/60) / 461 -> 4, and 12 is scaled down by at most half.
        var none = JevConfig.defaults();
        none.maxParallelism = 16;
        none.windowKeepsMargin = true;
        assertEquals(6, ruleTarget(none, NOW, vertex("agg", "operator", 12, 461, 797)));
    }

    @Test
    void theWindowCapsTheTrendProjectionAtTheHighestDemandItHasSeen() {
        var cfg = JevConfig.defaults(); // 70% target, 30 s restart, 60 s catch-up, 2 minute horizon
        cfg.maxParallelism = 32;
        cfg.scaleDownWindowSeconds = 600;

        // 1500 rec/s rising by 50% per minute is projected at 3000: 3000/0.7 = 4286 needs 9 of 500
        // rec/s, so 6 are too few, and a restart is sized at (4286 + 1500 * 30/60) / 500 -> 11.
        var rising = vertex("agg", "operator", 6, 500, 1500);
        rising.demandTrend = 0.5;
        rising.peakRate = 1900;
        assertEquals(11, ruleTarget(cfg, NOW, rising));

        // With the cap the projection stops at the 1900 rec/s that the window saw before this rise:
        // 1900/0.7 = 2714 fits in 3000.
        cfg.windowCapsTrend = true;
        var back = vertex("agg", "operator", 6, 500, 1500);
        back.demandTrend = 0.5;
        back.peakRate = 1900;
        back.priorPeakRate = 1900;
        assertEquals(6, ruleTarget(cfg, NOW, back));

        // The reading at the jump is a few percent above the earlier level and is itself the window's
        // highest (ISSUES.md #119: 3227 against 3119). It still counts as that level.
        var jump = vertex("agg", "operator", 12, 500, 3227);
        jump.demandTrend = 0.9;
        jump.peakRate = 3227;
        jump.priorPeakRate = 3119;
        assertEquals(3227, new DecisionEngine(cfg, request -> null).projectedDemand(jump), 1e-6);
        assertEquals(12, ruleTarget(cfg, NOW, jump));

        // The margin is not added to the capped projection. A recorded evaluation (on-off run of the
        // cluster, 97 s after a rise): 12 subtasks of 436 rec/s, demand 3169, the window's earlier
        // level 3483. Capped at the level, 3483/0.7 = 4976 fits in 12 * 436 = 5232. Capped at the
        // level plus 10% it needed 13 and the job scaled up to 16 again.
        var recorded = vertex("agg", "operator", 12, 436, 3169);
        recorded.demandTrend = 0.9;
        recorded.peakRate = 3483;
        recorded.priorPeakRate = 3483;
        assertEquals(3483, new DecisionEngine(cfg, request -> null).projectedDemand(recorded), 1e-6);
        assertEquals(12, ruleTarget(cfg, NOW, recorded));

        // A demand above the earlier level and its margin is a new high and is projected as before.
        var high = vertex("agg", "operator", 6, 500, 1500);
        high.demandTrend = 0.5;
        high.peakRate = 1500;
        high.priorPeakRate = 1300;
        assertEquals(11, ruleTarget(cfg, NOW, high));

        // A window too young to hold an earlier level caps nothing.
        var young = vertex("agg", "operator", 6, 500, 1500);
        young.demandTrend = 0.5;
        young.peakRate = 1900;
        assertEquals(11, ruleTarget(cfg, NOW, young));
    }

    @Test
    void aScaleDownThatFreesTooFewSlotsIsHeldUntilAnotherChangeIsDue() {
        var cfg = JevConfig.defaults();
        cfg.maxParallelism = 32;
        cfg.provider = "rule";

        // The sink needs 2 of its 4 subtasks. Aggregate keeps its 8, and they are the job's slots.
        var without =
                new DecisionEngine(cfg, request -> null)
                        .decide(
                                List.of(vertex("agg", "operator", 8, 500, 2500), vertex("sink", "operator", 4, 500, 300)),
                                List.of(), null, NOW, new DecisionEngine.Breaker());
        assertEquals(8, without.decisions.get("agg").target);
        assertEquals(2, without.decisions.get("sink").target);

        cfg.scaleDownMinSlots = 2;
        var engine = new DecisionEngine(cfg, request -> null);
        var held =
                engine.decide(
                        List.of(vertex("agg", "operator", 8, 500, 2500), vertex("sink", "operator", 4, 500, 300)),
                        List.of(), null, NOW, new DecisionEngine.Breaker());
        assertEquals(4, held.decisions.get("sink").target);
        assertEquals("held_frees_0_slots", held.decisions.get("sink").reason);
        assertFalse(held.hasChanges());

        // When Aggregate goes down as well (8 to 4), four slots are freed and the sink goes along.
        var both =
                engine.decide(
                        List.of(vertex("agg", "operator", 8, 500, 600), vertex("sink", "operator", 4, 500, 300)),
                        List.of(), null, NOW, new DecisionEngine.Breaker());
        assertEquals(4, both.decisions.get("agg").target);
        assertEquals(2, both.decisions.get("sink").target);

        // 16 subtasks where 13 are needed and only 15 divides the key groups: one slot, held.
        var aligned = vertex("agg", "operator", 16, 500, 3300);
        aligned.alignTo = 120;
        var one = engine.decide(List.of(aligned), List.of(), null, NOW, new DecisionEngine.Breaker());
        assertEquals(15, one.decisions.get("agg").ruleChoice);
        assertEquals(16, one.decisions.get("agg").target);
        assertEquals("held_frees_1_slots", one.decisions.get("agg").reason);

        // A scale-up is never held, and a scale-down in the same plan is made with it.
        var mixed =
                engine.decide(
                        List.of(vertex("agg", "operator", 2, 500, 2000), vertex("sink", "operator", 4, 500, 300)),
                        List.of(), null, NOW, new DecisionEngine.Breaker());
        assertEquals(8, mixed.decisions.get("agg").target);
        assertEquals(2, mixed.decisions.get("sink").target);
    }

    @SuppressWarnings("unchecked")
    @Test
    void notesAndHistoryReachAGenerativeModelOnlyWhenTheJobHasThem() {
        var seen = new ModelRequest[1];
        Model.DecisionModel model =
                req -> {
                    seen[0] = req;
                    try {
                        return LlmClient.parse("{\"decisions\":[{\"vertex\":\"agg\",\"parallelism\":2}]}", req);
                    } catch (Exception e) {
                        throw new ModelException(e.getMessage(), -1, e);
                    }
                };
        var down = new ScalingEvent(NOW.minusSeconds(400), "agg", 8, 4);
        down.measuredAgainAfterSeconds = 120;
        down.backlogBefore = 100;
        down.backlogPeakAfter = 5000;
        down.backlogClearedAfterSeconds = 180;
        var up = new ScalingEvent(NOW.minusSeconds(200), "agg", 4, 8);
        var extra = new Model.Extra();
        extra.demandHistory.add(new double[] {60, 512});

        var plain = JevConfig.defaults();
        plain.provider = "anthropic";
        new DecisionEngine(plain, model)
                .decide(List.of(vertex("agg", "operator", 2, 500, 2000)), List.of(down, up), null, NOW, new DecisionEngine.Breaker(), extra);
        var state = (Map<String, Object>) seen[0].state;
        assertFalse(state.containsKey("operator_notes"));
        assertFalse(state.containsKey("demand_history"));
        assertFalse(((Map<String, Object>) state.get("job")).containsKey("now_utc"));
        assertFalse(seen[0].instructions.contains("Notes from the job's owner"));
        assertFalse(seen[0].instructions.contains("History:"));
        assertFalse(((List<Map<String, Object>>) state.get("recent_scaling_actions")).get(0).containsKey("followed_by"));

        var rich = JevConfig.defaults();
        rich.provider = "anthropic";
        rich.llmNotes = "The push notification goes out at 20:15 UTC.";
        rich.llmHistory = true;
        new DecisionEngine(rich, model)
                .decide(List.of(vertex("agg", "operator", 2, 500, 2000)), List.of(down, up), null, NOW, new DecisionEngine.Breaker(), extra);
        state = (Map<String, Object>) seen[0].state;
        assertEquals("The push notification goes out at 20:15 UTC.", state.get("operator_notes"));
        assertEquals("2026-11-21T20:13:00Z", ((Map<String, Object>) state.get("job")).get("now_utc"));
        assertTrue(seen[0].instructions.contains("less than 120 seconds"));
        assertTrue(seen[0].instructions.contains("History:"));
        var history = (List<Map<String, Object>>) state.get("demand_history");
        assertEquals(1.0, history.get(0).get("minutes_ago"));
        assertEquals(512L, history.get(0).get("source_records_per_sec"));
        // Newest first: the scale-up, then the scale-down it undid.
        var actions = (List<Map<String, Object>>) state.get("recent_scaling_actions");
        assertEquals(8, actions.get(0).get("to"));
        assertEquals(120L, actions.get(1).get("measured_again_after_seconds"));
        assertEquals(5000L, actions.get(1).get("backlog_peak_after"));
        assertEquals(180L, actions.get(1).get("backlog_cleared_after_seconds"));
        assertEquals("scaled up to 8 after 200 seconds", actions.get(1).get("followed_by"));
    }

    @Test
    void theJobsSamplesGiveThePeakTheDemandHistoryAndTheCostOfAChange() {
        var src = vertex("src", "source", 2, 5000, 400);
        var agg = vertex("agg", "operator", 4, 500, 400);
        var samples = new ArrayDeque<Sample>();
        // One sample every 15 s from 10 minutes ago, except for two minutes after a change 5 minutes ago.
        for (int age = 600; age >= 0; age -= 15) {
            if (age < 300 && age > 180) {
                continue;
            }
            var s = new Sample(NOW.minusSeconds(age));
            double rate = age >= 300 ? 1600 : 400;
            double backlog = age == 300 ? 100 : age == 180 ? 5000 : age == 165 ? 2000 : 50;
            s.perVertex.put("src", new double[] {rate, backlog});
            s.perVertex.put("agg", new double[] {rate, backlog});
            samples.addLast(s);
        }

        History.applyPeaks(samples, List.of(src, agg), NOW, 600, 90);
        assertEquals(1600, agg.peakRate);
        assertEquals(1600, agg.priorPeakRate);
        History.applyPeaks(samples, List.of(src, agg), NOW, 120, 90);
        assertEquals(400, agg.peakRate);
        assertEquals(400, agg.priorPeakRate);
        // Only samples older than the excluded span count for the earlier level.
        History.applyPeaks(samples, List.of(src, agg), NOW, 600, 400);
        assertEquals(1600, agg.priorPeakRate);
        History.applyPeaks(samples, List.of(src, agg), NOW, 200, 250);
        assertTrue(Double.isNaN(agg.priorPeakRate));

        var demand = History.demandHistory(samples, List.of(src, agg), NOW);
        assertEquals(60, demand.get(0)[0]);
        assertEquals(400, demand.get(0)[1]);
        // 3 minutes ago is the first sample after the gap; 5, 7 and 10 minutes ago are before it.
        assertEquals(List.of(60.0, 120.0, 180.0, 300.0, 420.0, 600.0), demand.stream().map(d -> d[0]).toList());
        assertEquals(1600, demand.get(demand.size() - 1)[1]);

        var change = new ScalingEvent(NOW.minusSeconds(300), "agg", 8, 4);
        var uncovered = new ScalingEvent(NOW.minusSeconds(3000), "agg", 4, 8);
        var history = new ArrayList<>(List.of(change, uncovered));
        History.applyOutcomes(samples, List.of(src, agg), history);
        assertEquals(120, change.measuredAgainAfterSeconds);
        assertEquals(100, change.backlogBefore);
        assertEquals(5000, change.backlogPeakAfter);
        // 2000 records is still above two seconds of input (800); 50 records, 150 s after the change, is not.
        assertEquals(150, change.backlogClearedAfterSeconds);
        assertEquals(-1, uncovered.measuredAgainAfterSeconds);
        assertTrue(Double.isNaN(uncovered.backlogPeakAfter));
    }

    @Test
    void settingsAreReadFromTheJobsConfiguration() {
        var note = "Push at 20:15 UTC: about 5x the usual rate, for 2 minutes.";
        var conf =
                Map.of(
                        JevConfig.PREFIX + "llm.notes-base64",
                        Base64.getUrlEncoder().withoutPadding().encodeToString(note.getBytes(StandardCharsets.UTF_8)),
                        JevConfig.PREFIX + "llm.history", "true",
                        JevConfig.PREFIX + "schedule", "100,220,2000; 300,200,5 ;x,y,z;400,460,800",
                        JevConfig.PREFIX + "scale-down.window-seconds", "600",
                        JevConfig.PREFIX + "scale-down.window-caps-trend", "true",
                        JevConfig.PREFIX + "scale-down.window-cap-margin", "0.05",
                        JevConfig.PREFIX + "scale-down.window-keeps-margin", "true",
                        JevConfig.PREFIX + "lag.step-aware", "true",
                        JevConfig.PREFIX + "lag.step-margin", "0.3",
                        JevConfig.PREFIX + "scale-down.min-slots", "2");
        var cfg = JevConfig.from(conf::get);
        assertTrue(cfg.windowCapsTrend);
        assertEquals(0.05, cfg.windowCapMargin);
        assertTrue(cfg.windowKeepsMargin);
        assertTrue(cfg.lagStepAware);
        assertEquals(0.3, cfg.lagStepMargin);
        assertEquals(2, cfg.scaleDownMinSlots);
        assertEquals(note, cfg.llmNotes);
        assertTrue(cfg.llmHistory);
        assertEquals(600, cfg.scaleDownWindowSeconds);
        assertEquals(2, cfg.schedule.size());
        assertEquals(800, cfg.schedule.get(1)[2]);

        var none = JevConfig.from(key -> null);
        assertEquals("", none.llmNotes);
        assertFalse(none.llmHistory);
        assertTrue(none.schedule.isEmpty());
        assertEquals(0, none.scaleDownWindowSeconds);
        assertFalse(none.windowCapsTrend);
        assertEquals(0.1, none.windowCapMargin);
        assertFalse(none.windowKeepsMargin);
        assertFalse(none.lagStepAware);
        assertEquals(0.2, none.lagStepMargin);
        assertEquals(0, none.scaleDownMinSlots);
    }
}
