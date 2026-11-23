package dev.jevscaler;

import dev.jevscaler.Model.DecisionModel;
import dev.jevscaler.Model.VertexSnapshot;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Replays recorded job states through the decision engine, outside the operator.
 *
 * <p>Each input line is one autoscaler cycle of one job: the vertices with what the autoscaler
 * believed about them. The engine decides on it with the chosen provider (rule, jev, anthropic),
 * and the output line holds, per vertex, the model's choice next to what the policy applied in
 * code chooses on the same inputs. No job is touched; the model providers are called for real.
 *
 * <pre>
 *   java -cp plugin.jar:slf4j-api.jar:slf4j-simple.jar dev.jevscaler.Replay \
 *        jev states.jsonl out.jsonl [repeats] [threads]
 * </pre>
 *
 * <p>Input line: {@code {"id": "...", "vertices": [{"id", "name", "role", "inputs": [...],
 * "parallelism", "capacity", "demand", "backlog", "backlog_growth", "demand_trend", "align_to",
 * "partitions", "max_parallelism"}], "excluded": ["vertex id", ...]}}. Settings are read from
 * system properties with the same names as in a job's configuration
 * ({@code -Djob.autoscaler.jev.catch-up-seconds=60}); keys come from the environment.
 */
public final class Replay {

    private Replay() {}

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("usage: Replay <rule|jev|anthropic> <states.jsonl> <out.jsonl> [repeats] [threads]");
            System.exit(2);
        }
        String provider = args[0];
        int repeats = args.length > 3 ? Integer.parseInt(args[3]) : 1;
        int threads = args.length > 4 ? Integer.parseInt(args[4]) : 1;
        var mapper = new ObjectMapper();
        List<String> lines = Files.readAllLines(Path.of(args[1]));

        JevConfig base = config(provider, List.of());
        DecisionModel model =
                base.isRule()
                        ? null
                        : base.isLlm()
                                ? new LlmClient(
                                        System.getenv(base.llmApiKeyEnv),
                                        base.llmModel,
                                        base.llmRequestTimeout,
                                        base.llmMaxRetries,
                                        base.llmEffort,
                                        base.llmMaxTokens)
                                : new JevClient(
                                        base.apiUrl, base.model, System.getenv(base.apiKeyEnv), base.requestTimeout, base.maxRetries);

        var pool = Executors.newFixedThreadPool(Math.max(1, threads));
        var futures = new ArrayList<Future<String>>();
        for (String line : lines) {
            if (line.isBlank()) {
                continue;
            }
            JsonNode state = mapper.readTree(line);
            for (int r = 0; r < repeats; r++) {
                final int repeat = r;
                futures.add(pool.submit((Callable<String>) () -> mapper.writeValueAsString(decide(state, provider, model, repeat))));
            }
        }
        try (var out = new PrintWriter(Files.newBufferedWriter(Path.of(args[2])))) {
            for (var f : futures) {
                out.println(f.get());
            }
        }
        pool.shutdown();
    }

    private static JevConfig config(String provider, List<String> excluded) {
        JevConfig cfg = JevConfig.from(System::getProperty);
        cfg.provider = provider;
        cfg.excludedVertices.addAll(excluded);
        return cfg;
    }

    private static LinkedHashMap<String, Object> decide(JsonNode state, String provider, DecisionModel model, int repeat) {
        var excluded = new ArrayList<String>();
        state.path("excluded").forEach(e -> excluded.add(e.asText()));
        JevConfig cfg = config(provider, excluded);

        var vertices = new ArrayList<VertexSnapshot>();
        for (JsonNode n : state.path("vertices")) {
            var v = new VertexSnapshot();
            v.id = n.path("id").asText();
            v.name = n.path("name").asText(v.id);
            v.role = n.path("role").asText("operator");
            n.path("inputs").forEach(i -> v.inputs.add(i.asText()));
            v.parallelism = n.path("parallelism").asInt();
            v.trueProcessingRate = n.path("capacity").asDouble() * v.parallelism;
            v.targetRate = n.path("demand").asDouble();
            v.busy = Math.min(1.0, v.targetRate / v.trueProcessingRate);
            v.backlog = n.path("backlog").asDouble(0);
            v.backlogGrowth = n.path("backlog_growth").asDouble(0);
            v.demandTrend = n.path("demand_trend").asDouble(0);
            v.alignTo = n.path("align_to").asInt(0);
            v.numSourcePartitions = n.path("partitions").asInt(0);
            v.maxParallelism = n.path("max_parallelism").asInt(Integer.MAX_VALUE);
            vertices.add(v);
        }

        var plan = new DecisionEngine(cfg, model).decide(vertices, List.of(), null, Instant.now(), new DecisionEngine.Breaker());
        var out = new LinkedHashMap<String, Object>();
        out.put("id", state.path("id").asText());
        out.put("repeat", repeat);
        out.put("provider", provider);
        out.put("status", String.valueOf(plan.status));
        out.put("reason", plan.reason);
        if (plan.response != null) {
            out.put("latency_ms", plan.response.latencyMs);
            out.put("input_tokens", plan.response.inputTokens);
            out.put("output_tokens", plan.response.outputTokens);
        }
        var vs = new LinkedHashMap<String, Object>();
        plan.decisions.forEach(
                (id, d) -> {
                    var m = new LinkedHashMap<String, Object>();
                    m.put("name", d.name);
                    m.put("current", d.current);
                    m.put("offered", d.offered);
                    m.put("rule_trigger", d.ruleTrigger);
                    m.put("rule_size", d.ruleSize);
                    m.put("rule_choice", d.ruleChoice);
                    m.put("choice", d.modelChoice);
                    m.put("target", d.target);
                    m.put("reason", d.reason);
                    if (!Double.isNaN(d.upProbability)) {
                        m.put("p_up", d.upProbability);
                        m.put("p_down", d.downProbability);
                    }
                    vs.put(id, m);
                });
        out.put("vertices", vs);
        return out;
    }
}
