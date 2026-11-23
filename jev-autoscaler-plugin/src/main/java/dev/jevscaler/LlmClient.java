package dev.jevscaler;

import dev.jevscaler.Model.ChoiceAnswer;
import dev.jevscaler.Model.DecisionModel;
import dev.jevscaler.Model.ModelException;
import dev.jevscaler.Model.ModelRequest;
import dev.jevscaler.Model.ModelResponse;

import com.anthropic.client.AnthropicClient;
import com.anthropic.client.okhttp.AnthropicOkHttpClient;
import com.anthropic.core.JsonValue;
import com.anthropic.errors.AnthropicException;
import com.anthropic.errors.AnthropicServiceException;
import com.anthropic.models.messages.JsonOutputFormat;
import com.anthropic.models.messages.Message;
import com.anthropic.models.messages.MessageCreateParams;
import com.anthropic.models.messages.OutputConfig;
import com.anthropic.models.messages.StopReason;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Asks a Claude model for one parallelism per vertex.
 *
 * <p>Unlike Jev, a generative model does the sizing itself, so it gets the raw measurements and
 * the policy in words, and answers with a number per vertex. The answer is translated into the
 * same {@link ChoiceAnswer} shape Jev returns (all probability on one option), so the decision
 * engine's limits, cooldowns and fallback apply unchanged. The API key is never logged.
 */
public final class LlmClient implements DecisionModel {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String model;
    private final String effort;
    private final long maxTokens;
    private final AnthropicClient client;

    public LlmClient(
            String apiKey, String model, Duration timeout, int maxRetries, String effort, long maxTokens) {
        this.model = model;
        this.effort = effort;
        this.maxTokens = maxTokens;
        this.client =
                apiKey == null || apiKey.isBlank()
                        ? null
                        : AnthropicOkHttpClient.builder()
                                .apiKey(apiKey)
                                .timeout(timeout)
                                .maxRetries(Math.max(0, maxRetries))
                                .build();
    }

    @Override
    public ModelResponse decide(ModelRequest request) throws ModelException {
        if (client == null) {
            throw new ModelException("API key is not set", 0, null);
        }
        String user;
        try {
            user = userMessage(request);
        } catch (Exception e) {
            throw new ModelException("Could not serialize request: " + e.getMessage(), 0, e);
        }
        var params =
                MessageCreateParams.builder()
                        .model(model)
                        .maxTokens(maxTokens)
                        .system(request.instructions == null ? "" : request.instructions)
                        .addUserMessage(user)
                        .outputConfig(
                                OutputConfig.builder()
                                        .effort(OutputConfig.Effort.of(effort))
                                        .format(JsonOutputFormat.builder().schema(schema()).build())
                                        .build())
                        .build();

        long start = System.nanoTime();
        Message message;
        try {
            message = client.messages().create(params);
        } catch (AnthropicServiceException e) {
            throw new ModelException("HTTP " + e.statusCode() + ": " + shorten(e.getMessage()), e.statusCode(), e);
        } catch (AnthropicException e) {
            throw new ModelException(
                    "Request failed: " + e.getClass().getSimpleName() + ": " + shorten(e.getMessage()), 0, e);
        }

        var stop = message.stopReason().orElse(null);
        if (!StopReason.END_TURN.equals(stop)) {
            // A refusal or a truncated answer is not a decision; the built-in autoscaler takes over.
            var category = message.stopDetails().flatMap(d -> d.category()).map(String::valueOf).orElse("");
            throw new ModelException(
                    "stop_reason " + stop + (category.isEmpty() ? "" : " (" + category + ")"), -1, null);
        }
        var text = new StringBuilder();
        message.content().forEach(block -> block.text().ifPresent(t -> text.append(t.text())));

        ModelResponse out;
        try {
            out = parse(text.toString(), request);
        } catch (Exception e) {
            throw new ModelException("Could not parse response: " + e.getMessage(), -1, e);
        }
        out.model = message.model().asString();
        out.inputTokens = message.usage().inputTokens();
        out.outputTokens = message.usage().outputTokens();
        out.latencyMs = (System.nanoTime() - start) / 1_000_000;
        return out;
    }

    /** The state plus, per vertex to decide, its name and the range the engine allows this cycle. */
    static String userMessage(ModelRequest request) throws Exception {
        var body = new LinkedHashMap<String, Object>();
        body.put("measurements", request.state);
        body.put("vertices_to_decide", new ArrayList<>(request.questions.values()));
        return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(body);
    }

    private static JsonOutputFormat.Schema schema() {
        var decision = new LinkedHashMap<String, Object>();
        decision.put("type", "object");
        decision.put(
                "properties",
                Map.of("vertex", Map.of("type", "string"), "parallelism", Map.of("type", "integer")));
        decision.put("required", List.of("vertex", "parallelism"));
        decision.put("additionalProperties", false);
        return JsonOutputFormat.Schema.builder()
                .putAdditionalProperty("type", JsonValue.from("object"))
                .putAdditionalProperty(
                        "properties",
                        JsonValue.from(Map.of("decisions", Map.of("type", "array", "items", decision))))
                .putAdditionalProperty("required", JsonValue.from(List.of("decisions")))
                .putAdditionalProperty("additionalProperties", JsonValue.from(false))
                .build();
    }

    /**
     * Maps {@code {"decisions":[{"vertex": name, "parallelism": n}]}} onto the questions that were
     * asked. A number outside a vertex's allowed range is moved to the nearest allowed value; a
     * vertex the model did not answer for gets no answer, which the engine treats as "hold".
     */
    @SuppressWarnings("unchecked")
    static ModelResponse parse(String json, ModelRequest request) throws Exception {
        JsonNode decisions = MAPPER.readTree(json).path("decisions");
        if (!decisions.isArray()) {
            throw new IllegalArgumentException("response has no decisions array");
        }
        var byName = new LinkedHashMap<String, Integer>();
        for (JsonNode d : decisions) {
            if (d.path("vertex").isTextual() && d.path("parallelism").canConvertToInt()) {
                byName.put(d.path("vertex").asText().trim(), d.path("parallelism").asInt());
            }
        }
        var out = new ModelResponse();
        request.questions.forEach(
                (id, q) -> {
                    var question = (Map<String, Object>) q;
                    var wanted = byName.get(String.valueOf(question.get("name")).trim());
                    if (wanted == null) {
                        return;
                    }
                    int lo = ((Number) question.get("min_allowed")).intValue();
                    int hi = ((Number) question.get("max_allowed")).intValue();
                    int chosen = Math.max(lo, Math.min(hi, wanted));
                    if (question.get("allowed_parallelisms") instanceof List<?> allowed && !allowed.isEmpty()) {
                        // Not every value in the range is offered: take the smallest offered one that is
                        // at least what the model asked for.
                        chosen = ((Number) allowed.get(allowed.size() - 1)).intValue();
                        for (Object o : allowed) {
                            if (((Number) o).intValue() >= wanted) {
                                chosen = ((Number) o).intValue();
                                break;
                            }
                        }
                    }
                    var a = new ChoiceAnswer();
                    a.choice = "p" + chosen;
                    a.confidence = 1.0;
                    a.probabilities.put(a.choice, 1.0);
                    out.answers.put(id, a);
                });
        return out;
    }

    private static String shorten(String s) {
        return s == null ? "" : s.substring(0, Math.min(300, s.length()));
    }
}
