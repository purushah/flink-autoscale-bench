package dev.jevscaler;

import dev.jevscaler.Model.ChoiceAnswer;
import dev.jevscaler.Model.DecisionModel;
import dev.jevscaler.Model.ModelException;
import dev.jevscaler.Model.ModelRequest;
import dev.jevscaler.Model.ModelResponse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;

/**
 * Minimal client for TypeSafe's System One endpoint ({@code POST /v1/systemone}). The API key is
 * only ever placed in the Authorization header; it is never logged.
 */
public final class JevClient implements DecisionModel {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String apiUrl;
    private final String model;
    private final String apiKey;
    private final Duration timeout;
    private final int maxRetries;
    private final HttpClient http;

    public JevClient(String apiUrl, String model, String apiKey, Duration timeout, int maxRetries) {
        this.apiUrl = apiUrl;
        this.model = model;
        this.apiKey = apiKey;
        this.timeout = timeout;
        this.maxRetries = Math.max(0, maxRetries);
        this.http = HttpClient.newBuilder().connectTimeout(timeout).build();
    }

    @Override
    public ModelResponse decide(ModelRequest request) throws ModelException {
        if (apiKey == null || apiKey.isBlank()) {
            throw new ModelException("API key is not set", 0, null);
        }
        byte[] body;
        try {
            var payload = new LinkedHashMap<String, Object>();
            payload.put("model", model);
            payload.put("state", request.state);
            payload.put("questions", request.questions);
            body = MAPPER.writeValueAsBytes(payload);
        } catch (Exception e) {
            throw new ModelException("Could not serialize request: " + e.getMessage(), 0, e);
        }

        long start = System.nanoTime();
        // The whole call, retries included, must finish well inside one reconcile loop.
        long deadline = start + timeout.toNanos() * 2;
        ModelException last = null;
        for (int attempt = 1; attempt <= maxRetries + 1; attempt++) {
            if (attempt > 1) {
                long backoffMs = 200L * (1L << (attempt - 2));
                if (System.nanoTime() + backoffMs * 1_000_000 >= deadline) {
                    break;
                }
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new ModelException("Interrupted while backing off", 0, ie);
                }
            }
            try {
                var response = send(body);
                response.latencyMs = (System.nanoTime() - start) / 1_000_000;
                response.attempts = attempt;
                return response;
            } catch (ModelException e) {
                last = e;
                if (!isRetryable(e.status)) {
                    throw e;
                }
            }
        }
        throw last != null ? last : new ModelException("No attempt was made", 0, null);
    }

    /** 0 means a transport error or timeout; those and overload/rate-limit answers are retried. */
    static boolean isRetryable(int status) {
        return status == 0 || status == 429 || status == 529 || (status >= 500 && status < 600);
    }

    private ModelResponse send(byte[] body) throws ModelException {
        HttpResponse<String> resp;
        try {
            var req =
                    HttpRequest.newBuilder(URI.create(apiUrl))
                            .timeout(timeout)
                            .header("Authorization", "Bearer " + apiKey)
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                            .build();
            resp = http.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ModelException("Interrupted during request", -1, ie);
        } catch (Exception e) {
            throw new ModelException(
                    "Request failed: " + e.getClass().getSimpleName() + ": " + e.getMessage(),
                    0,
                    e);
        }
        if (resp.statusCode() != 200) {
            var text = resp.body() == null ? "" : resp.body();
            throw new ModelException(
                    "HTTP " + resp.statusCode() + ": " + text.substring(0, Math.min(300, text.length())),
                    resp.statusCode(),
                    null);
        }
        try {
            return parse(resp.body());
        } catch (Exception e) {
            throw new ModelException("Could not parse response: " + e.getMessage(), -1, e);
        }
    }

    static ModelResponse parse(String json) throws Exception {
        JsonNode root = MAPPER.readTree(json);
        var out = new ModelResponse();
        out.model = root.path("model").asText(null);
        out.inputTokens = root.path("usage").path("input_tokens").asLong(0);
        out.outputTokens = root.path("usage").path("output_tokens").asLong(0);
        var answers = root.path("answers");
        if (!answers.isObject()) {
            throw new IllegalArgumentException("response has no answers object");
        }
        answers.fields()
                .forEachRemaining(
                        e -> {
                            var node = e.getValue();
                            if (!"choice".equals(node.path("type").asText())) {
                                return;
                            }
                            var a = new ChoiceAnswer();
                            a.choice = node.path("choice").asText(null);
                            a.confidence = node.path("confidence").asDouble(Double.NaN);
                            node.path("probabilities")
                                    .fields()
                                    .forEachRemaining(
                                            p ->
                                                    a.probabilities.put(
                                                            p.getKey(), p.getValue().asDouble(0)));
                            out.answers.put(e.getKey(), a);
                        });
        return out;
    }
}
