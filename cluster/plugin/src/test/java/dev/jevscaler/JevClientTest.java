package dev.jevscaler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.jevscaler.Model.ModelException;
import dev.jevscaler.Model.ModelRequest;

import com.sun.net.httpserver.HttpServer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

class JevClientTest {

    private static final String OK =
            "{\"model\":\"jev-1.14.0\",\"answers\":{\"v1\":{\"type\":\"choice\",\"choice\":\"p4\",\"confidence\":0.9,"
                    + "\"probabilities\":{\"p4\":0.9,\"p3\":0.1}}},\"usage\":{\"input_tokens\":1200,\"output_tokens\":80}}";

    private HttpServer server;
    private final AtomicInteger calls = new AtomicInteger();
    private final List<Integer> statuses = new ArrayList<>();
    private volatile String lastAuth;
    private volatile String lastBody;
    private volatile long delayMs;

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/v1/systemone",
                exchange -> {
                    int n = calls.getAndIncrement();
                    lastAuth = exchange.getRequestHeaders().getFirst("Authorization");
                    lastBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ignored) {
                        // Test server only.
                    }
                    int status = n < statuses.size() ? statuses.get(n) : 200;
                    byte[] body = (status == 200 ? OK : "{\"error\":\"nope\"}").getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(status, body.length);
                    exchange.getResponseBody().write(body);
                    exchange.close();
                });
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private JevClient client(Duration timeout, int retries) {
        var url = "http://127.0.0.1:" + server.getAddress().getPort() + "/v1/systemone";
        return new JevClient(url, "jev-1.14.0", "secret-key", timeout, retries);
    }

    private static ModelRequest request() {
        var r = new ModelRequest();
        r.state = Map.of("job", "x");
        r.questions.put("v1", Map.of("type", "choice", "instructions", "pick", "criteria", Map.of("p3", "a", "p4", "b")));
        return r;
    }

    @Test
    void sendsBearerKeyAndParsesTheAnswer() throws Exception {
        var response = client(Duration.ofSeconds(2), 0).decide(request());
        assertEquals("Bearer secret-key", lastAuth);
        assertTrue(lastBody.contains("\"model\":\"jev-1.14.0\""));
        assertEquals("p4", response.answers.get("v1").choice);
        assertEquals(0.1, response.answers.get("v1").probabilities.get("p3"), 1e-9);
        assertEquals(1200, response.inputTokens);
    }

    @Test
    void retriesOverloadAndRateLimitAnswers() throws Exception {
        statuses.addAll(List.of(529, 429));
        var response = client(Duration.ofSeconds(2), 2).decide(request());
        assertEquals(3, calls.get());
        assertEquals(3, response.attempts);
    }

    @Test
    void doesNotRetryClientErrors() {
        statuses.add(401);
        var e = assertThrows(ModelException.class, () -> client(Duration.ofSeconds(2), 2).decide(request()));
        assertEquals(401, e.status);
        assertEquals(1, calls.get());
    }

    @Test
    void timesOutInsteadOfBlockingTheAutoscaler() {
        delayMs = 1500;
        long start = System.nanoTime();
        assertThrows(ModelException.class, () -> client(Duration.ofMillis(300), 1).decide(request()));
        assertTrue((System.nanoTime() - start) / 1_000_000 < 1400, "must give up within its deadline");
    }

    @Test
    void missingKeyFailsWithoutAnyRequest() {
        var c = new JevClient("http://127.0.0.1:1/v1/systemone", "m", "", Duration.ofSeconds(1), 0);
        assertThrows(ModelException.class, () -> c.decide(request()));
        assertEquals(0, calls.get());
    }
}
