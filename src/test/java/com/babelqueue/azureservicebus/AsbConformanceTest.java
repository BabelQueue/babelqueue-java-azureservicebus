package com.babelqueue.azureservicebus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.azure.core.util.BinaryData;
import com.azure.core.util.IterableStream;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

/**
 * Azure Service Bus binding conformance against the vendored canonical suite's {@code asb}
 * block: the §4 native projection and the {@code attempts = max(body, DeliveryCount - 1)}
 * reconciliation. The final Service Bus types are mocked with Mockito — no Azure, no network.
 */
class AsbConformanceTest {

    private static final String URN = "urn:babel:orders:created";

    private static String resource(String path) throws Exception {
        try (InputStream in = AsbConformanceTest.class.getResourceAsStream("/conformance/" + path)) {
            if (in == null) {
                throw new IllegalStateException("vendored conformance resource missing: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static JSONObject asbBlock() throws Exception {
        return new JSONObject(resource("manifest.json")).getJSONObject("asb");
    }

    @Test
    void propertyProjectionMatchesGolden() throws Exception {
        JSONObject projection = asbBlock().getJSONObject("property_projection");
        Envelope envelope = EnvelopeCodec.decode(resource(projection.getString("envelope_file")));
        ServiceBusMessage msg = AsbMessages.project(envelope);

        JSONObject message = projection.getJSONObject("message");
        assertEquals(message.getString("subject"), msg.getSubject());
        assertEquals(message.getString("correlation_id"), msg.getCorrelationId());
        assertEquals(message.getString("message_id"), msg.getMessageId());
        assertEquals(message.getString("content_type"), msg.getContentType());

        JSONObject want = projection.getJSONObject("application_properties");
        Map<String, Object> got = msg.getApplicationProperties();
        assertEquals(want.keySet(), got.keySet());
        for (String key : want.keySet()) {
            // Compare as strings — golden ints/longs and the projected boxed values render alike.
            assertEquals(String.valueOf(want.get(key)), String.valueOf(got.get(key)), key);
        }
    }

    @Test
    void attemptsReconciliationMatchesGolden() throws Exception {
        JSONArray cases = asbBlock().getJSONObject("attempts_reconciliation").getJSONArray("cases");
        for (int i = 0; i < cases.length(); i++) {
            JSONObject testCase = cases.getJSONObject(i);
            Envelope base = EnvelopeCodec.make(URN, Map.of("x", 1), "orders", null);
            Envelope bumped = new Envelope(
                base.job(), base.traceId(), base.data(), base.meta(), testCase.getInt("body_attempts"), null);

            ServiceBusReceivedMessage msg = mock(ServiceBusReceivedMessage.class);
            when(msg.getBody()).thenReturn(BinaryData.fromString(EnvelopeCodec.encode(bumped)));
            when(msg.getDeliveryCount()).thenReturn((long) testCase.getInt("delivery_count"));

            ServiceBusReceiverClient receiver = mock(ServiceBusReceiverClient.class);
            when(receiver.receiveMessages(anyInt())).thenReturn(new IterableStream<>(List.of(msg)));

            int[] seen = {-1};
            AsbConsumer.builder(receiver)
                .handler(URN, (env, message) -> seen[0] = env.attempts())
                .build()
                .poll();

            assertEquals(testCase.getInt("expected_attempts"), seen[0], testCase.getString("name"));
        }
    }
}
