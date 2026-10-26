package com.babelqueue.azureservicebus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.core.util.BinaryData;
import com.azure.core.util.IterableStream;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.azure.messaging.servicebus.models.DeadLetterOptions;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Azure Service Bus binding conformance against the vendored canonical suite's {@code asb}
 * block: the §4 native projection and the {@code attempts = max(body, DeliveryCount - 1)}
 * reconciliation, plus the ADR-0037 {@code version_gate_asb} settle rule and the ADR-0035 {@code poison_dlq} record
 * shape driven through this consumer. The final Service Bus types are mocked with Mockito — no Azure, no network.
 */
class AsbConformanceTest {

    private static final String URN = "urn:babel:orders:created";

    /** Marker for a present-but-null application property (distinct from an absent one). */
    private static final Object NULL_PROPERTY = new Object();

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

    @Test
    void schemaVersionGateMatchesGolden() throws Exception {
        JSONObject gate = asbBlock().getJSONObject("schema_version_gate");
        String property = gate.getString("property");
        String fixture = resource(gate.getString("fixture"));
        JSONArray cases = gate.getJSONArray("cases");
        assertTrue(cases.length() > 0);

        for (int i = 0; i < cases.length(); i++) {
            JSONObject testCase = cases.getJSONObject(i);
            boolean decode = "decode".equals(testCase.getString("expect"));
            // A JSON number stands for a native AMQP integer: exercise integral values at int32 and int64
            // width (a non-integral number is a double).
            String valueType = testCase.optString("value_type", "");
            assertTrue(valueType.isEmpty() || "float".equals(valueType),
                "unknown value_type '" + valueType + "' (this reader only understands \"float\")");
            List<Object> variants = new ArrayList<>();
            if (testCase.has("value") && testCase.get("value") instanceof Number number) {
                if ("float".equals(testCase.optString("value_type"))) {
                    // An explicit floating-point case (e.g. 1.0): hand over real floats, never integers.
                    variants.add(number.doubleValue());
                    variants.add(number.floatValue());
                } else if (number.doubleValue() == Math.rint(number.doubleValue())) {
                    variants.add(number.intValue());
                    variants.add(number.longValue());
                } else {
                    variants.add(number.doubleValue());
                }
            } else if (testCase.has("value") && testCase.isNull("value")) {
                // A present-but-null property: must behave exactly like an absent one (decode).
                variants.add(NULL_PROPERTY);
            } else if (testCase.has("value")) {
                variants.add(testCase.getString("value"));
            } else {
                variants.add(null);
            }

            for (Object variant : variants) {
                String label = (testCase.has("value") ? "value=" + testCase.get("value") : "absent")
                    + " (" + (variant == null ? "none" : variant == NULL_PROPERTY ? "null" : variant.getClass().getSimpleName()) + ")";
                ServiceBusReceivedMessage msg = mock(ServiceBusReceivedMessage.class);
                when(msg.getBody()).thenReturn(BinaryData.fromString(fixture));
                when(msg.getDeliveryCount()).thenReturn(1L);
                Map<String, Object> props = new HashMap<>();
                if (variant == NULL_PROPERTY) {
                    props.put(property, null);
                } else if (variant != null) {
                    props.put(property, variant);
                }
                when(msg.getApplicationProperties()).thenReturn(props);

                ServiceBusReceiverClient receiver = mock(ServiceBusReceiverClient.class);
                when(receiver.receiveMessages(anyInt())).thenReturn(new IterableStream<>(List.of(msg)));

                int[] handled = {0};
                AsbConsumer.builder(receiver)
                    .handler(URN, (env, message) -> handled[0]++)
                    .build()
                    .poll();

                assertEquals(decode ? 1 : 0, handled[0], label);
                verify(receiver, times(decode ? 1 : 0)).complete(any(ServiceBusReceivedMessage.class));
                verify(receiver, times(decode ? 0 : 1))
                    .deadLetter(any(ServiceBusReceivedMessage.class), any(DeadLetterOptions.class));
            }
        }
    }
    // ---- poison_dlq (ADR-0035 shape, ADR-0037 decision 2) -----------------------------------------

    @Test
    void poisonDlqRecordShapeMatchesGolden() throws Exception {
        JSONArray cases = new JSONObject(resource("manifest.json")).getJSONObject("poison_dlq").getJSONArray("cases");
        assertTrue(cases.length() > 0);
        for (int i = 0; i < cases.length(); i++) {
            JSONObject testCase = cases.getJSONObject(i);
            String name = testCase.getString("name");
            ServiceBusReceivedMessage msg = mock(ServiceBusReceivedMessage.class);
            // Service Bus carries raw bytes, so every case (invalid UTF-8 included) goes through the consumer.
            when(msg.getBody()).thenReturn(BinaryData.fromBytes(PoisonRecordChecks.bodyOf(testCase)));
            when(msg.getDeliveryCount()).thenReturn(1L);
            ServiceBusReceiverClient receiver = mock(ServiceBusReceiverClient.class);
            when(receiver.receiveMessages(anyInt())).thenReturn(new IterableStream<>(List.of(msg)));
            when(receiver.getEntityPath()).thenReturn("orders");
            ServiceBusSenderClient sender = mock(ServiceBusSenderClient.class);
            int[] handled = {0};

            AsbConsumer.builder(receiver)
                .handler(URN, (env, message) -> handled[0]++)
                .deadLetterSender(sender)
                .build()
                .poll();

            assertEquals(0, handled[0], name);
            ArgumentCaptor<ServiceBusMessage> sent = ArgumentCaptor.forClass(ServiceBusMessage.class);
            verify(sender, times(1)).sendMessage(sent.capture());
            verify(receiver, times(1)).complete(msg);
            PoisonRecordChecks.check(testCase, sent.getValue().getBody().toString());
        }
    }

    // ---- version_gate_asb (ADR-0037 decision 1) ---------------------------------------------------

    @Test
    void versionGateSettleRuleMatchesGolden() throws Exception {
        JSONObject section = new JSONObject(resource("manifest.json")).getJSONObject("version_gate_asb");
        JSONArray cases = section.getJSONArray("cases");
        JSONArray bodies = section.getJSONArray("incoming_bodies");
        assertEquals(17, cases.length());
        int asserted = 0;
        for (int i = 0; i < cases.length(); i++) {
            JSONObject testCase = cases.getJSONObject(i);
            if (!"fail".equals(testCase.optString("on_unknown_urn", "fail"))) {
                // The Java Azure Service Bus consumer has only an onUnknownUrn callback and no strategy option (it
                // behaves as `fail`): the suite tells Java, .NET and Node runners to skip these rows, never to pass
                // them.
                continue;
            }
            asserted++;
            for (int b = 0; b < bodies.length(); b++) {
                runVersionGateCase(testCase, bodies.getJSONObject(b).getString("body"));
            }
        }
        assertTrue(asserted > 0);
    }

    private static void runVersionGateCase(JSONObject testCase, String body) {
        String label = testCase.getString("name");
        boolean deadLetter = "on".equals(testCase.getString("dead_letter"));
        JSONObject expect = testCase.getJSONObject("expect");

        ServiceBusReceivedMessage msg = mock(ServiceBusReceivedMessage.class);
        when(msg.getBody()).thenReturn(BinaryData.fromString(body));
        when(msg.getDeliveryCount()).thenReturn(testCase.has("bq_attempts") ? 8L : 1L);
        Map<String, Object> props = new HashMap<>();
        props.put("bq-schema-version", 2);
        if (testCase.has("bq_attempts")) {
            props.put("bq-attempts", testCase.getString("bq_attempts"));
        }
        when(msg.getApplicationProperties()).thenReturn(props);
        ServiceBusReceiverClient receiver = mock(ServiceBusReceiverClient.class);
        when(receiver.receiveMessages(anyInt())).thenReturn(new IterableStream<>(List.of(msg)));
        ServiceBusSenderClient sender = mock(ServiceBusSenderClient.class);
        if ("fails".equals(testCase.optString("dlq_write", "ok"))) {
            org.mockito.Mockito.doThrow(new IllegalStateException("dlq write failed")).when(sender)
                .sendMessage(any(ServiceBusMessage.class));
        }
        int[] handled = {0};
        AsbConsumer.Builder builder = AsbConsumer.builder(receiver).handler(URN, (env, message) -> handled[0]++);
        if (deadLetter) {
            builder.deadLetterSender(sender);
        }
        builder.build().poll();

        assertEquals(0, handled[0], label);
        // "off" means no DLQ writer exists in this consumer (no sender): outcome_no_writer applies.
        String outcome = !deadLetter && expect.has("outcome_no_writer")
            ? expect.getString("outcome_no_writer") : expect.getString("outcome");
        switch (outcome) {
            case "dlq_raw" -> {
                ArgumentCaptor<ServiceBusMessage> sent = ArgumentCaptor.forClass(ServiceBusMessage.class);
                verify(sender, times(1)).sendMessage(sent.capture());
                assertEquals(body, sent.getValue().getBody().toString(), label);
                assertTrue(!sent.getValue().getBody().toString().contains("\"dead_letter\""), label);
                verify(receiver, times(1)).complete(msg);
                verify(receiver, never()).abandon(any(ServiceBusReceivedMessage.class));
            }
            case "unsettled" -> {
                verify(receiver, times(1)).abandon(msg);
                verify(receiver, never()).complete(any(ServiceBusReceivedMessage.class));
            }
            case "native" -> {
                // An explicit native DeadLetter (this consumer's choice) or an Abandon: never Complete, never SDK retry.
                verify(sender, never()).sendMessage(any(ServiceBusMessage.class));
                verify(receiver, never()).complete(any(ServiceBusReceivedMessage.class));
                verify(receiver, times(1)).deadLetter(any(ServiceBusReceivedMessage.class), any(DeadLetterOptions.class));
            }
            default -> throw new AssertionError("unknown outcome in " + label + ": " + outcome);
        }
    }
}
