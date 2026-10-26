package com.babelqueue.azureservicebus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.core.util.BinaryData;
import com.azure.core.util.IterableStream;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.azure.messaging.servicebus.models.DeadLetterOptions;
import com.babelqueue.BabelQueueException;
import com.babelqueue.DeadLetters;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import com.babelqueue.UnknownUrnException;
import java.math.BigInteger;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Consumer behaviour against a mocked receiver (no broker): attempts = DeliveryCount - 1,
 * Complete on success, Abandon on failure / unmapped URN, and the unknown-URN hooks.
 * Mockito 5's inline mock-maker mocks the final Service Bus types.
 */
class AsbConsumerTest {

    private static final String URN = "urn:babel:orders:created";

    private static String envelopeJson() {
        return EnvelopeCodec.encode(EnvelopeCodec.make(URN, Map.of("order_id", 1), "orders", null));
    }

    private static ServiceBusReceivedMessage received(long deliveryCount, String body) {
        ServiceBusReceivedMessage msg = mock(ServiceBusReceivedMessage.class);
        when(msg.getBody()).thenReturn(BinaryData.fromString(body));
        when(msg.getDeliveryCount()).thenReturn(deliveryCount);
        return msg;
    }

    private static ServiceBusReceiverClient receiverWith(ServiceBusReceivedMessage... messages) {
        ServiceBusReceiverClient receiver = mock(ServiceBusReceiverClient.class);
        when(receiver.receiveMessages(anyInt())).thenReturn(new IterableStream<>(List.of(messages)));
        return receiver;
    }

    @Test
    void attemptsIsDeliveryCountMinusOneAndCompletes() {
        ServiceBusReceiverClient receiver = receiverWith(received(3, envelopeJson()));
        int[] seen = { -1 };
        AsbConsumer consumer = AsbConsumer.builder(receiver)
            .handler(URN, (env, msg) -> seen[0] = env.attempts())
            .build();

        int count = consumer.poll();

        assertEquals(1, count);
        assertEquals(2, seen[0]);
        verify(receiver).complete(any(ServiceBusReceivedMessage.class));
    }

    /** Body carrying a top-level {@code extra_top} and {@code meta.vendor_flag} (ADR-0031). */
    private static String jsonWithExtras() {
        return "{\"job\":\"" + URN + "\",\"trace_id\":\"trace-1\",\"data\":{\"order_id\":7},"
            + "\"meta\":{\"id\":\"m-1\",\"queue\":\"orders\",\"lang\":\"php\",\"schema_version\":1,"
            + "\"created_at\":1700000000000,\"vendor_flag\":true},\"attempts\":0,\"extra_top\":1}";
    }

    @Test
    void deliveryCountReconcilePreservesUnknownTopLevelAndMetaKeys() {
        ServiceBusReceiverClient receiver = receiverWith(received(3, jsonWithExtras())); // attempts 0 -> 2
        Envelope[] seen = { null };
        AsbConsumer.builder(receiver).handler(URN, (env, msg) -> seen[0] = env).build().poll();

        assertEquals(2, seen[0].attempts());
        assertEquals(1L, seen[0].extras().get("extra_top"));
        assertEquals(true, seen[0].meta().extras().get("vendor_flag"));
    }

    /**
     * Core-level lock, not an adapter DLQ test: ASB dead-letters natively and the broker keeps the original
     * body, so this adapter has no path that rewrites the body into a DLQ (its only dead-letter call is the
     * schema-version quarantine, covered by the native {@code deadLetter} tests). What the adapter does own is the envelope it hands to
     * {@code onError}: that must still carry the unknown keys (first two assertions), and the core
     * {@link DeadLetters#annotate} an application builds on it must keep them (remaining assertions, which
     * exercise the core, not this adapter).
     */
    @Test
    void onErrorEnvelopeKeepsUnknownKeysAndCoreDeadLetterAnnotationRetainsThem() {
        ServiceBusReceiverClient receiver = receiverWith(received(3, jsonWithExtras()));
        Envelope[] failed = { null };
        AsbConsumer.builder(receiver)
            .handler(URN, (env, msg) -> { throw new IllegalStateException("boom"); })
            .onError((e, env, msg) -> failed[0] = env)
            .build()
            .poll();

        assertEquals(1L, failed[0].extras().get("extra_top"), "adapter: onError envelope keeps top-level extras");
        assertEquals(true, failed[0].meta().extras().get("vendor_flag"), "adapter: onError envelope keeps meta extras");

        Envelope dead = EnvelopeCodec.decode(EnvelopeCodec.encode(
            DeadLetters.annotate(failed[0], "failed", "orders", failed[0].attempts(), "boom",
                IllegalStateException.class.getName())));
        assertEquals(1L, dead.extras().get("extra_top"));
        assertEquals(true, dead.meta().extras().get("vendor_flag"));
        assertEquals("failed", dead.deadLetter().reason());
        assertEquals("orders", dead.deadLetter().originalQueue());
    }

    @Test
    void firstDeliveryIsZeroAttempts() {
        ServiceBusReceiverClient receiver = receiverWith(received(1, envelopeJson()));
        int[] seen = { -1 };
        AsbConsumer.builder(receiver).handler(URN, (env, msg) -> seen[0] = env.attempts()).build().poll();
        assertEquals(0, seen[0]);
    }

    @Test
    void throwingHandlerAbandonsAndReportsOnError() {
        ServiceBusReceiverClient receiver = receiverWith(received(1, envelopeJson()));
        Throwable[] reported = { null };
        AsbConsumer.builder(receiver)
            .handler(URN, (env, msg) -> { throw new IllegalStateException("boom"); })
            .onError((e, env, msg) -> reported[0] = e)
            .build()
            .poll();

        assertInstanceOf(IllegalStateException.class, reported[0]);
        verify(receiver).abandon(any(ServiceBusReceivedMessage.class));
        verify(receiver, never()).complete(any(ServiceBusReceivedMessage.class));
    }

    @Test
    void unknownUrnWithHookCompletes() {
        ServiceBusReceiverClient receiver = receiverWith(received(1, envelopeJson()));
        boolean[] called = { false };
        AsbConsumer.builder(receiver)
            .onUnknownUrn((env, msg) -> called[0] = true)
            .build()
            .poll();

        assertTrue(called[0]);
        verify(receiver).complete(any(ServiceBusReceivedMessage.class));
    }

    @Test
    void unknownUrnWithoutHookAbandonsAndReportsOnError() {
        ServiceBusReceiverClient receiver = receiverWith(received(1, envelopeJson()));
        Throwable[] reported = { null };
        AsbConsumer.builder(receiver).onError((e, env, msg) -> reported[0] = e).build().poll();

        assertInstanceOf(UnknownUrnException.class, reported[0]);
        verify(receiver).abandon(any(ServiceBusReceivedMessage.class));
    }

    @Test
    void nonConformantEnvelopeAbandonsAndReportsOnError() {
        String badBody = "{\"trace_id\":\"t\",\"data\":{\"x\":1},"
            + "\"meta\":{\"id\":\"m\",\"queue\":\"q\",\"lang\":\"java\",\"schema_version\":1,\"created_at\":1},"
            + "\"attempts\":0}";
        ServiceBusReceiverClient receiver = receiverWith(received(1, badBody));
        Throwable[] reported = { null };
        AsbConsumer.builder(receiver).onError((e, env, msg) -> reported[0] = e).build().poll();

        assertInstanceOf(BabelQueueException.class, reported[0]);
        verify(receiver).abandon(any(ServiceBusReceivedMessage.class));
    }

    @Test
    void maxWaitTimeUsesTimedReceive() {
        ServiceBusReceiverClient receiver = mock(ServiceBusReceiverClient.class);
        when(receiver.receiveMessages(anyInt(), any())).thenReturn(new IterableStream<>(List.of()));

        int count = AsbConsumer.builder(receiver).maxWaitTime(Duration.ofSeconds(2)).maxMessages(5).build().poll();

        assertEquals(0, count);
        verify(receiver).receiveMessages(anyInt(), any());
    }

    @Test
    void runStopsWhenSupplierIsFalse() {
        ServiceBusReceiverClient receiver = receiverWith();
        AsbConsumer.builder(receiver).build().run(() -> false);
        verify(receiver, never()).receiveMessages(anyInt());
    }

    // ---- §4.7 schema-version gate -------------------------------------------------------------

    private static ServiceBusReceivedMessage withVersion(String body, Object version, boolean present) {
        ServiceBusReceivedMessage msg = received(1, body);
        Map<String, Object> props = new HashMap<>();
        if (present) {
            props.put("bq-schema-version", version);
        }
        when(msg.getApplicationProperties()).thenReturn(props);
        return msg;
    }

    @Test
    void schemaVersionGateDecodesWhenMissingBlankOrSupported() {
        Object[][] cases = {
            {null, false}, {"", true}, {" \t", true}, {"\n\u000b\f\r", true}, {"1", true},
            {1, true}, {1L, true}, {(short) 1, true}, {(byte) 1, true},
        };
        for (Object[] c : cases) {
            ServiceBusReceiverClient receiver = receiverWith(withVersion(envelopeJson(), c[0], (Boolean) c[1]));
            int[] handled = {0};
            AsbConsumer.builder(receiver).handler(URN, (env, msg) -> handled[0]++).build().poll();

            assertEquals(1, handled[0], "version=" + c[0]);
            verify(receiver).complete(any(ServiceBusReceivedMessage.class));
            verify(receiver, never()).deadLetter(any(ServiceBusReceivedMessage.class), any(DeadLetterOptions.class));
            verify(receiver, never()).abandon(any(ServiceBusReceivedMessage.class));
        }
    }

    @Test
    void schemaVersionGateDeadLettersUnsupportedWithoutDecodingOrHandler() {
        // BigInteger 2^64 + 1 must not truncate onto 1 (longValue() would).
        Object[] versions = {"2", "x", 2, 2L, 0, 1.5, BigInteger.ONE.shiftLeft(64).add(BigInteger.ONE)};
        for (Object version : versions) {
            // A non-JSON body proves the body is never decoded: decoding it would throw out of poll().
            ServiceBusReceivedMessage message = withVersion("this is not json", version, true);
            ServiceBusReceiverClient receiver = receiverWith(message);

            int[] handled = {0};
            List<Object> envelopes = new ArrayList<>();
            List<Throwable> errors = new ArrayList<>();
            AsbConsumer.builder(receiver)
                .handler(URN, (env, msg) -> handled[0]++)
                .onError((e, env, msg) -> {
                    errors.add(e);
                    envelopes.add(env);
                })
                .build().poll();

            assertEquals(0, handled[0], "version=" + version);
            assertEquals(1, errors.size(), "version=" + version);
            assertInstanceOf(BabelQueueException.class, errors.get(0));
            assertNull(envelopes.get(0), "version=" + version);

            ArgumentCaptor<DeadLetterOptions> options = ArgumentCaptor.forClass(DeadLetterOptions.class);
            verify(receiver, times(1)).deadLetter(eq(message), options.capture());
            assertEquals("unsupported schema version", options.getValue().getDeadLetterReason());
            verify(receiver, never()).abandon(any(ServiceBusReceivedMessage.class));
            verify(receiver, never()).complete(any(ServiceBusReceivedMessage.class));
        }
    }

    @Test
    void schemaVersionGateDeadLettersEvenWhenErrorHandlerThrows() {
        ServiceBusReceivedMessage message = withVersion("this is not json", "2", true);
        ServiceBusReceiverClient receiver = receiverWith(message);
        AsbConsumer consumer = AsbConsumer.builder(receiver)
            .onError((e, env, msg) -> env.traceId())
            .build();

        assertThrows(NullPointerException.class, consumer::poll);

        // The message is settled before the (throwing) callback runs, so it cannot loop to MaxDeliveryCount.
        verify(receiver, times(1)).deadLetter(eq(message), any(DeadLetterOptions.class));
        verify(receiver, never()).abandon(any(ServiceBusReceivedMessage.class));
    }

    @Test
    void schemaVersionGateTruncatesLongRejectedValueInDescriptionAndReport() {
        String value = "9".repeat(5000);
        ServiceBusReceivedMessage message = withVersion("this is not json", value, true);
        ServiceBusReceiverClient receiver = receiverWith(message);
        List<Throwable> errors = new ArrayList<>();
        AsbConsumer.builder(receiver).onError((e, env, msg) -> errors.add(e)).build().poll();

        ArgumentCaptor<DeadLetterOptions> options = ArgumentCaptor.forClass(DeadLetterOptions.class);
        verify(receiver, times(1)).deadLetter(eq(message), options.capture());
        String description = options.getValue().getDeadLetterErrorDescription();
        assertTrue(description.length() < 4096, "description length=" + description.length());
        assertTrue(description.contains("'" + "9".repeat(64) + "...'"), description);
        assertTrue(!description.contains("9".repeat(65)), description);
        assertEquals(1, errors.size());
        assertEquals(description, errors.get(0).getMessage());
    }

    @Test
    void schemaVersionGateKeepsShortRejectedValueIntact() {
        String value = "7".repeat(64);
        ServiceBusReceivedMessage message = withVersion("this is not json", value, true);
        ServiceBusReceiverClient receiver = receiverWith(message);
        AsbConsumer.builder(receiver).build().poll();

        ArgumentCaptor<DeadLetterOptions> options = ArgumentCaptor.forClass(DeadLetterOptions.class);
        verify(receiver).deadLetter(eq(message), options.capture());
        assertTrue(options.getValue().getDeadLetterErrorDescription().contains("'" + value + "'"));
        assertTrue(!options.getValue().getDeadLetterErrorDescription().contains("..."));
    }
    // ---- ADR-0037: poison record, version gate and the DLQ sender --------------------------------

    private static ServiceBusReceiverClient receiverFor(String entity, ServiceBusReceivedMessage... messages) {
        ServiceBusReceiverClient receiver = receiverWith(messages);
        when(receiver.getEntityPath()).thenReturn(entity);
        return receiver;
    }

    private static ServiceBusMessage sentTo(ServiceBusSenderClient sender) {
        ArgumentCaptor<ServiceBusMessage> sent = ArgumentCaptor.forClass(ServiceBusMessage.class);
        verify(sender, times(1)).sendMessage(sent.capture());
        return sent.getValue();
    }

    @Test
    void poisonBodyWithDlqSenderIsWrittenAsTheAdr0035RecordThenCompleted() {
        ServiceBusReceivedMessage message = received(4, "{\"not\":\"an envelope\",\"big\":12345678901234567890}");
        when(message.getMessageId()).thenReturn("m-1");
        ServiceBusReceiverClient receiver = receiverFor("orders", message);
        ServiceBusSenderClient sender = mock(ServiceBusSenderClient.class);
        int[] handled = {0};

        AsbConsumer.builder(receiver).handler(URN, (env, msg) -> handled[0]++).deadLetterSender(sender).build().poll();

        assertEquals(0, handled[0]);
        ServiceBusMessage record = sentTo(sender);
        assertEquals("m-1", record.getMessageId());
        assertEquals("application/json", record.getContentType());
        String text = record.getBody().toString();
        // An object body is spliced: its members stay as written (no re-encoding), plus the dead_letter block.
        assertTrue(text.contains("\"big\":12345678901234567890"), text);
        org.json.JSONObject block = new org.json.JSONObject(text).getJSONObject("dead_letter");
        assertEquals("poison", block.getString("reason"));
        assertEquals("orders", block.getString("original_queue"));
        assertEquals(3, block.getInt("attempts"));
        verify(receiver).complete(message);
        verify(receiver, never()).abandon(any(ServiceBusReceivedMessage.class));
    }

    @Test
    void poisonBytesThatAreNotUtf8AreBase64WrappedExactly() {
        ServiceBusReceivedMessage message = mock(ServiceBusReceivedMessage.class);
        when(message.getBody()).thenReturn(BinaryData.fromBytes(new byte[] {(byte) 0xFF, (byte) 0xFE, 'a'}));
        when(message.getDeliveryCount()).thenReturn(1L);
        ServiceBusReceiverClient receiver = receiverFor("orders/subscriptions/audit", message);
        ServiceBusSenderClient sender = mock(ServiceBusSenderClient.class);

        AsbConsumer.builder(receiver).deadLetterSender(sender).build().poll();

        org.json.JSONObject record = new org.json.JSONObject(sentTo(sender).getBody().toString());
        assertEquals("//5h", record.getString("raw"));
        assertEquals("base64", record.getString("raw_encoding"));
        // The entity of a topic subscription is the topic.
        assertEquals("orders", record.getJSONObject("dead_letter").getString("original_queue"));
        verify(receiver).complete(message);
    }

    @Test
    void validEnvelopeWithOneInvalidUtf8ByteIsPoisonNotHandedToAHandler() {
        // Decoded leniently (U+FFFD) this would be a valid envelope; it must be dead-lettered byte for byte instead.
        String json = EnvelopeCodec.encode(EnvelopeCodec.make(URN, Map.of("note", "ab"), "orders", null));
        byte[] text = json.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        int at = json.indexOf("ab");
        byte[] bad = text.clone();
        bad[at] = (byte) 0xFF; // one invalid byte inside a string value of an otherwise valid envelope
        ServiceBusReceivedMessage message = mock(ServiceBusReceivedMessage.class);
        when(message.getBody()).thenReturn(BinaryData.fromBytes(bad));
        when(message.getDeliveryCount()).thenReturn(1L);
        ServiceBusReceiverClient receiver = receiverFor("orders", message);
        ServiceBusSenderClient sender = mock(ServiceBusSenderClient.class);
        int[] handled = {0};

        AsbConsumer.builder(receiver).handler(URN, (env, msg) -> handled[0]++).deadLetterSender(sender).build().poll();

        assertEquals(0, handled[0]);
        org.json.JSONObject record = new org.json.JSONObject(sentTo(sender).getBody().toString());
        assertEquals("base64", record.getString("raw_encoding"));
        assertEquals(java.util.Base64.getEncoder().encodeToString(bad), record.getString("raw"));
        assertEquals("poison", record.getJSONObject("dead_letter").getString("reason"));
        verify(receiver).complete(message);
    }

    @Test
    void invalidUtf8BodyWithoutDlqSenderIsAbandoned() {
        ServiceBusReceivedMessage message = mock(ServiceBusReceivedMessage.class);
        when(message.getBody()).thenReturn(BinaryData.fromBytes(new byte[] {'{', (byte) 0xFF, '}'}));
        when(message.getDeliveryCount()).thenReturn(1L);
        ServiceBusReceiverClient receiver = receiverFor("orders", message);
        int[] handled = {0};

        AsbConsumer.builder(receiver).handler(URN, (env, msg) -> handled[0]++).build().poll();

        assertEquals(0, handled[0]);
        verify(receiver).abandon(message);
    }

    @Test
    void poisonFailedDlqSendAbandonsAndReports() {
        ServiceBusReceivedMessage message = received(1, "{}");
        ServiceBusReceiverClient receiver = receiverFor("orders", message);
        ServiceBusSenderClient sender = mock(ServiceBusSenderClient.class);
        org.mockito.Mockito.doThrow(new IllegalStateException("dlq down")).when(sender)
            .sendMessage(any(ServiceBusMessage.class));
        List<Throwable> errors = new ArrayList<>();

        AsbConsumer.builder(receiver).deadLetterSender(sender).onError((e, env, m) -> errors.add(e)).build().poll();

        verify(receiver).abandon(message);
        verify(receiver, never()).complete(any(ServiceBusReceivedMessage.class));
        assertEquals(2, errors.size());
        assertEquals("dlq down", errors.get(1).getMessage());
    }

    @Test
    void poisonWithoutDlqSenderIsAbandonedLeftToTheNativeMechanism() {
        ServiceBusReceivedMessage message = received(1, "{}");
        ServiceBusReceiverClient receiver = receiverFor("orders", message);

        AsbConsumer.builder(receiver).build().poll();

        verify(receiver).abandon(message);
        verify(receiver, never()).complete(any(ServiceBusReceivedMessage.class));
        verify(receiver, never()).deadLetter(any(ServiceBusReceivedMessage.class), any(DeadLetterOptions.class));
    }

    @Test
    void versionGatedWithDlqSenderForwardsTheRawBytesThenCompletes() {
        // Deliberately non-canonical: a writer that decodes and re-encodes would change these bytes.
        String raw = "{\"meta\": {\"schema_version\": 2}, \"job\": \"" + URN + "\", \"data\": {\"n\": 1.50}}\n";
        ServiceBusReceivedMessage message = withVersion(raw, 2, true);
        when(message.getContentType()).thenReturn("application/json");
        when(message.getMessageId()).thenReturn("m-9");
        ServiceBusReceiverClient receiver = receiverFor("orders", message);
        ServiceBusSenderClient sender = mock(ServiceBusSenderClient.class);
        int[] handled = {0};
        int[] unknown = {0};
        List<Throwable> errors = new ArrayList<>();

        AsbConsumer.builder(receiver)
            .handler(URN, (env, msg) -> handled[0]++)
            .onUnknownUrn((env, msg) -> unknown[0]++)
            .onError((e, env, m) -> errors.add(e))
            .deadLetterSender(sender)
            .build().poll();

        assertEquals(0, handled[0]);
        assertEquals(0, unknown[0]);
        ServiceBusMessage forwarded = sentTo(sender);
        assertEquals(raw, forwarded.getBody().toString());
        assertEquals("m-9", forwarded.getMessageId());
        assertEquals("application/json", forwarded.getContentType());
        verify(receiver).complete(message);
        verify(receiver, never()).deadLetter(any(ServiceBusReceivedMessage.class), any(DeadLetterOptions.class));
        verify(receiver, never()).abandon(any(ServiceBusReceivedMessage.class));
        assertEquals(1, errors.size());
    }

    @Test
    void versionGatedFailedDlqSendAbandonsAndReports() {
        ServiceBusReceivedMessage message = withVersion("not json", "2", true);
        ServiceBusReceiverClient receiver = receiverFor("orders", message);
        ServiceBusSenderClient sender = mock(ServiceBusSenderClient.class);
        org.mockito.Mockito.doThrow(new IllegalStateException("dlq down")).when(sender)
            .sendMessage(any(ServiceBusMessage.class));
        List<Throwable> errors = new ArrayList<>();

        AsbConsumer.builder(receiver).deadLetterSender(sender).onError((e, env, m) -> errors.add(e)).build().poll();

        verify(receiver).abandon(message);
        verify(receiver, never()).complete(any(ServiceBusReceivedMessage.class));
        verify(receiver, never()).deadLetter(any(ServiceBusReceivedMessage.class), any(DeadLetterOptions.class));
        assertEquals(2, errors.size());
        assertTrue(errors.get(0).getMessage().contains("bq-schema-version"));
        assertEquals("dlq down", errors.get(1).getMessage());
    }

    @Test
    void versionGatedHookOnlyConsumerNeverRunsTheHookAndDeadLettersNatively() {
        // A hook never affects a version-gated delivery (ADR-0037, "Configured strategy only"): it must not Complete.
        ServiceBusReceivedMessage message = withVersion(envelopeJson(), 2, true);
        ServiceBusReceiverClient receiver = receiverFor("orders", message);
        int[] unknown = {0};

        AsbConsumer.builder(receiver).onUnknownUrn((env, msg) -> unknown[0]++).build().poll();

        assertEquals(0, unknown[0]);
        verify(receiver).deadLetter(eq(message), any(DeadLetterOptions.class));
        verify(receiver, never()).complete(any(ServiceBusReceivedMessage.class));
    }
}
