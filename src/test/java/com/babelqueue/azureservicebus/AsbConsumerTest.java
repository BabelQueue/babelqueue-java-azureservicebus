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
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
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
     * ASB dead-letters natively (the broker keeps the original body), so the adapter's DLQ-adjacent path is the
     * envelope handed to {@code onError} on a failing handler: a user republishing or annotating it must still
     * see the extras and a {@code dead_letter} block built from it must carry them.
     */
    @Test
    void failingHandlerEnvelopeKeepsUnknownKeysThroughDeadLetterAnnotation() {
        ServiceBusReceiverClient receiver = receiverWith(received(3, jsonWithExtras()));
        Envelope[] failed = { null };
        AsbConsumer.builder(receiver)
            .handler(URN, (env, msg) -> { throw new IllegalStateException("boom"); })
            .onError((e, env, msg) -> failed[0] = env)
            .build()
            .poll();

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
}
