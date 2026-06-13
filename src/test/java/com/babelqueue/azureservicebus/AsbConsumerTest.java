package com.babelqueue.azureservicebus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.core.util.BinaryData;
import com.azure.core.util.IterableStream;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.babelqueue.BabelQueueException;
import com.babelqueue.EnvelopeCodec;
import com.babelqueue.UnknownUrnException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

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
}
