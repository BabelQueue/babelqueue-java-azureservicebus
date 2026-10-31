package com.babelqueue.azureservicebus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** The publisher projects the envelope and returns meta.id — against a mocked sender (no broker). */
class AsbPublisherTest {

    @Test
    void publishProjectsSubjectAndReturnsMessageId() {
        ServiceBusSenderClient sender = mock(ServiceBusSenderClient.class);
        when(sender.getEntityPath()).thenReturn("orders");
        ArgumentCaptor<ServiceBusMessage> captor = ArgumentCaptor.forClass(ServiceBusMessage.class);

        String id = AsbPublisher.create(sender).publish("urn:babel:orders:created", Map.of("order_id", 7), "trace-1");

        verify(sender).sendMessage(captor.capture());
        ServiceBusMessage sent = captor.getValue();
        assertEquals("urn:babel:orders:created", sent.getSubject());
        assertEquals("trace-1", sent.getCorrelationId());
        assertEquals(id, sent.getMessageId());
        assertEquals("application/json", sent.getContentType());
    }

    @Test
    void publishWithDelaySchedulesInsteadOfSends() {
        ServiceBusSenderClient sender = mock(ServiceBusSenderClient.class);
        when(sender.getEntityPath()).thenReturn("orders");
        when(sender.scheduleMessage(any(ServiceBusMessage.class), any(OffsetDateTime.class))).thenReturn(1L);

        AsbPublisher.create(sender).publish("urn:babel:orders:created", Map.of(), null, Duration.ofSeconds(30));

        verify(sender).scheduleMessage(any(ServiceBusMessage.class), any(OffsetDateTime.class));
        verify(sender, never()).sendMessage(any(ServiceBusMessage.class));
    }

    @Test
    void publishTwoArgMintsTrace() {
        ServiceBusSenderClient sender = mock(ServiceBusSenderClient.class);
        when(sender.getEntityPath()).thenReturn("orders");
        ArgumentCaptor<ServiceBusMessage> captor = ArgumentCaptor.forClass(ServiceBusMessage.class);

        AsbPublisher.create(sender).publish("urn:babel:orders:created", Map.of("order_id", 1));

        verify(sender).sendMessage(captor.capture());
        assertEquals("urn:babel:orders:created", captor.getValue().getSubject());
    }

    private static final String TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    private static final String URN = "urn:babel:orders:created";

    private static ServiceBusMessage sendWithHeaders(Envelope env, Map<String, String> headers) {
        ServiceBusSenderClient sender = mock(ServiceBusSenderClient.class);
        when(sender.getEntityPath()).thenReturn("orders");
        ArgumentCaptor<ServiceBusMessage> captor = ArgumentCaptor.forClass(ServiceBusMessage.class);

        AsbPublisher.create(sender).publishWithHeaders(env, headers);

        verify(sender).sendMessage(captor.capture());
        return captor.getValue();
    }

    @Test
    void publishWithHeadersCarriesTraceparentBesideContractPropertiesAndNotInTheBody() {
        Envelope env = EnvelopeCodec.make(URN, Map.of("order_id", 7), "orders", "trace-1");

        ServiceBusMessage sent = sendWithHeaders(env, Map.of("traceparent", TRACEPARENT, "tracestate", "vendor=1"));

        assertEquals(TRACEPARENT, sent.getApplicationProperties().get("traceparent"));
        assertEquals("vendor=1", sent.getApplicationProperties().get("tracestate"));
        assertEquals(env.meta().schemaVersion(), sent.getApplicationProperties().get("bq-schema-version"));
        assertEquals(URN, sent.getSubject());
        String body = sent.getBody().toString();
        assertFalse(body.contains("traceparent"));
        assertFalse(body.contains(TRACEPARENT));
    }

    @Test
    void bodyAndPropertiesAreIdenticalWithoutHeaders() {
        Envelope env = EnvelopeCodec.make(URN, Map.of("order_id", 7), "orders", "trace-1");

        ServiceBusMessage withHeaders = sendWithHeaders(env, Map.of("traceparent", TRACEPARENT));
        ServiceBusMessage plain = sendWithHeaders(env, null);
        ServiceBusMessage empty = sendWithHeaders(env, Map.of("traceparent", ""));

        assertEquals(plain.getBody().toString(), withHeaders.getBody().toString());
        assertEquals(plain.getApplicationProperties(), empty.getApplicationProperties());
        assertFalse(plain.getApplicationProperties().containsKey("traceparent"));
    }

    @Test
    void contractPropertyWinsACollision() {
        Envelope env = EnvelopeCodec.make(URN, Map.of(), "orders", "trace-1");

        ServiceBusMessage sent = sendWithHeaders(env, Map.of("bq-schema-version", "99", "traceparent", TRACEPARENT));

        assertEquals(env.meta().schemaVersion(), sent.getApplicationProperties().get("bq-schema-version"));
        assertEquals(TRACEPARENT, sent.getApplicationProperties().get("traceparent"));
    }

    @Test
    void publishOverloadWithHeadersRidesTheScheduledMessageToo() {
        ServiceBusSenderClient sender = mock(ServiceBusSenderClient.class);
        when(sender.getEntityPath()).thenReturn("orders");
        when(sender.scheduleMessage(any(ServiceBusMessage.class), any(OffsetDateTime.class))).thenReturn(1L);
        ArgumentCaptor<ServiceBusMessage> captor = ArgumentCaptor.forClass(ServiceBusMessage.class);

        AsbPublisher.create(sender)
            .publish(URN, Map.of(), null, Duration.ofSeconds(30), Map.of("traceparent", TRACEPARENT));

        verify(sender).scheduleMessage(captor.capture(), any(OffsetDateTime.class));
        assertEquals(TRACEPARENT, captor.getValue().getApplicationProperties().get("traceparent"));
        assertEquals(30000L, captor.getValue().getApplicationProperties().get("bq-delay"));
    }

    @Test
    void publishWithHeadersMatchesTheOtelHeaderSenderContract() {
        ServiceBusSenderClient sender = mock(ServiceBusSenderClient.class);
        when(sender.getEntityPath()).thenReturn("orders");
        ArgumentCaptor<ServiceBusMessage> captor = ArgumentCaptor.forClass(ServiceBusMessage.class);
        AsbPublisher publisher = AsbPublisher.create(sender);
        com.babelqueue.otel.HeaderSender headerSender = publisher::publishWithHeaders;

        assertDoesNotThrow(() -> headerSender.send(EnvelopeCodec.make(URN, Map.of(), "orders", "t"),
            Map.of("traceparent", TRACEPARENT)));

        verify(sender).sendMessage(captor.capture());
        assertEquals(TRACEPARENT, captor.getValue().getApplicationProperties().get("traceparent"));
    }
}
