package com.babelqueue.azureservicebus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
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
}
