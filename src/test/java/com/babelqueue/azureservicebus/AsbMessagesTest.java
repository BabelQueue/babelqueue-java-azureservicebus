package com.babelqueue.azureservicebus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.azure.messaging.servicebus.ServiceBusMessage;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * §4 native projection (no broker): Subject = URN, CorrelationId = trace_id,
 * MessageId = meta.id, plus the bq- ApplicationProperties; the body stays the
 * canonical envelope.
 */
class AsbMessagesTest {

    private static Envelope sample() {
        return EnvelopeCodec.make("urn:babel:orders:created", Map.of("order_id", 1042), "orders", "trace-xyz");
    }

    @Test
    void projectsNativeFieldsAndApplicationProperties() {
        Envelope env = sample();
        ServiceBusMessage msg = AsbMessages.project(env);

        assertEquals("urn:babel:orders:created", msg.getSubject());
        assertEquals("trace-xyz", msg.getCorrelationId());
        assertEquals(env.meta().id(), msg.getMessageId());
        assertEquals("application/json", msg.getContentType());
        assertEquals(env.meta().schemaVersion(), msg.getApplicationProperties().get("bq-schema-version"));
        assertEquals(env.meta().lang(), msg.getApplicationProperties().get("bq-source-lang"));
        assertEquals(env.meta().createdAt(), msg.getApplicationProperties().get("bq-created-at"));
    }

    @Test
    void bodyIsTheCanonicalEnvelope() {
        ServiceBusMessage msg = AsbMessages.project(sample());
        Envelope decoded = EnvelopeCodec.decode(msg.getBody().toString());
        assertTrue(EnvelopeCodec.accepts(decoded));
        assertEquals("urn:babel:orders:created", EnvelopeCodec.urn(decoded));
    }
}
