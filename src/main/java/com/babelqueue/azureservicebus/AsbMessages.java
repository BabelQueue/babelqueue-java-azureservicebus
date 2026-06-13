package com.babelqueue.azureservicebus;

import com.azure.core.util.BinaryData;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;

/**
 * Projects the envelope's contract fields onto native Service Bus message fields —
 * {@code Subject} = URN, {@code CorrelationId} = {@code trace_id}, {@code MessageId} =
 * {@code meta.id}, plus the {@code bq-} {@code ApplicationProperties}. The body stays
 * authoritative (Contract §4.2–§4.3).
 */
final class AsbMessages {

    private AsbMessages() {}

    static ServiceBusMessage project(Envelope envelope) {
        ServiceBusMessage message =
            new ServiceBusMessage(BinaryData.fromString(EnvelopeCodec.encode(envelope)))
                .setContentType("application/json")
                .setSubject(envelope.job())
                .setCorrelationId(envelope.traceId());

        if (envelope.meta() != null) {
            if (envelope.meta().id() != null && !envelope.meta().id().isEmpty()) {
                message.setMessageId(envelope.meta().id());
            }
            message.getApplicationProperties().put("bq-schema-version", envelope.meta().schemaVersion());
            if (envelope.meta().lang() != null && !envelope.meta().lang().isEmpty()) {
                message.getApplicationProperties().put("bq-source-lang", envelope.meta().lang());
            }
            message.getApplicationProperties().put("bq-created-at", envelope.meta().createdAt());
        }
        return message;
    }
}
