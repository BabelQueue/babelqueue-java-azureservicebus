package com.babelqueue.azureservicebus;

import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Objects;

/**
 * Sends canonical-envelope messages to one Service Bus entity (queue or topic) with the §4
 * native projection — {@code Subject} = URN, {@code CorrelationId} = {@code trace_id},
 * {@code MessageId} = {@code meta.id}, plus the {@code bq-} {@code ApplicationProperties} —
 * so a consumer can route and correlate without decoding the body. The envelope is unchanged
 * ({@code schema_version} stays 1); Azure Service Bus is purely additive.
 *
 * <pre>{@code
 * ServiceBusSenderClient sender = new ServiceBusClientBuilder()
 *     .connectionString(cs).sender().queueOrTopicName("orders").buildClient();
 * String id = AsbPublisher.create(sender).publish("urn:babel:orders:created", Map.of("order_id", 1042));
 * }</pre>
 */
public final class AsbPublisher {

    private final ServiceBusSenderClient sender;

    private AsbPublisher(ServiceBusSenderClient sender) {
        this.sender = Objects.requireNonNull(sender, "sender");
    }

    /** A publisher over the given sender (one queue or topic). */
    public static AsbPublisher create(ServiceBusSenderClient sender) {
        return new AsbPublisher(sender);
    }

    /** Publish {@code (urn, data)} as a canonical envelope; returns the message id ({@code meta.id}). */
    public String publish(String urn, Map<String, Object> data) {
        return publish(urn, data, null, null);
    }

    /** Publish, continuing an existing {@code traceId} (or {@code null} to mint a fresh one). */
    public String publish(String urn, Map<String, Object> data, String traceId) {
        return publish(urn, data, traceId, null);
    }

    /**
     * Publish with an optional relative {@code delay} — scheduled natively via
     * {@code ScheduledEnqueueTime} (the broker withholds the message until {@code now + delay}).
     */
    public String publish(String urn, Map<String, Object> data, String traceId, Duration delay) {
        Envelope envelope = EnvelopeCodec.make(urn, data, sender.getEntityPath(), traceId);
        ServiceBusMessage message = AsbMessages.project(envelope);

        if (delay != null && !delay.isZero() && !delay.isNegative()) {
            message.getApplicationProperties().put("bq-delay", delay.toMillis());
            sender.scheduleMessage(message, OffsetDateTime.now(ZoneOffset.UTC).plus(delay));
        } else {
            sender.sendMessage(message);
        }
        return envelope.meta().id();
    }
}
