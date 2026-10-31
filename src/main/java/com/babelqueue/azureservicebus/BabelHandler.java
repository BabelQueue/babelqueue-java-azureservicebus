package com.babelqueue.azureservicebus;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.babelqueue.Envelope;
import java.util.Map;

/**
 * Processes one decoded, validated envelope and the raw Service Bus message it arrived on.
 *
 * <p>The raw message carries the out-of-band transport headers (ADR-0028) as {@code ApplicationProperties}.
 * {@link #headersOf} surfaces them as a plain map, ready for the optional OpenTelemetry wrapper so the
 * consumer span becomes a true child of the producer span:
 *
 * <pre>{@code
 * AsbConsumer.builder(receiver)
 *     .handler(urn, (env, message) -> Tracing
 *         .wrapHandler(tracer, h, () -> BabelHandler.headersOf(message))
 *         .handle(env))
 *     .build();
 * }</pre>
 */
@FunctionalInterface
public interface BabelHandler {

    /**
     * Handle a message. Returning normally acknowledges it (the consumer {@code Complete}s it);
     * throwing leaves it for the broker to redeliver (the consumer {@code Abandon}s it,
     * incrementing {@code DeliveryCount}).
     */
    void handle(Envelope envelope, ServiceBusReceivedMessage message) throws Exception;

    /**
     * The message's string-typed {@code ApplicationProperties} as a {@code Map<String, String>} (non-blank values
     * only) — the delivered-headers {@code Supplier} source for {@code com.babelqueue.otel.Tracing#wrapHandler}.
     * Contains any out-of-band header (e.g. {@code traceparent}, {@code tracestate}). Empty when {@code message}
     * is {@code null}. Needs no OpenTelemetry dependency.
     */
    static Map<String, String> headersOf(ServiceBusReceivedMessage message) {
        return AsbMessages.toMap(message);
    }
}
