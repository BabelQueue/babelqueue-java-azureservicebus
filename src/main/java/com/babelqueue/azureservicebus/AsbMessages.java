package com.babelqueue.azureservicebus;

import com.azure.core.util.BinaryData;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

    /**
     * Adds the out-of-band {@code extra} headers (e.g. {@code traceparent}, ADR-0028) to {@code message} as string
     * {@code ApplicationProperties} beside the contract {@code bq-} properties. Blank keys/values are skipped, a
     * property already present always wins (the contract is never clobbered), and keys are added in sorted order
     * so the result is deterministic. A {@code null}/empty map leaves the message untouched.
     */
    static void addOutOfBand(ServiceBusMessage message, Map<String, String> extra) {
        if (extra == null || extra.isEmpty()) {
            return;
        }
        Map<String, Object> properties = message.getApplicationProperties();
        List<String> keys = new ArrayList<>(extra.keySet());
        keys.removeIf(k -> k == null || k.isEmpty());
        keys.sort(String::compareTo);
        for (String key : keys) {
            String value = extra.get(key);
            if (value != null && !value.isEmpty() && !properties.containsKey(key)) {
                properties.put(key, value);
            }
        }
    }

    /**
     * A delivered message's {@code ApplicationProperties} as a flat {@code Map<String, String>} (string-typed,
     * non-blank values only) — the consume-side counterpart of {@link #addOutOfBand}. Both the contract
     * {@code bq-} properties that are strings and out-of-band ones (e.g. {@code traceparent}) surface; numeric
     * contract properties ({@code bq-schema-version}, {@code bq-created-at}) are not strings and are left out.
     * Empty when the message is {@code null} or has none.
     */
    static Map<String, String> toMap(ServiceBusReceivedMessage message) {
        Map<String, String> out = new LinkedHashMap<>();
        Map<String, Object> properties = message == null ? null : message.getApplicationProperties();
        if (properties == null) {
            return out;
        }
        for (Map.Entry<String, Object> e : properties.entrySet()) {
            if (e.getKey() != null && e.getValue() instanceof CharSequence value && value.length() > 0) {
                out.put(e.getKey(), value.toString());
            }
        }
        return out;
    }
}
