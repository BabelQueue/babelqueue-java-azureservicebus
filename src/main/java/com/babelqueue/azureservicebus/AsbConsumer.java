package com.babelqueue.azureservicebus;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.models.DeadLetterOptions;
import com.babelqueue.BabelQueueException;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import com.babelqueue.UnknownUrnException;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;

/**
 * Receives from a Service Bus entity in {@code PeekLock} mode, decodes and validates each
 * message, routes it to the handler registered for its URN, and {@code Complete}s it on
 * success. A throwing handler {@code Abandon}s the message — the broker redelivers it and
 * increments {@code DeliveryCount} (at-least-once). {@code attempts} is reconciled to
 * {@code DeliveryCount - 1} (broker-authoritative on ASB) for the handler. The poll loop never
 * stops on a bad message — observe via {@code onError}/{@code onUnknownUrn}.
 *
 * <p>Per §4.7 the {@code bq-schema-version} application property is checked <b>before</b> the body is
 * decoded: when it is present and is not the schema version the core supports
 * ({@link EnvelopeCodec#SCHEMA_VERSION}), the body is never decoded or routed. Producers write an
 * integer {@code 1}, so any integral numeric type with value {@code 1} and the string {@code "1"}
 * (compared exactly as sent, no trimming) are accepted; every other number ({@code 2}, {@code 0},
 * {@code 1.5}), string ({@code "2"}, {@code "x"}, {@code "01"}, {@code " 1"}) or type is rejected.
 * A rejected message is explicitly <b>dead-lettered</b> (reason {@code unsupported schema version}) —
 * never abandoned, which would redeliver it until {@code MaxDeliveryCount} — and {@code onError} is
 * notified (with a {@code null} envelope). A missing, {@code null}, or blank string (empty or
 * ASCII-whitespace-only: space, tab, LF, VT, FF, CR) property changes nothing, and a supported
 * version still goes through the post-decode {@link EnvelopeCodec#accepts} check.
 */
public final class AsbConsumer {

    private static final String SCHEMA_VERSION_PROPERTY = "bq-schema-version";
    private static final int MAX_REPORTED_VALUE_LENGTH = 64;
    private static final String SCHEMA_VERSION_DEAD_LETTER_REASON = "unsupported schema version";

    /**
     * Notified of a non-conformant envelope, an unmapped URN (no {@code onUnknownUrn}), a throwing handler,
     * or a message rejected by the §4.7 schema-version gate. {@code envelope} is {@code null} when the body
     * was not decoded (a version-gated message) — guard against it before dereferencing.
     */
    @FunctionalInterface
    public interface ErrorHandler {
        void onError(Throwable error, Envelope envelope, ServiceBusReceivedMessage message);
    }

    /** Called instead of erroring when a URN has no handler; the message is then Completed. */
    @FunctionalInterface
    public interface UnknownUrnHandler {
        void onUnknownUrn(Envelope envelope, ServiceBusReceivedMessage message);
    }

    private final ServiceBusReceiverClient receiver;
    private final Map<String, BabelHandler> handlers;
    private final int maxMessages;
    private final Duration maxWaitTime;
    private final ErrorHandler onError;
    private final UnknownUrnHandler onUnknownUrn;

    private AsbConsumer(Builder builder) {
        this.receiver = builder.receiver;
        this.handlers = Map.copyOf(builder.handlers);
        this.maxMessages = builder.maxMessages;
        this.maxWaitTime = builder.maxWaitTime;
        this.onError = builder.onError;
        this.onUnknownUrn = builder.onUnknownUrn;
    }

    public static Builder builder(ServiceBusReceiverClient receiver) {
        return new Builder(receiver);
    }

    /** Receive one batch, route each message, settle each. Returns the batch size. */
    public int poll() {
        int count = 0;
        Iterable<ServiceBusReceivedMessage> messages = maxWaitTime != null
            ? receiver.receiveMessages(maxMessages, maxWaitTime)
            : receiver.receiveMessages(maxMessages);
        for (ServiceBusReceivedMessage message : messages) {
            handle(message);
            count++;
        }
        return count;
    }

    /** Poll until the current thread is interrupted. */
    public void run() {
        run(() -> !Thread.currentThread().isInterrupted());
    }

    /** Poll while {@code shouldContinue} returns true. */
    public void run(BooleanSupplier shouldContinue) {
        while (shouldContinue.getAsBoolean()) {
            poll();
        }
    }

    private void handle(ServiceBusReceivedMessage message) {
        if (!passesSchemaVersionGate(message)) {
            Object declared = message.getApplicationProperties().get(SCHEMA_VERSION_PROPERTY);
            String description = "Rejected a BabelQueue message with unsupported " + SCHEMA_VERSION_PROPERTY
                + " property '" + truncateForReport(declared) + "' (supported: " + EnvelopeCodec.SCHEMA_VERSION + "); body not decoded.";
            // Settle first: the message is terminal, so a throwing onError must not leave it unsettled.
            receiver.deadLetter(message, new DeadLetterOptions()
                .setDeadLetterReason(SCHEMA_VERSION_DEAD_LETTER_REASON)
                .setDeadLetterErrorDescription(description));
            report(new BabelQueueException(description), null, message);
            return;
        }

        String body = message.getBody() == null ? "" : message.getBody().toString();
        Envelope envelope = reconcile(EnvelopeCodec.decode(body), message.getDeliveryCount());

        if (!EnvelopeCodec.accepts(envelope)) {
            report(new BabelQueueException("Rejected a non-conformant BabelQueue envelope from Azure Service Bus."),
                envelope, message);
            receiver.abandon(message);
            return;
        }

        String urn = EnvelopeCodec.urn(envelope);
        BabelHandler handler = handlers.get(urn);
        if (handler == null) {
            if (onUnknownUrn != null) {
                onUnknownUrn.onUnknownUrn(envelope, message);
                receiver.complete(message);
            } else {
                report(new UnknownUrnException(urn), envelope, message);
                receiver.abandon(message);
            }
            return;
        }

        try {
            handler.handle(envelope, message);
            receiver.complete(message);
        } catch (Exception error) {
            // Abandon releases the lock — the broker redelivers and increments DeliveryCount.
            report(error, envelope, message);
            receiver.abandon(message);
        }
    }

    /**
     * Renders a rejected property value for the dead-letter description and the error message: at most the first
     * {@value #MAX_REPORTED_VALUE_LENGTH} characters, followed by {@code ...} when cut. Service Bus caps the
     * dead-letter description at 4096 characters, so an unbounded echo of a hostile value would make
     * {@code deadLetter} itself throw.
     */
    private static String truncateForReport(Object value) {
        String text = String.valueOf(value);
        if (text.length() <= MAX_REPORTED_VALUE_LENGTH) {
            return text;
        }
        return text.substring(0, MAX_REPORTED_VALUE_LENGTH) + "...";
    }

    /**
     * The §4.7 gate: {@code true} when the message may be decoded (property absent, {@code null}, a blank
     * string, integral {@code 1} or the string {@code "1"}), {@code false} when it must be rejected undecoded.
     */
    private static boolean passesSchemaVersionGate(ServiceBusReceivedMessage message) {
        Map<String, Object> properties = message.getApplicationProperties();
        Object declared = properties == null ? null : properties.get(SCHEMA_VERSION_PROPERTY);
        if (declared == null) {
            return true;
        }
        if (declared instanceof CharSequence text) {
            String value = text.toString();
            return isBlankSchemaVersion(value) || value.equals(Integer.toString(EnvelopeCodec.SCHEMA_VERSION));
        }
        if (declared instanceof Float || declared instanceof Double || declared instanceof BigDecimal) {
            return false;
        }
        if (declared instanceof Byte || declared instanceof Short || declared instanceof Integer
            || declared instanceof Long) {
            return ((Number) declared).longValue() == EnvelopeCodec.SCHEMA_VERSION;
        }
        if (declared instanceof Number number) {
            // Other integral widths (e.g. the AMQP unsigned types, BigInteger): compare the exact decimal
            // form rather than longValue(), which would truncate out-of-range values onto 1.
            return number.toString().equals(Integer.toString(EnvelopeCodec.SCHEMA_VERSION));
        }
        return false;
    }

    /**
     * The shared cross-SDK "blank" definition for {@code bq-schema-version}: the empty string, or a value made up
     * <b>only</b> of ASCII whitespace ({@code ' '}, {@code \t}, {@code \n}, U+000B, {@code \f}, {@code \r}). Anything else
     * (NBSP, U+001C–U+001F, U+0085, U+FEFF, …) is <b>not</b> blank, unlike {@link String#isBlank()}.
     */
    private static boolean isBlankSchemaVersion(String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c != ' ' && (c < '\t' || c > '\r')) {
                return false;
            }
        }
        return true;
    }

    /**
     * Set {@code attempts} to {@code max(current, DeliveryCount - 1)}. {@code DeliveryCount} is
     * 1-based and broker-authoritative on ASB (first delivery = 1 → attempts 0); the max never
     * lowers a higher body count carried by a message republished from another SDK.
     */
    private static Envelope reconcile(Envelope envelope, long deliveryCount) {
        int nativeAttempts = (int) deliveryCount - 1;
        if (nativeAttempts <= envelope.attempts()) {
            return envelope;
        }
        return envelope.withAttempts(nativeAttempts);
    }

    private void report(Throwable error, Envelope envelope, ServiceBusReceivedMessage message) {
        if (onError != null) {
            onError.onError(error, envelope, message);
        }
    }

    /** Fluent builder for {@link AsbConsumer}. */
    public static final class Builder {
        private final ServiceBusReceiverClient receiver;
        private final Map<String, BabelHandler> handlers = new HashMap<>();
        private int maxMessages = 10;
        private Duration maxWaitTime;
        private ErrorHandler onError;
        private UnknownUrnHandler onUnknownUrn;

        private Builder(ServiceBusReceiverClient receiver) {
            this.receiver = Objects.requireNonNull(receiver, "receiver");
        }

        /** Register {@code handler} for {@code urn} (the last registration wins). */
        public Builder handler(String urn, BabelHandler handler) {
            this.handlers.put(urn, handler);
            return this;
        }

        public Builder handlers(Map<String, BabelHandler> handlers) {
            this.handlers.putAll(handlers);
            return this;
        }

        /** Max messages per receive (default 10). */
        public Builder maxMessages(int max) {
            this.maxMessages = max;
            return this;
        }

        /** Max time to wait for a batch; {@code null} uses the client default. */
        public Builder maxWaitTime(Duration maxWaitTime) {
            this.maxWaitTime = maxWaitTime;
            return this;
        }

        public Builder onError(ErrorHandler handler) {
            this.onError = handler;
            return this;
        }

        public Builder onUnknownUrn(UnknownUrnHandler handler) {
            this.onUnknownUrn = handler;
            return this;
        }

        public AsbConsumer build() {
            return new AsbConsumer(this);
        }
    }
}
