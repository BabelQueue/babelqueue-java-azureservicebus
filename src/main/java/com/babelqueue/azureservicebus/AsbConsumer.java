package com.babelqueue.azureservicebus;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.babelqueue.BabelQueueException;
import com.babelqueue.Envelope;
import com.babelqueue.EnvelopeCodec;
import com.babelqueue.UnknownUrnException;
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
 */
public final class AsbConsumer {

    /** Notified of a non-conformant envelope, an unmapped URN (no {@code onUnknownUrn}), or a throwing handler. */
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
     * Set {@code attempts} to {@code max(current, DeliveryCount - 1)}. {@code DeliveryCount} is
     * 1-based and broker-authoritative on ASB (first delivery = 1 → attempts 0); the max never
     * lowers a higher body count carried by a message republished from another SDK.
     */
    private static Envelope reconcile(Envelope envelope, long deliveryCount) {
        int nativeAttempts = (int) deliveryCount - 1;
        if (nativeAttempts <= envelope.attempts()) {
            return envelope;
        }
        return new Envelope(
            envelope.job(), envelope.traceId(), envelope.data(), envelope.meta(),
            nativeAttempts, envelope.deadLetter());
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
