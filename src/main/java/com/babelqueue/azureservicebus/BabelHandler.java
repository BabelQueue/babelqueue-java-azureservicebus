package com.babelqueue.azureservicebus;

import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.babelqueue.Envelope;

/** Processes one decoded, validated envelope and the raw Service Bus message it arrived on. */
@FunctionalInterface
public interface BabelHandler {

    /**
     * Handle a message. Returning normally acknowledges it (the consumer {@code Complete}s it);
     * throwing leaves it for the broker to redeliver (the consumer {@code Abandon}s it,
     * incrementing {@code DeliveryCount}).
     */
    void handle(Envelope envelope, ServiceBusReceivedMessage message) throws Exception;
}
