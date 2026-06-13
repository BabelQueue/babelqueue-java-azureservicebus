/**
 * Azure Service Bus transport for BabelQueue — a canonical-envelope publisher and a
 * URN-routed consumer over {@code azure-messaging-servicebus}, on the framework-agnostic
 * core. Implements §4 of the broker-bindings contract: {@code Subject} = URN,
 * {@code CorrelationId} = {@code trace_id}, {@code MessageId} = {@code meta.id}, the
 * {@code bq-} application properties, and {@code attempts = DeliveryCount - 1}.
 */
package com.babelqueue.azureservicebus;
