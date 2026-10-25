# BabelQueue — Azure Service Bus (Java)

`com.babelqueue:babelqueue-azure-servicebus` — an Azure Service Bus transport for
[BabelQueue](https://babelqueue.com), built on `azure-messaging-servicebus` and the
framework-agnostic [`babelqueue-core`](https://github.com/BabelQueue/babelqueue-java).

A canonical-envelope **publisher** and a URN-routed **consumer**, so an Azure Service
Bus-based Java service speaks the same wire contract (envelope shape, URN identity, trace
propagation) as the .NET, Python, Go and Node SDKs. Implements
[§4 of the broker-bindings contract](https://babelqueue.com/docs/spec/1.x/broker-bindings#azure-service-bus).

## Install (Maven)

```xml
<dependency>
  <groupId>com.babelqueue</groupId>
  <artifactId>babelqueue-azure-servicebus</artifactId>
  <version>1.0.2</version>
</dependency>
```

It pulls `babelqueue-core` and `com.azure:azure-messaging-servicebus` transitively.

## Use

```java
ServiceBusClientBuilder builder = new ServiceBusClientBuilder().connectionString(cs); // or .credential(ns, cred)

// produce
ServiceBusSenderClient sender = builder.sender().queueOrTopicName("orders").buildClient();
String id = AsbPublisher.create(sender)
    .publish("urn:babel:orders:created", Map.of("order_id", 1042));

// consume (PeekLock)
ServiceBusReceiverClient receiver = builder.receiver().queueName("orders").buildClient();
AsbConsumer consumer = AsbConsumer.builder(receiver)
    .handler("urn:babel:orders:created", (env, msg) -> {
        // env.data(), env.traceId(), env.attempts() ...
    })
    .onError((err, env, msg) -> err.printStackTrace())
    .build();
consumer.run(); // poll until the thread is interrupted
```

Delayed delivery: `publish(urn, data, traceId, Duration.ofMinutes(5))` → native
`ScheduledEnqueueTime`. Auth: a connection string, or the fully-qualified namespace + a
`TokenCredential` (`DefaultAzureCredential`).

## Contract mapping (§4)

| Envelope | Azure Service Bus |
| :--- | :--- |
| body | `Body` (byte-identical across SDKs) |
| `job` (URN) | `Subject` |
| `trace_id` | `CorrelationId` |
| `meta.id` | `MessageId` |
| `meta.schema_version` | `ApplicationProperties["bq-schema-version"]` |
| `meta.lang` | `ApplicationProperties["bq-source-lang"]` |
| `meta.created_at` | `ApplicationProperties["bq-created-at"]` (ms) |
| `attempts` | `DeliveryCount − 1` (broker-authoritative) |
| reserve / ack / retry | PeekLock → `complete` / `abandon` |

`DeliveryCount` is the **authoritative** attempts source on ASB (native, 1-based) — the
contract `attempts` is `DeliveryCount − 1`. A throwing handler `abandon`s the message, so
the broker redelivers it (at-least-once); at `MaxDeliveryCount` it auto-moves to the native
dead-letter sub-queue. The poll loop never stops on a bad message — observe via
`onError`/`onUnknownUrn`. The envelope is unchanged (`schema_version` stays `1`); Azure
Service Bus is purely additive.

**Schema-version gate (§4.7).** Before decoding the body the consumer reads
`ApplicationProperties["bq-schema-version"]`. Missing, null or blank (empty / ASCII whitespace
only) → normal decode. Integer `1` (any integral width) and the string `"1"` → normal decode. Any
other value (`2`, `1.5`, `"2"`, `"x"`, `" 1"`, other types) → the body is **not** decoded, the handler
is not called, no hook runs, `onError` is notified with a `null` envelope, and the message is settled on its
**first** delivery (ADR-0037): with a `deadLetterSender` (bound to `<queue>.dlq`) the body is sent there
**exactly as received** (no `dead_letter` block) and `complete`d after the send succeeded (a failed send
`abandon`s it, so the write is retried); without one it is explicitly **dead-lettered** to the native
sub-queue (reason `unsupported schema version`) — never abandoned.

**Poison bodies (ADR-0037).** A body that is not a valid envelope (not JSON, no identity, unsupported
`meta.schema_version`) never reaches a handler and is never retried by the SDK. With a `deadLetterSender` it is
sent to `<queue>.dlq` as the ADR-0035 record (`reason: "poison"`; the original bytes kept, spliced or under
`raw`) and `complete`d after the send; without one it is `abandon`ed, so `MaxDeliveryCount` moves it to the
native dead-letter sub-queue.

```java
ServiceBusSenderClient dlq = new ServiceBusClientBuilder()
    .connectionString(cs).sender().queueOrTopicName("orders.dlq").buildClient();
AsbConsumer.builder(receiver).handler(urn, handler).deadLetterSender(dlq).build();
```

## Build & test

```bash
mvn verify
```

The final `ServiceBusSenderClient` / `ServiceBusReceiverClient` / `ServiceBusReceivedMessage`
are mocked with Mockito 5's inline mock-maker — no Azure, no network. JUnit 5, JaCoCo ≥90%
line coverage.

## License

MIT
