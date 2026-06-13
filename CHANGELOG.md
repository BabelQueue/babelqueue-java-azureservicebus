# Changelog

All notable changes to `com.babelqueue:babelqueue-azure-servicebus` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [1.0.0] - 2026-06-13

### Added
- Initial release. An Azure Service Bus transport on `babelqueue-core` +
  `azure-messaging-servicebus`: `AsbPublisher` (canonical-envelope send with the §4 native
  projection — `Subject` = URN, `CorrelationId` = `trace_id`, `MessageId` = `meta.id`, plus
  `bq-schema-version`/`bq-source-lang`/`bq-created-at` application properties; native
  `ScheduledEnqueueTime` for delays) and `AsbConsumer` (PeekLock receive → URN-routed
  `BabelHandler`s → `complete`; a throwing handler `abandon`s for at-least-once redelivery;
  `attempts` reconciled to the broker-authoritative `DeliveryCount − 1`; `onError`/
  `onUnknownUrn` hooks). Java 17, JUnit 5, JaCoCo ≥90% line coverage; the final Service Bus
  clients + `ServiceBusReceivedMessage` are mocked with Mockito 5's inline mock-maker (no
  Azure, no network). The envelope is unchanged (`schema_version: 1`); Azure Service Bus is
  purely additive.
