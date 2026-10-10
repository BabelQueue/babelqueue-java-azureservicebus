# Changelog

All notable changes to `com.babelqueue:babelqueue-azure-servicebus` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

## [1.0.1] - 2026-10-10

### Fixed

- `AsbConsumer` now applies the §4.7 schema-version gate **before** decoding the body: when the
  `bq-schema-version` application property is present and is not the supported version, the body is
  never decoded and the handler is never called; `onError` is notified (with a `null` envelope) and the
  message is explicitly **dead-lettered** (reason `unsupported schema version`) — never abandoned, which
  would redeliver it until `MaxDeliveryCount`. Integer `1` of any integral width and the string `"1"` are
  accepted; other numbers (`2`, `0`, `1.5`), strings (`"2"`, `"x"`, `" 1"`) and types are rejected. A
  missing, null or blank (empty / ASCII-whitespace-only) property keeps the existing decode path.
- `onError` may now receive a `null` envelope for a schema-version gate rejection (the body is not decoded, so
  there is no envelope). The message is dead-lettered **before** `onError` is invoked, so a callback that
  dereferences the envelope cannot leave it unsettled.
- The rejected `bq-schema-version` value echoed in the dead-letter description and the `onError` message is
  capped at its first 64 characters (`...` appended when cut), since Service Bus limits the description to 4096
  characters.

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
