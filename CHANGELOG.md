# Changelog

All notable changes to `com.babelqueue:babelqueue-azure-servicebus` are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The envelope wire format is versioned separately by `meta.schema_version`
(currently **1**) — see the contract at [babelqueue.com](https://babelqueue.com).

## [Unreleased]

MINOR (ADR-0037): the consumer gains an opt-in BabelQueue DLQ and settles poison bodies and version-gated
deliveries on their first delivery. The envelope is unchanged (`schema_version` stays `1`).

### Added

- `AsbConsumer.Builder.deadLetterSender(ServiceBusSenderClient)` (opt-in): a sender bound to the `<queue>.dlq`
  entity (an ordinary Service Bus queue, §4.5; it must exist and belongs to the caller). "Dead-lettering on"
  means this sender is set.

### Changed (behaviour)

- **Poison body** (not JSON, not an envelope, no valid identity, or an unsupported `meta.schema_version`):
  never handed to a handler and never retried by the SDK. With a DLQ sender it is sent as the ADR-0035 record
  (`PoisonRecord`, `reason: "poison"`, `original_queue` = the receiver's entity, the exact body bytes kept) and
  `Complete`d **only after** the send succeeded; a failed send `Abandon`s it and is reported. With no DLQ
  sender nothing changes: it is abandoned and reported, so `MaxDeliveryCount` moves it to the native
  sub-queue.
- **Version-gated delivery** (`bq-schema-version` present, not null, not blank, not `1`): settled on the first
  delivery, never decoded, no handler or hook, no retry. With a DLQ sender the **raw body, byte for byte** (no
  `dead_letter` block) is sent to `<queue>.dlq` and the message `Complete`d after the send (a failed send
  `Abandon`s it, so the write is retried on redelivery). With no DLQ sender nothing changes: it is dead-lettered
  into the native `$DeadLetterQueue` (reason `unsupported schema version`). This consumer has no unknown-URN
  strategy option, so the rule is `fail`; an `onUnknownUrn` hook never affects it.
- Requires `babelqueue-core` 1.13.0 (was 1.11.0) for `PoisonRecord`.
- Vendored the cross-SDK conformance suite v1.8.0 (`version_gate_asb`, `poison_dlq` runners added) and moved
  the CI pin to it.

## [1.0.2] - 2026-10-17

### Fixed

- Unknown top-level keys and unknown `meta` keys (ADR-0031 extension keys, e.g. `extra_top`, `meta.vendor_flag`) are no longer dropped when `AsbConsumer` reconciles `attempts` with `DeliveryCount`: the envelope handed to handlers, `onUnknownUrn` and `onError` is rebuilt with `Envelope.withAttempts`, which carries the extras, instead of the 6-argument constructor. Regression tests cover the handler envelope and a dead-letter annotation built from the `onError` envelope (ASB dead-letters natively, so the broker keeps the original body).

### Changed

- The `babelqueue-core` dependency is raised from 1.0.0 to 1.11.0. 1.8.0 is the first core release whose `Envelope` carries extras and exposes `withAttempts`; 1.11.0 adds the ADR-0031 decode behaviour (`job` + `urn` warning, closed ASCII blank set) and encode-time extension-key rejection.

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
