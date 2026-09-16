# AI Workflow Rules

## Required Context Pass

Before implementing a feature or making an architectural decision, read these files in order:

1. `project-overview.md`
2. `architecture.md`
3. `ai-workflow-rules.md`
4. `progress-tracker.md`

Then inspect the relevant implementation and configuration. The context files explain intent, but checked-in code establishes current behavior. If they disagree, do not guess: document the mismatch and resolve it in the same change when it is in scope.

## Documentation Research

- Use Context7 for current documentation whenever work involves a library, framework, SDK, API, CLI tool, Docker image, or cloud service.
- Start with Context7 `resolve-library-id` unless the exact `/org/project` identifier is already supplied, then call `query-docs` with one focused concept per query.
- Prefer the official or highest-reputation match and use a version-specific entry when the repository pins a version and Context7 provides it.
- Record the source version or image tag that informed a consequential decision. Do not copy a configuration for a different major version without verifying compatibility.
- Use primary official documentation when Context7 cannot answer a required question. Distinguish documented guarantees from behavior measured in this repository.
- Context7 research does not replace reading local source, configuration, runtime logs, or test results.

## Implementation Approach

- Work in small, end-to-end increments that can be verified independently.
- Preserve unrelated working-tree changes. Do not replace or delete a user's local configuration to simplify an implementation.
- Keep source-system capture, event transport, normalization, and destination delivery as separate responsibilities.
- Prefer configuration over new hard-coded endpoints, credentials, table names, or network assumptions.
- Do not add a dependency or infrastructure service until its role, version, security impact, and verification method are documented.
- Do not claim that a component is running because its configuration parses or its source passes syntax checks.

## Scoping Rules

Split work when a change combines more than one independently testable risk, especially:

- ERPNext or MariaDB configuration and Debezium connector behavior.
- Kafka topology or offset policy and blockchain API transformation logic.
- Delivery reliability changes and performance optimization.
- Secret management and unrelated functional changes.
- A new sink adapter and a rewrite of the existing blockchain adapter.

For a new captured table, treat source schema selection, topic/event inspection, normalized mapping, destination contract, delete behavior, and tests as one vertical feature. Do not enable a table in production before every part is defined.

## CDC and Kafka Safety Rules

1. **Inspect the real event first**: Capture representative create, update, delete, snapshot, and null/tombstone records before changing parsing logic.
2. **Protect offset semantics**: Review when Kafka offsets are committed relative to destination success. A message must not be considered safely delivered merely because it entered an in-memory queue.
3. **Assume replay**: Connector snapshots, restarts, rebalances, retries, and offset resets can repeat events. Destination writes must be idempotent.
4. **Define ordering explicitly**: Document keys, partitions, and version comparison. Do not assume ordering between Employee and Attendance topics or across partitions.
5. **Preserve delete meaning**: Test both Debezium envelope deletes and configured unwrap/delete markers. Decide whether the destination uses a soft delete, tombstone, or immutable deletion event.
6. **Handle poison events durably**: Parsing, validation, and destination failures need bounded retries and a diagnosable dead-letter or quarantine path before production.
7. **Treat snapshots separately**: State whether a consumer should process historical snapshot rows, only new changes, or both. Verify `fromBeginning`, connector offsets, and consumer group offsets together.
8. **Control schema evolution**: Adding, renaming, or changing a column or payload type requires backward-compatibility analysis and fixtures for old and new events.

## Blockchain Delivery Rules

- Confirm the external API contract rather than inferring new routes or response fields.
- Keep record IDs stable and define a single timestamp type and unit at the API boundary.
- Do not treat every failed lookup as proof that a record does not exist; distinguish not-found, authentication, timeout, and server failures.
- Classify API failures as retryable or permanent and apply bounded exponential backoff. Record exhausted failures durably.
- Never log the blockchain private key or include it in Kafka messages, metrics, fixtures, screenshots, or committed connector files.
- Before production, replace raw private-key transport with an approved signer or key-custody model and authenticated, encrypted API transport.
- Performance improvements must preserve offset safety, per-record ordering, and destination idempotency.

## Configuration and Security Rules

- Local defaults such as `root` / `admin` are development-only. Never introduce them as production recommendations.
- Commit a redacted environment template when configuration work is in scope; keep `.env`, `.env.local`, private keys, and generated secret-bearing connector configurations ignored.
- Use a least-privileged MariaDB account with only the replication and table access required by the selected Debezium version.
- Pin deployable image and package versions. A floating `latest` tag may be used only as a recorded development exception.
- Review exposed ports, Docker networks, authentication, encryption, PII retention, and log redaction whenever a service boundary changes.
- Never run destructive volume removal, offset reset, connector deletion, topic deletion, or database migration without confirming the exact target and recovery plan. Connector replacement is destructive to connector state and must be intentional.

## Verification Matrix

Use the checks relevant to the changed layer:

| Change | Minimum verification |
| --- | --- |
| Markdown/context only | Cross-file consistency, local links, stale-term search, and comparison against checked-in source |
| Docker Compose | `docker compose ... config --quiet`, image/version review, network and volume inspection, then service health checks when runtime access is available |
| Node.js source | `node --check` for changed scripts plus focused automated tests |
| Debezium connector | Config validation, connector/task `RUNNING`, expected topics, and create/update/delete event inspection |
| Consumer mapping | Fixtures for envelope and unwrapped events, every target table, deletes, missing IDs, and timestamp types |
| Delivery behavior | Success, duplicate, stale version, 404, timeout, authentication failure, 5xx, restart, and rebalance scenarios |
| End-to-end pipeline | ERPNext mutation traced through binlog/connector, Kafka, consumer, API, and destination retrieval |
| Performance | Named hardware/configuration, data volume, concurrency, warm-up, repetitions, percentiles, errors, and queue growth |

Syntax and configuration checks are necessary but do not prove live connectivity or data delivery. Never record a live test as passed unless it was actually executed and its conditions are stated.

## Documentation and Progress Tracking

Update the context files as part of the same meaningful change:

- Update `project-overview.md` when goals, supported records, scope, or success criteria change.
- Update `architecture.md` when components, versions, networks, topics, schemas, API contracts, state, security boundaries, or invariants change.
- Update this file when the engineering or verification process changes.
- Always update `progress-tracker.md` after a meaningful documentation or implementation change.

The progress tracker must separate:

- files or functionality that exist,
- checks performed without live infrastructure,
- behavior demonstrated end to end,
- current work,
- unresolved decisions and known defects.

Do not mark a feature complete because code exists. Completion requires the acceptance checks appropriate to that feature.

## Completion Checklist

Before moving to the next unit:

1. The implementation matches the documented scope and contracts.
2. Relevant static, unit, integration, failure, and recovery checks pass.
3. No architectural invariant is violated.
4. Secrets and personnel data are absent from commits and logs.
5. Operational changes include a safe rollout and recovery path.
6. `progress-tracker.md` records exactly what was changed, what was verified, and what remains unverified.
