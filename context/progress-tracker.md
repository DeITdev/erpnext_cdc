# Progress Tracker

Last updated: 2026-09-16

## Current Phase

- **Phase 0 — Repository discovery and context alignment: complete.** The context documentation now describes the ERPNext CDC repository instead of the unrelated project that previously occupied these files.
- **Phase 1 — Local integration hardening: not complete.** The repository contains each main pipeline layer except the external blockchain system, but configuration gaps prevent claiming a reproducible end-to-end run.
- **Phase 2 — Delivery reliability and production readiness: not started.** Durable retries, offset-safe delivery, security hardening, and production deployment remain future work.

## Current Goal

Make the checked-in ERPNext, Debezium/Kafka, and Node.js consumer setup reproducibly run one Employee and one Attendance create/update/delete sequence through the external blockchain API, with documented configuration and no committed secrets.

## Completed Assets

### ERPNext Stack

- `ERPNext/docker-compose.yml` defines ERPNext `v16.19.1`, MariaDB `11.8`, Redis cache/queue, backend, frontend, WebSocket, scheduler, workers, configurator, and site creation.
- MariaDB is configured for row-based binary logging with full row images.
- Named volumes are defined for database data, Redis data, ERPNext sites, and logs.
- ERPNext is exposed locally on port `8080`.

### CDC Stack

- `kafka-debezium/docker-compose.yml` defines ZooKeeper, a single Kafka broker, Debezium Kafka Connect, and Kafka UI.
- Kafka is exposed to host clients on `29092`; Kafka Connect uses `8083`; Kafka UI uses `8085`.
- The Debezium connector utility discovers the generated ERPNext database name, filters configured tables, writes connector JSON, deploys the connector, and checks status.
- The default connector captures `tabEmployee` and `tabAttendance`, unwraps row values, rewrites deletes, and uses snapshots when needed.

### Consumer and Utilities

- `consumer-erp/server.js` implements topic discovery, Employee/Attendance filtering, event parsing, soft deletes, in-memory deduplication and batching, bounded HTTP concurrency, destination checks, blockchain API posts, statistics, and signal handling.
- `consumer-erp/performance-monitor.js` measures pipeline segments and can export CSV results.
- Utilities exist for database connection/discovery, connector deployment, Kafka topic/event inspection, and blockchain API testing.
- The codebase declares Node.js 16+ and dependencies including KafkaJS, Axios, `mysql2`, and `dotenv`.

### Documentation and Static Checks

- Replaced all four context files with repository-specific product, architecture, workflow, and progress documentation on 2026-09-16.
- Consulted current Debezium documentation through Context7 to confirm the general snapshot-to-binlog flow and `ExtractNewRecordState` role; local version-specific behavior still requires runtime verification against Debezium `2.4`.
- `docker compose -f ERPNext/docker-compose.yml config --quiet` passes.
- `docker compose -f kafka-debezium/docker-compose.yml config --quiet` passes.
- `node --check` passes for `server.js`, `performance-monitor.js`, and every checked-in JavaScript utility.

These are static checks only. No container startup, live connector, Kafka event, blockchain request, or destination read was executed during the documentation update.

## In Progress

- Aligning the repository's documentation and source-of-truth boundaries before attempting a live end-to-end run.
- Identifying configuration and delivery-semantics gaps that must be resolved by small, testable implementation units.

## Next Up

1. **Repair the executable contract**: Point `package.json` scripts and `main` to files that exist, or add the intentionally missing CLI/test structure. Ensure commands load the same environment file.
2. **Choose one database network path**: Either safely publish MariaDB for host/`host.docker.internal` access or attach Kafka Connect and MariaDB to a shared network. Document and test the selected path.
3. **Normalize local configuration**: Add a redacted `.env.example`, reconcile the blockchain API port, remove environment-specific generated connector state from the reusable template, and document startup order.
4. **Run the infrastructure**: Start both stacks, confirm service health, discover the ERPNext database, deploy the connector, and verify connector/task `RUNNING` status.
5. **Prove CDC behavior**: Capture representative snapshot, create, update, delete, and tombstone values for Employee and Attendance and convert them into fixtures.
6. **Confirm the blockchain API contract**: Resolve timestamp type/unit, route behavior, authentication/key custody, response schema, not-found semantics, and idempotent update behavior with the external service owner.
7. **Make delivery recoverable**: Tie offset progression to successful destination handling, add classified retries/backoff and a durable failure path, and test restart/rebalance/outage behavior.
8. **Measure the full pipeline**: Run repeatable latency and throughput tests only after correctness and recovery behavior are established.
9. **Add other destinations only after the blockchain path is stable**: Define a normalized event and sink interface, then implement and contract-test each new database/service adapter separately.

## Known Gaps and Open Questions

### Execution and Repository Consistency

- `consumer-erp/package.json` names `consumer-blockchain.js` as `main` and in start scripts, but the implemented entrypoint is `server.js`.
- Package scripts reference `cli/cdc-manager.js`, `test/test-adapters.js`, and a root-level `test-blockchain-integration.js`; those paths are absent or differ from the checked-in `utils/test-blockchain-integration.js`.
- There is no checked-in lockfile or redacted environment example. The lockfile is currently ignored.
- The working tree already contains unrelated user changes, including replacement of an older ERPNext Compose file; those changes were not modified by this documentation work.

### Connectivity and Configuration

- The ERPNext and Kafka/Debezium stacks use separate Docker networks.
- MariaDB port `3306` is not published by the current ERPNext Compose file, while the generated connector expects `host.docker.internal:3306` when `DB_HOST` is local. The intended connection path is therefore unresolved.
- The checked-in connector JSON embeds a generated ERPNext database name and development database password. It is environment-specific and is also overwritten by the deployment utility.
- The consumer and performance monitor default `API_ENDPOINT` to port `4000`; the blockchain integration utility defaults to `4001` and loads a different environment path.
- The external blockchain API and chain are not present in this repository, so their availability and contract cannot be established locally from source.

### Correctness and Reliability

- The consumer subscribes with `fromBeginning: false`; the intended treatment of initial snapshot/history events needs an explicit acceptance test.
- In-memory batching can return control to Kafka before a timer-triggered batch completes. Offset commit timing and crash-loss behavior have not been verified.
- HTTP failures are counted but are not durably retried or routed to a dead-letter store.
- Any blockchain lookup error is currently treated as a missing record, which can turn timeouts or authentication failures into attempted writes.
- Deduplication is process-local, expires quickly, and keys primarily by record ID/content rather than a durable source position.
- Timestamp comments, ERPNext values, comparison logic, and integration fixtures do not establish one consistent timestamp type or unit.
- Per-record ordering under concurrent requests is not demonstrated.
- Delete behavior is a destination soft delete; the exact blockchain contract and behavior for Debezium tombstones remain unverified.

### Security and Operations

- Development database credentials are hard-coded in Compose and connector defaults.
- The current API payload includes the raw blockchain private key. Production key custody and authenticated encrypted transport are undecided.
- Kafka, Kafka Connect, and local database access have no documented authentication or TLS configuration.
- Kafka/ZooKeeper persistence, retention, backups, monitoring, PII handling, and recovery procedures are not defined.
- Kafka UI uses the floating `latest` image tag.

## Architecture Decisions

- **AD-01 — ERPNext remains authoritative**: The blockchain copy is downstream audit/storage data. This pipeline does not write blockchain state back into ERPNext.
- **AD-02 — CDC is asynchronous**: ERPNext transactions must not depend on Kafka or blockchain availability.
- **AD-03 — Current capture set**: Employee and Attendance are the default and only specifically mapped tables. More tables require schema and destination-contract work.
- **AD-04 — Blockchain is the current sink**: Other databases/services are future adapters, not an existing capability.
- **AD-05 — Deletes are represented as soft deletes**: The current consumer writes deletion metadata through the normal record route; this remains subject to external API confirmation.
- **AD-06 — Delivery is at-least-once in intent, not yet guaranteed safely**: Replays must be tolerated, but current batching, offset, and retry behavior requires correction and tests before a formal guarantee.
- **AD-07 — Local defaults are not production policy**: Root credentials, plaintext local listeners, and raw private-key payloads are development artifacts that require hardening.
- **AD-08 — Context7 before dependency decisions**: Library, framework, API, CLI, Docker image, and service decisions use current documentation through the resolve-then-query workflow before implementation.

## Session Notes

- **2026-09-16 — Context replacement**: Inspected the four stale context documents, both Compose files, the consumer, performance monitor, connector generator/configuration, utilities, package manifest, Git state, and recent history. Rewrote the context set for the ERPNext CDC project.
- **2026-09-16 — Verification**: Both Compose configurations parsed successfully, and every checked-in consumer JavaScript file passed `node --check`. No live services or external APIs were invoked.
- Resume with the executable-contract and database-network increments, not with performance tuning or a generic multi-database abstraction.
