# Project Overview: ERPNext Change Data Capture

## Overview

This repository is a local development setup for running ERPNext and forwarding selected ERPNext database changes to downstream systems through a Change Data Capture (CDC) pipeline.

ERPNext stores its operational data in MariaDB. MariaDB row-based binary logging exposes committed row changes to Debezium, which publishes them to Apache Kafka. A Node.js consumer reads the selected Kafka topics, converts ERPNext rows into a destination payload, and sends them to an external blockchain API. The blockchain API and blockchain network are dependencies of this project, but their implementations are not contained in this repository.

Blockchain is the current destination. A future version may introduce adapters for other databases or services, but the checked-in consumer is blockchain-specific and must not be described as a completed multi-destination framework.

## Problem Statement

ERPNext is the operational source of truth, but selected HR records also need an independently stored, append-resistant audit representation. Directly modifying ERPNext for every downstream integration would couple business transactions to external systems and make downstream outages affect the ERP workflow.

The CDC pipeline separates those concerns:

- ERPNext continues to write to MariaDB normally.
- Debezium observes database changes without requiring application-level hooks.
- Kafka buffers and distributes change events.
- The consumer applies destination-specific filtering, transformation, deduplication, and delivery.
- The blockchain API owns blockchain transactions and record retrieval.

## Goals

1. **Reproducible ERPNext environment**: Run ERPNext, Frappe workers, Redis, and a binlog-enabled MariaDB through Docker Compose.
2. **Near-real-time CDC**: Capture selected committed row changes from MariaDB and publish them as Kafka events through Debezium.
3. **Decoupled delivery**: Keep blockchain availability and transaction latency outside the ERPNext request path.
4. **Employee and attendance synchronization**: Support `tabEmployee` and `tabAttendance` as the current default capture set.
5. **Traceable transformations**: Preserve the ERPNext record identifier, creation and modification timestamps, modifier, and remaining row fields in the destination request.
6. **Safe operation**: Make credentials, offsets, delete behavior, retry behavior, and delivery guarantees explicit before production use.
7. **Measurable performance**: Provide tooling to observe database-to-Kafka, Kafka-to-consumer, and consumer-to-blockchain latency.

## Core Data Flow

1. A user or integration creates, updates, or deletes an Employee or Attendance record in ERPNext.
2. MariaDB writes the committed row change to its row-format binary log with a full row image.
3. The Debezium MySQL connector reads the binlog. When required by its stored offsets, it can first snapshot the selected tables.
4. Debezium publishes one topic per captured table using the pattern `<topic-prefix>.<database>.<table>`.
5. The Node.js consumer discovers topics whose final segment matches a configured target table and subscribes with the `blockchain-consumer-group` consumer group.
6. The consumer unwraps the JSON value, detects delete markers, suppresses recent duplicates, optionally checks the destination for an equal or newer record, and maps the row to a blockchain API payload.
7. The consumer calls the appropriate blockchain API route. Employee rows use `/employees`; Attendance rows use `/attendances`.

## Current Capabilities

### ERPNext Runtime

- ERPNext/Frappe services for the backend, frontend, WebSocket server, scheduler, and workers.
- Redis instances for cache and queue workloads.
- Persistent Docker volumes for sites, logs, Redis data, and MariaDB data.
- MariaDB configured with row-based binlogging, full row images, and a server ID suitable for CDC evaluation.

### CDC Infrastructure

- A single-node Kafka development broker backed by ZooKeeper.
- Debezium Kafka Connect with the MySQL connector available.
- Kafka UI for local inspection of brokers, topics, and connector status.
- A connector utility that discovers the generated ERPNext database name, validates requested tables, generates connector configuration, and deploys the connector through Kafka Connect.
- A checked-in example connector configuration for Employee and Attendance tables.

### Blockchain Consumer

- Dynamic discovery of configured ERPNext table topics.
- Create, update, and soft-delete handling.
- Employee, Attendance, and generic document payload wrappers.
- In-memory batching, bounded HTTP concurrency, short-window content deduplication, and optional destination existence checks.
- Graceful signal handling and process statistics.
- Utilities for database discovery, topic inspection, raw Kafka event inspection, blockchain API checks, and latency measurement.

## Scope

### In Scope

- Local Docker-based ERPNext and CDC infrastructure.
- MariaDB CDC through Debezium and Kafka.
- Configurable selection of ERPNext tables, currently defaulting to Employee and Attendance.
- A Node.js process that translates selected CDC records and submits them to a blockchain API.
- Development diagnostics and performance measurements for the end-to-end data path.
- Documentation of operational gaps that must be resolved before production deployment.

### Out of Scope

- The blockchain API, smart contracts, blockchain nodes, consensus configuration, and key custody implementation.
- A production-grade Kafka cluster, schema registry, TLS/SASL configuration, or multi-node failover.
- General-purpose sink adapters for arbitrary databases; these are a future extension point.
- Bidirectional synchronization from blockchain back into ERPNext.
- Treating the blockchain copy as ERPNext's transactional source of truth.
- Production deployment, backup, retention, disaster recovery, and observability infrastructure.

## Success Criteria

1. Both Docker Compose files pass configuration validation and their services become healthy in a documented local environment.
2. The Debezium connector reaches `RUNNING` state for both its connector and task and produces the expected Employee and Attendance topics.
3. A controlled ERPNext create, update, and delete produces the expected Kafka record without changing the ERPNext application write path.
4. The consumer sends the documented payload to the correct blockchain API route and handles the API response without exposing the private key.
5. Replayed, duplicate, stale, and delete events have tested, documented outcomes; no delivery guarantee stronger than the implementation provides is claimed.
6. Connector offsets and Kafka consumer offsets survive an ordinary restart, and recovery behavior is tested for Kafka and blockchain API outages.
7. No development password or blockchain private key is committed or used as a production default.
8. End-to-end latency and failure counts can be measured under named test conditions.

## Source of Truth

- `architecture.md` defines component boundaries, data contracts, configuration, and invariants.
- `ai-workflow-rules.md` defines how changes must be researched, implemented, and verified.
- `progress-tracker.md` distinguishes completed assets from unverified behavior and records the next work.
- Checked-in source and Compose configuration take precedence when documentation and implementation disagree; the mismatch must then be corrected and recorded.
