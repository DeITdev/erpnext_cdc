# Architecture Context

## System Summary

The system is an asynchronous, one-way integration:

```text
ERPNext/Frappe
      |
      v
MariaDB row binlog
      |
      v
Debezium on Kafka Connect --> Kafka topics --> Node.js CDC consumer
                                                   |
                                                   v
                                      External blockchain API
                                                   |
                                                   v
                                      Blockchain network/storage
```

ERPNext and MariaDB remain the operational system of record. Kafka carries change events, and the blockchain destination is a derived representation. There is no reverse synchronization path.

## Technology Stack

| Layer | Checked-in technology | Responsibility |
| --- | --- | --- |
| ERP application | Local `erpnext-cdc-hrms:v16.19.1-hrms-v16.7.1`, based on `frappe/erpnext:v16.19.1` | ERPNext and HRMS UI, application server, scheduler, workers, and WebSocket service |
| Transactional database | `mariadb:11.8` | ERPNext persistence and row-based binary log source |
| ERP support services | `redis:6.2-alpine` | Frappe cache and job queues |
| CDC engine | `debezium/connect:2.4` | MariaDB/MySQL snapshot and binlog capture through Kafka Connect |
| Event broker | `confluentinc/cp-kafka:7.3.0` | CDC event and connector state transport |
| Kafka metadata | `confluentinc/cp-zookeeper:7.3.0` | Coordination for the checked-in Kafka development broker |
| Inspection UI | `provectuslabs/kafka-ui:latest` | Local Kafka and Kafka Connect inspection; the floating tag is unsuitable for reproducible production deployment |
| Consumer | Node.js 16+ with KafkaJS and Axios | Topic discovery, filtering, transformation, deduplication, and blockchain API delivery |
| Destination | External HTTP blockchain API | Blockchain writes and record reads; implementation is outside this repository |

## Repository Boundaries

- `ERPNext/Containerfile` builds the release-pinned ERPNext/HRMS application image used by every Frappe service.
- `ERPNext/docker-compose.yml` defines the ERPNext/Frappe/HRMS, MariaDB, and Redis stack.
- `kafka-debezium/docker-compose.yml` defines ZooKeeper, Kafka, Kafka Connect/Debezium, and Kafka UI.
- `consumer-erp/server.js` is the implemented blockchain CDC consumer entrypoint.
- `consumer-erp/performance-monitor.js` is a separate consumer that measures pipeline latency while also calling the blockchain API.
- `consumer-erp/utils/` contains discovery, connector deployment, connectivity, topic, event, and blockchain API utilities.
- `consumer-erp/utils/config/erpnext-connector.json` is generated or overwritten by the connector deployment utility and contains environment-specific database details.

The blockchain API and blockchain network are external boundaries. Documentation may describe the HTTP behavior expected by this repository, but it must not imply that their implementation is present here.

## Deployment Topology

The repository currently defines two independent Compose projects:

- The ERPNext stack uses an internal `frappe_network` bridge.
- Every Frappe application service uses the same locally built image, `erpnext-cdc-hrms:v16.19.1-hrms-v16.7.1`. The image derives from `frappe/erpnext:v16.19.1` (ERPNext `16.19.1`, Frappe `16.18.3`) and installs HRMS from the immutable `v16.7.1` tag. Runtime `bench get-app` is not part of the deployment path.
- The CDC stack uses the named `kafka_net` bridge.
- Kafka exposes host port `29092`, Kafka Connect exposes `8083`, Kafka UI exposes `8085`, and ERPNext exposes `8080`.
- MariaDB listens on port `3306` inside `frappe_network`, but the current ERPNext Compose file does not publish that port to the host and does not attach MariaDB to `kafka_net`.
- The generated Debezium connector converts a local database host to `host.docker.internal` and attempts port `3306` from the Kafka Connect container.

Consequently, the checked-in Compose files do not by themselves establish a self-contained MariaDB-to-Debezium network path. Before claiming end-to-end operation, choose and document one explicit topology: publish MariaDB to a host port for local-only access, or connect the relevant containers through a shared Docker network and use the database service name. Do not silently depend on an unrelated host database.

## CDC Configuration and Data Flow

### MariaDB Source

MariaDB is configured with:

- `--binlog-format=ROW`
- `--log-bin=mysql-bin`
- `--server-id=1`
- `--binlog-row-image=FULL`
- `--bind-address=0.0.0.0`

These settings make full row changes available to a compatible binlog reader. Network reachability, user privileges, retention, disk growth, and restart behavior still require explicit verification.

### Debezium Connector

`utils/add-erp-connector.js` performs the current connector workflow:

1. Connect to MariaDB and find the first database whose name matches an underscore followed by hexadecimal characters.
2. List ERPNext `tab*` tables and retain only configured target tables that exist.
3. Generate a timestamp-derived Debezium server ID.
4. Save the complete connector document to `utils/config/erpnext-connector.json`.
5. Delete any existing `erpnext-cdc-connector`, create the replacement through Kafka Connect, and inspect connector/task status.

The connector limits capture to the discovered database and selected tables. It uses `snapshot.mode=when_needed`, `snapshot.locking.mode=none`, JSON converters without schemas, and the `ExtractNewRecordState` transform. Delete handling is configured as a rewritten record with a `__deleted` marker while tombstones are retained.

The topic convention is:

```text
<TOPIC_PREFIX>.<ERPNext database name>.<table name>
```

With current defaults, examples are `erpnext.<database>.tabEmployee` and `erpnext.<database>.tabAttendance`. Schema history is stored in `schema-changes.<TOPIC_PREFIX>`; application consumers must not subscribe to that topic.

### Consumer Processing

`consumer-erp/server.js` currently:

1. Connects to Kafka as `blockchain-consumer` with group ID `blockchain-consumer-group`.
2. Discovers topics by prefix and final table-name segment.
3. Waits and polls when no matching topic exists.
4. Subscribes with `fromBeginning: false`.
5. Parses either a Debezium envelope or an already-unwrapped JSON row.
6. Uses `before` for envelope delete events or the `__deleted` marker for unwrapped deletes.
7. Drops records without `name` or `id` and ignores tables outside `TARGET_TABLES`.
8. Applies an in-memory content/timestamp duplicate window.
9. Unless disabled, reads the existing blockchain record and skips an equal or newer version.
10. Converts deletes into a soft-delete row containing `__deleted`, `__deletedAt`, `__deletedBy`, and `status: "DELETED"`.
11. Queues rows into an in-memory batch and bounds concurrent blockchain requests.
12. Posts the transformed payload and logs aggregate processed, skipped, and error counts.

This is not an exactly-once pipeline. The queue and deduplication map are process-local, failed HTTP writes have no durable retry or dead-letter path, destination read failures are treated as "not found," and messages can be acknowledged by the Kafka processing loop before a timer-triggered batch finishes. These limitations must be addressed or explicitly accepted before production use.

## Blockchain API Contract

The following contract is inferred from the consumer and utility scripts and must be confirmed against the external API.

### Routes

| Method | Route | Consumer expectation |
| --- | --- | --- |
| `GET` | `/` | Connectivity metadata such as service name/version |
| `GET` | `/employees/:recordId` | Existing Employee record and `modifiedTimestamp`, or a not-found response |
| `POST` | `/employees` | Create or update an Employee blockchain representation |
| `GET` | `/attendances/:recordId` | Existing Attendance record and `modifiedTimestamp`, or a not-found response |
| `POST` | `/attendances` | Create or update an Attendance blockchain representation |

The consumer treats a response body with `success: true` as a successful write. It looks for transaction metadata either under `blockchain.blockNumber` and `blockchain.transactionHash` or at the response root.

### Request Shape

```json
{
  "privateKey": "<configured secret>",
  "employeeData": {
    "recordId": "HR-EMP-00001",
    "createdTimestamp": "<ERPNext creation value>",
    "modifiedTimestamp": "<ERPNext modified value>",
    "modifiedBy": "administrator@example.com",
    "allData": {
      "...remaining ERPNext columns...": "..."
    }
  }
}
```

Attendance uses `attendanceData`; the fallback for another mapped table is `documentData`. The four mapped fields are removed from `allData`. Current comments describe timestamps as microseconds, but the code forwards the source value without normalization and test utilities sometimes send ISO strings. The API's required type and unit are therefore unresolved.

Sending a raw blockchain private key in each HTTP request is existing behavior, not a recommended production security model. It must be loaded from an ignored environment file, never logged, and replaced with an approved signing/key-custody design before production.

## Configuration Contract

The consumer and utilities load `consumer-erp/.env.local` in most paths. No example environment file is currently checked in.

| Variable | Current default | Used for |
| --- | --- | --- |
| `DB_HOST` | `localhost` | ERPNext database discovery and connector generation |
| `DB_PORT` | `3306` | MariaDB connection and connector configuration |
| `DB_USER` | `root` | Development database access |
| `DB_PASSWORD` | `admin` | Development database access; never acceptable as a production secret |
| `KAFKA_CONNECT_URL` | `http://localhost:8083` | Kafka Connect REST API |
| `KAFKA_BROKER` | `127.0.0.1:29092` | Host-side Kafka clients |
| `API_ENDPOINT` | `http://127.0.0.1:4000` | Consumer and performance monitor destination; one test utility instead defaults to port `4001` |
| `PRIVATE_KEY` | none | Blockchain signing credential passed to the API |
| `TOPIC_PREFIX` | `erpnext` | Topic discovery and connector naming |
| `TARGET_TABLES` | `tabEmployee,tabAttendance` | Captured and consumed ERPNext tables |
| `BATCH_SIZE` | `50` | Maximum messages removed per in-memory batch |
| `BATCH_TIMEOUT` | `50` | Batch timer in milliseconds |
| `MAX_CONCURRENT_REQUESTS` | `10` | Concurrent blockchain HTTP calls |
| `DEDUP_WINDOW_MS` | `10000` | In-memory duplicate retention window |
| `SKIP_BLOCKCHAIN_CHECK` | `false` | Skip destination existence/timestamp lookup when set to `true` |

## State and Persistence

- MariaDB, ERPNext sites/logs, and Redis use Docker named volumes.
- ERPNext, Frappe, HRMS, Python dependencies, and built assets reside in the common immutable application image. `ERPNext/Containerfile` rebuilds the complete asset manifest after installing HRMS and copies it to `/home/frappe/frappe-bench/assets`, which the container entrypoint links into the persistent `sites` volume. The volume contains site configuration and generated site state, not the application source checkout.
- Kafka and ZooKeeper have no declared volumes in the current CDC Compose file; broker and connector state should be treated as disposable across container recreation.
- Kafka Connect stores connector configurations, offsets, and statuses in Kafka internal topics with replication factor effectively limited to the single development broker.
- Consumer deduplication, batching, and counters exist only in memory.
- Optional performance CSV output is a local generated artifact, not an operational metrics store.

## Security and Trust Boundaries

- Compose passwords and the checked-in connector JSON are development configuration. Production credentials must be injected through an approved secret mechanism and use a least-privileged CDC database user.
- `PRIVATE_KEY` and database passwords must never be committed, printed, placed in topic values, or included in diagnostic artifacts.
- Kafka, Kafka Connect, MariaDB, ERPNext, and the blockchain API currently have no documented TLS or service authentication in this repository. Keep development ports bound to trusted interfaces and do not expose this topology publicly.
- CDC event values can contain personal and HR data. Topic ACLs, retention, log redaction, erasure/retention policy, and access auditing must be designed before using real personnel data.
- Blockchain immutability does not remove privacy, minimization, or regulatory obligations. Decide whether full HR row content belongs on-chain before production.

## Architectural Invariants

1. **One-way propagation**: ERPNext/MariaDB changes flow downstream; blockchain data must not write directly back into ERPNext through this pipeline.
2. **ERP availability isolation**: A Kafka, consumer, blockchain API, or blockchain outage must not block the original ERPNext transaction.
3. **Explicit schemas**: Changes to captured tables, topic values, timestamp types, delete markers, or API payloads require compatibility analysis and documentation.
4. **Idempotent destination behavior**: Replays are normal in CDC systems. A destination write must be safely repeatable by stable record ID and version/timestamp before stronger recovery guarantees are claimed.
5. **Durable failure handling before production**: Events must not be silently lost after a transient destination failure. Offset commits, retries, backoff, poison records, and dead-letter behavior must be designed and tested together.
6. **Ordering is scoped**: Do not assume global ordering across topics or partitions. Any required per-record ordering must be enforced through keys, partitioning, version checks, and tests.
7. **Secrets stay out of source and telemetry**: Credentials and private keys must remain in ignored secret storage and be redacted from logs and artifacts.
8. **Measured claims only**: Connector health, event delivery, blockchain persistence, and latency are not complete until demonstrated in the named environment.

## Extension Boundary

Future database or service destinations should be introduced behind an explicit sink interface that accepts a normalized CDC record and returns a durable success/failure result. Destination-specific mapping, credentials, idempotency, and retry rules belong in the adapter. Do not generalize the existing blockchain-specific functions by name alone or claim multi-database support until at least one additional adapter and its contract tests exist.
