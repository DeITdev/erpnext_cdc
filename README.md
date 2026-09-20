# ERPNext Change Data Capture

This repository runs ERPNext on MariaDB and captures selected row changes with Debezium into a local Kafka broker. The default capture set is `tabEmployee` and `tabAttendance`.

## Start the local CDC stack

Start ERPNext first so its external Docker network and MariaDB service are available:

```bash
docker compose -f ERPNext/docker-compose.yml up -d
docker compose -f kafka-debezium/docker-compose.yml up -d
./kafka-debezium/scripts/bootstrap-cdc.sh
```

The bootstrap discovers the generated ERPNext database name, validates the target tables, creates or rotates a dedicated MariaDB replication user, and creates or updates `erpnext-cdc-connector`. It does not write the connector password to the repository. Optional overrides are documented in `kafka-debezium/.env.example`; place local values in the ignored `kafka-debezium/.env` file.

Local endpoints:

- ERPNext: <http://localhost:8080>
- Kafka broker: `localhost:29092`
- Kafka Connect: <http://localhost:8083>
- Kafka UI: <http://localhost:8085>

Check connector health:

```bash
curl -fsS http://localhost:8083/connectors/erpnext-cdc-connector/status | jq
```

Expected topics follow `erpnext.<database>.<table>`, including the default Employee and Attendance topics. Kafka data, connector configuration, schema history, and offsets persist in the `kafka-debezium_kafka-data` named volume. `docker compose -f kafka-debezium/docker-compose.yml down` keeps that volume unless `--volumes` is explicitly supplied.

This is a plaintext, single-node development setup. The downstream blockchain API is external to this repository and is not required to verify MariaDB-to-Kafka CDC.
