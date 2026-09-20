#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
CDC_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
REPO_DIR="$(cd -- "${CDC_DIR}/.." && pwd)"
ERP_COMPOSE="${REPO_DIR}/ERPNext/docker-compose.yml"

if [[ -f "${CDC_DIR}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${CDC_DIR}/.env"
  set +a
fi

ERP_DB_ROOT_USER="${ERP_DB_ROOT_USER:-root}"
ERP_DB_ROOT_PASSWORD="${ERP_DB_ROOT_PASSWORD:-admin}"
CDC_DB_USER="${CDC_DB_USER:-debezium}"
CDC_DB_PASSWORD="${CDC_DB_PASSWORD:-}"
DEBEZIUM_SERVER_ID="${DEBEZIUM_SERVER_ID:-184054}"
TOPIC_PREFIX="${TOPIC_PREFIX:-erpnext}"
TARGET_TABLES="${TARGET_TABLES:-tabEmployee,tabAttendance}"
ERP_DB_NAME="${ERP_DB_NAME:-}"
KAFKA_CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"

if [[ -z "${CDC_DB_PASSWORD}" ]]; then
  CDC_DB_PASSWORD="$(openssl rand -hex 24)"
fi

if [[ ! "${CDC_DB_USER}" =~ ^[A-Za-z0-9_-]+$ ]]; then
  echo "CDC_DB_USER contains unsupported characters" >&2
  exit 1
fi
if [[ ! "${CDC_DB_PASSWORD}" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "CDC_DB_PASSWORD must contain only letters, digits, dot, underscore, or hyphen" >&2
  exit 1
fi
if [[ ! "${DEBEZIUM_SERVER_ID}" =~ ^[0-9]+$ ]] || (( DEBEZIUM_SERVER_ID < 1 || DEBEZIUM_SERVER_ID > 4294967295 )); then
  echo "DEBEZIUM_SERVER_ID must be an integer from 1 through 4294967295" >&2
  exit 1
fi

db_query() {
  docker compose -f "${ERP_COMPOSE}" exec -T \
    -e "MYSQL_PWD=${ERP_DB_ROOT_PASSWORD}" db \
    mariadb --batch --skip-column-names -u "${ERP_DB_ROOT_USER}" "$@"
}

if [[ -z "${ERP_DB_NAME}" ]]; then
  ERP_DB_NAME="$(db_query -e "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME REGEXP '^_[0-9A-Fa-f]+$' ORDER BY SCHEMA_NAME LIMIT 1;")"
fi

if [[ ! "${ERP_DB_NAME}" =~ ^_[0-9A-Fa-f]+$ ]]; then
  echo "Could not discover a valid ERPNext database name" >&2
  exit 1
fi

IFS=',' read -r -a requested_tables <<< "${TARGET_TABLES}"
table_include_list=""
for raw_table in "${requested_tables[@]}"; do
  table="${raw_table#"${raw_table%%[![:space:]]*}"}"
  table="${table%"${table##*[![:space:]]}"}"
  if [[ ! "${table}" =~ ^tab[A-Za-z0-9_-]+$ ]]; then
    echo "Unsupported target table name: ${table}" >&2
    exit 1
  fi
  table_exists="$(db_query -e "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='${ERP_DB_NAME}' AND TABLE_NAME='${table}';")"
  if [[ "${table_exists}" != "1" ]]; then
    echo "Target table does not exist: ${ERP_DB_NAME}.${table}" >&2
    exit 1
  fi
  [[ -n "${table_include_list}" ]] && table_include_list+=","
  table_include_list+="${ERP_DB_NAME}.${table}"
done

db_query <<SQL
CREATE USER IF NOT EXISTS '${CDC_DB_USER}'@'%' IDENTIFIED BY '${CDC_DB_PASSWORD}';
ALTER USER '${CDC_DB_USER}'@'%' IDENTIFIED BY '${CDC_DB_PASSWORD}';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '${CDC_DB_USER}'@'%';
FLUSH PRIVILEGES;
SQL

plugins="$(curl --fail --silent --show-error "${KAFKA_CONNECT_URL}/connector-plugins")"
if ! jq -e '.[] | select(.class == "io.debezium.connector.mariadb.MariaDbConnector")' >/dev/null <<< "${plugins}"; then
  echo "Debezium MariaDB connector plugin is not available" >&2
  exit 1
fi

connector_config="$(mktemp)"
trap 'rm -f "${connector_config}"' EXIT

jq -n \
  --arg db_name "${ERP_DB_NAME}" \
  --arg db_user "${CDC_DB_USER}" \
  --arg db_password "${CDC_DB_PASSWORD}" \
  --arg server_id "${DEBEZIUM_SERVER_ID}" \
  --arg topic_prefix "${TOPIC_PREFIX}" \
  --arg table_list "${table_include_list}" \
  '{
    "connector.class": "io.debezium.connector.mariadb.MariaDbConnector",
    "tasks.max": "1",
    "database.hostname": "db",
    "database.port": "3306",
    "database.user": $db_user,
    "database.password": $db_password,
    "database.server.id": $server_id,
    "topic.prefix": $topic_prefix,
    "database.include.list": $db_name,
    "table.include.list": $table_list,
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "schema.history.internal.kafka.topic": ("schema-changes." + $topic_prefix),
    "schema.history.internal.store.only.captured.tables.ddl": "true",
    "include.schema.changes": "false",
    "snapshot.mode": "when_needed",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.delete.tombstone.handling.mode": "rewrite-with-tombstone",
    "decimal.handling.mode": "string",
    "bigint.unsigned.handling.mode": "long",
    "time.precision.mode": "adaptive_time_microseconds"
  }' > "${connector_config}"

curl --fail --silent --show-error \
  -X PUT \
  -H 'Content-Type: application/json' \
  --data-binary "@${connector_config}" \
  "${KAFKA_CONNECT_URL}/connectors/erpnext-cdc-connector/config" >/dev/null

for attempt in {1..30}; do
  status="$(curl --silent --show-error "${KAFKA_CONNECT_URL}/connectors/erpnext-cdc-connector/status")"
  connector_state="$(jq -r '.connector.state // "UNKNOWN"' <<< "${status}")"
  task_state="$(jq -r '.tasks[0].state // "UNKNOWN"' <<< "${status}")"
  if [[ "${connector_state}" == "RUNNING" && "${task_state}" == "RUNNING" ]]; then
    echo "Connector and task are RUNNING"
    echo "Database: ${ERP_DB_NAME}"
    echo "Tables: ${table_include_list}"
    echo "Topics use prefix: ${TOPIC_PREFIX}"
    exit 0
  fi
  if [[ "${connector_state}" == "FAILED" || "${task_state}" == "FAILED" ]]; then
    jq -r '.tasks[0].trace // .connector.trace // "Connector failed without a trace"' <<< "${status}" >&2
    exit 1
  fi
  sleep 2
done

echo "Connector did not reach RUNNING state within 60 seconds" >&2
jq . <<< "${status}" >&2
exit 1
