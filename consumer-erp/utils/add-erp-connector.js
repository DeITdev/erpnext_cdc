#!/usr/bin/env node

/**
 * Add ERPNext CDC Connector
 * 
 * Auto-discovers ERPNext database and deploys Debezium connector
 * for the tables specified in TARGET_TABLES.
 * 
 * Usage: node utils/add-erp-connector.js
 */

const mysql = require('mysql2/promise');
const axios = require('axios');
const fs = require('fs');
const path = require('path');

// Load environment variables
require('dotenv').config({ path: path.join(__dirname, '..', '.env.local') });

// Configuration
const DB_HOST = process.env.DB_HOST || 'localhost';
const DB_PORT = process.env.DB_PORT || 3306;
const DB_USER = process.env.DB_USER || 'root';
const DB_PASSWORD = process.env.DB_PASSWORD || 'admin';
const KAFKA_CONNECT_URL = process.env.KAFKA_CONNECT_URL || 'http://localhost:8083';
const TOPIC_PREFIX = process.env.TOPIC_PREFIX || 'erpnext';
const TARGET_TABLES = process.env.TARGET_TABLES || 'tabEmployee,tabAttendance';

// Config output directory
const CONFIG_DIR = path.join(__dirname, 'config');

async function discoverERPDatabase(connection) {
  // Get all databases
  const [databases] = await connection.execute('SHOW DATABASES');
  const dbNames = databases.map(db => db.Database);

  // Filter for ERPNext database (starts with underscore and has hex-like name)
  const erpDatabases = dbNames.filter(name => /^_[a-f0-9]+$/i.test(name));

  if (erpDatabases.length === 0) {
    throw new Error('No ERPNext database found! Expected format: _1f2b3e1ef71e8d5b');
  }

  return erpDatabases[0];
}

async function getAvailableTables(connection, dbName) {
  const [tables] = await connection.execute(`
    SELECT TABLE_NAME as name, TABLE_ROWS as \`rows\`
    FROM information_schema.TABLES 
    WHERE TABLE_SCHEMA = ? AND TABLE_NAME LIKE 'tab%'
    ORDER BY TABLE_NAME
  `, [dbName]);

  return tables;
}

function generateConnectorConfig(dbName, tables) {
  // Convert localhost to host.docker.internal for Docker
  let dbHost = DB_HOST;
  if (dbHost === 'localhost' || dbHost === '127.0.0.1') {
    dbHost = 'host.docker.internal';
  }

  // Build table include list
  const tableList = tables.map(t => `${dbName}.${t}`).join(',');

  // Generate UNIQUE server ID for each deployment (prevents state conflicts after clearing)
  // Using timestamp ensures each run gets a fresh ID, so Debezium treats it as a new connector
  const serverId = 184000 + (Date.now() % 100000);

  return {
    name: 'erpnext-cdc-connector',
    config: {
      'connector.class': 'io.debezium.connector.mysql.MySqlConnector',
      'tasks.max': '1',
      'database.hostname': dbHost,
      'database.port': String(DB_PORT),
      'database.user': DB_USER,
      'database.password': DB_PASSWORD,
      'database.server.id': String(serverId),
      'topic.prefix': TOPIC_PREFIX,
      'database.include.list': dbName,
      'table.include.list': tableList,
      'schema.history.internal.kafka.bootstrap.servers': 'kafka:9092',
      'schema.history.internal.kafka.topic': `schema-changes.${TOPIC_PREFIX}`,
      'schema.history.internal.consumer.security.protocol': 'PLAINTEXT',
      'schema.history.internal.producer.security.protocol': 'PLAINTEXT',
      'schema.history.internal.store.only.captured.tables.ddl': 'true',
      'include.schema.changes': 'false',
      'transforms': 'unwrap',
      'transforms.unwrap.type': 'io.debezium.transforms.ExtractNewRecordState',
      'transforms.unwrap.drop.tombstones': 'false',
      'transforms.unwrap.delete.handling.mode': 'rewrite',
      'snapshot.mode': 'when_needed',
      'snapshot.locking.mode': 'none',
      'database.allowPublicKeyRetrieval': 'true',
      'decimal.handling.mode': 'string',
      'bigint.unsigned.handling.mode': 'long',
      'time.precision.mode': 'adaptive_time_microseconds'
    }
  };
}

async function saveConfig(config) {
  // Ensure config directory exists
  if (!fs.existsSync(CONFIG_DIR)) {
    fs.mkdirSync(CONFIG_DIR, { recursive: true });
  }

  const configPath = path.join(CONFIG_DIR, 'erpnext-connector.json');
  fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
  console.log(`[OK] Config saved to: ${configPath}`);

  return configPath;
}

async function deployConnector(config) {
  console.log(`\nDeploying connector to Kafka Connect...`);
  console.log(`  URL: ${KAFKA_CONNECT_URL}`);

  try {
    // Check if Kafka Connect is available
    await axios.get(KAFKA_CONNECT_URL, { timeout: 5000 });
  } catch (error) {
    throw new Error(`Cannot connect to Kafka Connect at ${KAFKA_CONNECT_URL}. Is it running?`);
  }

  // Delete existing connector if present
  try {
    await axios.delete(`${KAFKA_CONNECT_URL}/connectors/${config.name}`);
    console.log('  Deleted existing connector');
    await new Promise(resolve => setTimeout(resolve, 2000));
  } catch (e) {
    // Connector doesn't exist, which is fine
  }

  // Create new connector
  await axios.post(`${KAFKA_CONNECT_URL}/connectors`, config, {
    headers: { 'Content-Type': 'application/json' }
  });
  console.log('[OK] Connector deployed!');

  // Wait and check status
  console.log('\nWaiting for connector to start...');
  await new Promise(resolve => setTimeout(resolve, 3000));

  const status = await axios.get(`${KAFKA_CONNECT_URL}/connectors/${config.name}/status`);
  const connectorState = status.data.connector?.state || 'UNKNOWN';
  const taskState = status.data.tasks?.[0]?.state || 'UNKNOWN';

  console.log(`\nConnector Status: ${connectorState}`);
  console.log(`Task Status: ${taskState}`);

  if (status.data.tasks?.[0]?.trace) {
    console.error(`\n[WARNING] Task Error:\n${status.data.tasks[0].trace.substring(0, 300)}...`);
    return false;
  }

  return connectorState === 'RUNNING' && taskState === 'RUNNING';
}

async function addERPConnector() {
  console.log('='.repeat(60));
  console.log('Add ERPNext CDC Connector');
  console.log('='.repeat(60));
  console.log('');

  let connection;

  try {
    // Connect to database
    console.log(`Connecting to database at ${DB_HOST}:${DB_PORT}...`);
    connection = await mysql.createConnection({
      host: DB_HOST,
      port: parseInt(DB_PORT),
      user: DB_USER,
      password: DB_PASSWORD,
      connectTimeout: 10000
    });
    console.log('[OK] Connected to database\n');

    // Discover ERPNext database
    console.log('Discovering ERPNext database...');
    const dbName = await discoverERPDatabase(connection);
    console.log(`[OK] Found ERPNext database: ${dbName}\n`);

    // Get available tables
    const allTables = await getAvailableTables(connection, dbName);
    console.log(`Available DocType tables: ${allTables.length}`);

    // Parse target tables from env
    const targetTables = TARGET_TABLES.split(',').map(t => t.trim());
    console.log(`\nTarget tables from .env.local:`);
    targetTables.forEach(table => {
      const found = allTables.find(t => t.name === table);
      console.log(`  ${found ? '[OK]' : '[X]'} ${table}${found ? ` (${found.rows} rows)` : ' - NOT FOUND!'}`);
    });

    // Filter to only existing tables
    const validTables = targetTables.filter(t => allTables.find(at => at.name === t));

    if (validTables.length === 0) {
      throw new Error('No valid target tables found! Check TARGET_TABLES in .env.local');
    }

    // Generate connector config
    console.log('\nGenerating connector configuration...');
    const config = generateConnectorConfig(dbName, validTables);

    // Save config to file
    await saveConfig(config);

    // Deploy connector
    const success = await deployConnector(config);

    if (success) {
      console.log('\n' + '='.repeat(60));
      console.log('[SUCCESS] Connector deployed successfully!');
      console.log('='.repeat(60));
      console.log('\nExpected Kafka topics:');
      validTables.forEach(table => {
        console.log(`  -> ${TOPIC_PREFIX}.${dbName}.${table}`);
      });
      console.log('\nRun `node utils/check-topics.js` to verify topics are created.');
    } else {
      console.log('\n[WARNING] Connector deployed but may have issues. Check logs.');
    }

  } catch (error) {
    console.error('\n[ERROR]', error.message);
    if (error.response?.data) {
      console.error('Response:', JSON.stringify(error.response.data, null, 2));
    }
  } finally {
    if (connection) {
      await connection.end();
    }
  }
}

// Run if executed directly
if (require.main === module) {
  addERPConnector();
}

module.exports = { addERPConnector };
