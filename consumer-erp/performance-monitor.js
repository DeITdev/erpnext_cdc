/**
 * Performance Monitor for Blockchain-ERP Integration
 * 
 * Monitors and reports latency metrics for the CDC pipeline:
 * - DB to Kafka latency (avg)
 * - Kafka to Consumer latency (avg)
 * - Consumer to Blockchain latency (avg)
 * - Total end-to-end latency (sum of averages)
 * 
 * Runs forever until stopped with Ctrl+C.
 * 
 * Usage: node performance-monitor.js [options]
 *   --output <file>      Output CSV file for results
 *   --interval <seconds> Stats display interval (default: 10)
 */

const { Kafka } = require('kafkajs');
const axios = require('axios');
const fs = require('fs');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '.env.local') });

// Configuration
const KAFKA_BROKER = process.env.KAFKA_BROKER || '127.0.0.1:29092';
const API_ENDPOINT = process.env.API_ENDPOINT || 'http://127.0.0.1:4000';
const PRIVATE_KEY = process.env.PRIVATE_KEY;
const TOPIC_PREFIX = process.env.TOPIC_PREFIX || 'erpnext';
const TARGET_TABLES = (process.env.TARGET_TABLES || 'tabEmployee,tabAttendance').split(',').map(t => t.trim());

// Parse command line arguments
const args = process.argv.slice(2);
const getArg = (name, defaultValue) => {
  const index = args.indexOf(name);
  return index !== -1 && args[index + 1] ? args[index + 1] : defaultValue;
};

const OUTPUT_FILE = getArg('--output', null);
const STATS_INTERVAL = parseInt(getArg('--interval', '10')) * 1000;

// Metrics storage
const metrics = {
  events: [],
  startTime: null,
  summary: {
    totalEvents: 0,
    uniqueEvents: 0,
    duplicatesSkipped: 0,
    dbToKafka: { sum: 0, count: 0 },
    queueTime: { sum: 0, count: 0 },        // Total wait in Kafka queue
    kafkaToConsumer: { sum: 0, count: 0 },   // Pure delivery time (after consumer ready)
    consumerToBlockchain: { sum: 0, count: 0 }
  }
};

// Deduplication - only process first event per recordId+modified combination
const processedRecords = new Map();

// Track when the last message processing ended (for measuring Kafka delivery time, not queue time)
let lastProcessingEndTime = null;
const DEDUP_WINDOW_MS = 5000; // 5 seconds window for duplicates

// Kafka setup
const kafka = new Kafka({
  clientId: 'performance-monitor',
  brokers: [KAFKA_BROKER],
  connectionTimeout: 5000,
  requestTimeout: 30000
});

const consumer = kafka.consumer({
  groupId: 'performance-monitor-group',
  sessionTimeout: 120000,
  heartbeatInterval: 10000
});

/**
 * Get endpoint for table
 */
function getEndpoint(tableName) {
  const endpoints = {
    'tabEmployee': '/employees',
    'tabAttendance': '/attendances'
  };
  return endpoints[tableName] || `/${tableName.toLowerCase()}`;
}

/**
 * Transform data for blockchain API
 */
function transformForBlockchain(tableName, data) {
  const recordId = data.name || `${tableName}-${Date.now()}`;
  const createdTimestamp = data.creation || Date.now() * 1000;
  const modifiedTimestamp = data.modified || Date.now() * 1000;
  const modifiedBy = data.modified_by || 'performance-monitor';

  const { name, creation, modified, modified_by, ...restData } = data;
  const allData = restData;

  if (tableName === 'tabEmployee') {
    return {
      employeeData: { recordId, createdTimestamp, modifiedTimestamp, modifiedBy, allData }
    };
  } else if (tableName === 'tabAttendance') {
    return {
      attendanceData: { recordId, createdTimestamp, modifiedTimestamp, modifiedBy, allData }
    };
  } else {
    return {
      documentData: { recordId, createdTimestamp, modifiedTimestamp, modifiedBy, allData }
    };
  }
}

/**
 * Check if record exists on blockchain and get its timestamp
 */
async function checkBlockchainRecord(endpoint, recordId) {
  try {
    const response = await axios.get(`${API_ENDPOINT}${endpoint}/${recordId}`, {
      timeout: 10000
    });
    if (response.data.success && response.data.recordId) {
      // Return the modified timestamp so we can compare
      return { exists: true, modifiedTimestamp: response.data.modifiedTimestamp };
    }
    return { exists: false };
  } catch (error) {
    // API throws error when record doesn't exist
    return { exists: false };
  }
}

/**
 * Send to blockchain and measure time
 * Checks if record exists AND has older timestamp - allows updates
 */
async function sendToBlockchain(endpoint, transformedData, recordId, newTimestamp) {
  const startTime = Date.now();

  try {
    // Check if record already exists on blockchain
    const blockchainRecord = await checkBlockchainRecord(endpoint, recordId);

    if (blockchainRecord.exists) {
      // Compare timestamps - skip only if blockchain has same or newer data
      const blockchainTs = parseInt(blockchainRecord.modifiedTimestamp) || 0;
      const incomingTs = parseInt(newTimestamp) || 0;

      if (blockchainTs >= incomingTs) {
        // Blockchain has same or newer data, skip
        const endTime = Date.now();
        return { success: true, latency: endTime - startTime, skipped: true };
      }
      // Blockchain has older data, proceed with update
    }

    // Write to blockchain (new record or update)
    const fullPayload = { privateKey: PRIVATE_KEY, ...transformedData };
    const response = await axios.post(`${API_ENDPOINT}${endpoint}`, fullPayload, {
      timeout: 60000,
      headers: { 'Content-Type': 'application/json' }
    });

    const endTime = Date.now();
    const latency = endTime - startTime;

    return { success: response.data.success, latency, skipped: false };
  } catch (error) {
    const endTime = Date.now();
    return { success: false, latency: endTime - startTime, skipped: false };
  }
}

/**
 * Print real-time summary
 */
function printStats() {
  const summary = metrics.summary;
  const count = summary.uniqueEvents;

  // Timer not started yet (no events received)
  if (metrics.startTime === null) {
    console.log(`\n[STATS] Waiting for first event to start timer...\n`);
    return;
  }

  const runtimeSeconds = (Date.now() - metrics.startTime) / 1000;

  if (count === 0) {
    console.log(`\n[STATS] Runtime: ${runtimeSeconds.toFixed(0)}s | Events: 0 | Waiting for CDC events...\n`);
    return;
  }

  const avgDbKafka = (summary.dbToKafka.sum / summary.dbToKafka.count / 1000).toFixed(3);
  const avgQueueTime = (summary.queueTime.sum / summary.queueTime.count / 1000).toFixed(3);
  const avgKafkaConsumer = (summary.kafkaToConsumer.sum / summary.kafkaToConsumer.count / 1000).toFixed(3);
  const avgConsumerBlockchain = (summary.consumerToBlockchain.sum / summary.consumerToBlockchain.count / 1000).toFixed(3);
  const avgTotal = (parseFloat(avgDbKafka) + parseFloat(avgKafkaConsumer) + parseFloat(avgConsumerBlockchain)).toFixed(3);

  console.log('\n' + '='.repeat(70));
  console.log(`PERFORMANCE METRICS (Runtime: ${runtimeSeconds.toFixed(0)}s | Unique: ${count} | Skipped: ${summary.duplicatesSkipped})`);
  console.log('='.repeat(70));
  console.log(`| Metric                     | Average (s) |`);
  console.log(`|----------------------------|-------------|`);
  const avgDbKafkaSec = (summary.dbToKafka.sum / summary.dbToKafka.count / 1000).toFixed(3);
  console.log(`| DB to Kafka (s)            | ${avgDbKafkaSec.padStart(11)} |`);
  console.log(`| Queue Time (s)             | ${avgQueueTime.padStart(11)} |`);
  console.log(`| Kafka to Consumer (s)      | ${avgKafkaConsumer.padStart(11)} |`);
  console.log(`| Consumer to Blockchain (s) | ${avgConsumerBlockchain.padStart(11)} |`);
  console.log(`|----------------------------|-------------|`);
  console.log(`| Total Time (s)             | ${avgTotal.padStart(11)} |`);
  console.log('='.repeat(70));
  console.log(`Throughput: ${(count / runtimeSeconds).toFixed(2)} events/sec\n`);
}

/**
 * Print final summary
 */
function printFinalSummary() {
  const summary = metrics.summary;
  const count = summary.uniqueEvents;
  const totalEvents = summary.totalEvents;

  console.log('\n\n' + '='.repeat(70));
  console.log('FINAL PERFORMANCE REPORT');
  console.log('='.repeat(70));

  // Handle case when no events were received
  if (metrics.startTime === null) {
    console.log('\nNo events were received during this session.');
    console.log('='.repeat(70));
    return;
  }

  const runtimeSeconds = (Date.now() - metrics.startTime) / 1000;

  console.log(`\nTest Duration: ${runtimeSeconds.toFixed(1)} seconds`);
  console.log(`Total Kafka Events: ${totalEvents} (Unique: ${count}, Duplicates Skipped: ${summary.duplicatesSkipped})`);
  console.log(`Average Throughput: ${(count / runtimeSeconds).toFixed(2)} unique events/sec`);

  if (count > 0) {
    const avgDbKafka = (summary.dbToKafka.sum / summary.dbToKafka.count / 1000).toFixed(3);
    const avgQueueTime = (summary.queueTime.sum / summary.queueTime.count / 1000).toFixed(3);
    const avgKafkaConsumer = (summary.kafkaToConsumer.sum / summary.kafkaToConsumer.count / 1000).toFixed(3);
    const avgConsumerBlockchain = (summary.consumerToBlockchain.sum / summary.consumerToBlockchain.count / 1000).toFixed(3);
    const avgTotal = (parseFloat(avgDbKafka) + parseFloat(avgKafkaConsumer) + parseFloat(avgConsumerBlockchain)).toFixed(3);

    console.log('\n--- Latency Summary ---\n');
    console.log(`| Metric                     | Average (s) |`);
    console.log(`|----------------------------|-------------|`);
    const avgDbKafkaSec = (summary.dbToKafka.sum / summary.dbToKafka.count / 1000).toFixed(3);
    console.log(`| DB to Kafka (s)            | ${avgDbKafkaSec.padStart(11)} |`);
    console.log(`| Queue Time (s)             | ${avgQueueTime.padStart(11)} |`);
    console.log(`| Kafka to Consumer (s)      | ${avgKafkaConsumer.padStart(11)} |`);
    console.log(`| Consumer to Blockchain (s) | ${avgConsumerBlockchain.padStart(11)} |`);
    console.log(`|----------------------------|-------------|`);
    console.log(`| Total Time (s)             | ${avgTotal.padStart(11)} |`);
  }

  console.log('\n' + '='.repeat(70));
}

/**
 * Export results to CSV
 */
function exportToCSV(filename) {
  const header = 'timestamp,topic,tableName,recordId,dbToKafka_ms,kafkaToConsumer_ms,consumerToBlockchain_ms,total_ms\n';

  const rows = metrics.events.map(e =>
    `${e.timestamp},${e.topic},${e.tableName},${e.recordId},${e.dbToKafka},${e.kafkaToConsumer},${e.consumerToBlockchain},${e.totalLatency}`
  ).join('\n');

  fs.writeFileSync(filename, header + rows);
  console.log(`\nResults exported to: ${filename}`);
}

/**
 * Discover topics based on configuration
 */
async function discoverTopics() {
  const admin = kafka.admin();
  await admin.connect();

  const allTopics = await admin.listTopics();
  const relevantTopics = allTopics.filter(topic => {
    if (topic.includes('schema-changes')) return false;
    if (!topic.startsWith(TOPIC_PREFIX)) return false;
    const tableName = topic.split('.').pop();
    return TARGET_TABLES.includes(tableName);
  });

  await admin.disconnect();
  return relevantTopics;
}

/**
 * Process a CDC message
 */
async function processMessage(topic, message) {
  const consumerReceiveTime = Date.now();
  const kafkaTimestamp = parseInt(message.timestamp);

  try {
    const messageValue = message.value?.toString();
    if (!messageValue) return;


    const changeEvent = JSON.parse(messageValue);
    // Extract table name and data FIRST (need it for modified timestamp)
    const topicParts = topic.split('.');
    const tableName = topicParts[topicParts.length - 1];

    let changeData;
    if (changeEvent.payload) {
      changeData = changeEvent.payload.after || changeEvent.payload.before;
    } else {
      changeData = changeEvent;
    }

    if (!changeData) return;

    const recordId = changeData.name || changeData.id || 'unknown';
    const modifiedKey = `${recordId}:${changeData.modified || kafkaTimestamp}`;

    // Deduplication: skip if we've seen this exact record+modified combination recently
    metrics.summary.totalEvents++;
    const now = Date.now();
    if (processedRecords.has(modifiedKey)) {
      metrics.summary.duplicatesSkipped++;
      return; // Skip duplicate
    }

    // Mark as processed and clean up old entries
    processedRecords.set(modifiedKey, now);
    for (const [key, timestamp] of processedRecords.entries()) {
      if (now - timestamp > DEDUP_WINDOW_MS) {
        processedRecords.delete(key);
      }
    }

    metrics.summary.uniqueEvents++;

    // Start timer on first event (not at script startup)
    if (metrics.startTime === null) {
      metrics.startTime = Date.now();
      console.log(`\n[OK] First event received - Timer started at ${new Date().toISOString()}\n`);
    }

    // Extract DB event timestamp - use the record's modified field from ERPNext
    // This is the actual database modification time
    // We'll work in MICROSECONDS for precision, then convert for display
    let dbEventTimeMicros = kafkaTimestamp * 1000; // Default: kafka timestamp in microseconds
    let kafkaTimestampMicros = kafkaTimestamp * 1000;

    // Try to get from record's modified field
    if (changeData.modified) {
      if (typeof changeData.modified === 'string') {
        // ERPNext format: "2025-12-29 15:50:00.123456"
        const modifiedDate = new Date(changeData.modified.replace(' ', 'T') + 'Z');
        if (!isNaN(modifiedDate.getTime())) {
          dbEventTimeMicros = modifiedDate.getTime() * 1000;
        }
      } else if (typeof changeData.modified === 'number') {
        // Debezium likely sends in microseconds (16+ digits like 1767050081341894)
        if (changeData.modified > 1e15) {
          // Already microseconds
          dbEventTimeMicros = changeData.modified;
        } else if (changeData.modified > 1e12) {
          // Milliseconds - convert to microseconds
          dbEventTimeMicros = changeData.modified * 1000;
        } else {
          // Unix seconds - convert to microseconds
          dbEventTimeMicros = changeData.modified * 1000000;
        }
      }
    }

    // Fallback: try Debezium payload structure
    if (dbEventTimeMicros === kafkaTimestampMicros) {
      if (changeEvent.payload?.source?.ts_ms) {
        dbEventTimeMicros = changeEvent.payload.source.ts_ms * 1000;
      } else if (changeEvent.payload?.ts_ms) {
        dbEventTimeMicros = changeEvent.payload.ts_ms * 1000;
      } else if (changeEvent.source?.ts_ms) {
        dbEventTimeMicros = changeEvent.source.ts_ms * 1000;
      }
    }

    // Calculate latencies in MICROSECONDS for precision
    // Note: ERPNext modified is in LOCAL time (UTC+7), Kafka is in UTC
    // Adjust for 7 hour timezone offset (7 * 60 * 60 * 1000 * 1000 microseconds)
    const TIMEZONE_OFFSET_MICROS = 7 * 60 * 60 * 1000 * 1000; // UTC+7
    const adjustedDbTimeMicros = dbEventTimeMicros - TIMEZONE_OFFSET_MICROS;
    const dbToKafkaLatencyMicros = Math.max(0, kafkaTimestampMicros - adjustedDbTimeMicros);
    const dbToKafkaLatency = dbToKafkaLatencyMicros / 1000; // Convert to ms for storage

    // Queue Time: total wait from Kafka production to consumption (includes consumer busy time)
    const queueTime = Math.max(0, consumerReceiveTime - kafkaTimestamp);

    // Kafka to Consumer: time from when Kafka received the message to when consumer received it
    // This is the same as queue time - the actual delivery latency
    const kafkaToConsumerLatency = queueTime;

    // Send to blockchain and measure time
    const endpoint = getEndpoint(tableName);
    const transformedData = transformForBlockchain(tableName, changeData);
    // Get modified timestamp for comparison
    const dataKey = Object.keys(transformedData)[0];
    const modifiedTimestamp = transformedData[dataKey]?.modifiedTimestamp || 0;
    const { success, latency: blockchainLatency, skipped: blockchainSkipped } = await sendToBlockchain(endpoint, transformedData, recordId, modifiedTimestamp);

    const totalLatency = dbToKafkaLatency + kafkaToConsumerLatency + blockchainLatency;

    // Update metrics
    metrics.summary.totalEvents++;
    metrics.summary.dbToKafka.sum += dbToKafkaLatency;
    metrics.summary.dbToKafka.count++;
    metrics.summary.queueTime.sum += queueTime;
    metrics.summary.queueTime.count++;
    metrics.summary.kafkaToConsumer.sum += kafkaToConsumerLatency;
    metrics.summary.kafkaToConsumer.count++;
    metrics.summary.consumerToBlockchain.sum += blockchainLatency;
    metrics.summary.consumerToBlockchain.count++;

    // Store event
    metrics.events.push({
      timestamp: consumerReceiveTime,
      topic,
      tableName,
      recordId,
      dbToKafka: dbToKafkaLatency,
      queueTime: queueTime,
      kafkaToConsumer: kafkaToConsumerLatency,
      consumerToBlockchain: blockchainLatency,
      totalLatency
    });

    // Log event
    const status = success ? '[OK]' : '[X]';
    console.log(`${status} ${tableName} ${recordId} | DB->Kafka: ${dbToKafkaLatency.toFixed(3)}ms | Queue: ${queueTime}ms | Kafka->Consumer: ${kafkaToConsumerLatency}ms | Consumer->Blockchain: ${blockchainLatency}ms`);

    // Update last processing end time for next message's Kafka delivery calculation
    lastProcessingEndTime = Date.now();

  } catch (error) {
    console.error(`[X] Error processing message: ${error.message}`);
  }
}

/**
 * Main function
 */
async function main() {
  console.log('='.repeat(70));
  console.log('Blockchain-ERP Integration Performance Monitor');
  console.log('='.repeat(70));
  console.log(`\nConfiguration:`);
  console.log(`  Kafka Broker: ${KAFKA_BROKER}`);
  console.log(`  API Endpoint: ${API_ENDPOINT}`);
  console.log(`  Topic Prefix: ${TOPIC_PREFIX}`);
  console.log(`  Target Tables: ${TARGET_TABLES.join(', ')}`);
  console.log(`  Stats Interval: ${STATS_INTERVAL / 1000} seconds`);
  console.log(`  Output File: ${OUTPUT_FILE || 'None'}`);
  console.log(`\n  Press Ctrl+C to stop and see final report.\n`);

  try {
    // Connect to Kafka
    await consumer.connect();
    console.log('[OK] Connected to Kafka\n');

    // Discover and subscribe to topics
    let topics = await discoverTopics();

    if (topics.length === 0) {
      console.log('[WARNING] No CDC topics found yet.');
      console.log('  Waiting for topics to appear...');

      while (topics.length === 0) {
        await new Promise(resolve => setTimeout(resolve, 5000));
        topics = await discoverTopics();
        if (topics.length === 0) {
          process.stdout.write('.');
        }
      }
      console.log('\n[OK] Topics found!');
    }

    console.log(`[OK] Discovered ${topics.length} topic(s):`);
    topics.forEach(t => console.log(`  - ${t}`));
    await consumer.subscribe({ topics, fromBeginning: false });

    // Timer starts on first event, not here
    metrics.startTime = null;
    console.log(`\n[OK] Monitoring started at ${new Date().toISOString()}`);
    console.log('     Runs forever - make changes in ERPNext to generate CDC events.\n');

    // Print stats periodically
    const statsInterval = setInterval(printStats, STATS_INTERVAL);

    // Process messages
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        await processMessage(topic, message);
      }
    });

    // Handle graceful shutdown
    process.on('SIGINT', async () => {
      clearInterval(statsInterval);
      console.log('\n\nShutting down...');
      await consumer.disconnect();
      printFinalSummary();

      if (OUTPUT_FILE) {
        exportToCSV(OUTPUT_FILE);
      }

      process.exit(0);
    });

  } catch (error) {
    console.error(`\n[ERROR] ${error.message}`);
    process.exit(1);
  }
}

// Run
main().catch(console.error);
