#!/usr/bin/env node

/**
 * Test script to see exactly what Kafka is sending
 * Shows raw events without any filtering
 * 
 * Usage: node utils/test-kafka-events.js
 */

const { Kafka } = require('kafkajs');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env.local') });

const KAFKA_BROKER = process.env.KAFKA_BROKER || '127.0.0.1:29092';
const TOPIC_PREFIX = process.env.TOPIC_PREFIX || 'erpnext';

const kafka = new Kafka({
  clientId: 'kafka-event-tester',
  brokers: [KAFKA_BROKER]
});

const admin = kafka.admin();
const consumer = kafka.consumer({ groupId: 'test-event-viewer-' + Date.now() });

let eventCount = 0;
const eventsByRecord = new Map();

async function main() {
  console.log('='.repeat(60));
  console.log('Kafka Event Viewer - Raw Events');
  console.log('='.repeat(60));
  console.log(`Broker: ${KAFKA_BROKER}`);
  console.log(`Topic Prefix: ${TOPIC_PREFIX}`);
  console.log('\nPress Ctrl+C to stop and see summary\n');

  await admin.connect();
  const topics = await admin.listTopics();
  const cdcTopics = topics.filter(t => t.startsWith(TOPIC_PREFIX) && (t.includes('tabEmployee') || t.includes('tabAttendance')));
  await admin.disconnect();

  if (cdcTopics.length === 0) {
    console.log('[ERROR] No CDC topics found');
    process.exit(1);
  }

  console.log(`Watching topics: ${cdcTopics.join(', ')}\n`);

  await consumer.connect();
  await consumer.subscribe({ topics: cdcTopics, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      eventCount++;
      const tableName = topic.split('.').pop();

      try {
        const data = JSON.parse(message.value.toString());
        const recordId = data.name || data.id || 'unknown';
        const modified = data.modified || 'N/A';

        // Track events per record
        const key = `${tableName}:${recordId}`;
        if (!eventsByRecord.has(key)) {
          eventsByRecord.set(key, []);
        }
        eventsByRecord.get(key).push({ eventCount, modified, timestamp: message.timestamp });

        console.log(`[Event #${eventCount}] ${tableName} | ${recordId} | modified: ${modified}`);
      } catch (e) {
        console.log(`[Event #${eventCount}] ${tableName} | Parse error: ${e.message}`);
      }
    }
  });

  // Cleanup on Ctrl+C
  process.on('SIGINT', async () => {
    console.log('\n\n' + '='.repeat(60));
    console.log('SUMMARY');
    console.log('='.repeat(60));
    console.log(`Total Kafka events received: ${eventCount}`);
    console.log(`Unique records: ${eventsByRecord.size}`);
    console.log(`Average events per record: ${(eventCount / eventsByRecord.size).toFixed(1)}`);

    console.log('\n--- Events per Record ---');
    for (const [key, events] of eventsByRecord.entries()) {
      console.log(`${key}: ${events.length} events`);
    }

    console.log('\n' + '='.repeat(60));
    console.log('This proves ERPNext sends multiple DB writes per record.');
    console.log('Deduplication must happen at the consumer level.');
    console.log('='.repeat(60));

    await consumer.disconnect();
    process.exit(0);
  });
}

main().catch(console.error);
