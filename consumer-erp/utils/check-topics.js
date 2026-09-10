#!/usr/bin/env node

/**
 * Check Kafka Topics
 * 
 * Lists all available topics in Kafka.
 * 
 * Usage: node utils/check-topics.js
 */

const { Kafka } = require('kafkajs');
const path = require('path');

// Load environment variables
require('dotenv').config({ path: path.join(__dirname, '..', '.env.local') });

const KAFKA_BROKER = process.env.KAFKA_BROKER || '127.0.0.1:29092';

async function checkTopics() {
  const kafka = new Kafka({
    clientId: 'topic-checker',
    brokers: [KAFKA_BROKER]
  });

  const admin = kafka.admin();

  try {
    await admin.connect();
    console.log('Connected to Kafka\n');

    const topics = await admin.listTopics();

    // Sort topics alphabetically
    const sortedTopics = topics.sort();

    console.log(`Total topics: ${sortedTopics.length}`);
    sortedTopics.forEach(topic => {
      console.log(`- ${topic}`);
    });

    await admin.disconnect();

  } catch (error) {
    console.error('Error:', error.message);
    console.error('\nMake sure Kafka is running at:', KAFKA_BROKER);
  }
}

// Run if executed directly
if (require.main === module) {
  checkTopics();
}

module.exports = { checkTopics };