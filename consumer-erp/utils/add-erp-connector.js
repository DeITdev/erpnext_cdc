#!/usr/bin/env node

const path = require('path');
const { spawnSync } = require('child_process');

function addERPConnector() {
  const bootstrapScript = path.resolve(
    __dirname,
    '..',
    '..',
    'kafka-debezium',
    'scripts',
    'bootstrap-cdc.sh'
  );

  const result = spawnSync(bootstrapScript, {
    stdio: 'inherit',
    env: process.env
  });

  if (result.error) {
    throw result.error;
  }
  if (result.status !== 0) {
    throw new Error(`CDC bootstrap failed with exit code ${result.status}`);
  }
}

if (require.main === module) {
  try {
    addERPConnector();
  } catch (error) {
    console.error(`[ERROR] ${error.message}`);
    process.exitCode = 1;
  }
}

module.exports = { addERPConnector };
