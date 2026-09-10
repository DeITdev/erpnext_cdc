#!/usr/bin/env node

/**
 * Check ERPNext Database - Discover database schema and list tables
 * 
 * Usage: node utils/check-erp-database.js
 */

const mysql = require('mysql2/promise');
const path = require('path');

// Load environment variables
require('dotenv').config({ path: path.join(__dirname, '..', '.env.local') });

// Database configuration
const DB_HOST = process.env.DB_HOST || 'localhost';
const DB_PORT = process.env.DB_PORT || 3306;
const DB_USER = process.env.DB_USER || 'root';
const DB_PASSWORD = process.env.DB_PASSWORD || 'admin';

async function checkERPDatabase() {
  console.log('='.repeat(60));
  console.log('ERPNext Database Discovery');
  console.log('='.repeat(60));
  console.log('');

  let connection;

  try {
    // Connect to database
    console.log(`Connecting to ${DB_HOST}:${DB_PORT}...`);
    connection = await mysql.createConnection({
      host: DB_HOST,
      port: parseInt(DB_PORT),
      user: DB_USER,
      password: DB_PASSWORD,
      connectTimeout: 10000
    });
    console.log('[OK] Connected successfully!\n');

    // Get database version
    const [versionResult] = await connection.execute('SELECT VERSION() as version');
    console.log(`Database Version: ${versionResult[0].version}`);

    // Get all databases
    const [databases] = await connection.execute('SHOW DATABASES');
    const dbNames = databases.map(db => db.Database);

    // Filter for ERPNext database (starts with underscore and has hex-like name)
    const erpDatabases = dbNames.filter(name =>
      /^_[a-f0-9]+$/i.test(name)
    );

    console.log(`\nAll Databases (${dbNames.length}):`);
    dbNames.forEach(db => {
      const isErp = erpDatabases.includes(db);
      console.log(`   ${isErp ? '->' : '-'} ${db}${isErp ? ' (ERPNext)' : ''}`);
    });

    if (erpDatabases.length === 0) {
      console.log('\n[WARNING] No ERPNext database found!');
      console.log('   ERPNext databases typically have names like: _1f2b3e1ef71e8d5b');
      return;
    }

    // Use the first ERPNext database found
    const erpDbName = erpDatabases[0];
    console.log(`\n${'='.repeat(60)}`);
    console.log(`ERPNext Database: ${erpDbName}`);
    console.log(`${'='.repeat(60)}`);

    // Switch to ERPNext database
    await connection.changeUser({ database: erpDbName });

    // Get all tables with row counts
    const [tables] = await connection.execute(`
      SELECT 
        TABLE_NAME as name,
        TABLE_ROWS as \`rows\`,
        ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) as size_mb,
        UPDATE_TIME as last_updated
      FROM information_schema.TABLES 
      WHERE TABLE_SCHEMA = ?
      ORDER BY TABLE_ROWS DESC
    `, [erpDbName]);

    console.log(`\nTotal Tables: ${tables.length}`);

    // Show top tables by row count
    console.log(`\nTop 20 Tables by Row Count:`);
    console.log('   ' + '-'.repeat(55));
    tables.slice(0, 20).forEach((table, i) => {
      const rows = table.rows || 0;
      const size = table.size_mb || 0;
      console.log(`   ${String(i + 1).padStart(2)}. ${table.name.padEnd(35)} ${String(rows).padStart(8)} rows  (${size} MB)`);
    });

    // Show commonly monitored tables
    const commonTables = ['tabEmployee', 'tabAttendance', 'tabSalary Slip', 'tabLeave Application',
      'tabSales Invoice', 'tabPurchase Invoice', 'tabStock Entry'];

    const foundCommon = tables.filter(t => commonTables.includes(t.name));

    if (foundCommon.length > 0) {
      console.log(`\nCommon CDC Tables Found:`);
      console.log('   ' + '-'.repeat(55));
      foundCommon.forEach(table => {
        const rows = table.rows || 0;
        console.log(`   -> ${table.name.padEnd(35)} ${String(rows).padStart(8)} rows`);
      });
    }

    // Show all tables starting with 'tab' (ERPNext DocTypes)
    const docTypeTables = tables.filter(t => t.name.startsWith('tab'));
    console.log(`\nAll DocType Tables (${docTypeTables.length}):`);
    console.log('   ' + '-'.repeat(55));

    // Group by first letter after 'tab' for easier reading
    const groupedTables = {};
    docTypeTables.forEach(table => {
      const name = table.name.replace(/^tab/, '');
      const firstLetter = name.charAt(0).toUpperCase();
      if (!groupedTables[firstLetter]) {
        groupedTables[firstLetter] = [];
      }
      groupedTables[firstLetter].push(table.name);
    });

    Object.keys(groupedTables).sort().forEach(letter => {
      console.log(`   [${letter}] ${groupedTables[letter].join(', ')}`);
    });

    // Summary for .env.local
    console.log(`\n${'='.repeat(60)}`);
    console.log('Suggested .env.local Configuration:');
    console.log(`${'='.repeat(60)}`);
    console.log(`DB_HOST=${DB_HOST}`);
    console.log(`DB_PORT=${DB_PORT}`);
    console.log(`DB_USER=${DB_USER}`);
    console.log(`DB_PASSWORD=***`);
    console.log(`DB_NAME=${erpDbName}`);
    console.log(`TARGET_TABLES=tabEmployee,tabAttendance`);

  } catch (error) {
    console.error('[ERROR]', error.message);
    console.error('\nTroubleshooting:');
    console.error('   1. Check if MariaDB/MySQL is running');
    console.error('   2. Verify DB_HOST, DB_PORT, DB_USER, DB_PASSWORD in .env.local');
    console.error('   3. Ensure the database allows external connections');
  } finally {
    if (connection) {
      await connection.end();
    }
  }
}

// Run if executed directly
if (require.main === module) {
  checkERPDatabase();
}

module.exports = { checkERPDatabase };
