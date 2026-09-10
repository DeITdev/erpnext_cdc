const mysql = require('mysql2/promise');
require('dotenv').config();

async function testConnection() {
  const config = {
    host: process.env.DB_HOST || 'localhost',
    port: process.env.DB_PORT || 3306,
    user: process.env.DB_USER || 'root',
    password: process.env.DB_PASSWORD || 'admin',
    connectTimeout: 10000
  };

  console.log('Testing connection with config:', {
    ...config,
    password: '***hidden***'
  });

  try {
    console.log('Attempting to connect...');
    const connection = await mysql.createConnection(config);
    console.log('[OK] Connected successfully!');

    const [result] = await connection.execute('SELECT VERSION() as version');
    console.log('Database version:', result[0].version);

    const [databases] = await connection.execute('SHOW DATABASES');
    console.log('Available databases:');
    databases.forEach(db => console.log(`   - ${db.Database}`));

    await connection.end();
    console.log('[OK] Connection test completed successfully!');

  } catch (error) {
    console.error('[ERROR] Connection failed:', error.message);
    console.error('Troubleshooting tips:');
    console.error('   1. Check if MariaDB/MySQL is running');
    console.error('   2. Verify host and port settings');
    console.error('   3. Check username/password');
    console.error('   4. Ensure database allows external connections');
  }
}

testConnection();