const axios = require('axios');
require('dotenv').config();

// Test configuration
const API_ENDPOINT = process.env.API_ENDPOINT || 'http://127.0.0.1:4001';
const PRIVATE_KEY = process.env.PRIVATE_KEY;

console.log('Testing Blockchain Consumer Integration\n');

/**
 * Test blockchain API connectivity
 */
async function testBlockchainAPI() {
  console.log('1. Testing Blockchain API connectivity...');
  try {
    const response = await axios.get(API_ENDPOINT, { timeout: 5000 });
    console.log(`[OK] API is reachable: ${response.data.name}`);
    console.log(`   Version: ${response.data.version}`);
    console.log(`   Blockchain URL: ${response.data.blockchain?.url}`);
    console.log(`   Supported contracts: ${response.data.contracts?.join(', ')}`);
    return true;
  } catch (error) {
    console.log(`[ERROR] API test failed: ${error.message}`);
    return false;
  }
}

/**
 * Test employee data submission
 */
async function testEmployeeSubmission() {
  console.log('\n2. Testing Employee data submission...');

  const testEmployeeData = {
    privateKey: PRIVATE_KEY,
    employeeData: {
      recordId: `TEST-EMP-${Date.now()}`,
      createdTimestamp: new Date().toISOString(),
      modifiedTimestamp: new Date().toISOString(),
      modifiedBy: 'test-consumer',
      allData: {
        name: `TEST-EMP-${Date.now()}`,
        employee_name: 'John Doe Test',
        department: 'IT Department',
        company: 'Test Company',
        status: 'Active',
        date_of_joining: '2025-01-01',
        designation: 'Software Engineer',
        email: 'john.doe@test.com',
        phone: '+1234567890'
      }
    }
  };

  try {
    const response = await axios.post(`${API_ENDPOINT}/employees`, testEmployeeData, {
      timeout: 30000,
      headers: { 'Content-Type': 'application/json' }
    });

    if (response.data.success) {
      console.log(`[OK] Employee test successful`);
      console.log(`   Transaction Hash: ${response.data.transactionHash}`);
      console.log(`   Block Number: ${response.data.blockNumber}`);
      console.log(`   Record ID: ${response.data.recordId}`);
      return response.data.recordId;
    } else {
      console.log(`[ERROR] Employee test failed: ${JSON.stringify(response.data)}`);
      return null;
    }
  } catch (error) {
    console.log(`[ERROR] Employee test error: ${error.message}`);
    if (error.response?.data) {
      console.log(`   Response: ${JSON.stringify(error.response.data)}`);
    }
    return null;
  }
}

/**
 * Test attendance data submission
 */
async function testAttendanceSubmission() {
  console.log('\n3. Testing Attendance data submission...');

  const testAttendanceData = {
    privateKey: PRIVATE_KEY,
    attendanceData: {
      recordId: `TEST-ATT-${Date.now()}`,
      createdTimestamp: new Date().toISOString(),
      modifiedTimestamp: new Date().toISOString(),
      modifiedBy: 'test-consumer',
      allData: {
        name: `TEST-ATT-${Date.now()}`,
        employee: 'TEST-EMP-001',
        employee_name: 'John Doe Test',
        attendance_date: '2025-09-12',
        status: 'Present',
        department: 'IT Department',
        company: 'Test Company',
        working_hours: 8.0,
        in_time: '09:00:00',
        out_time: '17:00:00',
        late_entry: false,
        early_exit: false
      }
    }
  };

  try {
    const response = await axios.post(`${API_ENDPOINT}/attendances`, testAttendanceData, {
      timeout: 30000,
      headers: { 'Content-Type': 'application/json' }
    });

    if (response.data.success) {
      console.log(`[OK] Attendance test successful`);
      console.log(`   Transaction Hash: ${response.data.blockchain?.transactionHash}`);
      console.log(`   Block Number: ${response.data.blockchain?.blockNumber}`);
      console.log(`   Record ID: ${response.data.recordId}`);
      return response.data.recordId;
    } else {
      console.log(`[ERROR] Attendance test failed: ${JSON.stringify(response.data)}`);
      return null;
    }
  } catch (error) {
    console.log(`[ERROR] Attendance test error: ${error.message}`);
    if (error.response?.data) {
      console.log(`   Response: ${JSON.stringify(error.response.data)}`);
    }
    return null;
  }
}

/**
 * Test data retrieval
 */
async function testDataRetrieval(recordId, endpoint) {
  console.log(`\n4. Testing data retrieval from ${endpoint}...`);

  try {
    const response = await axios.get(`${API_ENDPOINT}${endpoint}/${recordId}`, {
      timeout: 10000
    });

    if (response.data.success) {
      console.log(`[OK] Data retrieval successful`);
      console.log(`   Record ID: ${response.data.recordId}`);
      console.log(`   Created: ${response.data.createdTimestamp}`);
      console.log(`   Modified By: ${response.data.modifiedBy}`);
      return true;
    } else {
      console.log(`[ERROR] Data retrieval failed: ${JSON.stringify(response.data)}`);
      return false;
    }
  } catch (error) {
    console.log(`[ERROR] Data retrieval error: ${error.message}`);
    return false;
  }
}

/**
 * Run all tests
 */
async function runTests() {
  console.log(`Configuration:`);
  console.log(`   API Endpoint: ${API_ENDPOINT}`);
  console.log(`   Private Key: ${PRIVATE_KEY ? '[OK] Configured' : '[ERROR] Missing'}`);
  console.log('');

  if (!PRIVATE_KEY) {
    console.log('[ERROR] PRIVATE_KEY is required in .env file');
    return;
  }

  let allTestsPassed = true;

  // Test 1: API connectivity
  const apiTest = await testBlockchainAPI();
  allTestsPassed = allTestsPassed && apiTest;

  if (!apiTest) {
    console.log('\n[ERROR] API connectivity failed. Cannot proceed with other tests.');
    return;
  }

  // Test 2: Employee submission
  const employeeRecordId = await testEmployeeSubmission();
  allTestsPassed = allTestsPassed && (employeeRecordId !== null);

  // Test 3: Attendance submission
  const attendanceRecordId = await testAttendanceSubmission();
  allTestsPassed = allTestsPassed && (attendanceRecordId !== null);

  // Test 4: Data retrieval
  if (employeeRecordId) {
    const employeeRetrieval = await testDataRetrieval(employeeRecordId, '/employees');
    allTestsPassed = allTestsPassed && employeeRetrieval;
  }

  if (attendanceRecordId) {
    const attendanceRetrieval = await testDataRetrieval(attendanceRecordId, '/attendances');
    allTestsPassed = allTestsPassed && attendanceRetrieval;
  }

  // Summary
  console.log('\nTEST SUMMARY');
  console.log('================');
  if (allTestsPassed) {
    console.log('[OK] All tests passed! Consumer should work correctly.');
  } else {
    console.log('[ERROR] Some tests failed. Check the blockchain API and configuration.');
  }
}

// Run the tests
runTests().catch(error => {
  console.log(`[ERROR] Test suite failed: ${error.message}`);
  process.exit(1);
});
