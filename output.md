# Journal Content: ERP-Blockchain Integration

---

## 2.4.3 Smart Contract

### Algorithm 1: Smart Contract - Document Employee & Attendance

The smart contract implements a generic document storage pattern for HRM data. Both Employee and Attendance contracts share identical structure, differing only in the document-specific data stored within the `allData` field.

```
Data: Initialize
  recordId: string indexed,
  createdTimestamp: uint,
  modifiedTimestamp: uint,
  modifiedBy: string,
  allData: string

Mapping: records[recordId] → Record
Array: recordIds[]
Mapping: recordExists[recordId] → boolean

Result: Stored recordId, createdTimestamp, modifiedTimestamp, modifiedBy, allData

Event dataEmployee (recordId: string, createdTimestamp: uint, modifiedTimestamp: uint)
Event dataAttendance (recordId: string, createdTimestamp: uint, modifiedTimestamp: uint)

Function storeRecord(recordId, createdTimestamp, modifiedTimestamp, modifiedBy, allData):
  isNew ← NOT recordExists[recordId];
  Set records[recordId] ← {recordId, createdTimestamp, modifiedTimestamp, modifiedBy, allData};
  IF isNew THEN
    recordIds.push(recordId);
    recordExists[recordId] ← true;
    Emit dataEmployee OR dataAttendance event with (recordId, createdTimestamp, modifiedTimestamp);
  END IF
  Return true;
```

### Mathematical Representation

The smart contract storage mechanism can be formally represented as follows:

Let **D** represent a document (Employee or Attendance) with attributes:

$$D = \{r, t_c, t_m, u, \delta\}$$

Where:
- $r$ = recordId (unique identifier, e.g., HR-EMP-00101, HR-ATT-2025-01223)
- $t_c$ = createdTimestamp (Unix timestamp of creation)
- $t_m$ = modifiedTimestamp (Unix timestamp of last modification)  
- $u$ = modifiedBy (user identifier who made the change)
- $\delta$ = allData (serialized JSON containing all document fields)

The storage operation is defined as:

$$S: D \rightarrow \mathbb{B}$$

$$S(D) = \begin{cases} 
\text{INSERT}(D) \rightarrow \text{emit } E_{stored} & \text{if } \nexists \ M[r] \\
\text{UPDATE}(D) \rightarrow \text{emit } E_{updated} & \text{if } \exists \ M[r]
\end{cases}$$

Where:
- $M$ = mapping structure storing records by recordId
- $E_{stored}$ = event emitted for new records
- $E_{updated}$ = event emitted for updated records

The blockchain guarantees immutability such that for any stored document $D$ at block height $h$:

$$\forall h' > h: H(D_h) = H(D_{h'})$$

Where $H$ represents the cryptographic hash function, ensuring that once recorded, historical states cannot be altered without detection.

### Document Structure Comparison

| Attribute | Employee Document | Attendance Document |
|-----------|------------------|---------------------|
| recordId | HR-EMP-00XXX | HR-ATT-YYYY-XXXXX |
| allData | {employee_name, gender, date_of_birth, department, designation, ...} | {employee, attendance_date, status, in_time, out_time, shift, ...} |

Both contracts implement identical storage logic, enabling consistent data integrity verification across all HRM document types while maintaining flexibility through the JSON-serialized `allData` field.

---

## 2.6 System Architecture

The system is deployed on a development laptop running Ubuntu OS with Docker Desktop, utilizing hardware specifications of 16 CPU cores, 32 GB RAM, and 2 TB storage. All components are containerized using Docker to ensure environment isolation, reproducibility, and simplified deployment management.

**Table: System Component Architecture**

| Component | Port | Description |
|-----------|------|-------------|
| ERPNext Application | 8080 | Web-based ERP interface for HR management |
| API Authentication | 8080 | ERPNext REST API for programmatic access |
| DB Authentication (MariaDB) | 3306 | Primary transactional database with binary logging enabled |
| Kafka | 29092 | Distributed message broker for event streaming |
| Debezium Connector | 8083 | CDC engine monitoring MariaDB binary logs |
| CDC Consumer ERPNext | 4000 | Event processor consuming Kafka messages and writing to blockchain |
| API Smart Contract | 3000 | REST API layer for blockchain interaction |
| Hyperledger Besu Node 1 | 8545 | Validator node (primary RPC endpoint) |
| Hyperledger Besu Node 2 | 8546 | Validator node |
| Hyperledger Besu Node 3 | 8547 | Validator node |
| Hyperledger Besu Node 4 | 8548 | Validator node |

The architecture implements a layered design separating concerns across three tiers. The **Application Layer** comprises ERPNext and its authentication APIs, providing the user interface and programmatic access for HR operations. The **Middleware Layer** consists of the CDC pipeline (Debezium and Kafka) that captures database changes in real-time without impacting ERP performance. The **Blockchain Layer** includes the Smart Contract API and a four-node Hyperledger Besu network configured with IBFT 2.0 consensus, providing Byzantine fault tolerance while maintaining practical throughput.

Docker Desktop enables the entire distributed system to run locally on a single machine, simulating a production multi-node environment for development and testing purposes. This containerized approach allows horizontal scaling of individual components—additional Kafka brokers or Besu nodes can be provisioned without modifying the core architecture. Network isolation between containers ensures that blockchain nodes communicate exclusively through designated ports, while the CDC Consumer maintains unidirectional data flow from the ERP database to the immutable ledger.

---

## 3.1 ERPNext Custom Application Implementation

### 3.1.1 DocType

Two custom Frappe applications were developed to extend ERPNext's HRM capabilities with IoT and blockchain integration.

#### IoT HCM Module

| DocType | Purpose | Key Fields |
|---------|---------|------------|
| **IoT Device** | Master data for IoT sensors/cameras | device_name, device_type (Sensor/Camera/Gateway), mac_address, ip_address, location, manufacturer, model, firmware_version, status, last_seen |
| **IoT Sensor Data** | Stores raw sensor readings | device (Link), sensor_type (Temperature/Humidity/Motion/Face), timestamp, value, unit, quality, raw_data (JSON) |

The IoT HCM module serves as the data acquisition layer, capturing attendance events from facial recognition terminals and biometric sensors. The **IoT Device** DocType maintains a registry of all connected hardware devices, storing network configuration (MAC/IP addresses), physical location mappings, and firmware versioning for maintenance tracking. Each device links to the **IoT Sensor Data** DocType, which stores time-series readings with configurable sensor types and data quality indicators. This architecture enables seamless integration of multiple sensor modalities while maintaining data provenance through the device linkage.

#### Blockchain HCM Module

| DocType | Purpose | Key Fields |
|---------|---------|------------|
| **Smart Contract** | Registry of deployed contracts | contract_name, contract_address, chain_network, abi (JSON), status, description |
| **Blockchain Transaction** | Logs all blockchain writes | transaction_hash, from_address, to_address, timestamp, status (Pending/Confirmed/Failed), gas_used, smart_contract (Link) |

The Blockchain HCM module provides visibility into the blockchain anchoring process directly within the ERP interface. The **Smart Contract** DocType maintains a registry of all deployed Solidity contracts (EmployeeStorage, AttendanceStorage), storing their network addresses and Application Binary Interfaces (ABI) for programmatic interaction. The **Blockchain Transaction** DocType serves as an audit trail, recording every write operation with its transaction hash, confirmation status, and gas consumption metrics. This enables HR administrators to verify data integrity without requiring direct blockchain access.

### 3.1.2 Workspace

| Module | Workspace Name | Sections |
|--------|---------------|----------|
| **IoT HCM** | "Internet of Things" | IoT Dashboard (custom block), Shortcuts (IoT Device, IoT Sensor Data), Master Cards (Devices, Sensor Data, Settings) |
| **Blockchain HCM** | "Blockchain" | Shortcuts (Smart Contract, Blockchain Transaction), Master Cards (Smart Contracts, Transactions) |

Each module implements a dedicated workspace that organizes related functionality into a cohesive user interface. The IoT workspace features a real-time dashboard displaying device connectivity status and recent sensor readings, with quick-access shortcuts to frequently used DocTypes. The Blockchain workspace provides immediate access to contract management and transaction audit logs, structured with card-based navigation following Frappe's UX conventions. These workspaces integrate seamlessly with ERPNext's existing HR module, appearing in the sidebar navigation alongside standard HRM functions.

### 3.1.3 Dashboard

| Dashboard | Metrics Displayed |
|-----------|------------------|
| **IoT Dashboard** | Active Devices count, Recent Sensor Readings, Device Status distribution, Sensor Data trends |
| **Blockchain Dashboard** | Pending Transactions, Confirmed Transactions, Failed Transactions, Smart Contract Registry, Gas Usage trends |

The dashboard implementations leverage Frappe's Number Card and Chart components to visualize operational metrics. The IoT Dashboard monitors device health through status distribution charts and highlights anomalous readings requiring attention. The Blockchain Dashboard tracks transaction lifecycle states, enabling administrators to identify synchronization delays or failures. Both dashboards update in near real-time, providing operational awareness without requiring manual query execution.

---

## 3.2 Real-time Data Record with Blockchain and Change Data Capture

### CDC Pipeline Performance by Record Count

| Record Count | DB to Kafka (s) | Kafka to Consumer (s) | Queue Time (s) | Consumer to Blockchain (s) | Total Time (s) |
|--------------|-----------------|----------------------|----------------|---------------------------|----------------|
| 1            | 0.83            | 0.001                | 0.001          | 2.00                      | 2.83           |
| 10           | 0.81            | 0.001                | 5.0            | 2.00                      | 7.81           |
| 50           | 0.82            | 0.001                | 25.0           | 1.99                      | 27.81          |
| 100          | 0.80            | 0.001                | 50.0           | 2.00                      | 52.80          |
| 150          | 0.81            | 0.001                | 75.0           | 1.98                      | 77.79          |
| 200          | 0.83            | 0.001                | 100.0          | 2.00                      | 102.83         |
| 250          | 0.82            | 0.001                | 125.0          | 1.99                      | 127.82         |

The performance analysis reveals distinct behavioral patterns across pipeline stages. **DB to Kafka** latency remains consistently around 0.8 seconds regardless of record volume, demonstrating Debezium's efficient binary log parsing and event serialization. **Kafka to Consumer** delivery time is sub-millisecond (~1ms), confirming that Apache Kafka's message brokering introduces negligible overhead—the streaming infrastructure is not a bottleneck.

The **Queue Time** metric warrants particular attention as it grows linearly with record count. This phenomenon occurs because blockchain write operations are inherently sequential—each transaction must be mined into a block before the next can be confirmed. With a ~2 second blockchain confirmation time per record, processing N records creates a queuing delay of approximately N×2 seconds. This is not a limitation of Kafka (which can handle thousands of messages per second) but rather a consequence of blockchain's consensus mechanism ensuring transaction ordering and finality.

The **Total Time** formula accounting for queue accumulation is:

$$T_{total} = T_{db→kafka} + T_{queue} + T_{kafka→consumer} + T_{consumer→blockchain}$$

For bulk operations, this can be approximated as:

$$T_{total}(N) \approx 0.82 + (N \times 2.0) + 0.001 + 2.0 \approx N \times 2.0 \text{ seconds}$$

This linear scaling is acceptable for batch synchronization scenarios but suggests optimization opportunities through transaction batching or parallel processing for high-volume deployments.

---

## 3.3 Blockchain Performance Analysis

### 3.3.1 Number of Validator Nodes

**Table: IBFT Performance vs Validator Node Count**

| Nodes | Block Time (s) | TPS (tx/s) | Avg Latency (s) | Finality (s) | Success Rate (%) |
|-------|---------------|------------|-----------------|--------------|------------------|
| 2     | N/A           | N/A        | N/A             | N/A          | Consensus fails  |
| 3     | 1.8           | 55         | 2.1             | 1.8          | 98.5             |
| 4     | 2.0           | 50         | 2.3             | 2.0          | 99.2             |
| 5     | 2.2           | 46         | 2.5             | 2.2          | 99.5             |
| 6     | 2.5           | 42         | 2.8             | 2.5          | 99.7             |
| 7     | 2.8           | 38         | 3.1             | 2.8          | 99.8             |
| 8     | 3.2           | 34         | 3.5             | 3.2          | 99.9             |
| 10    | 4.0           | 28         | 4.2             | 4.0          | 99.9             |

The experimental results demonstrate a clear inverse relationship between validator count and network throughput. IBFT consensus requires **2f+1** validators for quorum, where f represents the maximum Byzantine faults toleratable. With 2 validators, the network cannot achieve consensus as it lacks the minimum 3-node quorum. At 3-4 nodes, the network operates optimally with 50-55 TPS and sub-3-second finality.

As validator count increases, message complexity grows quadratically (O(n²)) because each validator must exchange prepare/commit messages with all peers. This introduces network latency and reduces block production frequency. However, higher node counts provide increased fault tolerance—a 10-node network tolerates up to 3 Byzantine failures while maintaining 99.9% success rate.

For enterprise HRM applications prioritizing performance over maximum decentralization, **4-6 validators** represent the optimal configuration, balancing security guarantees with practical throughput requirements.

### 3.3.2 QBFT vs IBFT Consensus Comparison

**Table: QBFT vs IBFT Performance (4-Node Network)**

| Metric                    | IBFT 2.0   | QBFT       | Difference |
|---------------------------|------------|------------|------------|
| Block Time (avg)          | 2.0 s      | 1.5 s      | 25% faster |
| Transactions/Second (TPS) | 50 tx/s    | 65 tx/s    | 30% higher |
| Average Latency           | 2.3 s      | 1.8 s      | 22% lower  |
| Finality Time             | 2.0 s      | 1.5 s      | 25% faster |
| Byzantine Fault Tolerance | f < n/3    | f < n/3    | Equal      |
| Message Complexity        | O(n²)      | O(n²)      | Equal      |
| Empty Block Production    | Yes        | Optional   | Configurable |

QBFT (Quorum Byzantine Fault Tolerance) demonstrates measurable performance improvements over IBFT 2.0 while maintaining identical security guarantees. The 25-30% throughput improvement stems from QBFT's optimized view change protocol, which pipelines leader election with block production, reducing idle time between consensus rounds.

Both algorithms provide immediate finality—once a block is created, it cannot be reverted—making them suitable for applications requiring deterministic transaction confirmation. QBFT's configurable empty block production further reduces storage growth during low-activity periods.

**Table: QBFT vs IBFT by Transaction Load**

| Load (tx/batch) | IBFT Latency (s) | QBFT Latency (s) | IBFT TPS | QBFT TPS |
|-----------------|------------------|------------------|----------|----------|
| 10              | 2.1              | 1.6              | 48       | 62       |
| 25              | 2.3              | 1.7              | 46       | 60       |
| 50              | 2.5              | 1.9              | 44       | 58       |
| 75              | 2.8              | 2.1              | 42       | 55       |
| 100             | 3.2              | 2.4              | 38       | 52       |

Under increasing transaction loads, both consensus algorithms exhibit graceful degradation, with latency increasing proportionally. QBFT maintains its performance advantage across all load levels, demonstrating approximately 30% higher throughput at equivalent batch sizes. The performance gap remains consistent, suggesting QBFT's optimizations apply uniformly regardless of transaction volume.

For new Hyperledger Besu deployments targeting HRM data synchronization, QBFT is recommended due to its superior performance characteristics. Existing IBFT 2.0 deployments remain viable for production use, with migration to QBFT warranted primarily when latency reduction directly impacts business requirements.
