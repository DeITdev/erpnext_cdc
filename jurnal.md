### **Abstract**

This paper proposes a novel architecture for securing Human Resource (HR) data within an Enterprise Resource Planning (ERP) system. While ERP systems like ERPNext provide robust operational efficiency, centralized databases remain vulnerable to data manipulation and lack immutable audit trails. To address this, we developed a customizable HR module using the Frappe Framework integrated with a private blockchain network (Hyperledger Besu). The integration utilizes a Change Data Capture (CDC) mechanism via Debezium and Apache Kafka to ensure real-time, event-driven synchronization between the ERP database (MariaDB) and the blockchain ledger. Results demonstrate that the system achieves data synchronization within approximately 2.5 seconds, balancing operational speed with high-security guarantees. This study further analyzes the impact of validator node scalability on network latency.

---

### **1. Introduction**

ERP systems like ERPNext have revolutionized organizational efficiency, but their reliance on centralized, mutable relational databases introduces the **"Super-user Paradox"**. This vulnerability allows privileged administrators to alter transactional data and tamper with audit logs without detection. In HRM, the trustworthiness of attendance records is paramount as they influence payroll and legal compliance. While IoT-enabled attendance (RFID, facial, or speech recognition) improves capture efficiency, the data is typically stored in conventional SQL databases exposed to unauthorized "manager overrides". To address this, we propose an architecture that couples Frappe/ERPNext with a private Hyperledger Besu blockchain via a CDC pipeline. This ensures every attendance event is recorded immutably, providing cryptographic proof of authenticity.

---

### **2. Method (Architecture)**

The system follows a microservices-oriented architecture comprising three main layers: the ERP Platform (Application Layer), the Event Streaming Layer (Middleware), and the Blockchain Network (Security Layer).

* 
**2.1 ERPNext**: Selected for its zero licensing costs and highly customizable architecture. It is deployed using Docker containers with MariaDB 10.6 as the primary transactional database.


* 
**2.2 Frappe Framework**: A metadata-driven framework using a DocType system. Two custom apps (IoT and Blockchain) were developed to manage smart sensors and track blockchain transaction confirmations directly within the ERP interface.


* 
**2.3 Human Resource Management**: Focuses on two key entities to prevent payroll fraud:


* 
**2.3.1 Employee**: Master data creation/modification is captured by CDC to anchor a cryptographic hash on the network, preventing quiet alterations to the workforce identity.


* 
**2.3.2 Attendance**: Processes raw check-in/check-out events from face recognition systems via webhooks. The CDC pipeline anchors these hashes to prevent retroactive changes to working hours.




* 
**2.4 Blockchain System**: Hyperledger Besu establishes a private permissioned network.


* 
**2.4.1 Hyperledger Besu**: Configured as a private consortium chain using IBFT 2.0 consensus for immediate finality and deterministic performance.


* 
**2.4.2 Node Validator**: Responsible for verifying transactions and assembling blocks; requires a supermajority (2f+1) approval.


* 
**2.4.3 Smart Contract**: A Solidity contract maps ERP identifiers to their data hashes and metadata, acting as the immutable "source of truth".




* **2.5 Change Data Capture (CDC)**:
* 
**2.5.1 Debezium Connector**: Monitors the MariaDB binary log (binlog) in real-time, capturing row-level changes (INSERT, UPDATE, DELETE) in full-row image format.


* 
**2.5.2 Apache Kafka**: Decouples the ERP from the blockchain, buffering messages during congestion and allowing for horizontal scaling.




* 
**2.6 System Integration**: The workflow follows a 6-step pipeline: User Action  DB Commit  Debezium Capture  Kafka Queue  Consumer Signing (via HashiCorp Vault)  Blockchain Confirmation.



---

### **3. Results and Discussion**

* 
**3.1 ERP Implementation**: Successfully integrated blockchain visibility into the ERP UI.


* 
**3.1.1 Doctype**: Modified the "Attendance" DocType to display read-only Transaction Hashes and Block Numbers.


* 
**3.1.2 Workspace**: A "Blockchain Audit" workspace allows auditors to verify that CDC services are running correctly.


* 
**3.1.3 Dashboard**: Monitors metrics like "Pending Verifications," "Verified Records," and "Failed Syncs".




* 
**3.2 Real-time Data Record**: Performance tests showed the system handles up to 250 records with a total sync time consistently under 3 seconds. The "Consumer to Blockchain" stage is the primary bottleneck (~1.5s), while the "DB to Kafka" overhead is only 0.06s, ensuring the user experience remains fast.


* **3.3 Blockchain Performance Analysis**:
* 
**3.3.1 Number of Nodes Validator**: Stress tests confirm that as validator counts increase, network throughput (TPS) decreases and latency increases due to higher communication complexity. A count of 4 to 6 nodes is optimal.


* 
**3.3.2 QBFT & IBFT Consensus**: Comparative testing shows the choice of consensus algorithm significantly impacts the latency of the anchoring process.





---

### **4. Conclusion**

The proposed system bridges the gap between ERP flexibility and rigorous data integrity. Using the Frappe Framework and a CDC-Kafka-Blockchain pipeline, we achieved tamper-proof attendance records with near real-time synchronization (under 3 seconds) without compromising the performance of the core ERP. Future work will explore Zero-Knowledge Proofs (ZKP) to enhance employee data privacy on the ledger.

Would you like me to help you format a specific table of these results for the AI that will generate your mock data?