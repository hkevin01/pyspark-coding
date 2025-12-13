#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
ZOOKEEPER & KAFKA - Distributed Coordination Deep Dive
================================================================================

MODULE OVERVIEW:
----------------
Apache ZooKeeper is a distributed coordination service that acts as the "central
nervous system" for distributed applications. In the context of Kafka, ZooKeeper
manages cluster metadata, leader election, and configuration.

This module explains:
• What ZooKeeper is and why it exists
• How Kafka uses ZooKeeper (legacy) and KRaft (new)
• Distributed coordination patterns
• Leader election mechanisms
• Configuration management
• Service discovery
• Production deployment strategies

WHAT IS ZOOKEEPER?
------------------

ZooKeeper is a distributed, hierarchical key-value store that provides:
1. **Coordination**: Synchronize actions across distributed nodes
2. **Configuration**: Centralized configuration management
3. **Naming**: Service discovery and registry
4. **Synchronization**: Distributed locks and barriers
5. **Leader Election**: Choose a leader from multiple nodes

Think of ZooKeeper as:
┌────────────────────────────────────────────────────────────────┐
│ "A Filing System + Notification System for Distributed Apps"  │
│                                                                │
│  Filing System:                    Notification System:        │
│  • Hierarchical namespace          • Watch for changes        │
│  • Store small data (KB)           • Get notified instantly   │
│  • Atomic operations               • React to cluster events  │
└────────────────────────────────────────────────────────────────┘

ZOOKEEPER DATA MODEL:
---------------------

ZooKeeper uses a hierarchical namespace similar to a file system:

```
/                              ← Root
├── kafka                      ← Kafka cluster metadata
│   ├── brokers                ← Broker information
│   │   ├── ids                ← Active broker IDs
│   │   │   ├── 0              ← Broker 0 details
│   │   │   ├── 1              ← Broker 1 details
│   │   │   └── 2              ← Broker 2 details
│   │   └── topics             ← Topic metadata
│   │       ├── user-events    ← Topic configuration
│   │       └── orders         ← Topic configuration
│   ├── controller             ← Current controller broker ID
│   ├── controller_epoch       ← Controller version number
│   └── config                 ← Cluster configuration
│       ├── topics             ← Topic-specific configs
│       └── brokers            ← Broker configs
├── consumers                  ← Consumer group metadata
│   └── my-consumer-group      ← Consumer group state
│       ├── offsets            ← Committed offsets
│       └── owners             ← Partition ownership
└── spark                      ← Spark cluster coordination
    ├── masters                ← Spark master nodes
    └── workers                ← Spark worker registration
```

Each node (znode) can:
• Store data (up to 1MB, typically KB)
• Have children (like a directory)
• Be watched for changes
• Be ephemeral (disappears when client disconnects)
• Be sequential (auto-incrementing suffix)

ZOOKEEPER ARCHITECTURE:
-----------------------

```
ZooKeeper Ensemble (Cluster):
┌──────────────────────────────────────────────────────────────┐
│                     ZooKeeper Cluster                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │   Leader     │  │  Follower 1  │  │  Follower 2  │       │
│  │  (ZK Node 1) │  │  (ZK Node 2) │  │  (ZK Node 3) │       │
│  │              │  │              │  │              │       │
│  │  Handles     │  │  Forwards    │  │  Forwards    │       │
│  │  all writes  │  │  writes to   │  │  writes to   │       │
│  │              │  │  leader      │  │  leader      │       │
│  │  Handles     │  │  Handles     │  │  Handles     │       │
│  │  reads       │  │  reads       │  │  reads       │       │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘       │
│         └──────────────────┴──────────────────┘              │
│                   Quorum Protocol (Zab)                      │
│             (Majority must agree on writes)                  │
└──────────────────────────────────────────────────────────────┘
                              │
                    ┌─────────┴─────────┐
                    │                   │
         ┌──────────▼─────────┐  ┌─────▼──────────┐
         │  Kafka Brokers     │  │  Spark Master  │
         │  (Clients)         │  │  (Client)      │
         │  • Register        │  │  • Register    │
         │  • Watch topics    │  │  • Elect leader│
         │  • Elect controller│  │  • Get config  │
         └────────────────────┘  └────────────────┘

Key Properties:
• Odd number of nodes (3, 5, or 7 typical)
• Quorum = Majority (3 nodes → need 2, 5 nodes → need 3)
• Can tolerate (n-1)/2 failures (3 nodes → 1 failure, 5 nodes → 2 failures)
• All writes go through leader (consistency)
• Reads can be served by any node (scalability)
```

WHY KAFKA NEEDS ZOOKEEPER (Legacy Architecture):
-------------------------------------------------

Before Kafka 2.8, ZooKeeper was essential for:

1. **Broker Registration & Discovery**
   ```
   When Kafka broker starts:
   1. Broker connects to ZooKeeper
   2. Creates ephemeral znode: /brokers/ids/<broker-id>
   3. Stores broker metadata (host, port, topics)
   4. If broker crashes → znode disappears automatically
   5. Other brokers detect the change via watches
   ```

2. **Controller Election**
   ```
   Controller = The boss broker that manages the cluster

   Election Process:
   1. All brokers watch /controller znode
   2. First broker to create /controller becomes controller
   3. Controller gets a unique epoch number
   4. If controller fails → znode disappears
   5. New election triggered automatically
   6. New controller gets higher epoch number

   Controller Responsibilities:
   • Assign partitions to brokers
   • Monitor broker failures
   • Trigger partition leader elections
   • Manage partition replicas
   ```

3. **Topic Configuration**
   ```
   Topic metadata stored in ZooKeeper:
   /brokers/topics/<topic-name>
   {
     "version": 1,
     "partitions": {
       "0": [1, 2, 3],    # Partition 0: replicas on brokers 1,2,3
       "1": [2, 3, 1],    # Partition 1: replicas on brokers 2,3,1
       "2": [3, 1, 2]     # Partition 2: replicas on brokers 3,1,2
     }
   }

   • All brokers watch topic changes
   • Instant cluster-wide notification
   • Consistent view across cluster
   ```

4. **Partition Leader Election**
   ```
   Each partition has:
   • Leader: Handles all reads/writes
   • Followers: Replicate data from leader

   Leader Election (via ZooKeeper):
   /brokers/topics/<topic>/partitions/<partition>/state
   {
     "leader": 1,              # Broker 1 is leader
     "isr": [1, 2, 3],         # In-Sync Replicas
     "controller_epoch": 5,    # Controller version
     "leader_epoch": 12        # Leader version
   }

   If leader fails:
   1. Controller detects via ZooKeeper watch
   2. Selects new leader from ISR
   3. Updates partition state in ZooKeeper
   4. All brokers notified via watches
   ```

5. **Consumer Group Coordination** (Old Consumer)
   ```
   Consumer group metadata:
   /consumers/<group-id>/ids/<consumer-id>  # Active consumers
   /consumers/<group-id>/owners/<topic>/<partition>  # Who owns what
   /consumers/<group-id>/offsets/<topic>/<partition>  # Committed offsets

   Rebalancing Process:
   1. New consumer joins → creates znode
   2. All consumers watch group membership
   3. Change detected → trigger rebalance
   4. Partitions reassigned
   5. New ownership written to ZooKeeper
   ```

KAFKA WITHOUT ZOOKEEPER: KRaft MODE
------------------------------------

Starting Kafka 2.8+ (Production-ready in 3.3+), Kafka can run without ZooKeeper!

**KRaft (Kafka Raft)**: Kafka's own consensus protocol

```
Old Architecture (with ZooKeeper):          New Architecture (KRaft):
┌──────────────┐                           ┌──────────────────┐
│  ZooKeeper   │                           │  Kafka Cluster   │
│   Cluster    │                           │  (Self-managed)  │
│  (3 nodes)   │                           │                  │
└──────┬───────┘                           │  ┌────────────┐  │
       │                                   │  │ Controller │  │
       │                                   │  │  Quorum    │  │
┌──────▼────────────────┐                  │  │ (3 nodes)  │  │
│   Kafka Brokers       │                  │  └─────┬──────┘  │
│  ┌─────┐ ┌─────┐      │                  │        │         │
│  │Brkr1│ │Brkr2│      │                  │  ┌─────▼──────┐  │
│  └─────┘ └─────┘      │                  │  │  Brokers   │  │
└───────────────────────┘                  │  │ ┌────┬────┐│  │
                                           │  │ │Br1 │Br2 ││  │
Total: 6 nodes (3 ZK + 3 Kafka)            │  │ └────┴────┘│  │
                                           │  └────────────┘  │
                                           └──────────────────┘
                                          Total: 3 nodes only!

Benefits of KRaft:
✅ Simpler deployment (no separate ZooKeeper cluster)
✅ Faster metadata operations (no network hop)
✅ Better scalability (millions of partitions)
✅ Faster recovery (no ZooKeeper bottleneck)
✅ Easier operations (one system to manage)
```

ZOOKEEPER OPERATIONS:
---------------------

Basic Operations (via Python kazoo library):

```python
from kazoo.client import KazooClient

# 1. Connect to ZooKeeper
zk = KazooClient(hosts='localhost:2181')
zk.start()

# 2. Create a node (znode)
zk.create("/myapp", b"my data")

# 3. Read data from node
data, stat = zk.get("/myapp")
print(f"Data: {data}, Version: {stat.version}")

# 4. Set data (update)
zk.set("/myapp", b"new data")

# 5. Create ephemeral node (disappears when client disconnects)
zk.create("/myapp/temp", b"temp data", ephemeral=True)

# 6. Create sequential node (auto-incrementing)
path = zk.create("/myapp/item-", b"data", sequence=True)
# Creates: /myapp/item-0000000001, /myapp/item-0000000002, etc.

# 7. Watch for changes
@zk.DataWatch("/myapp")
def watch_node(data, stat):
    print(f"Data changed: {data}")

# 8. Check if node exists
if zk.exists("/myapp"):
    print("Node exists")

# 9. List children
children = zk.get_children("/myapp")
print(f"Children: {children}")

# 10. Delete node
zk.delete("/myapp")

zk.stop()
```

DISTRIBUTED COORDINATION PATTERNS:
----------------------------------

1. **Leader Election Pattern**
   ```python
   from kazoo.recipe.election import Election

   # Multiple processes compete to be leader
   election = Election(zk, "/election", "candidate-1")

   # Block until this process becomes leader
   election.run(leader_function)

   def leader_function():
       print("I am the leader!")
       # Do leader work...
       # If process crashes, leadership automatically transfers
   ```

2. **Distributed Lock Pattern**
   ```python
   from kazoo.recipe.lock import Lock

   # Only one process can hold the lock
   lock = Lock(zk, "/locks/mylock")

   with lock:
       print("I have the lock!")
       # Critical section - only one process executes this
       # Atomic operation across cluster
   ```

3. **Service Discovery Pattern**
   ```python
   # Service registration
   service_path = "/services/my-service"
   zk.create(service_path, b'{"host": "10.0.0.1", "port": 8080}', ephemeral=True)

   # Service discovery
   @zk.ChildrenWatch("/services")
   def watch_services(children):
       print(f"Available services: {children}")
       for child in children:
           data, _ = zk.get(f"/services/{child}")
           print(f"Service {child}: {data}")
   ```

4. **Configuration Management Pattern**
   ```python
   # Centralized config
   config_path = "/config/myapp"
   zk.create(config_path, b'{"db": "mysql://localhost:3306"}')

   # All nodes watch for config changes
   @zk.DataWatch(config_path)
   def update_config(data, stat):
       config = json.loads(data)
       print(f"Config updated: {config}")
       # Reload application config
       reload_app_config(config)
   ```

PRODUCTION DEPLOYMENT:
----------------------

ZooKeeper Ensemble Sizing:
```
┌────────────────────────┬───────────────┬──────────────┐
│ Cluster Size           │ Fault Tolerance│ Use Case     │
├────────────────────────┼───────────────┼──────────────┤
│ 1 node (standalone)    │ 0 failures    │ Development  │
│ 3 nodes (ensemble)     │ 1 failure     │ Small prod   │
│ 5 nodes (ensemble)     │ 2 failures    │ Medium prod  │
│ 7 nodes (ensemble)     │ 3 failures    │ Large prod   │
└────────────────────────┴───────────────┴──────────────┘

⚠️  Never use even numbers! (2, 4, 6)
   • 3 nodes: Can tolerate 1 failure
   • 4 nodes: Can still only tolerate 1 failure (need 3 for quorum)
   • Result: Wasted resources
```

Best Practices:
```
✅ DO:
• Use odd number of ZooKeeper nodes (3, 5, 7)
• Deploy ZooKeeper on separate machines from Kafka
• Use SSDs for ZooKeeper data directory
• Monitor ZooKeeper health (latency, connections)
• Set appropriate JVM heap (typically 1-4GB)
• Enable ZooKeeper authentication (SASL)
• Use separate ZooKeeper for each Kafka cluster
• Backup ZooKeeper data directory
• Set up monitoring and alerting

❌ DON'T:
• Use even numbers (2, 4, 6 nodes)
• Collocate ZooKeeper with Kafka on same machine
• Use spinning disks (HDD) for ZooKeeper data
• Share ZooKeeper across multiple Kafka clusters
• Ignore ZooKeeper logs and metrics
• Run without authentication in production
• Store large data in ZooKeeper (>1MB)
• Perform frequent writes (ZooKeeper is for coordination, not storage)
```

MONITORING ZOOKEEPER:
---------------------

Key Metrics to Monitor:
```
1. Latency:
   • avg_latency: Average request latency
   • min_latency: Minimum latency
   • max_latency: Maximum latency
   • Target: < 10ms average

2. Connections:
   • num_alive_connections: Active client connections
   • Target: Stable count

3. Outstanding Requests:
   • outstanding_requests: Queued requests
   • Target: < 100

4. Znodes:
   • znode_count: Total nodes in namespace
   • Target: < 1 million

5. Data Size:
   • approximate_data_size: Total data stored
   • Target: < 1GB

6. Watch Count:
   • watch_count: Active watches
   • Target: Stable

7. Leadership:
   • leader_uptime: Time current leader has been stable
   • Target: Long uptime (no frequent elections)
```

Check ZooKeeper Status:
```bash
# Four-letter commands (telnet to port 2181)
echo stat | nc localhost 2181  # Server statistics
echo mntr | nc localhost 2181  # Monitoring data
echo conf | nc localhost 2181  # Configuration
echo cons | nc localhost 2181  # Connections
echo ruok | nc localhost 2181  # Health check (returns "imok")
```

MIGRATION PATH: ZOOKEEPER → KRAFT:
-----------------------------------

```
Step-by-Step Migration:

1. Current State: Kafka with ZooKeeper
   ┌───────────┐    ┌────────────┐
   │ ZooKeeper │───▶│   Kafka    │
   └───────────┘    └────────────┘

2. Upgrade Kafka to 3.3+ (supports both modes)
   ┌───────────┐    ┌────────────┐
   │ ZooKeeper │───▶│ Kafka 3.3+ │
   └───────────┘    └────────────┘

3. Create new KRaft cluster (parallel deployment)
   ┌───────────┐    ┌────────────┐
   │ ZooKeeper │───▶│  Old Kafka │
   └───────────┘    └────────────┘

                    ┌────────────┐
                    │ KRaft Kafka│ (New)
                    └────────────┘

4. Migrate topics/data to KRaft cluster
   (Use MirrorMaker 2.0 for live migration)

5. Switch producers/consumers to KRaft cluster

6. Decommission old ZooKeeper-based cluster
                    ┌────────────┐
                    │ KRaft Kafka│
                    └────────────┘

Timeline: 2-6 weeks depending on cluster size
```

WHY ZOOKEEPER IS AWESOME:
-------------------------

```
Use Cases Beyond Kafka:

1. Distributed Locking:
   • Multiple processes need exclusive access
   • Example: Batch job coordination

2. Leader Election:
   • Multiple nodes, only one does work
   • Example: Active/passive database setup

3. Configuration Management:
   • Centralized config for distributed apps
   • Example: Feature flags, A/B testing

4. Service Discovery:
   • Dynamic service registration/discovery
   • Example: Microservices finding each other

5. Distributed Queues:
   • Implement priority queues
   • Example: Task scheduling systems

6. Barriers & Semaphores:
   • Coordinate multiple processes
   • Example: MapReduce-style computation

Companies Using ZooKeeper:
• Netflix (service discovery)
• Yahoo (coordination service)
• Twitter (distributed systems)
• LinkedIn (Kafka coordination)
• Uber (service mesh coordination)
```

AUTHOR: PySpark Education Project
LICENSE: Educational Use - MIT License
VERSION: 1.0.0 - ZooKeeper & Kafka Coordination Guide
UPDATED: 2024
================================================================================
"""

import json
import time

from pyspark.sql import SparkSession


def explain_zookeeper_basics():
    """
    Explain ZooKeeper fundamentals with examples.
    """
    print("=" * 80)
    print("ZOOKEEPER FUNDAMENTALS")
    print("=" * 80)

    print(
        """
🔷 What is ZooKeeper?
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ZooKeeper is a centralized service for maintaining:
• Configuration information
• Naming
• Providing distributed synchronization
• Providing group services

Think of it as a "Coordination Kernel" for distributed systems.

🎯 Core Guarantees:
───────────────────
1. Sequential Consistency: Updates applied in order
2. Atomicity: Updates either succeed completely or fail
3. Single System Image: Clients see consistent view
4. Reliability: Changes persist once applied
5. Timeliness: Client view is up-to-date within time bound

📊 Data Model:
──────────────
Hierarchical namespace (like a file system):

    /
    ├── kafka
    │   ├── brokers
    │   │   └── ids
    │   │       ├── 0  (Broker 0 info)
    │   │       ├── 1  (Broker 1 info)
    │   │       └── 2  (Broker 2 info)
    │   ├── controller  (Current controller broker)
    │   └── topics
    │       ├── topic1
    │       └── topic2
    └── myapp
        ├── config  (App configuration)
        └── locks   (Distributed locks)

Each node (znode) can:
• Store data (up to 1MB, typically < 1KB)
• Have children
• Be watched for changes
• Be ephemeral (auto-deleted when client disconnects)
• Be persistent (survives client disconnect)
    """
    )

    print("\n" + "=" * 80)
    print("ZOOKEEPER OPERATIONS")
    print("=" * 80)

    print(
        """
Basic Operations (Python with kazoo library):

# 1. Installation
pip install kazoo

# 2. Connect to ZooKeeper
from kazoo.client import KazooClient
zk = KazooClient(hosts='localhost:2181')
zk.start()

# 3. Create a node
zk.create("/myapp", b"initial data")
zk.create("/myapp/config", b'{"timeout": 30}')

# 4. Read data
data, stat = zk.get("/myapp/config")
print(f"Data: {data}")
print(f"Version: {stat.version}")

# 5. Update data
zk.set("/myapp/config", b'{"timeout": 60}')

# 6. Create ephemeral node (disappears when client disconnects)
zk.create("/myapp/session-12345", ephemeral=True)

# 7. Watch for changes
@zk.DataWatch("/myapp/config")
def watch_config(data, stat):
    print(f"Config changed: {data}")
    # React to configuration changes

# 8. List children
children = zk.get_children("/myapp")
print(f"Children: {children}")

# 9. Delete node
zk.delete("/myapp/config")

# 10. Close connection
zk.stop()
    """
    )


def explain_kafka_zookeeper_relationship():
    """
    Explain how Kafka uses ZooKeeper.
    """
    print("\n" + "=" * 80)
    print("KAFKA + ZOOKEEPER RELATIONSHIP")
    print("=" * 80)

    print(
        """
🔷 How Kafka Uses ZooKeeper (Legacy Architecture):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. BROKER REGISTRATION
   ┌──────────────────────────────────────────────────────────┐
   │ When Kafka broker starts:                                │
   │                                                           │
   │ 1. Connects to ZooKeeper                                 │
   │ 2. Creates ephemeral znode: /brokers/ids/<broker-id>    │
   │ 3. Stores: host, port, endpoints, rack info              │
   │                                                           │
   │ Example: /brokers/ids/1                                  │
   │ {                                                         │
   │   "host": "kafka1.example.com",                          │
   │   "port": 9092,                                          │
   │   "rack": "rack-1"                                       │
   │ }                                                         │
   │                                                           │
   │ If broker crashes → znode disappears automatically!      │
   └──────────────────────────────────────────────────────────┘

2. CONTROLLER ELECTION
   ┌──────────────────────────────────────────────────────────┐
   │ Controller = "Boss Broker" that manages cluster          │
   │                                                           │
   │ Election Process:                                        │
   │ 1. All brokers try to create /controller znode          │
   │ 2. First one succeeds → becomes controller              │
   │ 3. Others watch /controller for changes                 │
   │ 4. If controller fails → znode deleted                  │
   │ 5. New election happens automatically                   │
   │                                                           │
   │ Controller Responsibilities:                             │
   │ • Manage partition leader elections                     │
   │ • Monitor broker failures                               │
   │ • Update metadata                                       │
   │ • Coordinate partition reassignments                    │
   └──────────────────────────────────────────────────────────┘

3. TOPIC METADATA
   ┌──────────────────────────────────────────────────────────┐
   │ Path: /brokers/topics/<topic-name>                      │
   │                                                           │
   │ Example: /brokers/topics/user-events                    │
   │ {                                                         │
   │   "version": 1,                                          │
   │   "partitions": {                                        │
   │     "0": [1, 2, 3],  # Replicas for partition 0        │
   │     "1": [2, 3, 1],  # Replicas for partition 1        │
   │     "2": [3, 1, 2]   # Replicas for partition 2        │
   │   }                                                       │
   │ }                                                         │
   │                                                           │
   │ All brokers watch topics → instant updates!             │
   └──────────────────────────────────────────────────────────┘

4. PARTITION LEADER ELECTION
   ┌──────────────────────────────────────────────────────────┐
   │ Path: /brokers/topics/<topic>/partitions/<id>/state     │
   │                                                           │
   │ Example: /brokers/topics/orders/partitions/0/state      │
   │ {                                                         │
   │   "leader": 1,              # Broker 1 is leader        │
   │   "isr": [1, 2, 3],         # In-Sync Replicas         │
   │   "controller_epoch": 5,                                │
   │   "leader_epoch": 12                                    │
   │ }                                                         │
   │                                                           │
   │ If leader fails:                                        │
   │ 1. Controller detects via ZooKeeper                     │
   │ 2. Picks new leader from ISR                           │
   │ 3. Updates state in ZooKeeper                          │
   │ 4. All brokers notified via watches                    │
   └──────────────────────────────────────────────────────────┘

5. CONSUMER GROUP COORDINATION (Old API)
   ┌──────────────────────────────────────────────────────────┐
   │ Path: /consumers/<group-id>                              │
   │                                                           │
   │ Structure:                                               │
   │ /consumers/my-group/                                     │
   │   ├── ids/                  # Active consumers          │
   │   │   ├── consumer-1                                    │
   │   │   └── consumer-2                                    │
   │   ├── owners/               # Partition ownership       │
   │   │   └── topic1/                                       │
   │   │       ├── 0 → consumer-1                           │
   │   │       └── 1 → consumer-2                           │
   │   └── offsets/              # Committed offsets         │
   │       └── topic1/                                       │
   │           ├── 0 → 12345                                │
   │           └── 1 → 67890                                │
   │                                                           │
   │ Note: New consumers use __consumer_offsets topic        │
   └──────────────────────────────────────────────────────────┘
    """
    )

    print("\n" + "=" * 80)
    print("KAFKA WITHOUT ZOOKEEPER: KRaft MODE")
    print("=" * 80)

    print(
        """
🚀 KRaft (Kafka Raft): ZooKeeper Removal
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Starting Kafka 2.8+ (Production-ready 3.3+)

Old: Kafka + ZooKeeper (2 systems)          New: Kafka Only (1 system)
┌─────────────────────────────┐            ┌──────────────────────┐
│   ZooKeeper Cluster         │            │   Kafka Cluster      │
│   ┌────┬────┬────┐          │            │   ┌──────────────┐   │
│   │ZK1 │ZK2 │ZK3 │          │            │   │ Controllers  │   │
│   └─┬──┴─┬──┴─┬──┘          │            │   │  (Quorum)    │   │
│     └────┴────┘             │            │   │ ┌───┬───┬───┐│   │
└─────────┬───────────────────┘            │   │ │C1 │C2 │C3 ││   │
          │                                │   │ └───┴───┴───┘│   │
┌─────────▼───────────────────┐            │   └──────┬───────┘   │
│   Kafka Brokers             │            │          │           │
│   ┌────┬────┬────┐          │            │   ┌──────▼───────┐   │
│   │BR1 │BR2 │BR3 │          │            │   │   Brokers    │   │
│   └────┴────┴────┘          │            │   │ ┌───┬───┬───┐│   │
└─────────────────────────────┘            │   │ │B1 │B2 │B3 ││   │
                                           │   │ └───┴───┴───┘│   │
6 servers total                            │   └──────────────┘   │
(3 ZK + 3 Kafka)                           └──────────────────────┘
                                           3 servers total
                                           (Kafka manages itself!)

✅ Benefits of KRaft:
────────────────────
• Simpler architecture (no ZooKeeper to manage)
• Faster metadata operations (no network hop)
• Better scalability (millions of partitions)
• Faster startup and recovery
• Reduced operational complexity
• Lower resource requirements

🔄 Migration Timeline:
─────────────────────
• Kafka 2.8 (2021): KRaft in early access
• Kafka 3.0 (2021): KRaft improvements
• Kafka 3.3 (2022): KRaft production-ready
• Kafka 4.0 (2024): ZooKeeper deprecated
• Future: ZooKeeper support removed

💡 Recommendation:
─────────────────
• New deployments: Use KRaft mode
• Existing deployments: Plan migration to KRaft
• ZooKeeper mode: Still supported but legacy
    """
    )


def demonstrate_coordination_patterns():
    """
    Demonstrate distributed coordination patterns.
    """
    print("\n" + "=" * 80)
    print("DISTRIBUTED COORDINATION PATTERNS")
    print("=" * 80)

    print(
        """
🔷 Common Patterns with ZooKeeper:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. LEADER ELECTION PATTERN
───────────────────────────
Use Case: Multiple processes, only one should be active

from kazoo.recipe.election import Election

# Process 1
election = Election(zk, "/election/myapp")
election.run(do_leader_work)  # Blocks until elected

def do_leader_work():
    print("I am the leader!")
    while True:
        # Do work only leader should do
        time.sleep(1)
    # If this process crashes, another becomes leader

Real-World Examples:
• Kafka Controller (only one broker is controller)
• Active/Passive Database (only one is active)
• Batch Job Coordinator (only one runs the job)


2. DISTRIBUTED LOCK PATTERN
────────────────────────────
Use Case: Ensure only one process accesses resource

from kazoo.recipe.lock import Lock

# Multiple processes compete for lock
lock = Lock(zk, "/locks/critical-resource")

with lock:
    print("I have exclusive access!")
    # Critical section - only one process here
    modify_shared_resource()
# Lock automatically released

Real-World Examples:
• Database migration (only one runs)
• File writing (prevent corruption)
• Resource allocation (assign work once)


3. SERVICE DISCOVERY PATTERN
─────────────────────────────
Use Case: Find available services dynamically

# Service Registration (by service)
service_info = {
    "host": "10.0.0.5",
    "port": 8080,
    "protocol": "http"
}
zk.create(
    "/services/api-server/instance-1",
    json.dumps(service_info).encode(),
    ephemeral=True  # Disappears if service crashes
)

# Service Discovery (by client)
@zk.ChildrenWatch("/services/api-server")
def watch_services(children):
    print(f"Available instances: {len(children)}")
    for child in children:
        data, _ = zk.get(f"/services/api-server/{child}")
        service = json.loads(data)
        print(f"  {child}: {service['host']}:{service['port']}")

Real-World Examples:
• Microservices finding each other
• Load balancer discovering backend servers
• Client finding available database replicas


4. CONFIGURATION MANAGEMENT PATTERN
────────────────────────────────────
Use Case: Centralized config with instant updates

# Set configuration (by admin)
config = {
    "max_connections": 100,
    "timeout": 30,
    "cache_size": "1GB"
}
zk.ensure_path("/config/myapp")
zk.set("/config/myapp", json.dumps(config).encode())

# Watch configuration (by all app instances)
@zk.DataWatch("/config/myapp")
def update_config(data, stat):
    config = json.loads(data)
    print(f"Config updated (version {stat.version})")
    # Apply new configuration
    app.reload_config(config)

Real-World Examples:
• Feature flags (enable/disable features)
• A/B testing (route % of traffic)
• Database connection strings
• API rate limits


5. BARRIER PATTERN
───────────────────
Use Case: Coordinate multiple processes to start together

from kazoo.recipe.barrier import Barrier

# All processes must reach barrier before any continue
barrier = Barrier(zk, "/barriers/start-processing")

# Wait for all participants
barrier.wait()  # Blocks until all processes call wait()

print("All processes ready - starting work!")
# Now all processes start simultaneously

Real-World Examples:
• Distributed testing (start all test runners together)
• MapReduce (wait for all mappers before reduce)
• Multi-stage pipelines (synchronize stages)


6. QUEUE PATTERN
─────────────────
Use Case: Distributed work queue

from kazoo.recipe.queue import Queue

# Producer
queue = Queue(zk, "/queues/tasks")
queue.put(b"task-1")
queue.put(b"task-2")

# Consumer
while True:
    task = queue.get()
    print(f"Processing: {task}")
    process_task(task)

Real-World Examples:
• Job scheduler
• Task distribution
• Message passing
    """
    )


def production_deployment_guide():
    """
    Guide for production ZooKeeper deployment.
    """
    print("\n" + "=" * 80)
    print("PRODUCTION DEPLOYMENT GUIDE")
    print("=" * 80)

    print(
        """
🔷 ZooKeeper Production Best Practices:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. CLUSTER SIZING
─────────────────
┌─────────────┬───────────────┬────────────────┬─────────────┐
│ Nodes       │ Fault Tolerance│ Quorum Needed │ Use Case    │
├─────────────┼───────────────┼────────────────┼─────────────┤
│ 1           │ 0             │ 1              │ Dev/Test    │
│ 3 (RECOMMENDED)│ 1          │ 2              │ Prod (small)│
│ 5 (RECOMMENDED)│ 2          │ 3              │ Prod (med)  │
│ 7           │ 3             │ 4              │ Prod (large)│
└─────────────┴───────────────┴────────────────┴─────────────┘

⚠️  NEVER use even numbers (2, 4, 6)!
   • 3 nodes: Tolerates 1 failure, needs 2 for quorum
   • 4 nodes: Still tolerates 1 failure, needs 3 for quorum
   • Result: 4 nodes = same fault tolerance as 3 nodes (wasted!)


2. HARDWARE REQUIREMENTS
────────────────────────
Minimum (Small Cluster):
• CPU: 2-4 cores
• RAM: 4-8 GB
• Disk: SSD with 100GB
• Network: 1 Gbps

Recommended (Medium Cluster):
• CPU: 4-8 cores
• RAM: 8-16 GB
• Disk: SSD with 200GB
• Network: 10 Gbps

Critical: USE SSDs!
• ZooKeeper writes transaction log to disk
• Latency-sensitive operations
• HDD = slow writes = cluster degradation


3. CONFIGURATION
────────────────
zoo.cfg (ZooKeeper config file):

# Data directory (must be on SSD)
dataDir=/var/lib/zookeeper/data
dataLogDir=/var/lib/zookeeper/log  # Separate disk if possible

# Client port
clientPort=2181

# Cluster members (3-node ensemble)
server.1=zk1.example.com:2888:3888
server.2=zk2.example.com:2888:3888
server.3=zk3.example.com:2888:3888

# Performance tuning
tickTime=2000                    # Basic time unit (ms)
initLimit=10                     # Follower connect timeout (10 ticks)
syncLimit=5                      # Follower sync timeout (5 ticks)
maxClientCnxns=60                # Max clients per IP
autopurge.snapRetainCount=3      # Keep 3 snapshots
autopurge.purgeInterval=24       # Cleanup every 24 hours

# JVM Settings (zkEnv.sh or systemd service)
JVMFLAGS="-Xms4G -Xmx4G"         # Heap size (1-4GB typical)


4. MONITORING
─────────────
Key Metrics:

Health Checks:
echo ruok | nc localhost 2181     # Returns "imok" if healthy
echo stat | nc localhost 2181     # Server statistics
echo mntr | nc localhost 2181     # Detailed monitoring metrics

Critical Metrics:
• avg_latency < 10ms              # Average request latency
• outstanding_requests < 100      # Queued requests
• znode_count < 1,000,000        # Total znodes
• watch_count (stable)           # Active watches
• leader_uptime (high)           # Leadership stability

Alerting Thresholds:
🔴 CRITICAL:
   • avg_latency > 50ms
   • outstanding_requests > 1000
   • Node disconnected from ensemble

🟡 WARNING:
   • avg_latency > 20ms
   • outstanding_requests > 100
   • Frequent leader elections


5. BACKUP & RECOVERY
────────────────────
Backup Strategy:

# Automatic snapshots (configured in zoo.cfg)
autopurge.snapRetainCount=3
autopurge.purgeInterval=24

# Manual backup
cp -r /var/lib/zookeeper/data /backup/zookeeper-$(date +%Y%m%d)

# Recovery
1. Stop ZooKeeper
2. Restore data directory from backup
3. Start ZooKeeper
4. Verify cluster health


6. SECURITY
───────────
Production Security Checklist:

✅ Enable SASL Authentication:
# zoo.cfg
authProvider.1=org.apache.zookeeper.server.auth.SASLAuthenticationProvider

✅ Enable SSL/TLS:
secureClientPort=2182
ssl.keyStore.location=/path/to/keystore.jks
ssl.trustStore.location=/path/to/truststore.jks

✅ Set ACLs (Access Control Lists):
from kazoo.security import make_digest_acl

acl = make_digest_acl("user", "password", all=True)
zk.create("/secure-node", b"data", acl=[acl])

✅ Network Isolation:
• Firewall rules (only allow known IPs)
• Private network for ZooKeeper cluster
• No public internet access


7. OPERATIONAL TIPS
───────────────────
✅ DO:
• Deploy ZooKeeper on separate machines from Kafka
• Use dedicated ZooKeeper per Kafka cluster
• Monitor latency and request queue
• Set up alerting
• Regular backups
• Test failure scenarios
• Plan capacity (znodes, connections, throughput)

❌ DON'T:
• Share ZooKeeper across multiple Kafka clusters
• Use spinning disks (HDD)
• Ignore monitoring
• Store large data in znodes (>1KB typical, 1MB max)
• Perform frequent writes
• Use even number of nodes
• Run without authentication in production


8. TROUBLESHOOTING
──────────────────
Common Issues:

Issue: High Latency
• Check disk I/O (must use SSD)
• Check network latency between nodes
• Increase JVM heap if lots of znodes
• Check for disk space

Issue: Frequent Leader Elections
• Check network connectivity
• Check if nodes are overloaded
• Verify syncLimit is appropriate

Issue: Out of Memory
• Too many znodes (limit to < 1M)
• Too many watches (limit connections)
• Increase JVM heap (-Xmx)

Issue: Connection Timeouts
• Check maxClientCnxns limit
• Verify firewall rules
• Check client connection timeout settings
    """
    )


def main():
    """
    Main execution function.
    """
    print("\n" + "🔷 " * 40)
    print("ZOOKEEPER & KAFKA COORDINATION - COMPREHENSIVE GUIDE")
    print("🔷 " * 40)

    # Explain ZooKeeper basics
    explain_zookeeper_basics()

    # Explain Kafka-ZooKeeper relationship
    explain_kafka_zookeeper_relationship()

    # Demonstrate coordination patterns
    demonstrate_coordination_patterns()

    # Production deployment guide
    production_deployment_guide()

    print("\n" + "=" * 80)
    print("✅ ZOOKEEPER & KAFKA GUIDE COMPLETE")
    print("=" * 80)

    print(
        """
📚 Key Takeaways:

1. ZooKeeper Purpose:
   • Distributed coordination service
   • Manages metadata and configuration
   • Enables leader election and locking
   • Provides consistency guarantees

2. Kafka + ZooKeeper (Legacy):
   • Broker registration
   • Controller election
   • Topic/partition metadata
   • Consumer group coordination

3. Kafka without ZooKeeper (KRaft):
   • Simpler architecture
   • Better performance
   • Production-ready in Kafka 3.3+
   • Future of Kafka

4. Production Deployment:
   • Use 3, 5, or 7 nodes (odd numbers only)
   • Deploy on SSDs
   • Monitor latency and health
   • Enable authentication and encryption
   • Regular backups

5. Coordination Patterns:
   • Leader election
   • Distributed locks
   • Service discovery
   • Configuration management
   • Barriers and queues

🎯 Recommendation:
   • New projects: Use Kafka with KRaft (no ZooKeeper)
   • Existing Kafka: Plan migration to KRaft
   • Other distributed apps: ZooKeeper still excellent choice

🔗 Related Files:
   • src/streaming/03_kafka_streaming.py (Kafka basics)
   • src/cluster_computing/* (Distributed coordination)
    """
    )


if __name__ == "__main__":
    main()
