# HDFS Complete Guide
## Comprehensive Reference for Hadoop Distributed File System

**Table of Contents**
1. [HDFS Components and Metadata](#hdfs-components-and-metadata)
2. [HDFS Blocks and Replication](#hdfs-blocks-and-replication)
3. [Rack Awareness](#rack-awareness)
4. [HDFS Read Mechanism](#hdfs-read-mechanism)
5. [HDFS Write Mechanism](#hdfs-write-mechanism)
6. [HDFS CLI Commands](#hdfs-cli-commands)
7. [File Permissions](#file-permissions)
8. [Best Practices](#best-practices)

---

## HDFS Components and Metadata

### Architecture Components

```
┌─────────────────────────────────────────────────────────────────┐
│                    HDFS Cluster Architecture                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              NameNode (Master)                           │   │
│  │  ┌──────────────────────────────────────────────────┐  │   │
│  │  │  FSImage (Filesystem Metadata on Disk)           │  │   │
│  │  │  - Complete namespace snapshot                    │  │   │
│  │  │  - Block to file mapping                          │  │   │
│  │  │  - File permissions, owner, timestamps            │  │   │
│  │  └──────────────────────────────────────────────────┘  │   │
│  │  ┌──────────────────────────────────────────────────┐  │   │
│  │  │  EditLog (Transaction Log)                       │  │   │
│  │  │  - Recent filesystem changes                      │  │   │
│  │  │  - Create, delete, rename operations             │  │   │
│  │  │  - Appended in real-time                          │  │   │
│  │  └──────────────────────────────────────────────────┘  │   │
│  │  ┌──────────────────────────────────────────────────┐  │   │
│  │  │  In-Memory Metadata                              │  │   │
│  │  │  /user/data/file.txt → [Block1, Block2, Block3] │  │   │
│  │  │  Block1 → [DN1, DN3, DN5] (locations)           │  │   │
│  │  │  Block2 → [DN2, DN4, DN6]                        │  │   │
│  │  └──────────────────────────────────────────────────┘  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                           │                                      │
│                           │ Heartbeat (3s) + Block Reports (1h) │
│                           ↓                                      │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    DataNodes (Workers)                   │   │
│  │                                                           │   │
│  │  ┌──────────┐    ┌──────────┐    ┌──────────┐          │   │
│  │  │ DataNode1│    │ DataNode2│    │ DataNode3│    ...   │   │
│  │  │ Rack 1   │    │ Rack 1   │    │ Rack 2   │          │   │
│  │  │          │    │          │    │          │          │   │
│  │  │ [Block1] │    │ [Block2] │    │ [Block1] │  Stores  │   │
│  │  │ [Block3] │    │ [Block4] │    │ [Block2] │  actual  │   │
│  │  │ [Block5] │    │ [Block6] │    │ [Block4] │  data    │   │
│  │  └──────────┘    └──────────┘    └──────────┘          │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │   Secondary NameNode (Checkpoint Helper)                │   │
│  │   - Merges FSImage + EditLog                            │   │
│  │   - Reduces NameNode restart time                        │   │
│  │   - NOT a backup! (common misconception)                │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### 1. NameNode (Master)

**Responsibilities:**
- Manages filesystem namespace (directory tree)
- Stores metadata in memory for fast access
- Tracks which blocks belong to which files
- Knows locations of all block replicas
- Handles client requests for file operations

**Metadata Storage:**

```
FSImage (Persistent on Disk):
├─ Filesystem namespace
├─ File → Block mapping
├─ File attributes (owner, permissions, timestamps)
└─ Checkpoint of metadata at specific time

EditLog (Transaction Log):
├─ All changes since last FSImage
├─ Create file: /user/data.txt
├─ Delete file: /tmp/old.dat
├─ Rename: /old → /new
└─ Append-only for durability

In-Memory (Fast Access):
/user/data/file.txt:
  - Size: 384 MB
  - Blocks: [blk_1001, blk_1002, blk_1003]
  - blk_1001 → [DataNode1, DataNode3, DataNode5]
  - blk_1002 → [DataNode2, DataNode4, DataNode6]
  - blk_1003 → [DataNode1, DataNode2, DataNode4]
```

**Memory Requirements:**
```
Rule of thumb: ~150 bytes per block

Example:
1 million files × 3 blocks avg = 3 million blocks
3M blocks × 150 bytes = 450 MB RAM

1 billion files = 450 GB RAM needed!
This is why small files are a problem.
```

### 2. DataNodes (Workers)

**Responsibilities:**
- Store actual data blocks on local disks
- Serve read/write requests from clients
- Send heartbeats to NameNode (every 3 seconds)
- Report block inventory (every hour)
- Execute block operations (create, delete, replicate)

**Heartbeat Protocol:**
```
Every 3 seconds:
DataNode → NameNode: "I'm alive! My disk usage: 45%"

If NameNode doesn't receive heartbeat for 10 minutes:
→ DataNode marked as dead
→ Blocks on dead node need re-replication
→ NameNode instructs other DataNodes to replicate
```

**Block Report:**
```
Every hour:
DataNode → NameNode: "I have these blocks:"
  - blk_1001: 128 MB, checksum: 0x4f3a2b1c
  - blk_1002: 128 MB, checksum: 0x9e8d7c6b
  - blk_1003: 64 MB, checksum: 0x2a1b0c9d

NameNode verifies:
✅ All blocks accounted for?
✅ Replication factor met?
❌ Missing replicas? → Trigger replication
❌ Excess replicas? → Delete extras
```

### 3. Secondary NameNode

**NOT A BACKUP!** (Common Misconception)

**Actual Purpose:** Checkpoint Helper

```
Process:
1. Download FSImage + EditLog from NameNode
2. Merge them into new FSImage
3. Upload new FSImage back to NameNode
4. NameNode switches to new FSImage, empties EditLog

Why needed?
- EditLog grows indefinitely without checkpointing
- Large EditLog → slow NameNode restart
- Checkpointing reduces restart time

Checkpoint Frequency:
- Every hour (default)
- Or when EditLog reaches 1M transactions
```

**Checkpoint Process:**
```
Time: 00:00 - FSImage.001 + EditLog (1M entries)
Time: 01:00 - Secondary NN triggers checkpoint
  → Download FSImage.001 + EditLog
  → Merge locally
  → Create FSImage.002
  → Upload to NameNode
  → NameNode renames FSImage.002 → FSImage.current
  → New empty EditLog starts
Time: 02:00 - Repeat...
```

---

## HDFS Blocks and Replication

### Block Fundamentals

```
File: 640 MB
Block Size: 128 MB
Number of Blocks: 5

File Split:
┌─────────────────────────────────────────────────────────┐
│                   Original File (640 MB)                 │
└─────────────────────────────────────────────────────────┘
                          ↓ Split into blocks
┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
│ Block 1  │ │ Block 2  │ │ Block 3  │ │ Block 4  │ │ Block 5  │
│ 128 MB   │ │ 128 MB   │ │ 128 MB   │ │ 128 MB   │ │ 128 MB   │
└──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘
```

### Why 128 MB Blocks?

**Optimization Trade-off:**

```
Small Blocks (e.g., 4 KB like Linux):
✅ Less wasted space for small files
❌ More metadata (1M blocks = 150 MB RAM)
❌ More network requests
❌ More disk seeks

Large Blocks (e.g., 128 MB):
✅ Less metadata (fewer blocks)
✅ Sequential I/O (fast!)
✅ Fewer network requests
❌ Wasted space for small files

Calculation for 1 TB file:
4 KB blocks:  262 million blocks (39 GB metadata!)
128 MB blocks: 8,000 blocks (1.2 MB metadata) ✅
```

**Seek Time vs Transfer Time:**
```
Disk specs:
- Seek time: 10 ms
- Transfer rate: 100 MB/s

For 4 KB block:
- Seek: 10 ms
- Transfer: 0.04 ms
- Total: 10.04 ms (99% seeking!)

For 128 MB block:
- Seek: 10 ms  
- Transfer: 1280 ms
- Total: 1290 ms (99% transferring!) ✅

Large blocks = efficient sequential I/O
```

### Replication Strategy

**Default Replication Factor: 3**

```
Original Block → 3 Replicas

Primary:   DataNode1 (same rack as client)
Replica 1: DataNode3 (same rack, different node)
Replica 2: DataNode5 (different rack)

Why 3 replicas?
- Tolerates 2 node failures
- Good balance: reliability vs storage cost
- Can read from any of 3 locations (load balancing)
```

**Replica Placement Strategy:**

```
Goal: Maximize reliability AND data locality

Placement Rules:
1st replica: Same rack as client (data locality)
2nd replica: Same rack, different node (local redundancy)
3rd replica: Different rack (rack failure tolerance)

Example:
Client on Rack 1, Node 2

Replica Placement:
┌─────────────────────┐  ┌─────────────────────┐
│      Rack 1         │  │      Rack 2         │
│  ┌────┐   ┌────┐   │  │  ┌────┐   ┌────┐   │
│  │ N1 │   │ N2 │   │  │  │ N5 │   │ N6 │   │
│  │    │   │ 1st│   │  │  │ 3rd│   │    │   │
│  └────┘   └────┘   │  │  └────┘   └────┘   │
│  ┌────┐   ┌────┐   │  │                     │
│  │ N3 │   │ N4 │   │  │                     │
│  │ 2nd│   │    │   │  │                     │
│  └────┘   └────┘   │  │                     │
└─────────────────────┘  └─────────────────────┘

Benefits:
✅ Fast write: 2/3 replicas local (same rack)
✅ Rack failure: 1 replica survives
✅ Read locality: Can read from local rack
```

### Block States

```
State Machine:
┌──────────────┐
│   CREATED    │  Block allocated, not yet written
└──────┬───────┘
       │ Write pipeline completes
       ↓
┌──────────────┐
│ UNDER_CONST  │  Being written, not finalized
└──────┬───────┘
       │ Close file
       ↓
┌──────────────┐
│  COMMITTED   │  Write complete, not yet reported
└──────┬───────┘
       │ Block report received
       ↓
┌──────────────┐
│   COMPLETE   │  Fully replicated, available for reads
└──────────────┘

Error States:
CORRUPT     → Checksum mismatch
MISSING     → Expected replica not found
EXCESS      → Too many replicas
```

---

## Rack Awareness

### Data Center Topology

```
┌─────────────────────────────────────────────────────────────┐
│                       Data Center                            │
│                                                               │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐ │
│  │     Rack 1      │  │     Rack 2      │  │   Rack 3    │ │
│  │  ┌───┐  ┌───┐   │  │  ┌───┐  ┌───┐   │  │  ┌───┐      │ │
│  │  │DN1│  │DN2│   │  │  │DN5│  │DN6│   │  │  │DN9│ ...  │ │
│  │  └───┘  └───┘   │  │  └───┘  └───┘   │  │  └───┘      │ │
│  │  ┌───┐  ┌───┐   │  │  ┌───┐  ┌───┐   │  │             │ │
│  │  │DN3│  │DN4│   │  │  │DN7│  │DN8│   │  │             │ │
│  │  └───┘  └───┘   │  │  └───┘  └───┘   │  │             │ │
│  │      ↓          │  │       ↓         │  │      ↓       │ │
│  │  [Switch 1]     │  │  [Switch 2]     │  │  [Switch 3]  │ │
│  └───────┬─────────┘  └─────────┬───────┘  └──────┬───────┘ │
│          │                      │                  │         │
│          └──────────────┬───────┴──────────────────┘         │
│                         │                                     │
│                   [Core Switch]                               │
└─────────────────────────────────────────────────────────────┘

Network Bandwidth:
- Within rack: 10 Gbps (fast!)
- Between racks: 1 Gbps (10x slower)
- Rack switch failure → entire rack offline
```

### Rack Awareness Benefits

**1. Replica Placement:**
```
Without Rack Awareness (Random):
All 3 replicas might be on Rack 1
→ Rack 1 fails → All replicas lost! ❌

With Rack Awareness:
Replica 1: Rack 1
Replica 2: Rack 1 (different node)
Replica 3: Rack 2
→ Rack 1 fails → Replica 3 survives ✅
```

**2. Read Performance:**
```
File has replicas on:
- Rack 1, Node 2
- Rack 1, Node 4  
- Rack 2, Node 6

Client on Rack 1:
→ Reads from Rack 1 (10 Gbps, local)
✅ Fast!

Client on Rack 3:
→ Must cross core switch (1 Gbps)
→ Reads from nearest replica
⚠️ Slower, but still works
```

**3. Write Pipeline:**
```
Write 1 block with replication=3:

Step 1: Client → DataNode2 (Rack 1) - fast local write
Step 2: DataNode2 → DataNode4 (Rack 1) - fast within rack
Step 3: DataNode4 → DataNode6 (Rack 2) - slower cross-rack

Pipeline:
Client ──────> DN2 ──────> DN4 ──────> DN6
         (local)     (same rack)  (different rack)
         10 Gbps       10 Gbps        1 Gbps

Total time optimized by pipelining!
```

### Configuring Rack Awareness

**rack-awareness.sh:**
```bash
#!/bin/bash
# Map IP addresses to rack names

if [ $# -eq 0 ]; then
    echo "Usage: rack-awareness.sh <ip-address>"
    exit 1
fi

IP=$1

# Define rack mapping
case $IP in
    10.0.1.*) echo "/rack1" ;;
    10.0.2.*) echo "/rack2" ;;
    10.0.3.*) echo "/rack3" ;;
    *)        echo "/default-rack" ;;
esac
```

**hdfs-site.xml:**
```xml
<property>
  <name>net.topology.script.file.name</name>
  <value>/etc/hadoop/conf/rack-awareness.sh</value>
</property>
```

---

## HDFS Read Mechanism

### Read Flow Architecture

```
Step-by-Step Read Process:

┌─────────┐
│ Client  │  "Read /user/data/file.txt"
└────┬────┘
     │ 1. Open file
     ↓
┌─────────────────┐
│    NameNode     │  "Here are block locations:"
│  (Metadata Only)│  Block1 → [DN1, DN3, DN5]
└────┬────────────┘  Block2 → [DN2, DN4, DN6]
     │ 2. Returns block locations (sorted by distance)
     │
     ↓
┌─────────┐
│ Client  │  3. Opens connection to closest DataNode
└────┬────┘     (DN1 for Block1)
     │
     │ 4. Read Block1 data
     ↓
┌─────────┐
│  DN1    │  5. Stream data to client
│ (Rack 1)│     Verify checksum
└─────────┘
     ↓ 6. Block1 complete
┌─────────┐
│ Client  │  7. Read Block2 from DN2
└────┬────┘
     │ 8. Stream data
     ↓
┌─────────┐
│  DN2    │  9. Complete read
│ (Rack 1)│
└─────────┘
```

### Detailed Read Steps

**1. Client Opens File:**
```python
# Using HDFS API
fs = hdfs.FileSystem()
file_handle = fs.open('/user/data/large_file.dat')

# Client contacts NameNode:
# "Give me metadata for /user/data/large_file.dat"
```

**2. NameNode Returns Metadata:**
```python
# NameNode response:
{
  "blocks": [
    {
      "blockId": "blk_1001",
      "size": 134217728,  # 128 MB
      "locations": [
        {"host": "dn1.example.com", "rack": "/rack1"},
        {"host": "dn3.example.com", "rack": "/rack1"},
        {"host": "dn5.example.com", "rack": "/rack2"}
      ]
    },
    {
      "blockId": "blk_1002",
      "size": 134217728,
      "locations": [
        {"host": "dn2.example.com", "rack": "/rack1"},
        {"host": "dn4.example.com", "rack": "/rack1"},
        {"host": "dn6.example.com", "rack": "/rack2"}
      ]
    }
  ]
}

# Locations sorted by:
# 1. Same node (if exists)
# 2. Same rack
# 3. Different rack
```

**3. Client Reads from DataNode:**
```python
# Client selects closest DataNode
# Reads Block1 from dn1.example.com

# Read process:
while not end_of_block:
    chunk = datanode.read(64 KB)  # Read in chunks
    verify_checksum(chunk)  # Verify integrity
    process(chunk)  # Use data
    
# If checksum fails:
# → Try next replica (dn3)
# → Report corrupt block to NameNode
```

**4. Data Locality:**
```
Best Case: Data on same node
  Read time: ~10 ms (disk seek) + transfer time
  
Good Case: Data on same rack  
  Read time: ~10 ms + network time (10 Gbps)
  
Worst Case: Data on different rack
  Read time: ~10 ms + network time (1 Gbps, slower)

HDFS + Spark optimization:
→ Schedule computation on nodes with data
→ Minimize network transfer
→ 10-100x faster processing!
```

### Error Handling

```python
# Checksum verification
def read_block_with_retry(block_locations):
    for location in block_locations:
        try:
            data = read_from_datanode(location)
            if verify_checksum(data):
                return data  # Success!
            else:
                report_corrupt_block(location)
                continue  # Try next replica
        except NetworkError:
            continue  # Try next replica
    
    raise Exception("All replicas failed!")

# NameNode notified of corruption:
# → Marks block as corrupt
# → Triggers re-replication from good replica
# → Eventually deletes corrupt replica
```

---

## HDFS Write Mechanism

### Write Flow Architecture

```
Write Process:

┌─────────┐
│ Client  │  "Write /user/data/new_file.txt"
└────┬────┘
     │ 1. Create file request
     ↓
┌─────────────────┐
│    NameNode     │  "OK, write to these DataNodes:"
│  (Metadata)     │  Pipeline: [DN1 → DN3 → DN5]
└────┬────────────┘
     │ 2. Returns write pipeline
     │
     ↓
┌─────────┐
│ Client  │  3. Establish pipeline
└────┬────┘     DN1 → DN3 → DN5
     │
     │ 4. Write data packets
     ↓
┌─────────┐     ┌─────────┐     ┌─────────┐
│  DN1    │────>│  DN3    │────>│  DN5    │
│ (Rack1) │ 5.  │ (Rack1) │ 6.  │ (Rack2) │
└────┬────┘     └────┬────┘     └────┬────┘
     │               │               │
     └───────────────┴───────────────┘
                     │
                7. ACK packets flow back
                     ↓
               ┌─────────┐
               │ Client  │  8. Close file
               └────┬────┘
                    │ 9. Finalize
                    ↓
               ┌─────────────────┐
               │    NameNode     │  10. Mark complete
               └─────────────────┘
```

### Detailed Write Steps

**1. Create File:**
```python
# Client requests file creation
fs = hdfs.FileSystem()
output = fs.create('/user/data/output.txt', replication=3)

# NameNode checks:
# ✅ File doesn't exist?
# ✅ Parent directory exists?
# ✅ Client has write permission?
# → Create metadata entry (size=0, no blocks yet)
```

**2. Request Block:**
```python
# When client has data to write:
# Client → NameNode: "Allocate block for me"

# NameNode response:
{
  "blockId": "blk_2001",
  "pipeline": [
    "dn2.example.com:50010",  # Primary
    "dn4.example.com:50010",  # Replica 1
    "dn6.example.com:50010"   # Replica 2
  ]
}

# NameNode selection criteria:
# - DataNodes with available space
# - Rack awareness (2 same rack, 1 different)
# - Load balancing
```

**3. Write Pipeline:**
```python
# Client establishes pipeline
# Client → DN2, DN2 → DN4, DN4 → DN6

# Write in packets (64 KB each)
for packet in data_packets:
    # Client sends to DN2
    dn2.write(packet)
    
    # DN2 forwards to DN4
    dn4.write(packet)
    
    # DN4 forwards to DN6
    dn6.write(packet)
    
    # ACKs flow back
    dn6 → dn4: ACK
    dn4 → dn2: ACK  
    dn2 → client: ACK
    
    # Client waits for ACK before sending next packet
```

**4. Pipeline Diagram:**
```
Time →

t0: Client sends Packet1 to DN2
t1: DN2 writes Packet1, forwards to DN4
t2: DN4 writes Packet1, forwards to DN6
    Client sends Packet2 to DN2
t3: DN6 writes Packet1, sends ACK
    DN2 writes Packet2, forwards to DN4
t4: DN4 sends ACK to DN2
    DN4 writes Packet2, forwards to DN6
t5: DN2 sends ACK to Client
    Client sends Packet3
...

Pipelining = parallelism = faster writes!
```

**5. Handling Failures:**
```python
# Scenario: DN4 fails during write

Pipeline: [DN2, DN4, DN6]
                ↓ FAILS

# Client action:
1. Remove DN4 from pipeline
2. New pipeline: [DN2, DN6]
3. Continue writing
4. Notify NameNode of partial replication

# NameNode action:
1. Mark block as under-replicated (2/3 replicas)
2. Schedule re-replication later
3. Choose DN8 for 3rd replica
4. DN2 → DN8: replicate block

Result: Block eventually has 3 replicas ✅
```

---

## HDFS CLI Commands

### Basic Commands

```bash
# Help
hdfs dfs -help
hdfs dfs -help ls

# List files
hdfs dfs -ls /user/data
hdfs dfs -ls -h /user/data  # Human-readable sizes
hdfs dfs -ls -R /user/data  # Recursive

# Create directory
hdfs dfs -mkdir /user/data
hdfs dfs -mkdir -p /user/data/year/month/day  # Create parents

# Upload to HDFS
hdfs dfs -put /local/file.txt /user/data/
hdfs dfs -put -f /local/file.txt /user/data/  # Force overwrite
hdfs dfs -copyFromLocal /local/file.txt /user/data/  # Same as put

# Download from HDFS
hdfs dfs -get /user/data/file.txt /local/
hdfs dfs -copyToLocal /user/data/file.txt /local/  # Same as get
hdfs dfs -getmerge /user/data/parts/* /local/merged.txt  # Merge files

# View files
hdfs dfs -cat /user/data/file.txt
hdfs dfs -head /user/data/file.txt  # First 1KB
hdfs dfs -tail /user/data/file.txt  # Last 1KB
hdfs dfs -cat /user/data/*.txt | grep "ERROR"  # Pipeline with grep

# File info
hdfs dfs -stat "%n %b %r" /user/data/file.txt  # name, size, replication
hdfs dfs -du /user/data  # Disk usage
hdfs dfs -du -h /user/data  # Human-readable
hdfs dfs -df -h /  # Filesystem space

# Copy/Move
hdfs dfs -cp /user/data/file.txt /user/backup/
hdfs dfs -mv /user/data/old.txt /user/data/new.txt

# Delete
hdfs dfs -rm /user/data/file.txt
hdfs dfs -rm -r /user/data/directory  # Recursive
hdfs dfs -rm -skipTrash /user/data/file.txt  # Skip trash

# Permissions
hdfs dfs -chmod 755 /user/data/file.txt
hdfs dfs -chown user:group /user/data/file.txt
hdfs dfs -chgrp group /user/data/file.txt

# Block information
hdfs fsck /user/data/file.txt -files -blocks -locations
```

### Advanced Commands

```bash
# Replication
hdfs dfs -setrep 5 /user/data/important.txt  # Set replication to 5
hdfs dfs -setrep -R 3 /user/data/  # Recursive

# Disk usage
hdfs dfs -count /user/data  # DIR_COUNT FILE_COUNT CONTENT_SIZE
hdfs dfs -count -q /user/data  # With quota info

# File checksum
hdfs dfs -checksum /user/data/file.txt

# Test
hdfs dfs -test -e /user/data/file.txt && echo "exists"
hdfs dfs -test -z /user/data/file.txt && echo "zero length"
hdfs dfs -test -d /user/data && echo "is directory"

# Append (HDFS 2.x+)
hdfs dfs -appendToFile /local/append.txt /user/data/file.txt

# Snapshot (if enabled)
hdfs dfs -createSnapshot /user/data snapshot1
hdfs dfs -renameSnapshot /user/data snapshot1 snapshot_backup
hdfs dfs -deleteSnapshot /user/data snapshot1
```

### HDFS Admin Commands

```bash
# Cluster health
hdfs dfsadmin -report  # Cluster summary
hdfs dfsadmin -safemode get  # Check safemode status

# Balancer (redistribute blocks evenly)
hdfs balancer -threshold 10  # Balance if deviation > 10%

# File system check
hdfs fsck /  # Check entire filesystem
hdfs fsck /user/data -files -blocks -locations -racks

# Block operations
hdfs fsck /user/data/file.txt -files -blocks -delete  # Delete corrupt
hdfs debug verifyMeta -block blk_1001  # Verify block checksum
```

### Practical Examples

```bash
# Example 1: Upload large dataset
hdfs dfs -mkdir -p /datasets/logs/2024
hdfs dfs -put -f /local/logs/*.gz /datasets/logs/2024/

# Example 2: Check file replication
hdfs dfs -stat "Replication: %r" /datasets/logs/2024/app.log.gz

# Example 3: Find large files
hdfs dfs -du -h /user/data | sort -h | tail -10

# Example 4: Count files by extension
hdfs dfs -ls -R /user/data | grep "\.csv$" | wc -l

# Example 5: Compress and upload
tar -czf - /local/data | hdfs dfs -put - /datasets/data.tar.gz

# Example 6: Download and process
hdfs dfs -cat /logs/*.log | grep "ERROR" > /local/errors.log

# Example 7: Copy between clusters
hdfs dfs -cp hdfs://cluster1:9000/data hdfs://cluster2:9000/data

# Example 8: Check block corruption
hdfs fsck / | grep -i corrupt

# Example 9: Monitor disk usage
watch -n 60 'hdfs dfs -df -h'

# Example 10: Backup critical data
hdfs dfs -get /user/critical/* /backup/$(date +%Y%m%d)/
```

---

## File Permissions

### Permission Model

```
HDFS permissions similar to POSIX (Linux):

Format: drwxrwxrwx owner group

Example:
-rw-r--r--  1 john  engineers  1048576  Dec 13 10:00  data.txt
│││││││││  │  │     │          │        │            │
││││││││└─ other execute (0)   │        │            │
│││││││└── other write (0)     │        │            └─ filename
││││││└─── other read (1)      │        └─ modification time
│││││└──── group execute (0)   └─ size in bytes
││││└───── group write (0)
│││└────── group read (1)
││└─────── owner execute (0)
│└──────── owner write (1)
└───────── owner read (1)

Permissions:
r (4) = read
w (2) = write  
x (1) = execute

755 = rwxr-xr-x
644 = rw-r--r--
777 = rwxrwxrwx
```

### Setting Permissions

```bash
# Change mode
hdfs dfs -chmod 755 /user/data/script.sh
hdfs dfs -chmod -R 644 /user/data/*.txt
hdfs dfs -chmod u+x,g+x /user/data/script.sh
hdfs dfs -chmod o-w /user/data/public.txt

# Change owner
hdfs dfs -chown john /user/data/file.txt
hdfs dfs -chown john:engineers /user/data/file.txt
hdfs dfs -chown -R john:engineers /user/data/

# Change group
hdfs dfs -chgrp engineers /user/data/project/*

# View permissions
hdfs dfs -ls /user/data
```

### Permission Checking

```
Access Check:
1. If user is superuser → Allow
2. If user is owner → Check owner permissions
3. If user in group → Check group permissions
4. Otherwise → Check other permissions

Example:
File: -rw-r----- john engineers data.txt

User: john   → Can read, write
User: jane (in engineers) → Can read only
User: bob (not in engineers) → No access
User: hdfs (superuser) → Full access
```

### ACLs (Access Control Lists)

```bash
# Set ACL
hdfs dfs -setfacl -m user:jane:rw- /user/data/file.txt
hdfs dfs -setfacl -m group:analysts:r-- /user/data/file.txt

# View ACL
hdfs dfs -getfacl /user/data/file.txt

# Remove ACL
hdfs dfs -setfacl -x user:jane /user/data/file.txt

# Remove all ACLs
hdfs dfs -setfacl -b /user/data/file.txt

# Recursive ACL
hdfs dfs -setfacl -R -m user:jane:rwx /user/data/directory

# Default ACL (for new files in directory)
hdfs dfs -setfacl -m default:user:jane:rw- /user/data/directory
```

---

## Best Practices

### 1. File Size Optimization

```
❌ Bad: 1 billion × 1KB files = 150 GB NameNode RAM
✅ Good: Combine into larger files (100MB+)

Solutions:
- Use sequence files or Parquet (columnar storage)
- Batch small files before uploading
- Use compression (gzip, snappy)
- Consider HBase for small records
```

### 2. Replication Strategy

```
Default: replication = 3

When to adjust:
- Critical data: replication = 4 or 5
- Temporary data: replication = 1 or 2
- Hot data: Higher replication for read load balancing
- Cold data: Lower replication to save space

Example:
hdfs dfs -setrep 1 /tmp/staging/*  # Temporary files
hdfs dfs -setrep 5 /critical/financial/*  # Critical data
```

### 3. Directory Organization

```
✅ Good structure:
/data/
  ├─ raw/          # Raw ingested data
  ├─ processed/    # Cleaned data
  ├─ aggregated/   # Summary tables
  ├─ archive/      # Old data
  └─ tmp/          # Temporary (replication=1)

/logs/
  ├─ year=2024/
  │   ├─ month=01/
  │   │   ├─ day=01/
  │   │   └─ day=02/
  │   └─ month=02/

Partitioning enables:
- Efficient pruning
- Easy deletion of old data
- Clear data lifecycle
```

### 4. Performance Tips

```bash
# Use compression
gzip large_file.txt
hdfs dfs -put large_file.txt.gz /data/

# Parallel uploads
ls /local/*.gz | xargs -P 10 -I {} hdfs dfs -put {} /data/

# Optimize block size for workload
hdfs dfs -D dfs.blocksize=256M -put huge_file.dat /data/

# Use distcp for large transfers
hadoop distcp /source /destination
hadoop distcp hdfs://cluster1/data hdfs://cluster2/data

# Monitor before large operations
hdfs dfsadmin -report  # Check available space
```

### 5. Data Integrity

```bash
# Always verify after upload
md5sum /local/file.txt
hdfs dfs -cat /hdfs/file.txt | md5sum

# Regular filesystem checks
hdfs fsck / -files -blocks

# Check specific file
hdfs fsck /important/data.txt -files -blocks -locations

# Enable checksums (default: ON)
hdfs dfs -Ddfs.client.use.datanode.hostname=true -put file.txt /data/
```

### 6. Monitoring

```bash
# Monitor NameNode
# Web UI: http://namenode:9870

# Monitor DataNodes
hdfs dfsadmin -report

# Check under-replicated blocks
hdfs fsck / -blocks | grep "Under replicated"

# Monitor space usage
hdfs dfs -du -h / | sort -h | tail -20

# Set quotas
hdfs dfsadmin -setSpaceQuota 1T /user/john
hdfs dfsadmin -setQuota 1000000 /user/john  # 1M files
```

---

## Summary

### HDFS Strengths ✅
- Scales to petabytes
- Fault-tolerant through replication
- High throughput for large files
- Cost-effective (commodity hardware)
- Data locality optimization

### HDFS Limitations ❌
- Not for small files (metadata overhead)
- High latency (not real-time)
- No random writes (append-only)
- NameNode is single point of failure (use HA)
- Not POSIX compliant

### Key Concepts Mastered 🎯
1. NameNode stores metadata, DataNodes store blocks
2. Default block size: 128 MB
3. Default replication: 3 copies
4. Rack awareness for reliability and performance
5. Write pipeline for efficient replication
6. Read optimization through data locality

### Next Steps
- ✅ HDFS fundamentals (COMPLETE)
- 📚 Practice CLI commands
- 🔧 Set up local HDFS cluster (Docker)
- 💻 Integrate with Spark for big data processing
- 📊 Monitor and optimize production clusters

**Remember**: HDFS is the storage foundation for the entire Hadoop/Spark ecosystem. Master it to understand how your data is actually stored and processed! 🚀
