# HDFS Package - Hadoop Distributed File System

Complete guide to HDFS concepts, commands, and operations for big data storage.

## 📚 Package Contents

### Documentation
- **01_hdfs_overview.md** - What is HDFS and why use it
- **HDFS_COMPLETE_GUIDE.md** - Comprehensive reference covering all topics

### Topics Covered

#### 1. **HDFS Fundamentals**
- Architecture (NameNode, DataNodes, Secondary NameNode)
- Components and their responsibilities
- Metadata management (FSImage, EditLog)
- Block-based storage system

#### 2. **Blocks and Replication**
- 128 MB default block size
- 3x replication strategy
- Replica placement policy
- Block states and lifecycle

#### 3. **Rack Awareness**
- Data center topology
- Cross-rack replication
- Network bandwidth optimization
- Fault tolerance across racks

#### 4. **Read/Write Mechanisms**
- Read flow with data locality
- Write pipeline architecture
- Checksum verification
- Failure handling

#### 5. **CLI Commands**
- File operations (ls, cat, put, get)
- Directory management
- Permissions and ACLs
- Admin operations

#### 6. **Best Practices**
- File size optimization
- Replication strategies
- Directory organization
- Performance tuning
- Monitoring and maintenance

## 🚀 Quick Start

### View HDFS Files
```bash
# List files
hdfs dfs -ls /user/data

# View file content
hdfs dfs -cat /user/data/file.txt

# Check file info
hdfs dfs -stat "%n: %b bytes, %r replicas" /user/data/file.txt
```

### Upload to HDFS
```bash
# Upload single file
hdfs dfs -put /local/file.txt /user/data/

# Upload directory
hdfs dfs -put -r /local/data/ /user/data/

# Upload with specific replication
hdfs dfs -D dfs.replication=5 -put critical.txt /user/data/
```

### Download from HDFS
```bash
# Download file
hdfs dfs -get /user/data/file.txt /local/

# Download directory
hdfs dfs -get /user/data/* /local/backup/

# Merge and download
hdfs dfs -getmerge /user/data/parts/* /local/merged.txt
```

## 📊 Key Concepts

### HDFS Architecture
```
NameNode (Master)          DataNodes (Workers)
├─ Metadata in memory      ├─ Store blocks on disk
├─ FSImage on disk         ├─ Send heartbeats (3s)
├─ EditLog for changes     ├─ Send block reports (1h)
└─ Block location mapping  └─ Serve read/write requests
```

### Block Replication
```
File → Split into 128 MB blocks → Replicate 3x

Block Placement:
- 1st replica: Same rack as client
- 2nd replica: Same rack, different node
- 3rd replica: Different rack

Benefits:
✅ Fault tolerance
✅ Load balancing
✅ Data locality
```

### Read Process
```
Client → NameNode: "Where is /user/data/file.txt?"
NameNode → Client: "Block1 at [DN1, DN3, DN5], Block2 at [DN2, DN4, DN6]"
Client → Closest DataNode: Read blocks directly
```

### Write Process
```
Client → NameNode: "Create /user/data/new.txt"
NameNode → Client: "Write to pipeline [DN1 → DN3 → DN5]"
Client → DN1 → DN3 → DN5: Pipeline write with ACKs
Client → NameNode: "Close file, mark complete"
```

## 💡 Common Use Cases

### 1. Data Lake Storage
```bash
# Organized by date partitions
hdfs dfs -mkdir -p /datalake/raw/year=2024/month=12/day=13
hdfs dfs -put daily_logs.gz /datalake/raw/year=2024/month=12/day=13/
```

### 2. ETL Staging
```bash
# Stage data for processing
hdfs dfs -put -f raw_data.csv /etl/staging/
# Process with Spark
spark-submit process.py
# Move to processed
hdfs dfs -mv /etl/staging/raw_data.csv /etl/processed/
```

### 3. ML Dataset Storage
```bash
# Store training data
hdfs dfs -put -r /local/imagenet/ /ml/datasets/imagenet/
# Train with Spark MLlib
spark-submit train_model.py --input hdfs:///ml/datasets/imagenet/
```

### 4. Log Aggregation
```bash
# Collect logs from multiple servers
for server in web1 web2 web3; do
  ssh $server "tar -czf - /var/log/app/*.log" | \
    hdfs dfs -put - /logs/$server/$(date +%Y%m%d).tar.gz
done
```

## 🔧 Practical Commands

### File Management
```bash
# Copy within HDFS
hdfs dfs -cp /user/data/file.txt /user/backup/

# Move/rename
hdfs dfs -mv /user/data/old.txt /user/data/new.txt

# Delete (moves to trash)
hdfs dfs -rm /user/data/file.txt

# Delete permanently
hdfs dfs -rm -skipTrash /user/data/temp.txt

# Delete directory
hdfs dfs -rm -r /user/data/old_project/
```

### Monitoring
```bash
# Cluster status
hdfs dfsadmin -report

# File system check
hdfs fsck / -files -blocks -locations

# Disk usage
hdfs dfs -du -h /user/data | sort -h | tail -10

# Find corrupt files
hdfs fsck / | grep -i corrupt
```

### Permissions
```bash
# Change permissions
hdfs dfs -chmod 755 /user/data/script.sh
hdfs dfs -chmod -R 644 /user/data/files/

# Change owner
hdfs dfs -chown john:engineers /user/data/project/

# ACL
hdfs dfs -setfacl -m user:jane:rw- /user/data/shared.txt
hdfs dfs -getfacl /user/data/shared.txt
```

## ⚠️ Common Pitfalls

### 1. Small Files Problem
```
❌ Problem: 1 million × 1 KB files = 150 MB NameNode RAM

✅ Solution:
- Combine into larger files
- Use SequenceFile or Parquet
- Compress before uploading
```

### 2. Insufficient Replication
```
❌ Problem: Single node failure → data loss

✅ Solution:
- Keep default replication = 3
- Increase for critical data: hdfs dfs -setrep 5 /critical/
```

### 3. Unbalanced Cluster
```
❌ Problem: Some DataNodes full, others empty

✅ Solution:
# Run balancer
hdfs balancer -threshold 10
```

### 4. NameNode Memory
```
❌ Problem: Too many small files → NameNode OOM

✅ Solution:
- Monitor: hdfs dfsadmin -report
- Set quotas: hdfs dfsadmin -setQuota 1000000 /user/john
- Consolidate small files
```

## 📈 Performance Tips

### 1. Optimize Block Size
```bash
# For large files (>1 GB), increase block size
hdfs dfs -D dfs.blocksize=256M -put huge_file.dat /data/

# Reduces NameNode metadata overhead
```

### 2. Use Compression
```bash
# Compress before upload
gzip large_file.txt
hdfs dfs -put large_file.txt.gz /data/

# Saves storage and network bandwidth
```

### 3. Parallel Uploads
```bash
# Upload multiple files in parallel
ls /local/*.gz | xargs -P 10 -I {} hdfs dfs -put {} /data/

# 10x faster than sequential
```

### 4. Use DistCp for Large Transfers
```bash
# Distributed copy (parallel)
hadoop distcp /source /destination
hadoop distcp hdfs://cluster1/data hdfs://cluster2/backup/

# Much faster than hdfs dfs -cp
```

## 🎯 Interview Questions Covered

### Basic
- What is HDFS?
- Why 128 MB blocks?
- What is replication factor?
- NameNode vs DataNode roles?

### Intermediate
- How does HDFS handle node failures?
- Explain rack awareness
- What is Secondary NameNode?
- How does HDFS read work?

### Advanced
- Explain write pipeline
- How does HDFS ensure data integrity?
- What happens when NameNode fails?
- How to optimize for small files?

## 🔗 Integration with Spark

### Reading from HDFS
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HDFS_Read").getOrCreate()

# Read from HDFS
df = spark.read.csv("hdfs:///user/data/sales.csv", header=True)

# Or use default HDFS
df = spark.read.csv("/user/data/sales.csv", header=True)

# Data locality: Spark executors read from local DataNodes!
```

### Writing to HDFS
```python
# Write to HDFS
df.write.parquet("hdfs:///user/data/output/")

# With compression
df.write.option("compression", "snappy") \
    .parquet("/user/data/output/")

# Partitioned write
df.write.partitionBy("year", "month") \
    .parquet("/user/data/partitioned/")
```

## 📚 Learning Path

1. ✅ Read 01_hdfs_overview.md (Fundamentals)
2. ✅ Read HDFS_COMPLETE_GUIDE.md (Deep dive)
3. 📝 Practice CLI commands (Hands-on)
4. 🔧 Set up local HDFS (Optional: Docker)
5. 💻 Integrate with Spark (Real-world usage)

## 🎓 Certification Topics

This package covers:
- HDFS architecture and components
- Block management and replication
- Rack awareness strategy
- Read/write mechanisms
- CLI operations
- Performance optimization
- Troubleshooting

**Ready for**: CCA Spark and Hadoop Developer, Cloudera certifications

## 📖 Additional Resources

- Apache HDFS Documentation: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html
- HDFS Commands Guide: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/FileSystemShell.html
- Hadoop: The Definitive Guide (Book)

---

**Status**: ✅ Complete HDFS coverage for big data engineers

**Next**: Integrate with Spark for distributed data processing!