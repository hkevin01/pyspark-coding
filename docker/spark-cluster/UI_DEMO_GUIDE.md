# 🌐 Spark UI Live Demo Guide

## 🎯 Quick Start

### Step 1: Open Browser First
```bash
# Open these URLs in separate browser tabs:
firefox http://localhost:4040 &  # Application UI (will show "not found" until job starts)
firefox http://localhost:9080 &  # Master UI (always available)
```

### Step 2: Run the Long Demo Job
```bash
cd /home/kevin/Projects/pyspark-coding/docker/spark-cluster

docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-apps/long_running_demo.py
```

### Step 3: Watch the UI Update in Real-Time! 🎬

## 📊 What You'll See (60+ second job)

### Timeline

**0-5 seconds**: Initial setup
- Application UI becomes active at http://localhost:4040
- See "UI Demo - Long Running Job" in Jobs tab

**5-15 seconds**: Stage 1 - Data Creation
- 10 million records across 24 partitions
- Watch task timeline in Stages tab
- See data cached in Storage tab

**15-25 seconds**: Stage 2 - Aggregations
- GroupBy operations across 100 categories
- Monitor shuffle metrics in Executors tab

**25-35 seconds**: Stage 3 - Window Functions
- Complex window operations
- Check SQL tab for physical plan

**35-45 seconds**: Stage 4 - Joins
- Join operations with shuffle
- Executors tab shows shuffle read/write

**45-55 seconds**: Stages 5-7 - Filters, Sorts, Stats
- Multiple transformation stages
- See task distribution across workers

**55-70 seconds**: Stage 8 - Final Aggregation
- Complex multi-operation aggregation
- UI stays active for exploration

## 🔍 What to Look For in Each Tab

### 1. Jobs Tab
✓ 8+ jobs listed (one per major operation)
✓ Click any job to see its stages
✓ View DAG visualization
✓ Check job duration and status

### 2. Stages Tab
✓ 24 tasks per stage (one per partition)
✓ Task timeline showing parallel execution
✓ Color-coded task status (running/completed)
✓ Input/Output/Shuffle metrics
✓ Locality: PROCESS_LOCAL is best

### 3. Storage Tab
✓ Cached DataFrame visible
✓ Memory usage: ~XX MB
✓ 24 partitions cached
✓ Fraction cached: 100%

### 4. Environment Tab
✓ Spark properties
✓ System properties
✓ Classpath entries
✓ Verify configurations

### 5. Executors Tab
✓ 3 executors + 1 driver
✓ Each executor: 2 cores, 2GB RAM
✓ Task time distribution
✓ Shuffle read/write metrics
✓ GC time monitoring
✓ Active/Complete/Failed tasks

### 6. SQL Tab
✓ DataFrame operations listed
✓ Physical plan visualization
✓ Click "Details" to expand
✓ See Catalyst optimizer work

## 🎨 Color Coding

- **Blue**: Running tasks
- **Green**: Completed successfully
- **Red**: Failed tasks
- **Orange**: Skipped tasks
- **Grey**: Pending tasks

## 📈 Key Metrics to Monitor

### Per-Stage Metrics
- Duration
- Input/Output size
- Shuffle read/write
- Records processed
- Task execution time

### Per-Executor Metrics
- Total tasks run
- Active/Failed/Completed tasks
- Storage memory used
- Shuffle read/write
- GC time

## 🚀 Alternative: Interactive Shell (UI Stays Open Indefinitely)

```bash
# Start PySpark shell
docker exec -it spark-master /opt/spark/bin/pyspark \
    --master spark://spark-master:7077

# Then run commands interactively:
>>> df = spark.range(10_000_000, numPartitions=24)
>>> df.count()
>>> df.groupBy((df.id % 100).alias("cat")).count().show()

# UI at http://localhost:4040 stays active until you exit
# Type exit() to quit
```

## 💡 Pro Tips

1. **Keep Jobs Tab Open**: Refresh to see new jobs as they complete
2. **Stage Detail View**: Click stage ID to see task-level details
3. **Timeline View**: Shows exact task execution on each executor
4. **Event Timeline**: See when tasks started/finished on each worker
5. **Executor Threads**: Watch real-time thread activity
6. **Failed Tasks**: Click to see error details and stack traces

## 📊 Comparison: Quick vs Long Demo

| Feature | example_job.py | long_running_demo.py |
|---------|----------------|----------------------|
| Duration | <1 second | ~60 seconds |
| Records | 1 million | 10 million |
| Partitions | 12 | 24 |
| Stages | 3 | 8+ |
| Operations | Basic | Complex (joins, windows, etc) |
| UI Time | Brief | Extended |
| Best For | Quick test | UI exploration |

## 🔄 Running Multiple Times

```bash
# Run 3 times in a row to see history
for i in {1..3}; do
    echo "Run $i of 3"
    docker exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark-apps/long_running_demo.py
    sleep 2
done

# Then check Master UI at http://localhost:9080
# See "Completed Applications" section with all runs
```

## 📸 Screenshot Checklist

While the job runs, capture:
- [ ] Jobs tab showing multiple jobs
- [ ] Stages tab with task timeline
- [ ] Storage tab with cached data
- [ ] Executors tab showing 3 workers
- [ ] SQL tab with query plans
- [ ] Stage detail page with tasks
- [ ] DAG visualization
- [ ] Event timeline

## 🎓 Learning Exercise

1. Run the long demo
2. Watch Stages tab during execution
3. Note which stages take longest
4. Check why (look at shuffle metrics)
5. Compare executor workloads
6. Identify bottlenecks
7. Think about optimizations

## ⚠️ Troubleshooting

**UI shows "not found"**
→ Job not running yet, refresh when terminal shows "Open Spark UI now"

**UI disappeared**
→ Job completed, check Master UI for history

**Slow performance**
→ Normal for first run (JVM warmup), subsequent runs faster

**Port already in use**
→ Another job running, wait for completion or use different port

---

**Happy UI Exploring! 🎉**
