# 🔴 Live Cluster Monitor

## 📖 Overview

The `monitor_job.sh` script provides **real-time terminal-based monitoring** of your Spark cluster. It auto-refreshes every 2 seconds, showing cluster status, active applications, workers, and completed jobs.

## 🚀 Quick Start

```bash
# From the spark-cluster directory
cd /home/kevin/Projects/pyspark-coding/docker/spark-cluster
./apps/monitor_job.sh
```

**That's it!** The monitor will start and show live updates.

Press **Ctrl+C** to stop.

---

## 📊 What It Shows

### 1. Cluster Status
```
✅ Cluster Status: RUNNING
```
Shows if all 4 containers are up (1 master + 3 workers)

### 2. Active Applications
```
📱 Active Applications:
   • Long Running Demo - State: RUNNING
   • Duration: 12s
```
Lists currently running Spark jobs

### 3. Worker Status
```
👷 Workers:
   Worker 1: ALIVE - 2 cores, 2GB
   Worker 2: ALIVE - 2 cores, 2GB
   Worker 3: ALIVE - 2 cores, 2GB
```
Shows all workers and their resources

### 4. Completed Applications
```
✅ Completed Applications (last 5):
   • Distributed Cluster Demo - Duration: 4349ms
   • Example Job - Duration: 2145ms
```
Shows the 5 most recent completed jobs

### 5. Application UI Status
```
🔴 Application UI: http://localhost:4040
   ✅ Click to view detailed job metrics!
```
Indicates if port 4040 is active (job running)

---

## 💡 Usage Patterns

### Pattern 1: Watch a Job Run

**Terminal 1** - Start monitor:
```bash
./apps/monitor_job.sh
```

**Terminal 2** - Run job:
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-apps/long_running_demo.py
```

Watch the monitor update in real-time as the job progresses!

---

### Pattern 2: Cluster Health Check

Just run the monitor to see:
- Are all workers alive?
- Any jobs running?
- Recent job durations
- Cluster resource usage

```bash
./apps/monitor_job.sh
```

Check for a few seconds, then Ctrl+C to exit.

---

### Pattern 3: Multi-Monitor Setup

**Terminal 1** - Live cluster monitor:
```bash
./apps/monitor_job.sh
```

**Terminal 2** - Follow job logs:
```bash
docker logs -f spark-master
```

**Browser** - Spark UI with auto-refresh:
```
http://localhost:4040/?showConsoleProgress=true
```

**Triple monitoring!** See cluster status, logs, and detailed UI all at once.

---

## 🎬 Live Demo

Try this to see the monitor in action:

```bash
# Terminal 1: Start monitor
cd /home/kevin/Projects/pyspark-coding/docker/spark-cluster
./apps/monitor_job.sh
```

In another terminal:

```bash
# Terminal 2: Run the long demo
cd /home/kevin/Projects/pyspark-coding/docker/spark-cluster
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-apps/long_running_demo.py
```

**What you'll see:**
- `0-5s`: Application appears in "Active Applications"
- `5-60s`: Duration counter increases every 2 seconds
- `60s`: Application moves to "Completed Applications"
- Throughout: Application UI indicator shows green

---

## ⚙️ How It Works

The script uses:
1. **Docker API** - Checks if containers are running
2. **Spark REST API** - Queries master (port 9080) for applications and workers
3. **Port Check** - Tests if Application UI (port 4040) is accessible
4. **jq** - Parses JSON responses

### Data Sources
```bash
# Cluster status
docker ps --filter "name=spark-" --format "{{.Status}}"

# Applications
curl -s http://localhost:9080/json/

# Workers  
curl -s http://localhost:9080/json/ | jq '.workers'

# Application UI
curl -s -o /dev/null -w "%{http_code}" http://localhost:4040
```

---

## 🔧 Dependencies

Required tools (already installed):
- `docker` - Container management
- `curl` - HTTP requests
- `jq` - JSON parsing
- `bash` - Script execution

The monitor will work on any system with these tools.

---

## 🎨 Customization

### Change Refresh Rate

Edit line 9 in `monitor_job.sh`:
```bash
# Change from 2 seconds to 5 seconds
sleep 5
```

### Change Output Format

The script uses these symbols:
- `✅` = Success/Running
- `🔴` = Active/Live
- `⚪` = Inactive
- `📱` = Applications
- `👷` = Workers

Modify them to your preference!

### Add More Metrics

The Spark REST API provides tons of data:
```bash
# See all available data
curl -s http://localhost:9080/json/ | jq
```

Add any field to the script using jq queries.

---

## 🐛 Troubleshooting

**Problem**: "command not found: jq"
**Solution**: Install jq
```bash
sudo apt install jq  # Ubuntu/Debian
brew install jq      # macOS
```

**Problem**: Shows "No active applications" when job is running
**Solution**: 
- Check if job is actually running: `docker logs spark-master`
- Verify master UI: http://localhost:9080
- Ensure job was submitted to spark://spark-master:7077

**Problem**: "Connection refused" errors
**Solution**:
- Verify cluster is running: `docker compose ps`
- Check port 9080 is accessible: `curl http://localhost:9080`
- Restart cluster: `docker compose restart`

**Problem**: Monitor is too slow/fast
**Solution**: Change `sleep 2` value in script

---

## 📝 Example Output

```
🔴 LIVE CLUSTER STATUS - 19:13:39
════════════════════════════════════════════════════════════════

✅ Cluster Status: RUNNING

📱 Active Applications:
   • Long Running Demo - State: RUNNING
   • Duration: 25s

👷 Workers:
   Worker 1: ALIVE - 2 cores, 2GB
   Worker 2: ALIVE - 2 cores, 2GB
   Worker 3: ALIVE - 2 cores, 2GB

✅ Completed Applications (last 5):
   • Distributed Cluster Demo - Duration: 4349ms
   • Example Job - Duration: 2145ms

🔴 Application UI: http://localhost:4040
   ✅ Click to view detailed job metrics!

════════════════════════════════════════════════════════════════
🔄 Refreshing in 2 seconds... (Ctrl+C to stop)
```

---

## 🎯 Pro Tips

1. **Split Screen**: Use tmux or screen to run monitor alongside other commands

2. **Log Output**: Redirect to file for later analysis
   ```bash
   ./apps/monitor_job.sh > monitor_log.txt
   ```

3. **Background Running**: Run in background (not recommended, can't see output)
   ```bash
   ./apps/monitor_job.sh &
   ```

4. **Remote Monitoring**: SSH tunnel to monitor remote cluster
   ```bash
   ssh -L 9080:localhost:9080 user@remote-host
   ./apps/monitor_job.sh
   ```

5. **Watch Specific Application**: Filter output with grep
   ```bash
   ./apps/monitor_job.sh | grep "Active Applications" -A 3
   ```

---

## 🔗 Related Files

- **ENABLE_AUTO_REFRESH.md** - 5 methods to enable auto-refresh in web UI
- **UI_DEMO_GUIDE.md** - Comprehensive guide to exploring Spark UIs
- **QUICK_REFERENCE.md** - All essential commands in one place
- **long_running_demo.py** - 60-second job perfect for monitoring

---

## ✨ Why Use This?

**Advantages over Web UI:**
- ✅ Auto-refreshes faster (2s vs 5s)
- ✅ Lightweight terminal interface
- ✅ No browser needed
- ✅ Easy to script/automate
- ✅ Works over SSH
- ✅ Can log to file

**When to use Web UI instead:**
- Need detailed stage/task metrics
- Want visual DAG graphs
- Exploring SQL query plans
- Analyzing executor memory usage
- Viewing detailed timeline

**Best practice**: Use **both**! Monitor script for overview, Web UI for deep dives.

---

**Happy monitoring! ��**