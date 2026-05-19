# 🚀 Launch Script Quick Reference

## What It Does

The `launch_monitoring.sh` script is a **one-command solution** that:

1. ✅ **Checks prerequisites** (Docker, npm, Node.js, Chrome)
2. ✅ **Verifies file structure** (all required files exist)
3. ✅ **Checks port availability** (detects conflicts)
4. ✅ **Installs npm dependencies** (if not already installed)
5. ✅ **Starts Docker services** (Spark cluster + Prometheus + Dashboard)
6. ✅ **Waits for services** (ensures everything is ready)
7. ✅ **Verifies health** (checks each service status)
8. ✅ **Opens Chrome** (launches dashboard at localhost:3000)
9. ✅ **Shows live logs** (streams Docker logs)

---

## Usage

### Quick Start (One Command)
```bash
cd /home/kevin/Projects/pyspark-coding
./launch_monitoring.sh
```

That's it! The script handles everything automatically.

---

## What You'll See

### Step-by-Step Output
```
════════════════════════════════════════════════════════════════
🚀 PySpark Monitoring Dashboard Launch Script
════════════════════════════════════════════════════════════════

📋 Step 1: Checking Prerequisites
✅ Docker is installed
✅ Docker Compose is installed
✅ npm is installed (10.2.3)
✅ Node.js is installed (v20.10.0)
✅ Chrome/Chromium is installed

📁 Step 2: Verifying File Structure
✅ Found: docker-compose.monitoring.yml
✅ Found: package.json
✅ Found: next.config.js
✅ Found: index.tsx
✅ Found: MONITORING_SETUP_GUIDE.md

🔌 Step 3: Checking Port Availability
✅ Port 3000 (Dashboard) is available
✅ Port 9080 (Spark Master) is available
✅ Port 9090 (Prometheus) is available
... (checking all ports)

📦 Step 4: Installing npm Dependencies
ℹ️  node_modules already exists, checking for updates...
✅ Dependencies are up to date

�� Step 5: Starting Docker Services
▶ Starting Spark cluster, Prometheus, and monitoring dashboard...
✅ Docker services started

⏳ Step 6: Waiting for Services to Initialize
▶ Waiting for Spark Master to be ready...
✅ Spark Master is ready!
▶ Waiting for Prometheus to be ready...
✅ Prometheus is ready!
▶ Waiting for Monitoring Dashboard to be ready...
✅ Monitoring Dashboard is ready!

✅ Step 7: Verifying Services
▶ Checking Spark Master...
✅ Spark Master is running
▶ Checking Prometheus...
✅ Prometheus is healthy
▶ Checking Workers...
ℹ️  Found 3 worker(s) running

🌐 Step 8: Service URLs
Dashboard:        http://localhost:3000
Spark Master UI:  http://localhost:9080
Prometheus:       http://localhost:9090
Worker 1 UI:      http://localhost:8081
Worker 2 UI:      http://localhost:8082
Worker 3 UI:      http://localhost:8083
App UI:           http://localhost:4040 (when job running)

🌍 Step 9: Launching Dashboard in Chrome
▶ Opening http://localhost:3000 in Chrome...
✅ Dashboard opened in Chrome

💡 Quick Tips
Dashboard Features:
  • Toggle auto-refresh in the header
  • Choose refresh interval: 2s, 5s, 10s, or 30s
  • View cluster overview with 4 metric cards
  • Monitor workers in real-time
  • Track active applications
  • Visualize performance trends

Test the Dashboard:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-apps/long_running_demo.py

🎉 SUCCESS! Monitoring Dashboard is Ready!
```

---

## Troubleshooting

### Port Conflicts
If ports are in use, the script will automatically attempt cleanup:
```bash
⚠️  Port 3000 (Dashboard) is already in use
⚠️  Some ports are in use. Attempting to clean up...
```

### Missing Dependencies
If prerequisites are missing:
```bash
❌ Docker is not installed
❌ Please install missing dependencies and try again
```

Install missing tools:
```bash
# Ubuntu/Debian
sudo apt-get install docker.io docker-compose nodejs npm

# macOS
brew install docker docker-compose node
```

### Services Not Starting
If a service fails to start within 60 seconds (30 attempts × 2s):
```bash
❌ Spark Master failed to start after 30 attempts
```

Check logs:
```bash
docker-compose -f docker-compose.monitoring.yml logs spark-master
```

---

## Manual Control

### Stop All Services
```bash
cd /home/kevin/Projects/pyspark-coding
docker-compose -f docker-compose.monitoring.yml down
```

### Restart Services
```bash
docker-compose -f docker-compose.monitoring.yml restart
```

### View Logs
```bash
# All services
docker-compose -f docker-compose.monitoring.yml logs -f

# Specific service
docker-compose -f docker-compose.monitoring.yml logs -f monitoring-dashboard
```

### Check Service Status
```bash
docker-compose -f docker-compose.monitoring.yml ps
```

---

## Script Features

### ✅ Color-Coded Output
- 🟢 **Green**: Success messages
- 🔴 **Red**: Error messages
- 🟡 **Yellow**: Warnings
- 🔵 **Blue**: Info messages
- 🟣 **Purple**: Step indicators
- 🔷 **Cyan**: Headers

### ✅ Automatic Port Detection
Checks all required ports before starting:
- 3000: Dashboard
- 9080: Spark Master
- 9090: Prometheus
- 8081-8083: Workers
- 4040: Application UI
- 7077: Spark cluster

### ✅ Health Checks
Verifies each service is responding before proceeding:
- HTTP health checks with retries
- 30 attempts × 2 seconds = 60 second timeout
- Graceful failure with clear error messages

### ✅ Browser Auto-Launch
Detects available browsers in order:
1. `google-chrome`
2. `google-chrome-stable`
3. `chromium-browser`
4. `chromium`
5. `xdg-open` (fallback)

---

## Testing the Dashboard

After the script launches, test with a sample job:

```bash
# Submit a long-running demo job
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/long_running_demo.py

# Watch the dashboard update in real-time:
# ✅ Active apps counter increases
# ✅ CPU/Memory usage increases
# ✅ Job appears in "Live Applications"
# ✅ Charts update with new data points
```

---

## What Happens Behind the Scenes

1. **Prerequisite Check**: Validates all tools are installed
2. **File Verification**: Ensures project structure is complete
3. **Port Check**: Detects conflicts and attempts cleanup
4. **Dependency Install**: Runs `npm install` if needed
5. **Docker Compose Up**: Starts all 6 services in detached mode
6. **Service Wait**: Polls each service until healthy
7. **Health Verification**: Confirms Spark Master and Prometheus
8. **Worker Count**: Checks number of active workers
9. **Browser Launch**: Opens Chrome to localhost:3000
10. **Log Streaming**: Shows live Docker logs (Ctrl+C to exit)

---

## Quick Commands Reference

```bash
# Launch everything (recommended)
./launch_monitoring.sh

# Stop everything
docker-compose -f docker-compose.monitoring.yml down

# Restart without full check
docker-compose -f docker-compose.monitoring.yml restart

# View specific service logs
docker-compose -f docker-compose.monitoring.yml logs -f spark-master
docker-compose -f docker-compose.monitoring.yml logs -f prometheus
docker-compose -f docker-compose.monitoring.yml logs -f monitoring-dashboard

# Check service health
curl http://localhost:9080  # Spark Master
curl http://localhost:9090/-/healthy  # Prometheus
curl http://localhost:3000  # Dashboard

# Run demo job
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/long_running_demo.py
```

---

## File Locations

- **Launch Script**: `/home/kevin/Projects/pyspark-coding/launch_monitoring.sh`
- **Docker Compose**: `/home/kevin/Projects/pyspark-coding/docker-compose.monitoring.yml`
- **Frontend Code**: `/home/kevin/Projects/pyspark-coding/src/monitoring_frontend/`
- **Setup Guide**: `/home/kevin/Projects/pyspark-coding/MONITORING_SETUP_GUIDE.md`

---

## Success Indicators

When everything works correctly, you should see:

✅ All prerequisite checks pass
✅ All required files found
✅ All ports available (or conflicts resolved)
✅ npm dependencies installed
✅ 6 Docker containers running
✅ All services respond to health checks
✅ 3 workers detected
✅ Chrome opens to dashboard
✅ Dashboard shows cluster metrics

---

## Support

For detailed documentation, see:
- `MONITORING_SETUP_GUIDE.md` - Complete setup guide
- `src/monitoring_frontend/README.md` - Dashboard documentation
- `docker/spark-cluster/README.md` - Cluster setup

For issues, check:
1. Docker is running: `docker ps`
2. Ports are free: `lsof -i :3000`
3. Services are healthy: `docker-compose ps`
4. Logs for errors: `docker-compose logs`

---

**🎉 Enjoy your automated monitoring solution!**