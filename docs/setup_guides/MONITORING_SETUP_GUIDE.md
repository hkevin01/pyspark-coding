# 🔥 Complete PySpark Monitoring Setup Guide

**Complete monitoring stack with Spark Cluster + Prometheus + Next.js Dashboard**

## 🎯 What You Get

```
┌─────────────────────────────────────────────────────────────────┐
│                  MONITORING ARCHITECTURE                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐  │
│  │   Spark      │      │  Prometheus  │      │   Next.js    │  │
│  │   Cluster    │─────▶│   Metrics    │─────▶│  Dashboard   │  │
│  │ (4 nodes)    │      │   Storage    │      │ (React/TS)   │  │
│  └──────────────┘      └──────────────┘      └──────────────┘  │
│       :9080                 :9090                 :3000         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Components

1. **Spark Cluster** (1 Master + 3 Workers)
2. **Prometheus** (Metrics collection & storage)
3. **Next.js Dashboard** (Beautiful web UI)
4. **REST APIs** (Real-time data)

## 🚀 Quick Start (5 Minutes)

### Method 1: Full Stack with Docker Compose (Recommended)

```bash
# 1. Navigate to project root
cd /home/kevin/Projects/pyspark-coding

# 2. Start everything
docker-compose -f docker-compose.monitoring.yml up -d

# 3. Wait 30 seconds for services to start

# 4. Open dashboard
open http://localhost:3000

# 5. View other UIs
open http://localhost:9080  # Spark Master
open http://localhost:9090  # Prometheus
open http://localhost:4040  # Spark Application (when job running)
```

That's it! 🎉

### Method 2: Standalone Dashboard (Development)

```bash
# 1. Start Spark cluster only
cd docker/spark-cluster
docker-compose up -d

# 2. Install dashboard dependencies
cd ../../src/monitoring_frontend
npm install

# 3. Start dev server
npm run dev

# 4. Open browser
open http://localhost:3000
```

## 📊 Services & Ports

| Service | Port | URL | Description |
|---------|------|-----|-------------|
| **Dashboard** | 3000 | http://localhost:3000 | Main monitoring UI |
| **Spark Master** | 9080 | http://localhost:9080 | Cluster management |
| **Spark App** | 4040 | http://localhost:4040 | Application UI (when running) |
| **Worker 1** | 8081 | http://localhost:8081 | Worker 1 UI |
| **Worker 2** | 8082 | http://localhost:8082 | Worker 2 UI |
| **Worker 3** | 8083 | http://localhost:8083 | Worker 3 UI |
| **Prometheus** | 9090 | http://localhost:9090 | Metrics storage |

## 🎬 Usage Demo

### 1. Start the Stack

```bash
docker-compose -f docker-compose.monitoring.yml up -d
```

**Expected output:**
```
✔ Container spark-master                   Started
✔ Container spark-worker-1                 Started
✔ Container spark-worker-2                 Started
✔ Container spark-worker-3                 Started
✔ Container prometheus                     Started
✔ Container pyspark-monitoring-dashboard   Started
```

### 2. Verify Services

```bash
# Check all containers are running
docker ps

# Should see 6 containers:
# - spark-master
# - spark-worker-1
# - spark-worker-2
# - spark-worker-3
# - prometheus
# - pyspark-monitoring-dashboard
```

### 3. Access Dashboard

Open http://localhost:3000

**You should see:**
- 4 metric cards (Workers, CPU, Memory, Active Apps)
- Empty state for applications
- 3 healthy workers in the workers table
- Live performance charts

### 4. Run a Demo Job

```bash
# Submit a long-running job to see live metrics
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-apps/long_running_demo.py
```

**Watch the dashboard update in real-time:**
- Active Apps card changes from 0 to 1
- CPU/Memory usage increases
- Live Jobs section shows running application
- Charts show resource utilization
- Application appears in recent applications list

### 5. Enable Auto-Refresh

In the dashboard header:
1. Toggle "Auto-Refresh" switch ON
2. Select refresh interval (2s for fastest updates)
3. Watch metrics update automatically

## 🎨 Dashboard Features

### Header Controls

**Auto-Refresh Toggle**
- Enable/disable automatic data refresh
- Green indicator when active

**Refresh Interval Selector**
- 2 seconds - Fastest (for active monitoring)
- 5 seconds - Default (balanced)
- 10 seconds - Slower (reduces API calls)
- 30 seconds - Slowest (minimal overhead)

### Cluster Overview Cards

**Workers Card** (Blue)
- Number of active worker nodes
- Should show: 3

**CPU Cores Card** (Green)
- Cores used vs total available
- Progress bar showing utilization %
- Should show: 0/6 when idle, increases during jobs

**Memory Card** (Purple)
- Memory used vs total available
- Progress bar showing utilization %
- Should show: ~0 GB / 6 GB when idle

**Active Apps Card** (Orange)
- Number of running applications
- Animated when apps are active
- Shows 0 when idle

### Live Applications Section

Shows real-time status of running applications:
- Application name and ID
- RUNNING status badge (green, animated)
- CPU cores allocated
- Memory per executor
- Runtime duration
- Direct link to Spark Application UI

### Recent Applications List

Shows active and recently completed applications:
- Application name and ID
- Status badge (color-coded)
- Cores allocated
- Memory per executor
- Total duration

### Workers List Table

Detailed view of each worker:
- Worker hostname and web UI link
- Status indicator (green checkmark for ALIVE)
- CPU usage with progress bar
- Memory usage (used/total)

### Performance Metrics Charts

**Resource Utilization Chart**
- CPU usage % over time (green area)
- Memory usage % over time (purple area)
- Last 20 data points
- Updates with each refresh

**Active Applications Chart**
- Number of active applications over time (orange line)
- Shows application lifecycle
- Useful for tracking workload patterns

## 🔧 Configuration

### Environment Variables

Edit `src/monitoring_frontend/.env`:

```env
# Default values (works with docker-compose)
NEXT_PUBLIC_SPARK_MASTER_URL=http://localhost:9080
NEXT_PUBLIC_SPARK_APP_URL=http://localhost:4040
NEXT_PUBLIC_PROMETHEUS_URL=http://localhost:9090
```

For remote Spark cluster:
```env
NEXT_PUBLIC_SPARK_MASTER_URL=http://spark-prod-master.company.com:9080
NEXT_PUBLIC_SPARK_APP_URL=http://spark-prod-master.company.com:4040
```

### Prometheus Configuration

Edit `src/prometheus/prometheus.yml` to add more scrape targets:

```yaml
scrape_configs:
  - job_name: 'spark-master'
    static_configs:
      - targets: ['spark-master:8080']
    metrics_path: '/metrics/master/prometheus/'
```

## 🧪 Testing the Setup

### Test 1: Cluster Health Check

```bash
# Check cluster status
curl http://localhost:9080/json/ | jq

# Expected: See 3 workers, 6 cores total, 6GB memory
```

### Test 2: Quick Job

```bash
# Run quick example job
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-apps/example_job.py

# Watch dashboard - should see activity for ~5 seconds
```

### Test 3: Long-Running Job

```bash
# Run 60-second demo
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-apps/long_running_demo.py

# Watch dashboard for 60 seconds
# Enable auto-refresh for best experience
```

### Test 4: Prometheus Metrics

```bash
# Check Prometheus is scraping
open http://localhost:9090/targets

# All targets should be "UP"
```

## 🐛 Troubleshooting

### Dashboard shows "Failed to load cluster status"

**Cause**: Cannot connect to Spark Master

**Solutions**:
```bash
# 1. Check Spark Master is running
docker ps | grep spark-master

# 2. Verify port 9080 is accessible
curl http://localhost:9080/json/

# 3. Check container logs
docker logs spark-master

# 4. Restart services
docker-compose -f docker-compose.monitoring.yml restart
```

### No applications shown in dashboard

**Cause**: No Spark jobs have been submitted

**Solution**: Run a demo job:
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark-apps/long_running_demo.py
```

### Dashboard not updating

**Cause**: Auto-refresh disabled or slow interval

**Solutions**:
1. Toggle auto-refresh ON
2. Select faster interval (2s or 5s)
3. Manually refresh browser (F5)

### Prometheus shows no data

**Cause**: Spark metrics endpoint not accessible

**Solution**:
```bash
# Test Spark metrics endpoints
curl http://localhost:9080/metrics/master/prometheus/
curl http://localhost:8081/metrics/prometheus/

# Should return Prometheus-formatted metrics
```

### Port conflicts

**Cause**: Another service using the same port

**Solutions**:
```bash
# Check what's using the port
sudo lsof -i :3000
sudo lsof -i :9080

# Stop conflicting service or change port in docker-compose.yml
```

### Docker build fails

**Cause**: Dependencies or network issues

**Solution**:
```bash
# Rebuild without cache
docker-compose -f docker-compose.monitoring.yml build --no-cache

# Pull latest images
docker-compose -f docker-compose.monitoring.yml pull
```

## 📈 Performance Tips

### For Large Clusters

1. **Increase refresh interval** to 10s or 30s
2. **Reduce data retention** in charts (edit MetricsChart.tsx)
3. **Add pagination** for workers/applications lists
4. **Use Prometheus** for historical queries instead of REST API

### For Production

1. **Enable authentication** (add Nginx proxy with auth)
2. **Use HTTPS** for all connections
3. **Monitor dashboard performance** (browser dev tools)
4. **Set up alerts** in Prometheus
5. **Add logging** (Winston, Pino)

## 🔒 Security Considerations

### Development (Current Setup)

- No authentication
- HTTP only
- Open ports on localhost
- **Perfect for learning and development**

### Production Setup

**Required changes:**
1. Add authentication (OAuth, LDAP, etc.)
2. Enable HTTPS with valid certificates
3. Restrict network access (firewall, VPN)
4. Use environment-specific configs
5. Enable CORS restrictions
6. Add rate limiting
7. Implement audit logging

**Example Nginx config with auth:**
```nginx
server {
    listen 443 ssl;
    server_name spark-monitor.company.com;
    
    ssl_certificate /etc/ssl/certs/cert.pem;
    ssl_certificate_key /etc/ssl/private/key.pem;
    
    auth_basic "Spark Monitoring";
    auth_basic_user_file /etc/nginx/.htpasswd;
    
    location / {
        proxy_pass http://localhost:3000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

## 🎓 Learning Path

### Beginner

1. **Start the stack** - Follow quick start
2. **Explore the dashboard** - Familiarize with UI
3. **Run demo jobs** - Watch metrics change
4. **Try auto-refresh** - See real-time updates

### Intermediate

1. **Modify refresh interval** - See impact on performance
2. **Customize colors** - Edit Tailwind config
3. **Add new metrics** - Extend API client
4. **Create new charts** - Use Recharts components

### Advanced

1. **Add WebSocket support** - Real-time push updates
2. **Implement alerts** - Email/Slack notifications
3. **Add historical views** - 24h/7d/30d metrics
4. **Multi-cluster support** - Monitor multiple Spark clusters
5. **Custom visualizations** - DAG viewer, SQL query analyzer

## 📚 Documentation

### Project Files

- `src/monitoring_frontend/README.md` - Dashboard documentation
- `docker/spark-cluster/README.md` - Cluster setup guide
- `src/prometheus/README.md` - Prometheus configuration
- `MONITORING_SETUP_GUIDE.md` - This file

### External Resources

- [Spark Monitoring Docs](https://spark.apache.org/docs/latest/monitoring.html)
- [Spark REST API](https://spark.apache.org/docs/latest/monitoring.html#rest-api)
- [Next.js Docs](https://nextjs.org/docs)
- [Prometheus Docs](https://prometheus.io/docs/)

## 🎯 Next Steps

Now that you have the monitoring stack running:

1. **Run the Spark examples** from `docker/spark-cluster/apps/`
2. **Experiment with settings** (auto-refresh, intervals)
3. **Learn Spark REST API** by inspecting browser network tab
4. **Customize the dashboard** to your needs
5. **Add more visualizations** (DAGs, SQL plans)
6. **Integrate with your workflows** (CI/CD, testing)

## 🤝 Contributing

Found a bug or have an idea? Contributions welcome!

1. Test your changes locally
2. Update documentation
3. Submit a pull request
4. Add examples if applicable

## 💡 Pro Tips

1. **Split screen setup**: Dashboard on left, Spark UI on right
2. **Keyboard shortcut**: Add bookmark with Cmd/Ctrl+D
3. **Multiple dashboards**: Open in separate browser windows
4. **Save configurations**: Use browser profiles
5. **Mobile access**: Works on tablets and phones

## 🎉 Success!

You now have a complete, modern monitoring solution for Apache Spark!

**What you accomplished:**
✅ Deployed full Spark cluster (4 nodes)
✅ Set up Prometheus metrics collection
✅ Launched beautiful web dashboard
✅ Configured auto-refresh monitoring
✅ Enabled real-time visualization

**Have fun monitoring your Spark clusters! 🚀**

---

**Questions?** Check the troubleshooting section or review the component-specific READMEs.
