# 🔥 PySpark Monitoring Dashboard

Modern, real-time web dashboard for monitoring Apache Spark clusters built with Next.js, React, and TypeScript.

## ✨ Features

### 📊 Real-Time Monitoring
- **Auto-refresh** with configurable intervals (2s, 5s, 10s, 30s)
- **Live metrics** from Spark REST API
- **Real-time charts** showing resource utilization
- **WebSocket-ready** architecture for instant updates

### 📈 Comprehensive Metrics
- **Cluster Overview**: Workers, CPU cores, memory, active applications
- **Worker Status**: Individual worker health and resource usage
- **Application Tracking**: Active and completed applications
- **Live Jobs**: Running jobs with detailed metrics
- **Performance Charts**: CPU, memory, and application trends

### 🎨 Modern UI
- **Dark theme** with Spark brand colors
- **Responsive design** for mobile, tablet, and desktop
- **Gradient cards** with smooth animations
- **Status indicators** with color coding
- **Loading states** and error handling

## 🚀 Quick Start

### Prerequisites
- Node.js 18+ 
- npm or yarn
- Running Spark cluster (see ../docker/spark-cluster)

### Installation

```bash
# Install dependencies
npm install

# Copy environment variables
cp .env.example .env

# Edit .env with your Spark URLs
nano .env
```

### Configuration

Edit `.env` file:

```env
NEXT_PUBLIC_SPARK_MASTER_URL=http://localhost:9080
NEXT_PUBLIC_SPARK_APP_URL=http://localhost:4040
NEXT_PUBLIC_PROMETHEUS_URL=http://localhost:9090
```

### Development

```bash
# Start development server
npm run dev

# Open browser
open http://localhost:3000
```

### Production

```bash
# Build for production
npm run build

# Start production server
npm start
```

## 🐳 Docker Deployment

### Build Docker Image

```bash
docker build -t pyspark-monitoring-dashboard .
```

### Run with Docker

```bash
docker run -p 3000:3000 \
  -e NEXT_PUBLIC_SPARK_MASTER_URL=http://spark-master:9080 \
  -e NEXT_PUBLIC_SPARK_APP_URL=http://spark-master:4040 \
  pyspark-monitoring-dashboard
```

### Docker Compose (Recommended)

See `docker-compose.monitoring.yml` for full stack deployment including:
- Spark Cluster (Master + 3 Workers)
- Prometheus (metrics storage)
- Monitoring Dashboard (this app)

```bash
docker-compose -f docker-compose.monitoring.yml up -d
```

## 📂 Project Structure

```
monitoring_frontend/
├── components/           # React components
│   ├── ClusterOverview.tsx    # Cluster stats cards
│   ├── WorkersList.tsx        # Workers table
│   ├── ApplicationsList.tsx   # Applications list
│   ├── LiveJobs.tsx          # Active jobs
│   └── MetricsChart.tsx      # Performance charts
├── lib/                  # Utilities
│   └── sparkApi.ts       # Spark REST API client
├── pages/               # Next.js pages
│   ├── index.tsx        # Main dashboard
│   ├── _app.tsx         # App wrapper
│   └── _document.tsx    # HTML document
├── styles/              # CSS styles
│   └── globals.css      # Global styles
├── public/              # Static assets
├── package.json         # Dependencies
├── tsconfig.json        # TypeScript config
├── tailwind.config.js   # Tailwind CSS config
└── next.config.js       # Next.js config
```

## 🔧 Configuration

### Auto-Refresh Settings

The dashboard auto-refreshes data at configurable intervals:

- **2 seconds**: Fastest, best for monitoring active jobs
- **5 seconds**: Default, balanced performance
- **10 seconds**: Slower refresh, reduces API calls
- **30 seconds**: Minimal refresh for overview monitoring

Toggle auto-refresh on/off with the switch in the header.

### API Endpoints

The dashboard uses these Spark REST API endpoints:

```
# Cluster Status
GET http://localhost:9080/json/

# Applications
GET http://localhost:4040/api/v1/applications

# Jobs (per application)
GET http://localhost:4040/api/v1/applications/{app-id}/jobs

# Stages (per application)
GET http://localhost:4040/api/v1/applications/{app-id}/stages

# Executors (per application)
GET http://localhost:4040/api/v1/applications/{app-id}/executors
```

## 📊 Metrics Displayed

### Cluster Overview
- Number of workers
- Total/used CPU cores
- Total/used memory
- Active applications count

### Worker Details
- Worker hostname and web UI
- Health status (ALIVE/DEAD)
- CPU cores (used/total)
- Memory (used/total)

### Application Tracking
- Application ID and name
- State (RUNNING/COMPLETED/FAILED)
- CPU cores allocated
- Memory per executor
- Duration/runtime

### Performance Charts
- CPU utilization over time
- Memory utilization over time
- Active applications count

## 🎯 Use Cases

### Development
Monitor local Spark jobs during development

### Testing
Track performance metrics during load testing

### Production
Real-time monitoring of production Spark clusters

### Demo/Training
Visualize Spark concepts for learning

## 🔌 Integration with Prometheus

For historical metrics and alerting, integrate with Prometheus:

1. Configure Prometheus to scrape Spark metrics
2. Set `NEXT_PUBLIC_PROMETHEUS_URL` in `.env`
3. Access Prometheus at http://localhost:9090

See `../prometheus/prometheus.yml` for configuration.

## 🛠️ Tech Stack

- **Framework**: Next.js 14
- **Language**: TypeScript
- **Styling**: Tailwind CSS
- **Charts**: Recharts
- **Data Fetching**: SWR (stale-while-revalidate)
- **HTTP Client**: Axios
- **Icons**: Lucide React

## 📖 API Reference

### `lib/sparkApi.ts`

#### Functions

```typescript
// Fetch cluster status
fetchClusterStatus(): Promise<ClusterStatus>

// Fetch applications
fetchApplications(appId?: string): Promise<Application[]>

// Fetch jobs for an application
fetchJobs(appId: string): Promise<SparkJob[]>

// Fetch stages for an application
fetchStages(appId: string): Promise<SparkStage[]>

// Fetch executors for an application
fetchExecutors(appId: string): Promise<Executor[]>
```

#### Utility Functions

```typescript
formatBytes(bytes: number): string
formatDuration(ms: number): string
formatTimestamp(timestamp: string | number): string
getStatusColor(status: string): string
getStatusBadgeColor(status: string): string
```

## 🐛 Troubleshooting

### Dashboard shows "Failed to load cluster status"

**Solution**: Check that Spark Master is running on port 9080:
```bash
curl http://localhost:9080/json/
```

### No applications shown

**Solution**: Make sure Spark Application UI is accessible on port 4040:
```bash
curl http://localhost:4040/api/v1/applications
```

### CORS errors in browser console

**Solution**: Add CORS headers to Spark configuration or use the Next.js proxy in `next.config.js` (already configured).

### Charts not updating

**Solution**: 
1. Check auto-refresh is enabled (toggle in header)
2. Verify refresh interval is not 0
3. Check browser console for errors

## 🚀 Performance Tips

### Optimize for Large Clusters

1. **Increase refresh interval** to 10s or 30s
2. **Limit history** in charts (modify `MetricsChart.tsx`)
3. **Enable pagination** for workers/applications lists

### Reduce API Calls

1. **Disable auto-refresh** when not actively monitoring
2. **Use longer intervals** (30s instead of 2s)
3. **Cache data** with SWR (already implemented)

## 🔐 Security

### Production Deployment

- **Use HTTPS** for all connections
- **Add authentication** (integrate with corporate SSO)
- **Restrict API access** to authorized networks
- **Enable CORS** only for trusted origins
- **Use environment variables** for sensitive config

### Example Nginx Proxy

```nginx
server {
    listen 443 ssl;
    server_name spark-monitor.company.com;
    
    location / {
        proxy_pass http://localhost:3000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
    
    # Add authentication
    auth_basic "Spark Monitoring";
    auth_basic_user_file /etc/nginx/.htpasswd;
}
```

## 📝 Future Enhancements

- [ ] WebSocket support for real-time updates
- [ ] Alerting system for failed jobs
- [ ] Historical metrics (24h, 7d, 30d)
- [ ] SQL query visualization
- [ ] DAG visualization
- [ ] Multi-cluster support
- [ ] Custom dashboards
- [ ] Export metrics to CSV
- [ ] Dark/Light theme toggle
- [ ] Mobile app (React Native)

## 🤝 Contributing

This is part of a PySpark learning project. Feel free to:
- Report bugs
- Suggest features
- Submit pull requests
- Improve documentation

## 📄 License

MIT License - see LICENSE file for details

## 🔗 Related

- **Spark Cluster**: `../docker/spark-cluster/` - Docker cluster setup
- **Prometheus**: `../prometheus/` - Metrics collection
- **Examples**: `../examples/` - PySpark example jobs

## 💡 Tips

1. **Start with slow refresh** (30s) then speed up if needed
2. **Open browser dev tools** to see API requests
3. **Run demo jobs** from docker/spark-cluster/apps/
4. **Check Spark UI** at http://localhost:4040 for detailed metrics
5. **Use split screen** - dashboard on left, Spark UI on right

## 📞 Support

For issues or questions:
1. Check the troubleshooting section above
2. Review Spark REST API docs
3. Check browser console for errors
4. Verify Spark cluster is running

---

**Built with ❤️ for PySpark Learning**
