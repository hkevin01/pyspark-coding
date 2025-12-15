# Prometheus Integration with PySpark

Complete guide to monitoring PySpark applications with Prometheus.

## 📚 Overview

This directory contains examples of integrating Prometheus monitoring with PySpark applications. Learn how to export metrics, track performance, monitor data quality, and set up alerting for production systems.

## 📋 Files

### 01_basic_metrics.py (⭐⭐⭐☆☆)
**Basic Prometheus metrics with PySpark**
- Export Spark job metrics to Prometheus
- Track DataFrame operations
- Monitor job execution times
- Custom application metrics
- **Metrics exposed:** job duration, row counts, executor info, query performance
- **Time:** 15 minutes

### 02_custom_metrics_alerts.py (⭐⭐⭐⭐☆)
**Custom business metrics and alert thresholds**
- Define custom business KPIs
- Set alert thresholds
- Monitor data quality scores
- Track SLA compliance
- Revenue and error rate monitoring
- **Metrics exposed:** data quality, SLA violations, revenue, error rates
- **Time:** 20 minutes

### 03_pushgateway_batch_jobs.py (⭐⭐⭐⭐☆)
**Prometheus Pushgateway for batch jobs**
- Push metrics from short-lived jobs
- Track batch job completion
- Monitor scheduled ETL pipelines
- Handle transient workloads
- **Use case:** Nightly batch jobs, one-time migrations
- **Time:** 20 minutes

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install prometheus-client pyspark
```

### 2. Run Basic Example

```bash
python 01_basic_metrics.py
```

Visit `http://localhost:8000/metrics` to see exported metrics in Prometheus format.

### 3. (Optional) Set Up Prometheus

```bash
# Download Prometheus
wget https://github.com/prometheus/prometheus/releases/download/v2.45.0/prometheus-2.45.0.linux-amd64.tar.gz
tar xvfz prometheus-2.45.0.linux-amd64.tar.gz
cd prometheus-2.45.0.linux-amd64

# Create config to scrape your Python app
cat > prometheus.yml << 'PROM'
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'pyspark_metrics'
    static_configs:
      - targets: ['localhost:8000']
PROM

# Start Prometheus
./prometheus --config.file=prometheus.yml
```

Visit `http://localhost:9090` for Prometheus UI.

### 4. (Optional) Set Up Pushgateway

```bash
# Using Docker
docker run -d -p 9091:9091 prom/pushgateway

# Or download binary
wget https://github.com/prometheus/pushgateway/releases/download/v1.6.0/pushgateway-1.6.0.linux-amd64.tar.gz
tar xvfz pushgateway-1.6.0.linux-amd64.tar.gz
cd pushgateway-1.6.0.linux-amd64
./pushgateway
```

Visit `http://localhost:9091` for Pushgateway UI.

## 📊 Metric Types

### Counter
Monotonically increasing value (never decreases)
```python
from prometheus_client import Counter

jobs_total = Counter('spark_jobs_total', 'Total jobs', ['status'])
jobs_total.labels(status='success').inc()
```

### Gauge
Value that can go up or down
```python
from prometheus_client import Gauge

active_executors = Gauge('spark_active_executors', 'Active executors')
active_executors.set(4)
```

### Histogram
Observations in buckets (for percentiles)
```python
from prometheus_client import Histogram

job_duration = Histogram(
    'spark_job_duration_seconds',
    'Job duration',
    buckets=(1, 5, 10, 30, 60, 120, 300)
)

with job_duration.time():
    # Your code here
    pass
```

### Summary
Like histogram but calculates quantiles
```python
from prometheus_client import Summary

query_duration = Summary('spark_query_duration_seconds', 'Query duration')

with query_duration.time():
    df.count()
```

## 🎯 Common Use Cases

### 1. Monitor Job Success Rate

```python
from prometheus_client import Counter

jobs_total = Counter('spark_jobs_total', 'Total jobs', ['job_name', 'status'])

try:
    # Run job
    process_data()
    jobs_total.labels(job_name='etl', status='success').inc()
except Exception as e:
    jobs_total.labels(job_name='etl', status='failure').inc()
```

**Prometheus Query:**
```promql
# Success rate over 1 hour
rate(spark_jobs_total{status="success"}[1h]) / rate(spark_jobs_total[1h])
```

### 2. Track Data Quality

```python
from prometheus_client import Gauge

data_quality = Gauge('data_quality_score', 'Quality score', ['dataset'])

null_count = df.filter(col("value").isNull()).count()
total_count = df.count()
quality_score = 100 * (1 - null_count / total_count)

data_quality.labels(dataset='customers').set(quality_score)
```

**Alert Rule:**
```yaml
- alert: LowDataQuality
  expr: data_quality_score < 85
  for: 5m
  annotations:
    summary: "Data quality below threshold"
```

### 3. Monitor Processing Latency

```python
from prometheus_client import Histogram

latency = Histogram('processing_latency_seconds', 'Processing time')

with latency.time():
    df.write.parquet("/output")
```

**Prometheus Query:**
```promql
# 95th percentile latency
histogram_quantile(0.95, processing_latency_seconds_bucket)
```

### 4. Track SLA Violations

```python
from prometheus_client import Counter, Gauge

sla_violations = Counter('sla_violations_total', 'SLA violations', ['severity'])
sla_compliance = Gauge('sla_compliance_rate', 'Compliance %')

# Process batch
if latency > 5.0:  # 5 second SLA
    sla_violations.labels(severity='high').inc()
    
compliance = (successful / total) * 100
sla_compliance.set(compliance)
```

## 🔍 Prometheus Queries

### Job Metrics
```promql
# Total jobs in last hour
increase(spark_jobs_total[1h])

# Job failure rate
rate(spark_jobs_total{status="failure"}[5m])

# Average job duration
avg(spark_job_duration_seconds)
```

### Performance Metrics
```promql
# 99th percentile query time
histogram_quantile(0.99, spark_query_duration_seconds)

# Records processed per second
rate(spark_dataframe_row_count[1m])

# Slow queries (>10s)
spark_query_duration_seconds > 10
```

### Health Checks
```promql
# Jobs that haven't run in 24 hours
(time() - batch_job_last_success_timestamp_seconds) > 86400

# High error rate
rate(application_errors_total[5m]) > 10

# Low data quality
data_quality_score_percentage < 85
```

## 📈 Grafana Dashboards

### Example Dashboard JSON

```json
{
  "dashboard": {
    "title": "PySpark Monitoring",
    "panels": [
      {
        "title": "Job Success Rate",
        "targets": [{
          "expr": "rate(spark_jobs_total{status=\"success\"}[5m])"
        }]
      },
      {
        "title": "Processing Latency (p95)",
        "targets": [{
          "expr": "histogram_quantile(0.95, processing_latency_seconds_bucket)"
        }]
      },
      {
        "title": "Data Quality Score",
        "targets": [{
          "expr": "data_quality_score_percentage"
        }]
      }
    ]
  }
}
```

## 🚨 Alert Rules

Create `alerts.yml`:

```yaml
groups:
  - name: pyspark_alerts
    rules:
      # Job failures
      - alert: HighJobFailureRate
        expr: rate(spark_jobs_total{status="failure"}[5m]) > 0.1
        for: 10m
        labels:
          severity: critical
        annotations:
          summary: "High job failure rate"
          description: "Job failure rate is {{ $value }} failures/sec"
      
      # Data quality
      - alert: LowDataQuality
        expr: data_quality_score_percentage < 85
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Data quality below threshold"
      
      # SLA violations
      - alert: SLAViolation
        expr: sla_compliance_rate_percentage < 99
        for: 15m
        labels:
          severity: high
        annotations:
          summary: "SLA compliance below 99%"
      
      # Stale jobs
      - alert: JobNotRunRecently
        expr: (time() - batch_job_last_success_timestamp_seconds) > 86400
        labels:
          severity: warning
        annotations:
          summary: "Job hasn't run in 24+ hours"
```

## 💡 Best Practices

### 1. Metric Naming
- Use descriptive names: `spark_job_duration_seconds` not `duration`
- Include units: `_seconds`, `_bytes`, `_total`
- Use snake_case

### 2. Labels
- Keep cardinality low (< 100 unique values per label)
- Don't use timestamps or IDs as labels
- Use labels for dimensions: job_name, status, region

### 3. Instrumentation
- Track both successes and failures
- Use histograms for latency metrics
- Set appropriate bucket sizes
- Add context with labels

### 4. Alerting
- Alert on symptoms, not causes
- Include clear descriptions
- Set appropriate thresholds
- Test alerts regularly

### 5. Performance
- Avoid high-cardinality metrics
- Limit label combinations
- Use Pushgateway for batch jobs
- Don't expose internal IDs

## 🔧 Troubleshooting

### Metrics Not Appearing

1. Check if HTTP server is running:
   ```bash
   curl http://localhost:8000/metrics
   ```

2. Verify Prometheus scraping:
   ```bash
   # Check Prometheus targets
   http://localhost:9090/targets
   ```

3. Check Prometheus logs:
   ```bash
   tail -f prometheus.log
   ```

### Pushgateway Issues

1. Verify Pushgateway is running:
   ```bash
   curl http://localhost:9091/metrics
   ```

2. Check pushed metrics:
   ```bash
   curl http://localhost:9091/metrics | grep batch_job
   ```

3. Delete old metrics:
   ```python
   from prometheus_client import delete_from_gateway
   delete_from_gateway('localhost:9091', job='my_job')
   ```

## 📖 Resources

- [Prometheus Documentation](https://prometheus.io/docs/)
- [Prometheus Python Client](https://github.com/prometheus/client_python)
- [Grafana Dashboards](https://grafana.com/grafana/dashboards/)
- [Alert Manager](https://prometheus.io/docs/alerting/latest/alertmanager/)
- [PySpark Monitoring](https://spark.apache.org/docs/latest/monitoring.html)

## 🎓 Learning Path

1. **Week 1:** Basic Metrics
   - Run `01_basic_metrics.py`
   - Understand metric types
   - View metrics endpoint

2. **Week 2:** Custom Metrics
   - Run `02_custom_metrics_alerts.py`
   - Define business KPIs
   - Set alert thresholds

3. **Week 3:** Production Setup
   - Run `03_pushgateway_batch_jobs.py`
   - Set up Prometheus server
   - Configure Pushgateway
   - Create Grafana dashboards

4. **Week 4:** Advanced Topics
   - Build production monitoring
   - Create alert rules
   - Optimize queries
   - Implement best practices

## 🚀 Next Steps

1. Install Prometheus and Pushgateway
2. Run the examples
3. Create custom metrics for your application
4. Set up Grafana dashboards
5. Configure alerting rules
6. Monitor production workloads

Happy monitoring! 📊
