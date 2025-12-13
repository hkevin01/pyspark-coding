# Cluster Managers Guide - YARN, Kubernetes, Standalone

This guide covers the three complete cluster deployment examples:
- **Example 10**: YARN Cluster
- **Example 11**: Kubernetes Cluster  
- **Example 12**: Standalone Cluster

## Overview Comparison

| Feature | YARN | Kubernetes | Standalone |
|---------|------|------------|------------|
| **Complexity** | High | Medium-High | Low |
| **Dependencies** | Hadoop ecosystem | K8s cluster | None (just Spark) |
| **Best For** | Enterprise Hadoop | Cloud-native | Dev/Small clusters |
| **Dynamic Allocation** | ✅ Yes | ✅ Yes (3.1+) | ❌ No |
| **Multi-Tenancy** | ✅ Strong (queues) | ✅ Yes (namespaces) | ❌ Limited |
| **Setup Difficulty** | Hard | Medium | Easy |
| **Resource Mgmt** | ResourceManager | K8s Scheduler | Simple FCFS |
| **HA Support** | ✅ Yes | ✅ Native | ⚠️  Via ZooKeeper |
| **Cloud Integration** | EMR, Dataproc | EKS, GKE, AKS | Any VM |
| **Monitoring** | YARN RM UI | kubectl + K8s UI | Master UI (8080) |
| **Typical Scale** | 100-10,000 nodes | 10-1,000 nodes | 2-100 nodes |

## Example 10: YARN Cluster (10_yarn_cluster_example.py)

### What It Shows
- Complete YARN configuration for production
- Dynamic allocation with shuffle service
- Queue management for multi-tenancy
- Real ETL pipeline processing 10M records
- Resource tuning (memory overhead, executor sizing)
- Client vs cluster deploy modes

### Key Features
```python
spark = SparkSession.builder \
    .master("yarn") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.maxExecutors", "20") \
    .config("spark.yarn.queue", "production") \
    .config("spark.shuffle.service.enabled", "true") \
    .getOrCreate()
```

### When to Use YARN
✅ Enterprise Hadoop clusters (HDFS, Hive, HBase)
✅ Need strong multi-tenancy (production/dev/adhoc queues)
✅ Large scale deployments (100+ nodes)
✅ Want mature, battle-tested resource management
✅ Already have Hadoop infrastructure

### spark-submit Example
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 10 \
  --executor-cores 5 \
  --executor-memory 10g \
  --conf spark.yarn.queue=production \
  --conf spark.dynamicAllocation.enabled=true \
  10_yarn_cluster_example.py
```

### Monitoring
- YARN ResourceManager UI: `http://rm-host:8088`
- Application logs: Aggregated in HDFS
- Metrics: YARN REST API

## Example 11: Kubernetes Cluster (11_kubernetes_cluster_example.py)

### What It Shows
- K8s-native Spark deployment with pod-based executors
- Complete YAML manifests (namespace, service account, RBAC)
- Resource requests and limits (CPU/memory)
- Dynamic allocation without external shuffle service
- Container image configuration
- Integration with cloud providers (EKS, GKE, AKS)

### Key Features
```python
spark = SparkSession.builder \
    .master("k8s://https://kubernetes.default.svc:443") \
    .config("spark.kubernetes.namespace", "spark") \
    .config("spark.kubernetes.container.image", "apache/spark-py:v3.5.0") \
    .config("spark.kubernetes.authenticate.driver.serviceAccountName", "spark") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.shuffleTracking.enabled", "true") \
    .getOrCreate()
```

### When to Use Kubernetes
✅ Cloud-native deployments (AWS EKS, GCP GKE, Azure AKS)
✅ Want containerized workloads
✅ Need fine-grained resource control (requests/limits)
✅ Modern infrastructure, microservices architecture
✅ Multi-tenant with namespace isolation
✅ Want to leverage K8s ecosystem (monitoring, autoscaling)

### spark-submit Example
```bash
spark-submit \
  --master k8s://https://k8s-api:6443 \
  --deploy-mode cluster \
  --conf spark.kubernetes.namespace=spark \
  --conf spark.kubernetes.container.image=apache/spark-py:v3.5.0 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.kubernetes.executor.request.cores=2 \
  --conf spark.kubernetes.executor.limit.cores=4 \
  11_kubernetes_cluster_example.py
```

### Kubernetes Manifests Included
1. `Namespace` - Isolate Spark workloads
2. `ServiceAccount` - RBAC permissions
3. `ClusterRole` - Define pod/service permissions
4. `ClusterRoleBinding` - Bind role to service account
5. `ConfigMap` - Spark configuration
6. `ResourceQuota` - Limit namespace resources

### Monitoring
```bash
# List pods
kubectl get pods -n spark

# View logs
kubectl logs -f <driver-pod> -n spark

# Port forward to Spark UI
kubectl port-forward <driver-pod> 4040:4040 -n spark
```

## Example 12: Standalone Cluster (12_standalone_cluster_example.py)

### What It Shows
- Simplest cluster setup (no external dependencies)
- Master and worker configuration scripts
- Shell scripts to start/stop cluster
- ETL pipeline on standalone cluster
- Web UI monitoring (Master:8080, Worker:8081)
- High Availability mode with ZooKeeper

### Key Features
```python
spark = SparkSession.builder \
    .master("spark://master-node:7077") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.memory", "8g") \
    .config("spark.cores.max", "40") \
    .getOrCreate()
```

### When to Use Standalone
✅ Simple development/testing environments
✅ Small production clusters (< 100 nodes)
✅ No Hadoop or K8s infrastructure
✅ Want minimal setup complexity
✅ Educational/learning purposes
✅ On-premises without complex infrastructure

### Cluster Setup
```bash
# On master node
$SPARK_HOME/sbin/start-master.sh

# On worker nodes
$SPARK_HOME/sbin/start-worker.sh spark://master-node:7077

# Or start entire cluster
$SPARK_HOME/sbin/start-all.sh
```

### spark-submit Example
```bash
spark-submit \
  --master spark://master-node:7077 \
  --deploy-mode cluster \
  --executor-memory 8g \
  --executor-cores 4 \
  --total-executor-cores 40 \
  --supervise \
  12_standalone_cluster_example.py
```

### Monitoring
- Master UI: `http://master-node:8080`
- Worker UI: `http://worker-node:8081`
- Application UI: `http://driver:4040`

## Quick Decision Matrix

### Choose YARN if:
- You have existing Hadoop ecosystem (HDFS, Hive, HBase)
- Need production-grade multi-tenancy with queues
- Running on-premises with 100+ nodes
- Want mature resource management and monitoring
- Team familiar with Hadoop operations

### Choose Kubernetes if:
- Deploying on cloud (AWS EKS, GCP GKE, Azure AKS)
- Want modern, container-based infrastructure
- Need fine-grained resource control
- Already using K8s for other services
- Want cloud-native autoscaling and monitoring
- Team familiar with K8s/Docker

### Choose Standalone if:
- Starting with Spark, learning cluster computing
- Small development or testing cluster
- No complex infrastructure requirements
- Want simplest possible setup
- Running temporary or short-lived clusters
- Less than 50 nodes

## Performance Characteristics

| Aspect | YARN | Kubernetes | Standalone |
|--------|------|------------|------------|
| **Startup Time** | 10-30s | 5-15s | 2-5s |
| **Overhead** | Medium | Low-Medium | Very Low |
| **Scalability** | Excellent (10K+) | Very Good (1K+) | Good (100) |
| **Resource Efficiency** | High | Very High | Medium |
| **Failure Recovery** | Excellent | Excellent | Good |

## Running the Examples

All three examples run in LOCAL mode for demonstration:

```bash
# YARN example
python 10_yarn_cluster_example.py

# Kubernetes example  
python 11_kubernetes_cluster_example.py

# Standalone example
python 12_standalone_cluster_example.py
```

Each example shows:
1. Complete configuration
2. Real ETL pipeline processing
3. Cluster-specific features
4. spark-submit commands
5. Monitoring approaches

## Production Deployment Checklist

### YARN
- [ ] Configure YARN queues and capacity
- [ ] Enable shuffle service for dynamic allocation
- [ ] Set up log aggregation to HDFS
- [ ] Configure memory overhead properly
- [ ] Test failover behavior
- [ ] Set up monitoring (YARN RM UI + metrics)

### Kubernetes
- [ ] Create namespace and service accounts
- [ ] Configure RBAC roles and bindings
- [ ] Set resource requests and limits
- [ ] Build custom container images with dependencies
- [ ] Configure image pull secrets for private registries
- [ ] Set up monitoring (Prometheus/Grafana)
- [ ] Configure network policies
- [ ] Test pod autoscaling

### Standalone
- [ ] Set up master node(s)
- [ ] Configure workers with appropriate resources
- [ ] Set environment variables (SPARK_HOME, etc.)
- [ ] Configure HA with ZooKeeper (optional)
- [ ] Set up log directories
- [ ] Test master/worker restart
- [ ] Document master URL for applications

## Common Issues & Solutions

### YARN
**Issue**: Dynamic allocation not working
- Solution: Enable `spark.shuffle.service.enabled=true` in cluster

**Issue**: Application stuck in ACCEPTED state
- Solution: Check queue capacity, increase resources

### Kubernetes
**Issue**: ImagePullBackOff
- Solution: Check image name, add imagePullSecrets for private registry

**Issue**: Pods stuck in Pending
- Solution: Check resource requests vs cluster capacity

### Standalone
**Issue**: Worker not connecting to master
- Solution: Verify master URL, check firewall rules on port 7077

**Issue**: Out of cores/memory
- Solution: Add more workers or increase `spark.cores.max`

## Next Steps

After understanding cluster managers:
1. **Optimize** resource allocation (see `07_resource_management.py`)
2. **Monitor** cluster health (see `09_cluster_monitoring.py`)
3. **Tune** shuffle performance (see `08_shuffle_optimization.py`)
4. **Deploy** to production with appropriate cluster manager

## Additional Resources

- Apache Spark Configuration: https://spark.apache.org/docs/latest/configuration.html
- Running on YARN: https://spark.apache.org/docs/latest/running-on-yarn.html
- Running on Kubernetes: https://spark.apache.org/docs/latest/running-on-kubernetes.html
- Standalone Mode: https://spark.apache.org/docs/latest/spark-standalone.html
