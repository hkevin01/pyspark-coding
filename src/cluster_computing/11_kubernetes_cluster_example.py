"""
11_kubernetes_cluster_example.py
=================================

Complete Kubernetes (K8s) cluster deployment example with manifests.

Kubernetes is the modern cloud-native orchestration platform.
Works on AWS EKS, Google GKE, Azure AKS, and on-premises K8s clusters.

This example shows:
1. K8s cluster configuration
2. Pod-based executor management
3. Service accounts and RBAC
4. Container image configuration
5. Resource requests and limits
6. Real ETL pipeline on K8s
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, expr
import time


def create_kubernetes_spark_session():
    """
    Create SparkSession configured for Kubernetes cluster.
    
    K8s Components:
    - Driver Pod: Runs the Spark driver
    - Executor Pods: Dynamic pod creation for executors
    - Service Account: RBAC permissions
    - ConfigMaps/Secrets: Configuration and credentials
    """
    print("=" * 80)
    print("CREATING KUBERNETES SPARK SESSION")
    print("=" * 80)
    
    spark = SparkSession.builder \
        .appName("K8sClusterETL") \
        .master("k8s://https://kubernetes.default.svc:443") \
        .config("spark.submit.deployMode", "cluster") \
        .config("spark.kubernetes.namespace", "spark") \
        .config("spark.kubernetes.container.image", "apache/spark-py:v3.5.0") \
        .config("spark.kubernetes.container.image.pullPolicy", "IfNotPresent") \
        .config("spark.kubernetes.authenticate.driver.serviceAccountName", "spark") \
        .config("spark.kubernetes.authenticate.executor.serviceAccountName", "spark") \
        .config("spark.executor.instances", "10") \
        .config("spark.executor.cores", "2") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.cores", "1") \
        .config("spark.kubernetes.executor.request.cores", "2") \
        .config("spark.kubernetes.executor.limit.cores", "4") \
        .config("spark.kubernetes.executor.request.memory", "4g") \
        .config("spark.kubernetes.executor.limit.memory", "6g") \
        .config("spark.kubernetes.driver.request.cores", "1") \
        .config("spark.kubernetes.driver.limit.cores", "2") \
        .config("spark.kubernetes.driver.pod.name", "spark-driver") \
        .config("spark.kubernetes.executor.label.app", "spark-executor") \
        .config("spark.kubernetes.executor.label.version", "3.5.0") \
        .config("spark.kubernetes.allocation.batch.size", "5") \
        .config("spark.kubernetes.allocation.batch.delay", "1s") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.dynamicAllocation.shuffleTracking.enabled", "true") \
        .config("spark.dynamicAllocation.minExecutors", "2") \
        .config("spark.dynamicAllocation.maxExecutors", "20") \
        .config("spark.dynamicAllocation.initialExecutors", "5") \
        .config("spark.dynamicAllocation.executorIdleTimeout", "60s") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    print(f"✅ Spark {spark.version} on Kubernetes")
    print(f"✅ Namespace: spark")
    print(f"✅ Container Image: apache/spark-py:v3.5.0")
    print(f"✅ Dynamic Allocation: 2-20 executor pods")
    
    return spark


def print_kubernetes_configuration(spark):
    """
    Display current Kubernetes configuration.
    """
    print("\n" + "=" * 80)
    print("KUBERNETES CLUSTER CONFIGURATION")
    print("=" * 80)
    
    sc = spark.sparkContext
    conf = spark.sparkContext.getConf()
    
    print("\n☸️  Kubernetes Settings:")
    print(f"   Master: {sc.master}")
    print(f"   Namespace: {conf.get('spark.kubernetes.namespace', 'default')}")
    print(f"   Container Image: {conf.get('spark.kubernetes.container.image', 'N/A')}")
    print(f"   Service Account: {conf.get('spark.kubernetes.authenticate.driver.serviceAccountName', 'N/A')}")
    
    print("\n📦 Pod Configuration:")
    print(f"   Driver Pod Name: {conf.get('spark.kubernetes.driver.pod.name', 'auto-generated')}")
    print(f"   Executor Label: {conf.get('spark.kubernetes.executor.label.app', 'N/A')}")
    
    print("\n💾 Resource Requests & Limits:")
    print(f"   Executor Request - Cores: {conf.get('spark.kubernetes.executor.request.cores', 'N/A')}")
    print(f"   Executor Request - Memory: {conf.get('spark.kubernetes.executor.request.memory', 'N/A')}")
    print(f"   Executor Limit - Cores: {conf.get('spark.kubernetes.executor.limit.cores', 'N/A')}")
    print(f"   Executor Limit - Memory: {conf.get('spark.kubernetes.executor.limit.memory', 'N/A')}")
    
    print("\n⚡ Dynamic Allocation:")
    print(f"   Enabled: {conf.get('spark.dynamicAllocation.enabled', 'false')}")
    print(f"   Shuffle Tracking: {conf.get('spark.dynamicAllocation.shuffleTracking.enabled', 'false')}")
    print(f"   Min Executors: {conf.get('spark.dynamicAllocation.minExecutors', 'N/A')}")
    print(f"   Max Executors: {conf.get('spark.dynamicAllocation.maxExecutors', 'N/A')}")


def generate_kubernetes_manifests():
    """
    Generate Kubernetes YAML manifests for Spark deployment.
    """
    print("\n" + "=" * 80)
    print("KUBERNETES MANIFESTS")
    print("=" * 80)
    
    print("""
📝 1. Namespace (spark-namespace.yaml):
──────────────────────────────────────────────────────────────────
apiVersion: v1
kind: Namespace
metadata:
  name: spark
  labels:
    name: spark
    environment: production
──────────────────────────────────────────────────────────────────

📝 2. Service Account (spark-serviceaccount.yaml):
──────────────────────────────────────────────────────────────────
apiVersion: v1
kind: ServiceAccount
metadata:
  name: spark
  namespace: spark
──────────────────────────────────────────────────────────────────

📝 3. ClusterRole (spark-clusterrole.yaml):
──────────────────────────────────────────────────────────────────
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: spark-role
rules:
- apiGroups: [""]
  resources: ["pods", "services", "configmaps"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
- apiGroups: [""]
  resources: ["pods/log"]
  verbs: ["get", "list"]
──────────────────────────────────────────────────────────────────

📝 4. ClusterRoleBinding (spark-clusterrolebinding.yaml):
──────────────────────────────────────────────────────────────────
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: spark-rolebinding
subjects:
- kind: ServiceAccount
  name: spark
  namespace: spark
roleRef:
  kind: ClusterRole
  name: spark-role
  apiGroup: rbac.authorization.k8s.io
──────────────────────────────────────────────────────────────────

📝 5. ConfigMap for Spark Configuration (spark-config.yaml):
──────────────────────────────────────────────────────────────────
apiVersion: v1
kind: ConfigMap
metadata:
  name: spark-config
  namespace: spark
data:
  spark-defaults.conf: |
    spark.sql.adaptive.enabled=true
    spark.sql.adaptive.coalescePartitions.enabled=true
    spark.sql.shuffle.partitions=200
    spark.serializer=org.apache.spark.serializer.KryoSerializer
  log4j.properties: |
    log4j.rootCategory=INFO, console
    log4j.appender.console=org.apache.log4j.ConsoleAppender
    log4j.appender.console.layout=org.apache.log4j.PatternLayout
──────────────────────────────────────────────────────────────────

📝 6. Resource Quota (spark-resourcequota.yaml):
──────────────────────────────────────────────────────────────────
apiVersion: v1
kind: ResourceQuota
metadata:
  name: spark-quota
  namespace: spark
spec:
  hard:
    requests.cpu: "100"
    requests.memory: 500Gi
    limits.cpu: "200"
    limits.memory: 1Ti
    pods: "100"
──────────────────────────────────────────────────────────────────

📝 7. Deployment Commands:
──────────────────────────────────────────────────────────────────
# Apply all manifests
kubectl apply -f spark-namespace.yaml
kubectl apply -f spark-serviceaccount.yaml
kubectl apply -f spark-clusterrole.yaml
kubectl apply -f spark-clusterrolebinding.yaml
kubectl apply -f spark-config.yaml
kubectl apply -f spark-resourcequota.yaml

# Verify deployment
kubectl get all -n spark
kubectl describe resourcequota spark-quota -n spark
──────────────────────────────────────────────────────────────────
    """)


def run_k8s_etl_pipeline(spark):
    """
    Run ETL pipeline on Kubernetes cluster.
    """
    print("\n" + "=" * 80)
    print("RUNNING ETL PIPELINE ON KUBERNETES")
    print("=" * 80)
    
    start_time = time.time()
    
    # Generate sample data
    print("\n📊 Generating sample data...")
    from pyspark.sql.functions import rand
    
    df = spark.range(0, 5_000_000) \
        .withColumn("customer_id", (rand() * 50000).cast("int")) \
        .withColumn("product_id", (rand() * 500).cast("int")) \
        .withColumn("amount", (rand() * 500 + 10).cast("double")) \
        .withColumn("region", expr("CASE " +
                                    "WHEN rand() < 0.25 THEN 'US-EAST' " +
                                    "WHEN rand() < 0.50 THEN 'US-WEST' " +
                                    "WHEN rand() < 0.75 THEN 'EU' " +
                                    "ELSE 'ASIA' END"))
    
    print(f"✅ Generated {df.count():,} records")
    print(f"✅ Distributed across {df.rdd.getNumPartitions()} partitions")
    
    # Regional analysis
    print("\n🌍 Regional Analysis:")
    regional_stats = df.groupBy("region").agg(
        count("*").alias("transactions"),
        _sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_transaction")
    ).orderBy(col("total_revenue").desc())
    
    regional_stats.show()
    
    # Top customers
    print("\n👥 Top 10 Customers by Spend:")
    top_customers = df.groupBy("customer_id").agg(
        count("*").alias("purchase_count"),
        _sum("amount").alias("total_spent")
    ).orderBy(col("total_spent").desc()).limit(10)
    
    top_customers.show()
    
    elapsed = time.time() - start_time
    print(f"\n⏱️  Pipeline completed in {elapsed:.2f} seconds")
    
    return regional_stats, top_customers


def demonstrate_k8s_features():
    """
    Demonstrate Kubernetes-specific features for Spark.
    """
    print("\n" + "=" * 80)
    print("KUBERNETES-SPECIFIC FEATURES")
    print("=" * 80)
    
    print("""
☸️  Pod-Based Architecture:
   - Each executor runs in its own pod
   - Driver can run in pod or externally (client mode)
   - Pods are ephemeral and dynamically created
   
🎯 Resource Management:
   - Requests: Guaranteed minimum resources
   - Limits: Maximum resources allowed
   - K8s scheduler places pods based on requests
   - OOM killer enforces limits
   
📦 Container Images:
   - Custom images with dependencies pre-installed
   - Image pull policies (Always, IfNotPresent, Never)
   - Private registries with imagePullSecrets
   
🔒 Security & RBAC:
   - Service accounts for driver and executors
   - Role-based access control (RBAC)
   - Network policies for pod communication
   - Secrets for sensitive data
   
⚡ Dynamic Allocation:
   - No external shuffle service needed (3.1+)
   - Shuffle tracking enabled by default
   - Pods created/destroyed based on workload
   
📊 Monitoring & Logging:
   - Spark UI via port-forward or ingress
   - Logs via kubectl logs
   - Integration with Prometheus/Grafana
   - K8s native monitoring tools
   
🌐 Multi-Tenancy:
   - Namespace isolation
   - Resource quotas per namespace
   - Network policies for isolation
   - Separate service accounts per team
   
☁️  Cloud Provider Integration:
   - AWS EKS: IAM roles, EBS volumes
   - GCP GKE: Workload Identity, GCE disks
   - Azure AKS: Managed identities, Azure disks
   - Autoscaling with cluster autoscaler
    """)


def kubernetes_submit_examples():
    """
    Print spark-submit examples for Kubernetes.
    """
    print("\n" + "=" * 80)
    print("KUBERNETES SPARK-SUBMIT EXAMPLES")
    print("=" * 80)
    
    print("""
📝 1. Basic K8s Submission:
──────────────────────────────────────────────────────────────────

spark-submit \\
  --master k8s://https://kubernetes.default.svc:443 \\
  --deploy-mode cluster \\
  --name k8s-spark-app \\
  --conf spark.kubernetes.namespace=spark \\
  --conf spark.kubernetes.container.image=apache/spark-py:v3.5.0 \\
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \\
  --conf spark.executor.instances=5 \\
  --conf spark.executor.memory=4g \\
  --conf spark.executor.cores=2 \\
  11_kubernetes_cluster_example.py


📝 2. With Dynamic Allocation:
──────────────────────────────────────────────────────────────────

spark-submit \\
  --master k8s://https://k8s-api.example.com:6443 \\
  --deploy-mode cluster \\
  --conf spark.kubernetes.namespace=spark \\
  --conf spark.kubernetes.container.image=apache/spark-py:v3.5.0 \\
  --conf spark.dynamicAllocation.enabled=true \\
  --conf spark.dynamicAllocation.shuffleTracking.enabled=true \\
  --conf spark.dynamicAllocation.minExecutors=2 \\
  --conf spark.dynamicAllocation.maxExecutors=20 \\
  --conf spark.dynamicAllocation.initialExecutors=5 \\
  --conf spark.kubernetes.allocation.batch.size=5 \\
  11_kubernetes_cluster_example.py


📝 3. With Resource Requests & Limits:
──────────────────────────────────────────────────────────────────

spark-submit \\
  --master k8s://https://k8s-api.example.com:6443 \\
  --deploy-mode cluster \\
  --conf spark.kubernetes.namespace=spark \\
  --conf spark.kubernetes.container.image=my-registry/spark:latest \\
  --conf spark.kubernetes.container.image.pullPolicy=Always \\
  --conf spark.kubernetes.container.image.pullSecrets=regcred \\
  --conf spark.executor.instances=10 \\
  --conf spark.kubernetes.executor.request.cores=2 \\
  --conf spark.kubernetes.executor.limit.cores=4 \\
  --conf spark.kubernetes.executor.request.memory=4g \\
  --conf spark.kubernetes.executor.limit.memory=6g \\
  --conf spark.kubernetes.driver.request.cores=1 \\
  --conf spark.kubernetes.driver.limit.cores=2 \\
  11_kubernetes_cluster_example.py


📝 4. With Custom Labels & Annotations:
──────────────────────────────────────────────────────────────────

spark-submit \\
  --master k8s://https://k8s-api.example.com:6443 \\
  --deploy-mode cluster \\
  --conf spark.kubernetes.namespace=spark \\
  --conf spark.kubernetes.container.image=apache/spark-py:v3.5.0 \\
  --conf spark.kubernetes.executor.label.app=spark-etl \\
  --conf spark.kubernetes.executor.label.team=data-eng \\
  --conf spark.kubernetes.executor.label.environment=production \\
  --conf spark.kubernetes.executor.annotation.prometheus.io/scrape=true \\
  --conf spark.kubernetes.executor.annotation.prometheus.io/port=8090 \\
  11_kubernetes_cluster_example.py


📝 5. With Volumes (Data Access):
──────────────────────────────────────────────────────────────────

spark-submit \\
  --master k8s://https://k8s-api.example.com:6443 \\
  --deploy-mode cluster \\
  --conf spark.kubernetes.namespace=spark \\
  --conf spark.kubernetes.container.image=apache/spark-py:v3.5.0 \\
  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.data.options.claimName=spark-pvc \\
  --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.data.mount.path=/data \\
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.data.options.claimName=spark-pvc \\
  --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.data.mount.path=/data \\
  11_kubernetes_cluster_example.py


📝 6. Client Mode (for development):
──────────────────────────────────────────────────────────────────

# Run from inside K8s cluster
spark-submit \\
  --master k8s://https://kubernetes.default.svc:443 \\
  --deploy-mode client \\
  --conf spark.kubernetes.namespace=spark \\
  --conf spark.kubernetes.container.image=apache/spark-py:v3.5.0 \\
  --conf spark.executor.instances=3 \\
  11_kubernetes_cluster_example.py


📝 7. Monitoring with Port Forward:
──────────────────────────────────────────────────────────────────

# After submission, get driver pod name
kubectl get pods -n spark -l spark-role=driver

# Port forward to access Spark UI
kubectl port-forward -n spark <driver-pod-name> 4040:4040

# Access UI at: http://localhost:4040
    """)


def kubernetes_useful_commands():
    """
    Print useful kubectl commands for managing Spark on K8s.
    """
    print("\n" + "=" * 80)
    print("USEFUL KUBECTL COMMANDS")
    print("=" * 80)
    
    print("""
📝 Monitoring:
──────────────────────────────────────────────────────────────────

# List all Spark pods
kubectl get pods -n spark -l spark-app-selector

# Watch pods in real-time
kubectl get pods -n spark --watch

# Get pod details
kubectl describe pod <pod-name> -n spark

# View pod logs
kubectl logs <pod-name> -n spark

# Follow logs in real-time
kubectl logs -f <pod-name> -n spark

# Get events
kubectl get events -n spark --sort-by='.lastTimestamp'


📝 Resource Management:
──────────────────────────────────────────────────────────────────

# Check resource usage
kubectl top pods -n spark

# View resource quotas
kubectl describe resourcequota -n spark

# Scale executors manually (if not using dynamic allocation)
kubectl scale --replicas=10 deployment/spark-executors -n spark


📝 Debugging:
──────────────────────────────────────────────────────────────────

# Execute command in pod
kubectl exec -it <pod-name> -n spark -- /bin/bash

# Copy files from pod
kubectl cp spark/<pod-name>:/path/to/file ./local-file

# Port forward for Spark UI
kubectl port-forward -n spark <driver-pod> 4040:4040


📝 Cleanup:
──────────────────────────────────────────────────────────────────

# Delete specific application pods
kubectl delete pod -n spark -l spark-app-selector=<app-name>

# Delete all Spark pods
kubectl delete pods -n spark -l spark-role

# Delete completed pods
kubectl delete pod -n spark --field-selector=status.phase=Succeeded
    """)


def main():
    """
    Main execution function.
    """
    print("\n" + "☸️ " * 40)
    print("KUBERNETES CLUSTER COMPUTING - COMPLETE EXAMPLE")
    print("☸️ " * 40)
    
    # Note: This will fail without a real K8s cluster
    # For demo purposes, we'll use local mode
    print("\n⚠️  Note: Running in LOCAL mode for demonstration")
    print("   In production, this would connect to K8s cluster")
    
    # Create Spark session (local for demo)
    spark = SparkSession.builder \
        .appName("K8sDemo") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    
    # Show what K8s configuration would look like
    print_kubernetes_configuration(spark)
    
    # Generate K8s manifests
    generate_kubernetes_manifests()
    
    # Run ETL pipeline
    regional, customers = run_k8s_etl_pipeline(spark)
    
    # Show K8s features
    demonstrate_k8s_features()
    
    # Show submit examples
    kubernetes_submit_examples()
    
    # Show useful commands
    kubernetes_useful_commands()
    
    print("\n" + "=" * 80)
    print("✅ KUBERNETES EXAMPLE COMPLETE")
    print("=" * 80)
    print("\n📚 Key Takeaways:")
    print("   1. K8s provides pod-based, cloud-native Spark deployment")
    print("   2. Each executor runs in its own pod for isolation")
    print("   3. Dynamic allocation works without external shuffle service (3.1+)")
    print("   4. Resource requests guarantee minimum, limits set maximum")
    print("   5. RBAC provides fine-grained security control")
    print("   6. Works seamlessly on AWS EKS, GCP GKE, Azure AKS")
    print("   7. Monitor via kubectl commands and K8s dashboards")
    print("   8. Container images allow custom dependencies")
    
    spark.stop()


if __name__ == "__main__":
    main()
