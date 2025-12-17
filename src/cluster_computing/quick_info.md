# 🖥️ Cluster Computing Quick Reference

## 📊 Core Components

### **Driver Node**
The machine where you launch your Python script. It contains the driver program that defines the job (transformations, actions).

### **Worker Nodes (Executors)**
Other machines in the cluster that actually perform the computations on partitions of the data.

### **Cluster Manager**
Software that coordinates resources across machines (e.g., YARN, Mesos, Kubernetes, or Spark's built-in standalone manager).

---

## 🔧 What Each Node Needs

### 1. **Cluster Manager** (YARN, Kubernetes, etc.)
→ Schedules tasks and allocates CPU/memory across nodes

### 2. **Distributed Framework Runtime** (Apache Spark)
→ Installed on all nodes so they can understand job instructions

### 3. **Python + Libraries**
→ Each worker needs compatible Python runtime and dependencies (NumPy, PyTorch, etc.)

### 4. **Communication Layer**
→ Spark uses JVM + Py4J bridge to talk between Python and JVM  
→ Workers communicate over TCP using Spark's RPC

---

## 🚀 What Happens When You Run a Python Script

### 1. **Submit Job**
```bash
spark-submit myscript.py
```

### 2. **Driver Starts**
The driver interprets your Python code and builds a logical plan of transformations (RDD/DataFrame operations)

### 3. **Cluster Manager Allocates Resources**
Executors are launched on worker nodes

### 4. **Code Distribution**
Spark ships your Python code (via serialization) and any dependencies to each executor

### 5. **Task Scheduling**
The driver breaks the job into tasks (units of work on partitions of data)

### 6. **Workers Execute**
Each executor runs the Python function (often a lambda in `map`, `filter`, etc.) on its partition

### 7. **Results Returned**
Executors send results back to the driver, which aggregates them and produces the final output

---

## 📈 Execution Flow Diagram

```
You → spark-submit → Driver Node → Cluster Manager
                         ↓              ↓
                    Build Plan    Allocate Resources
                         ↓              ↓
                    Task Schedule → Worker Nodes
                         ↓              ↓
                    Distribute Code → Executors Run
                         ↓              ↓
                    Aggregate ← Return Results
                         ↓
                    Final Output
```