import { useState } from 'react'
import { 
  Info, 
  ChevronDown, 
  ChevronRight, 
  Monitor,
  Server,
  Cpu,
  Layers,
  Shuffle,
  Grid3X3,
  ArrowRight,
  ArrowDown,
  Database,
  HardDrive,
  Zap,
  Network,
  Box,
  GitBranch,
  Code,
  Filter,
  List,
  Clock,
  Settings,
  FileText,
  Play,
  CheckCircle,
  Table
} from 'lucide-react'

interface EducationSectionProps {
  title: string
  icon: React.ElementType
  color: string
  children: React.ReactNode
  defaultExpanded?: boolean
}

function EducationSection({ title, icon: Icon, color, children, defaultExpanded = false }: EducationSectionProps) {
  const [isExpanded, setIsExpanded] = useState(defaultExpanded)
  
  return (
    <div className="bg-gray-800/50 border border-gray-700 rounded-lg overflow-hidden">
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="w-full flex items-center justify-between p-4 hover:bg-gray-800 transition-colors"
      >
        <div className="flex items-center">
          <Icon className={`h-5 w-5 mr-3 ${color}`} />
          <span className="text-white font-medium">{title}</span>
        </div>
        {isExpanded ? (
          <ChevronDown className="h-5 w-5 text-gray-400" />
        ) : (
          <ChevronRight className="h-5 w-5 text-gray-400" />
        )}
      </button>
      {isExpanded && (
        <div className="px-4 pb-4 border-t border-gray-700">
          {children}
        </div>
      )}
    </div>
  )
}

export default function SparkEducation() {
  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="bg-gradient-to-r from-purple-900/30 to-blue-900/30 border border-purple-700/50 rounded-lg p-4">
        <div className="flex items-start">
          <Info className="h-5 w-5 text-purple-400 mr-3 mt-0.5 flex-shrink-0" />
          <div>
            <h3 className="text-purple-300 font-medium">�� Spark Architecture Deep Dive</h3>
            <p className="text-sm text-gray-300 mt-1">
              Understand what's really happening inside your PySpark cluster. This guide explains 
              the core concepts with real-world analogies to help you optimize your jobs.
            </p>
          </div>
        </div>
      </div>

      {/* The Driver - The Boss */}
      <EducationSection title="🧠 The Driver - Your Job's Brain" icon={Monitor} color="text-yellow-400" defaultExpanded={true}>
        <div className="mt-4 space-y-4">
          <div className="bg-yellow-500/10 border border-yellow-500/30 rounded-lg p-4">
            <h4 className="text-yellow-400 font-medium mb-2">What is the Driver?</h4>
            <p className="text-gray-300 text-sm">
              The <span className="text-yellow-400 font-semibold">Driver</span> is the main process that runs your 
              PySpark application. It's like the <span className="text-white font-medium">project manager</span> of 
              your data processing job - it doesn't do the heavy lifting itself, but it plans everything, 
              coordinates all workers, and collects final results.
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="bg-gray-900/50 rounded-lg p-3">
              <h5 className="text-white font-medium mb-2 flex items-center">
                <Zap className="h-4 w-4 mr-2 text-yellow-400" />
                Driver Responsibilities
              </h5>
              <ul className="text-sm text-gray-400 space-y-1">
                <li>• Creates the <span className="text-blue-400">SparkContext</span> (entry point to Spark)</li>
                <li>• Converts your code into a <span className="text-purple-400">DAG</span> (execution plan)</li>
                <li>• Splits the DAG into <span className="text-green-400">stages</span> and <span className="text-orange-400">tasks</span></li>
                <li>• Schedules tasks on executors</li>
                <li>• Monitors task execution</li>
                <li>• Collects results back (e.g., when you call <code className="bg-gray-800 px-1 rounded">.collect()</code>)</li>
              </ul>
            </div>

            <div className="bg-gray-900/50 rounded-lg p-3">
              <h5 className="text-white font-medium mb-2 flex items-center">
                <HardDrive className="h-4 w-4 mr-2 text-yellow-400" />
                Where Does the Driver Run?
              </h5>
              <div className="text-sm text-gray-400 space-y-2">
                <p>
                  <span className="text-white font-medium">Client Mode:</span> Driver runs on the machine 
                  where you submitted the job (your laptop, Jupyter notebook, etc.)
                </p>
                <p>
                  <span className="text-white font-medium">Cluster Mode:</span> Driver runs inside the 
                  cluster on one of the nodes (better for production)
                </p>
                <p className="text-yellow-400 text-xs mt-2">
                  ⚠️ In this dashboard cluster, the driver runs on the spark-master container
                </p>
              </div>
            </div>
          </div>

          <div className="bg-red-500/10 border border-red-500/30 rounded-lg p-3">
            <h5 className="text-red-400 font-medium mb-1">⚠️ Common Driver Issue</h5>
            <p className="text-sm text-gray-300">
              <span className="text-red-400">OutOfMemory on Driver</span> - This happens when you call 
              <code className="bg-gray-800 px-1 mx-1 rounded">.collect()</code> on a large DataFrame. 
              All data gets sent to the Driver's memory! Use 
              <code className="bg-gray-800 px-1 mx-1 rounded">.take(n)</code> or 
              <code className="bg-gray-800 px-1 mx-1 rounded">.write</code> instead.
            </p>
          </div>
        </div>
      </EducationSection>

      {/* Executors - The Workers */}
      <EducationSection title="⚙️ Executors - The Actual Workers" icon={Cpu} color="text-blue-400">
        <div className="mt-4 space-y-4">
          <div className="bg-blue-500/10 border border-blue-500/30 rounded-lg p-4">
            <h4 className="text-blue-400 font-medium mb-2">What are Executors?</h4>
            <p className="text-gray-300 text-sm">
              <span className="text-blue-400 font-semibold">Executors</span> are JVM processes that run 
              on <span className="text-green-400">Worker nodes</span>. Think of them as 
              <span className="text-white font-medium"> factory workers</span> - each one has their own 
              workspace (memory), can work on multiple tasks simultaneously (threads), and follows 
              instructions from the Driver.
            </p>
          </div>

          <div className="bg-gray-900/50 rounded-lg p-4">
            <h5 className="text-white font-medium mb-3">Executor vs Worker - What's the Difference?</h5>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
              <div className="border border-green-500/30 rounded-lg p-3">
                <p className="text-green-400 font-medium mb-2">Worker Node (Physical)</p>
                <ul className="text-gray-400 space-y-1">
                  <li>• A physical or virtual machine</li>
                  <li>• Has CPU cores, RAM, disk</li>
                  <li>• Can host MULTIPLE executors</li>
                  <li>• Example: spark-worker-1, spark-worker-2</li>
                </ul>
              </div>
              <div className="border border-blue-500/30 rounded-lg p-3">
                <p className="text-blue-400 font-medium mb-2">Executor (Process)</p>
                <ul className="text-gray-400 space-y-1">
                  <li>• A JVM process on a Worker</li>
                  <li>• Gets assigned CPU cores & memory</li>
                  <li>• Runs tasks in parallel threads</li>
                  <li>• One per app per worker (usually)</li>
                </ul>
              </div>
            </div>
          </div>

          <div className="bg-gray-900/50 rounded-lg p-3">
            <h5 className="text-white font-medium mb-2">What Executors Actually Do</h5>
            <div className="flex flex-wrap gap-2 text-xs">
              <span className="bg-blue-500/20 text-blue-400 px-2 py-1 rounded">Run Tasks</span>
              <span className="bg-purple-500/20 text-purple-400 px-2 py-1 rounded">Cache Data</span>
              <span className="bg-green-500/20 text-green-400 px-2 py-1 rounded">Shuffle Data</span>
              <span className="bg-orange-500/20 text-orange-400 px-2 py-1 rounded">Report Status</span>
              <span className="bg-yellow-500/20 text-yellow-400 px-2 py-1 rounded">Store RDD Partitions</span>
            </div>
            <p className="text-gray-400 text-sm mt-3">
              Each executor in this cluster has <span className="text-white">2 cores</span> and 
              <span className="text-white"> 1GB memory</span>. With 3 workers, you have 
              <span className="text-green-400"> 6 total cores</span> for parallel processing.
            </p>
          </div>
        </div>
      </EducationSection>

      {/* Tasks - The Atomic Units */}
      <EducationSection title="📦 Tasks - The Smallest Unit of Work" icon={Box} color="text-green-400">
        <div className="mt-4 space-y-4">
          <div className="bg-green-500/10 border border-green-500/30 rounded-lg p-4">
            <h4 className="text-green-400 font-medium mb-2">What is a Task?</h4>
            <p className="text-gray-300 text-sm">
              A <span className="text-green-400 font-semibold">Task</span> is the smallest unit of work in Spark.
              <span className="text-white font-medium"> One task processes one partition of data</span>.
              If your DataFrame has 100 partitions, Spark creates 100 tasks.
            </p>
          </div>

          <div className="bg-gray-900/50 rounded-lg p-4">
            <h5 className="text-white font-medium mb-3">The Task = Partition Connection</h5>
            <div className="flex items-center justify-center py-4">
              <div className="flex items-center space-x-4 text-sm">
                <div className="bg-cyan-500/20 border border-cyan-500/30 rounded-lg p-3 text-center">
                  <Grid3X3 className="h-6 w-6 text-cyan-400 mx-auto mb-1" />
                  <p className="text-cyan-400">Partition 1</p>
                  <p className="text-gray-500 text-xs">~1000 rows</p>
                </div>
                <ArrowRight className="h-5 w-5 text-gray-500" />
                <div className="bg-green-500/20 border border-green-500/30 rounded-lg p-3 text-center">
                  <Box className="h-6 w-6 text-green-400 mx-auto mb-1" />
                  <p className="text-green-400">Task 1</p>
                  <p className="text-gray-500 text-xs">Processes it</p>
                </div>
                <ArrowRight className="h-5 w-5 text-gray-500" />
                <div className="bg-blue-500/20 border border-blue-500/30 rounded-lg p-3 text-center">
                  <Cpu className="h-6 w-6 text-blue-400 mx-auto mb-1" />
                  <p className="text-blue-400">Executor Core</p>
                  <p className="text-gray-500 text-xs">Runs the task</p>
                </div>
              </div>
            </div>
            <p className="text-gray-400 text-sm text-center mt-2">
              This is why <span className="text-white"># of partitions</span> should roughly match 
              <span className="text-white"> 2-4x the number of cores</span> in your cluster for optimal parallelism.
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="bg-gray-900/50 rounded-lg p-3">
              <h5 className="text-white font-medium mb-2">Task Types</h5>
              <ul className="text-sm text-gray-400 space-y-1">
                <li><span className="text-green-400">ShuffleMapTask:</span> Writes shuffle output</li>
                <li><span className="text-blue-400">ResultTask:</span> Sends results to driver</li>
              </ul>
            </div>
            <div className="bg-gray-900/50 rounded-lg p-3">
              <h5 className="text-white font-medium mb-2">Task Execution</h5>
              <ul className="text-sm text-gray-400 space-y-1">
                <li>• Tasks run in executor thread pools</li>
                <li>• Failed tasks are retried (up to 4x)</li>
                <li>• Tasks report progress to driver</li>
              </ul>
            </div>
          </div>
        </div>
      </EducationSection>

      {/* Stages - The Shuffle Boundaries */}
      <EducationSection title="📊 Stages - Shuffle Boundaries" icon={Layers} color="text-purple-400">
        <div className="mt-4 space-y-4">
          <div className="bg-purple-500/10 border border-purple-500/30 rounded-lg p-4">
            <h4 className="text-purple-400 font-medium mb-2">What is a Stage?</h4>
            <p className="text-gray-300 text-sm">
              A <span className="text-purple-400 font-semibold">Stage</span> is a group of tasks that can 
              run <span className="text-white font-medium">in parallel without shuffling data</span>.
              Stages are separated by <span className="text-yellow-400">shuffle operations</span> (like groupBy, join, repartition).
            </p>
          </div>

          <div className="bg-gray-900/50 rounded-lg p-4">
            <h5 className="text-white font-medium mb-3">Why Stages Exist</h5>
            <div className="space-y-3 text-sm">
              <div className="flex items-start">
                <span className="text-green-400 font-mono mr-3">1.</span>
                <div>
                  <span className="text-green-400 font-medium">Narrow Transformations</span>
                  <span className="text-gray-400"> (map, filter, select) - Same partition, same stage</span>
                  <p className="text-gray-500 text-xs mt-1">Data stays on the same node - FAST!</p>
                </div>
              </div>
              <div className="flex items-start">
                <span className="text-yellow-400 font-mono mr-3">2.</span>
                <div>
                  <span className="text-yellow-400 font-medium">Wide Transformations</span>
                  <span className="text-gray-400"> (groupBy, join, reduceByKey) - New stage!</span>
                  <p className="text-gray-500 text-xs mt-1">Data must shuffle across network - SLOW!</p>
                </div>
              </div>
            </div>
          </div>

          <div className="bg-gray-900/50 rounded-lg p-3">
            <h5 className="text-white font-medium mb-2">Example: How a Job Creates Stages</h5>
            <pre className="text-xs bg-gray-800 p-3 rounded overflow-x-auto">
              <code className="text-gray-300">{`df.filter(col("age") > 18)      # Stage 1 (narrow - no shuffle)
  .select("name", "city")        # Still Stage 1
  .groupBy("city")               # SHUFFLE! Stage 2 begins
  .count()                       # Stage 2 (processes shuffled data)
  .orderBy("count")              # SHUFFLE! Stage 3 begins
  .show()                        # Action triggers execution`}</code>
            </pre>
            <p className="text-gray-400 text-xs mt-2">
              This job has <span className="text-purple-400 font-medium">3 stages</span> because of 
              2 wide transformations (groupBy and orderBy).
            </p>
          </div>
        </div>
      </EducationSection>

      {/* Shuffle - The Expensive Operation */}
      <EducationSection title="🔀 Shuffle - The Most Expensive Operation" icon={Shuffle} color="text-yellow-400">
        <div className="mt-4 space-y-4">
          <div className="bg-yellow-500/10 border border-yellow-500/30 rounded-lg p-4">
            <h4 className="text-yellow-400 font-medium mb-2">What is Shuffle?</h4>
            <p className="text-gray-300 text-sm">
              <span className="text-yellow-400 font-semibold">Shuffle</span> is when Spark 
              <span className="text-white font-medium"> redistributes data across partitions</span>.
              It involves writing data to disk, transferring over the network, and reading into new partitions.
              <span className="text-red-400 font-medium"> This is the SLOWEST part of any Spark job!</span>
            </p>
          </div>

          <div className="bg-gray-900/50 rounded-lg p-4">
            <h5 className="text-white font-medium mb-3">What Happens During Shuffle</h5>
            <div className="grid grid-cols-3 gap-4 text-center text-sm py-4">
              <div>
                <div className="bg-blue-500/20 border border-blue-500/30 rounded-lg p-3 mb-2">
                  <HardDrive className="h-6 w-6 text-blue-400 mx-auto" />
                </div>
                <p className="text-blue-400 font-medium">1. Map Side</p>
                <p className="text-gray-500 text-xs">Each task writes shuffle files to local disk</p>
              </div>
              <div>
                <div className="bg-yellow-500/20 border border-yellow-500/30 rounded-lg p-3 mb-2">
                  <Network className="h-6 w-6 text-yellow-400 mx-auto" />
                </div>
                <p className="text-yellow-400 font-medium">2. Network</p>
                <p className="text-gray-500 text-xs">Data transfers across executors (N×N)</p>
              </div>
              <div>
                <div className="bg-green-500/20 border border-green-500/30 rounded-lg p-3 mb-2">
                  <Database className="h-6 w-6 text-green-400 mx-auto" />
                </div>
                <p className="text-green-400 font-medium">3. Reduce Side</p>
                <p className="text-gray-500 text-xs">Read and aggregate shuffled data</p>
              </div>
            </div>
          </div>

          <div className="bg-red-500/10 border border-red-500/30 rounded-lg p-3">
            <h5 className="text-red-400 font-medium mb-2">📊 In the Metrics You See:</h5>
            <ul className="text-sm text-gray-400 space-y-1">
              <li><span className="text-white">Shuffle Read:</span> Data received from other executors (MB)</li>
              <li><span className="text-white">Shuffle Write:</span> Data written for other stages to read (MB)</li>
              <li><span className="text-red-400">High shuffle = slow job!</span> Optimize with fewer groupBys, broadcasting small tables</li>
            </ul>
          </div>

          <div className="bg-green-500/10 border border-green-500/30 rounded-lg p-3">
            <h5 className="text-green-400 font-medium mb-2">💡 Tips to Reduce Shuffle</h5>
            <ul className="text-sm text-gray-400 space-y-1">
              <li>• <span className="text-white">broadcast()</span> small DataFrames in joins</li>
              <li>• <span className="text-white">coalesce()</span> instead of repartition() when reducing partitions</li>
              <li>• Filter data early before groupBy operations</li>
              <li>• Use <span className="text-white">reduceByKey</span> instead of groupByKey</li>
            </ul>
          </div>
        </div>
      </EducationSection>

      {/* Partitions - How Data is Split */}
      <EducationSection title="📁 Partitions - How Your Data is Split" icon={Grid3X3} color="text-cyan-400">
        <div className="mt-4 space-y-4">
          <div className="bg-cyan-500/10 border border-cyan-500/30 rounded-lg p-4">
            <h4 className="text-cyan-400 font-medium mb-2">What is a Partition?</h4>
            <p className="text-gray-300 text-sm">
              A <span className="text-cyan-400 font-semibold">Partition</span> is a 
              <span className="text-white font-medium"> chunk of your data</span> that lives on one node.
              Partitions enable parallelism - each partition can be processed by a different task simultaneously.
              <span className="text-yellow-400"> Partitions are NOT about filtering or organizing data semantically</span> - 
              they're about splitting data for parallel processing.
            </p>
          </div>

          <div className="bg-gray-900/50 rounded-lg p-4">
            <h5 className="text-white font-medium mb-3">Partition ≠ Filter</h5>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
              <div className="border border-red-500/30 rounded-lg p-3">
                <p className="text-red-400 font-medium mb-2">❌ Partition is NOT:</p>
                <ul className="text-gray-400 space-y-1">
                  <li>• "Only rows where city=NYC"</li>
                  <li>• "Only the filtered data"</li>
                  <li>• "Organized/sorted data"</li>
                </ul>
              </div>
              <div className="border border-green-500/30 rounded-lg p-3">
                <p className="text-green-400 font-medium mb-2">✅ Partition IS:</p>
                <ul className="text-gray-400 space-y-1">
                  <li>• A chunk of rows (any rows)</li>
                  <li>• Stored on one executor</li>
                  <li>• Processed by one task</li>
                </ul>
              </div>
            </div>
          </div>

          <div className="bg-gray-900/50 rounded-lg p-3">
            <h5 className="text-white font-medium mb-2">How Many Partitions?</h5>
            <div className="space-y-2 text-sm text-gray-400">
              <p><span className="text-white">Reading files:</span> Usually 1 partition per 128MB block</p>
              <p><span className="text-white">After shuffle:</span> spark.sql.shuffle.partitions (default: 200)</p>
              <p><span className="text-white">Recommended:</span> 2-4 partitions per CPU core</p>
              <div className="mt-3 bg-gray-800 p-2 rounded">
                <code className="text-xs text-gray-300">
                  {`# Check partitions: df.rdd.getNumPartitions()
# Change partitions: df.repartition(100) or df.coalesce(10)`}
                </code>
              </div>
            </div>
          </div>

          <div className="bg-purple-500/10 border border-purple-500/30 rounded-lg p-3">
            <h5 className="text-purple-400 font-medium mb-2">🎯 Partition By (File Storage)</h5>
            <p className="text-sm text-gray-400">
              <span className="text-white">df.write.partitionBy("date")</span> is different! 
              This creates <span className="text-purple-400">physical folders</span> on disk organized 
              by column values. It's for <span className="text-white">storage optimization</span>, 
              not in-memory parallelism. Spark can skip entire folders when filtering (partition pruning).
            </p>
          </div>
        </div>
      </EducationSection>

      {/* RDD - Resilient Distributed Dataset */}
      <EducationSection title="📦 RDD - What's Really Happening to Your Data" icon={Database} color="text-red-400">
        <div className="mt-4 space-y-4">
          <div className="bg-red-500/10 border border-red-500/30 rounded-lg p-4">
            <h4 className="text-red-400 font-medium mb-2">What is an RDD?</h4>
            <p className="text-gray-300 text-sm">
              <span className="text-red-400 font-semibold">RDD (Resilient Distributed Dataset)</span> is Spark's 
              foundational data structure. Think of it as a <span className="text-white font-medium">distributed 
              collection of objects</span> spread across your cluster. Even when you use DataFrames, 
              Spark converts them to RDDs internally for execution.
            </p>
          </div>

          <div className="bg-gray-900/50 rounded-lg p-4">
            <h5 className="text-white font-medium mb-3">What "Resilient Distributed Dataset" Means</h5>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
              <div className="border border-red-500/30 rounded-lg p-3">
                <p className="text-red-400 font-medium mb-2">Resilient</p>
                <p className="text-gray-400 text-xs">
                  Can be <span className="text-white">rebuilt</span> if a partition is lost. Spark tracks 
                  the "lineage" (chain of transformations) so it can recompute any lost data.
                </p>
              </div>
              <div className="border border-blue-500/30 rounded-lg p-3">
                <p className="text-blue-400 font-medium mb-2">Distributed</p>
                <p className="text-gray-400 text-xs">
                  Data is <span className="text-white">split across nodes</span>. Each partition lives on 
                  a different executor, enabling parallel processing.
                </p>
              </div>
              <div className="border border-green-500/30 rounded-lg p-3">
                <p className="text-green-400 font-medium mb-2">Dataset</p>
                <p className="text-gray-400 text-xs">
                  A <span className="text-white">collection of records</span>. Can be any Python object, 
                  but DataFrames are optimized for tabular data.
                </p>
              </div>
            </div>
          </div>

          <div className="bg-gray-900/50 rounded-lg p-4">
            <h5 className="text-white font-medium mb-3">📊 Visual: What Happens to Your Data</h5>
            <div className="space-y-4">
              <div className="flex items-center justify-between text-xs">
                <div className="text-center flex-1">
                  <div className="bg-blue-500/20 border border-blue-500/30 rounded-lg p-2 mb-1">
                    <Table className="h-5 w-5 text-blue-400 mx-auto" />
                  </div>
                  <p className="text-blue-400">Your CSV File</p>
                  <p className="text-gray-500">1 Million Rows</p>
                </div>
                <ArrowRight className="h-4 w-4 text-gray-500" />
                <div className="text-center flex-1">
                  <div className="bg-green-500/20 border border-green-500/30 rounded-lg p-2 mb-1">
                    <Grid3X3 className="h-5 w-5 text-green-400 mx-auto" />
                  </div>
                  <p className="text-green-400">Split into Partitions</p>
                  <p className="text-gray-500">8 chunks × 125K rows</p>
                </div>
                <ArrowRight className="h-4 w-4 text-gray-500" />
                <div className="text-center flex-1">
                  <div className="bg-purple-500/20 border border-purple-500/30 rounded-lg p-2 mb-1">
                    <Server className="h-5 w-5 text-purple-400 mx-auto" />
                  </div>
                  <p className="text-purple-400">Sent to Executors</p>
                  <p className="text-gray-500">Distributed across 3 workers</p>
                </div>
                <ArrowRight className="h-4 w-4 text-gray-500" />
                <div className="text-center flex-1">
                  <div className="bg-orange-500/20 border border-orange-500/30 rounded-lg p-2 mb-1">
                    <Cpu className="h-5 w-5 text-orange-400 mx-auto" />
                  </div>
                  <p className="text-orange-400">Processed in Parallel</p>
                  <p className="text-gray-500">8 tasks run simultaneously</p>
                </div>
              </div>
              
              <pre className="bg-gray-800 p-3 rounded text-xs overflow-x-auto">
                <code className="text-gray-300">{`# Your data journey:
df = spark.read.csv("data.csv")    # Creates RDD with 8 partitions
# Partition 0: rows 0-124,999     → Executor on Worker 1
# Partition 1: rows 125,000-249,999 → Executor on Worker 2
# Partition 2: rows 250,000-374,999 → Executor on Worker 3
# ... and so on, distributed across the cluster`}</code>
              </pre>
            </div>
          </div>

          <div className="bg-yellow-500/10 border border-yellow-500/30 rounded-lg p-3">
            <h5 className="text-yellow-400 font-medium mb-2">💡 RDD vs DataFrame - Which to Use?</h5>
            <div className="grid grid-cols-2 gap-4 text-xs">
              <div>
                <p className="text-white font-medium mb-1">Use DataFrame (Recommended)</p>
                <ul className="text-gray-400 space-y-1">
                  <li>• Tabular data with columns</li>
                  <li>• SQL-like operations</li>
                  <li>• Catalyst optimizer benefits</li>
                  <li>• 10-100x faster than RDD</li>
                </ul>
              </div>
              <div>
                <p className="text-white font-medium mb-1">Use RDD When</p>
                <ul className="text-gray-400 space-y-1">
                  <li>• Complex custom objects</li>
                  <li>• Fine-grained control needed</li>
                  <li>• Legacy code migration</li>
                  <li>• Unstructured data</li>
                </ul>
              </div>
            </div>
          </div>
        </div>
      </EducationSection>

      {/* Map Function Deep Dive */}
      <EducationSection title="🔧 Map & Transformations - What Computations Actually Happen" icon={Code} color="text-green-400">
        <div className="mt-4 space-y-4">
          <div className="bg-green-500/10 border border-green-500/30 rounded-lg p-4">
            <h4 className="text-green-400 font-medium mb-2">What is the Map Function?</h4>
            <p className="text-gray-300 text-sm">
              <span className="text-green-400 font-semibold">map()</span> applies a function to 
              <span className="text-white font-medium"> every single row</span> in your dataset.
              It's the most fundamental transformation - each input element produces exactly one output element.
            </p>
          </div>

          <div className="bg-gray-900/50 rounded-lg p-4">
            <h5 className="text-white font-medium mb-3">🔍 Step-by-Step: What Actually Happens</h5>
            <pre className="bg-gray-800 p-3 rounded text-xs overflow-x-auto mb-3">
              <code className="text-gray-300">{`# Your code:
df.select(col("price") * 1.1)  # Add 10% to every price

# What Spark actually does on EACH executor:
# 1. Read partition from memory/disk
# 2. For EACH row in partition:
#    - Extract 'price' column value
#    - Multiply by 1.1
#    - Store result in new partition
# 3. Keep result in memory for next operation`}</code>
            </pre>
            <div className="flex items-center justify-center py-3">
              <div className="flex items-center space-x-3 text-xs">
                <div className="bg-blue-500/20 border border-blue-500/30 rounded p-2 text-center">
                  <p className="text-blue-400 font-mono">Row 1</p>
                  <p className="text-white">price: 100</p>
                </div>
                <ArrowRight className="h-4 w-4 text-green-400" />
                <div className="bg-green-500/20 border border-green-500/30 rounded p-2 text-center">
                  <p className="text-green-400 font-mono">× 1.1</p>
                </div>
                <ArrowRight className="h-4 w-4 text-green-400" />
                <div className="bg-orange-500/20 border border-orange-500/30 rounded p-2 text-center">
                  <p className="text-orange-400 font-mono">Row 1</p>
                  <p className="text-white">price: 110</p>
                </div>
              </div>
            </div>
          </div>

          <div className="bg-gray-900/50 rounded-lg p-4">
            <h5 className="text-white font-medium mb-3">Common Transformations Explained</h5>
            <div className="space-y-3 text-sm">
              <div className="flex items-start border-b border-gray-700 pb-3">
                <code className="text-green-400 font-mono w-32 flex-shrink-0">.map()</code>
                <div className="flex-1">
                  <p className="text-gray-300">Transform each row → one output per input</p>
                  <p className="text-gray-500 text-xs">Example: Convert temperatures from C to F</p>
                </div>
                <span className="text-green-400 text-xs">Narrow ✓</span>
              </div>
              <div className="flex items-start border-b border-gray-700 pb-3">
                <code className="text-blue-400 font-mono w-32 flex-shrink-0">.filter()</code>
                <div className="flex-1">
                  <p className="text-gray-300">Keep rows matching condition</p>
                  <p className="text-gray-500 text-xs">Example: Keep only rows where age &gt; 18</p>
                </div>
                <span className="text-green-400 text-xs">Narrow ✓</span>
              </div>
              <div className="flex items-start border-b border-gray-700 pb-3">
                <code className="text-purple-400 font-mono w-32 flex-shrink-0">.flatMap()</code>
                <div className="flex-1">
                  <p className="text-gray-300">One input → zero or more outputs</p>
                  <p className="text-gray-500 text-xs">Example: Split sentences into words</p>
                </div>
                <span className="text-green-400 text-xs">Narrow ✓</span>
              </div>
              <div className="flex items-start border-b border-gray-700 pb-3">
                <code className="text-yellow-400 font-mono w-32 flex-shrink-0">.groupBy()</code>
                <div className="flex-1">
                  <p className="text-gray-300">Group rows by key (requires shuffle!)</p>
                  <p className="text-gray-500 text-xs">Example: Group sales by region</p>
                </div>
                <span className="text-yellow-400 text-xs">Wide ⚠️</span>
              </div>
              <div className="flex items-start">
                <code className="text-orange-400 font-mono w-32 flex-shrink-0">.reduceByKey()</code>
                <div className="flex-1">
                  <p className="text-gray-300">Combine values with same key</p>
                  <p className="text-gray-500 text-xs">Example: Sum quantities per product</p>
                </div>
                <span className="text-yellow-400 text-xs">Wide ⚠️</span>
              </div>
            </div>
          </div>

          <div className="bg-gray-900/50 rounded-lg p-3">
            <h5 className="text-white font-medium mb-2">🧮 The Actual Computation</h5>
            <pre className="bg-gray-800 p-3 rounded text-xs overflow-x-auto">
              <code className="text-gray-300">{`# What your code looks like:
df.filter(col("age") > 18).select(col("name").upper())

# What happens on each executor (pseudocode):
for row in partition:
    if row["age"] > 18:           # filter: evaluate condition
        new_row = Row(
            name = row["name"].upper()  # select: apply function
        )
        output_partition.append(new_row)

# This runs IN PARALLEL on all 6 cores across 3 workers!`}</code>
            </pre>
          </div>
        </div>
      </EducationSection>

      {/* Task Scheduling */}
      <EducationSection title="📋 Task Scheduling - How Work Gets Distributed" icon={Clock} color="text-blue-400">
        <div className="mt-4 space-y-4">
          <div className="bg-blue-500/10 border border-blue-500/30 rounded-lg p-4">
            <h4 className="text-blue-400 font-medium mb-2">How Does Spark Schedule Tasks?</h4>
            <p className="text-gray-300 text-sm">
              The <span className="text-yellow-400">Driver</span> acts as a 
              <span className="text-white font-medium"> task scheduler</span>. It maintains a queue of 
              tasks and assigns them to available executor cores using a <span className="text-blue-400">FIFO 
              or Fair scheduler</span>.
            </p>
          </div>

          <div className="bg-gray-900/50 rounded-lg p-4">
            <h5 className="text-white font-medium mb-3">📊 Task Scheduling Flow</h5>
            <div className="space-y-3">
              <div className="flex items-start">
                <span className="bg-blue-500/20 text-blue-400 px-2 py-1 rounded text-xs font-mono mr-3">1</span>
                <div>
                  <p className="text-white font-medium">DAG Scheduler splits job into stages</p>
                  <p className="text-gray-400 text-xs">Based on shuffle boundaries (wide transformations)</p>
                </div>
              </div>
              <div className="flex items-start">
                <span className="bg-blue-500/20 text-blue-400 px-2 py-1 rounded text-xs font-mono mr-3">2</span>
                <div>
                  <p className="text-white font-medium">Task Scheduler creates tasks for each partition</p>
                  <p className="text-gray-400 text-xs">100 partitions = 100 tasks to schedule</p>
                </div>
              </div>
              <div className="flex items-start">
                <span className="bg-blue-500/20 text-blue-400 px-2 py-1 rounded text-xs font-mono mr-3">3</span>
                <div>
                  <p className="text-white font-medium">Tasks placed in scheduling queue</p>
                  <p className="text-gray-400 text-xs">FIFO (default) or Fair scheduling</p>
                </div>
              </div>
              <div className="flex items-start">
                <span className="bg-blue-500/20 text-blue-400 px-2 py-1 rounded text-xs font-mono mr-3">4</span>
                <div>
                  <p className="text-white font-medium">Driver assigns tasks to free executor cores</p>
                  <p className="text-gray-400 text-xs">Prefers data locality (run where data lives)</p>
                </div>
              </div>
              <div className="flex items-start">
                <span className="bg-green-500/20 text-green-400 px-2 py-1 rounded text-xs font-mono mr-3">5</span>
                <div>
                  <p className="text-white font-medium">Executor runs task and reports completion</p>
                  <p className="text-gray-400 text-xs">Driver schedules next task to that core</p>
                </div>
              </div>
            </div>
          </div>

          <div className="bg-gray-900/50 rounded-lg p-4">
            <h5 className="text-white font-medium mb-3">🖥️ Example: Your Cluster</h5>
            <pre className="bg-gray-800 p-3 rounded text-xs overflow-x-auto">
              <code className="text-gray-300">{`# You have: 3 workers × 2 cores = 6 parallel slots
# Your job has: 24 tasks (24 partitions)

Wave 1: Tasks 1-6 run in parallel (all cores busy)
Wave 2: Tasks 7-12 run when first wave completes
Wave 3: Tasks 13-18 run
Wave 4: Tasks 19-24 run

Total: 4 waves to complete 24 tasks with 6 cores
Time ≈ 4 × (slowest task duration)`}</code>
            </pre>
          </div>

          <div className="bg-yellow-500/10 border border-yellow-500/30 rounded-lg p-3">
            <h5 className="text-yellow-400 font-medium mb-2">⚡ Data Locality Levels</h5>
            <div className="space-y-1 text-xs">
              <div className="flex items-center">
                <span className="text-green-400 w-24">PROCESS_LOCAL</span>
                <span className="text-gray-400">Data in same executor memory - fastest!</span>
              </div>
              <div className="flex items-center">
                <span className="text-blue-400 w-24">NODE_LOCAL</span>
                <span className="text-gray-400">Data on same node, different executor</span>
              </div>
              <div className="flex items-center">
                <span className="text-yellow-400 w-24">RACK_LOCAL</span>
                <span className="text-gray-400">Data on same rack, different node</span>
              </div>
              <div className="flex items-center">
                <span className="text-red-400 w-24">ANY</span>
                <span className="text-gray-400">Data anywhere - requires network transfer</span>
              </div>
            </div>
          </div>
        </div>
      </EducationSection>

      {/* Resource Allocation */}
      <EducationSection title="💰 Resource Allocation - Building the Execution Plan" icon={Settings} color="text-purple-400">
        <div className="mt-4 space-y-4">
          <div className="bg-purple-500/10 border border-purple-500/30 rounded-lg p-4">
            <h4 className="text-purple-400 font-medium mb-2">How Resources Get Allocated</h4>
            <p className="text-gray-300 text-sm">
              When you submit a job, the <span className="text-purple-400">Cluster Manager</span> 
              (Standalone, YARN, K8s) allocates <span className="text-white font-medium">executors</span> 
              based on your configuration. Each executor gets a fixed amount of CPU and memory.
            </p>
          </div>

          <div className="bg-gray-900/50 rounded-lg p-4">
            <h5 className="text-white font-medium mb-3">📊 Resource Allocation Flow</h5>
            <div className="grid grid-cols-4 gap-2 text-center text-xs mb-4">
              <div className="bg-yellow-500/10 rounded p-2">
                <p className="text-yellow-400 font-medium">Submit Job</p>
                <p className="text-gray-500">spark-submit</p>
              </div>
              <div className="bg-purple-500/10 rounded p-2">
                <p className="text-purple-400 font-medium">Request Resources</p>
                <p className="text-gray-500">Cluster Manager</p>
              </div>
              <div className="bg-blue-500/10 rounded p-2">
                <p className="text-blue-400 font-medium">Launch Executors</p>
                <p className="text-gray-500">On Workers</p>
              </div>
              <div className="bg-green-500/10 rounded p-2">
                <p className="text-green-400 font-medium">Start Processing</p>
                <p className="text-gray-500">Tasks Run</p>
              </div>
            </div>
            
            <pre className="bg-gray-800 p-3 rounded text-xs overflow-x-auto">
              <code className="text-gray-300">{`# Your resource configuration:
spark-submit \\
  --executor-memory 1g \\     # Each executor gets 1GB RAM
  --executor-cores 2 \\        # Each executor gets 2 CPU cores
  --num-executors 3 \\         # Request 3 executors
  my_job.py

# Result: 3 executors × 2 cores = 6 parallel task slots
#         3 executors × 1GB = 3GB total executor memory`}</code>
            </pre>
          </div>

          <div className="bg-gray-900/50 rounded-lg p-4">
            <h5 className="text-white font-medium mb-3">🏗️ Building the Execution Plan (DAG)</h5>
            <div className="space-y-2 text-sm">
              <p className="text-gray-400">When you write transformations, Spark builds a <span className="text-purple-400">DAG (Directed Acyclic Graph)</span>:</p>
              <pre className="bg-gray-800 p-3 rounded text-xs overflow-x-auto">
                <code className="text-gray-300">{`# Your code:
result = (df
    .filter(col("country") == "USA")    # Node 1: Filter
    .select("name", "sales")            # Node 2: Project
    .groupBy("name")                    # Node 3: Shuffle
    .sum("sales")                       # Node 4: Aggregate
)

# DAG Created:
#   [Read] → [Filter] → [Project] → [Shuffle] → [Aggregate]
#   └────── Stage 1 ──────────────┘  └─── Stage 2 ────────┘`}</code>
              </pre>
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="bg-gray-900/50 rounded-lg p-3">
              <h5 className="text-white font-medium mb-2">Catalyst Optimizer</h5>
              <ul className="text-xs text-gray-400 space-y-1">
                <li>• <span className="text-white">Predicate Pushdown:</span> Filter early</li>
                <li>• <span className="text-white">Column Pruning:</span> Read only needed columns</li>
                <li>• <span className="text-white">Constant Folding:</span> Pre-compute expressions</li>
                <li>• <span className="text-white">Join Reordering:</span> Optimize join sequence</li>
              </ul>
            </div>
            <div className="bg-gray-900/50 rounded-lg p-3">
              <h5 className="text-white font-medium mb-2">Tungsten Engine</h5>
              <ul className="text-xs text-gray-400 space-y-1">
                <li>• <span className="text-white">Memory Management:</span> Off-heap storage</li>
                <li>• <span className="text-white">Code Generation:</span> JIT-compiled bytecode</li>
                <li>• <span className="text-white">Cache-aware:</span> CPU cache optimization</li>
                <li>• <span className="text-white">Whole-stage codegen:</span> Fused operators</li>
              </ul>
            </div>
          </div>
        </div>
      </EducationSection>

      {/* Aggregation and Final Output */}
      <EducationSection title="📤 Aggregation & Final Output - Getting Your Results" icon={FileText} color="text-orange-400">
        <div className="mt-4 space-y-4">
          <div className="bg-orange-500/10 border border-orange-500/30 rounded-lg p-4">
            <h4 className="text-orange-400 font-medium mb-2">How Results Are Aggregated</h4>
            <p className="text-gray-300 text-sm">
              After executors process their partitions, results must be 
              <span className="text-white font-medium"> combined (aggregated)</span>. This happens in 
              two phases: <span className="text-green-400">local aggregation</span> on each executor, 
              then <span className="text-yellow-400">global aggregation</span> (often requiring shuffle).
            </p>
          </div>

          <div className="bg-gray-900/50 rounded-lg p-4">
            <h5 className="text-white font-medium mb-3">📊 Aggregation Example: .count()</h5>
            <div className="space-y-3">
              <pre className="bg-gray-800 p-3 rounded text-xs overflow-x-auto">
                <code className="text-gray-300">{`# You call:
total = df.count()

# What happens:
# Phase 1: Each executor counts its partitions
#   Executor 1: partition 0 has 5000 rows, partition 1 has 4800 rows
#   Executor 2: partition 2 has 5100 rows, partition 3 has 5050 rows
#   Executor 3: partition 4 has 4900 rows, partition 5 has 5150 rows

# Phase 2: Partial counts sent to driver
#   Executor 1 sends: 9800
#   Executor 2 sends: 10150
#   Executor 3 sends: 10050

# Phase 3: Driver sums them
#   Total = 9800 + 10150 + 10050 = 30000`}</code>
              </pre>
            </div>
          </div>

          <div className="bg-gray-900/50 rounded-lg p-4">
            <h5 className="text-white font-medium mb-3">🔄 Two-Phase Aggregation (groupBy)</h5>
            <div className="flex items-center justify-between text-xs py-4">
              <div className="text-center">
                <div className="bg-blue-500/20 border border-blue-500/30 rounded p-2 mb-1">
                  <p className="text-blue-400 font-mono">Partition 1</p>
                  <p className="text-gray-500">NYC: 100, LA: 50</p>
                </div>
              </div>
              <div className="text-center">
                <div className="bg-blue-500/20 border border-blue-500/30 rounded p-2 mb-1">
                  <p className="text-blue-400 font-mono">Partition 2</p>
                  <p className="text-gray-500">NYC: 80, LA: 70</p>
                </div>
              </div>
              <ArrowRight className="h-4 w-4 text-yellow-400" />
              <div className="text-center">
                <div className="bg-yellow-500/20 border border-yellow-500/30 rounded p-2 mb-1">
                  <p className="text-yellow-400 font-mono">Shuffle</p>
                  <p className="text-gray-500">By City</p>
                </div>
              </div>
              <ArrowRight className="h-4 w-4 text-yellow-400" />
              <div className="text-center">
                <div className="bg-green-500/20 border border-green-500/30 rounded p-2 mb-1">
                  <p className="text-green-400 font-mono">Final</p>
                  <p className="text-gray-500">NYC: 180, LA: 120</p>
                </div>
              </div>
            </div>
          </div>

          <div className="bg-gray-900/50 rounded-lg p-4">
            <h5 className="text-white font-medium mb-3">📤 Final Output Options</h5>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
              <div className="border border-blue-500/30 rounded-lg p-3">
                <p className="text-blue-400 font-medium mb-2">To Driver Memory</p>
                <pre className="bg-gray-800 p-2 rounded text-xs">
                  <code>{`.collect()  # All data to driver (dangerous!)
.take(10)   # First 10 rows only (safe)
.first()    # First row only (safe)
.count()    # Just a number (safe)`}</code>
                </pre>
              </div>
              <div className="border border-green-500/30 rounded-lg p-3">
                <p className="text-green-400 font-medium mb-2">To Storage</p>
                <pre className="bg-gray-800 p-2 rounded text-xs">
                  <code>{`.write.parquet("path")   # Columnar format
.write.csv("path")       # Text format
.write.jdbc(url, table)  # Database
.write.json("path")      # JSON files`}</code>
                </pre>
              </div>
            </div>
          </div>

          <div className="bg-red-500/10 border border-red-500/30 rounded-lg p-3">
            <h5 className="text-red-400 font-medium mb-2">⚠️ Common Mistakes with Results</h5>
            <ul className="text-sm text-gray-400 space-y-1">
              <li>• <span className="text-white">.collect() on large data</span> → Driver OOM crash</li>
              <li>• <span className="text-white">Calling actions in a loop</span> → Multiple job submissions</li>
              <li>• <span className="text-white">Not caching before multiple actions</span> → Recomputation</li>
              <li>• <span className="text-white">Printing inside transformations</span> → Runs on executors, not visible</li>
            </ul>
          </div>
        </div>
      </EducationSection>

      {/* How It All Works Together */}
      <EducationSection title="🔄 How It All Works Together" icon={GitBranch} color="text-spark-orange">
        <div className="mt-4 space-y-4">
          <div className="bg-spark-orange/10 border border-spark-orange/30 rounded-lg p-4">
            <h4 className="text-spark-orange font-medium mb-3">The Complete Flow of a Spark Job</h4>
            <div className="space-y-4">
              <div className="flex items-start">
                <span className="bg-yellow-500/20 text-yellow-400 px-2 py-1 rounded text-xs font-mono mr-3">1</span>
                <div>
                  <p className="text-white font-medium">You write PySpark code</p>
                  <p className="text-gray-400 text-sm">df.filter(...).groupBy(...).count().show()</p>
                </div>
              </div>
              <div className="flex items-start">
                <span className="bg-yellow-500/20 text-yellow-400 px-2 py-1 rounded text-xs font-mono mr-3">2</span>
                <div>
                  <p className="text-white font-medium">Driver creates a DAG (Directed Acyclic Graph)</p>
                  <p className="text-gray-400 text-sm">Logical plan of operations, optimized by Catalyst</p>
                </div>
              </div>
              <div className="flex items-start">
                <span className="bg-yellow-500/20 text-yellow-400 px-2 py-1 rounded text-xs font-mono mr-3">3</span>
                <div>
                  <p className="text-white font-medium">DAG is split into Stages</p>
                  <p className="text-gray-400 text-sm">Each shuffle boundary creates a new stage</p>
                </div>
              </div>
              <div className="flex items-start">
                <span className="bg-yellow-500/20 text-yellow-400 px-2 py-1 rounded text-xs font-mono mr-3">4</span>
                <div>
                  <p className="text-white font-medium">Each Stage is split into Tasks</p>
                  <p className="text-gray-400 text-sm">One task per partition (e.g., 100 partitions = 100 tasks)</p>
                </div>
              </div>
              <div className="flex items-start">
                <span className="bg-yellow-500/20 text-yellow-400 px-2 py-1 rounded text-xs font-mono mr-3">5</span>
                <div>
                  <p className="text-white font-medium">Tasks are scheduled to Executors</p>
                  <p className="text-gray-400 text-sm">Driver sends tasks to available executor cores</p>
                </div>
              </div>
              <div className="flex items-start">
                <span className="bg-yellow-500/20 text-yellow-400 px-2 py-1 rounded text-xs font-mono mr-3">6</span>
                <div>
                  <p className="text-white font-medium">Executors process data and shuffle if needed</p>
                  <p className="text-gray-400 text-sm">Results flow back to driver or to next stage</p>
                </div>
              </div>
              <div className="flex items-start">
                <span className="bg-green-500/20 text-green-400 px-2 py-1 rounded text-xs font-mono mr-3">✓</span>
                <div>
                  <p className="text-white font-medium">Final results returned to Driver</p>
                  <p className="text-gray-400 text-sm">Or written to storage (Parquet, database, etc.)</p>
                </div>
              </div>
            </div>
          </div>

          <div className="bg-gray-900/50 rounded-lg p-3">
            <h5 className="text-white font-medium mb-2">🖥️ In This Dashboard Cluster</h5>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3 text-center text-sm">
              <div className="bg-yellow-500/10 rounded p-2">
                <p className="text-yellow-400 font-medium">Driver</p>
                <p className="text-gray-400 text-xs">spark-master</p>
              </div>
              <div className="bg-green-500/10 rounded p-2">
                <p className="text-green-400 font-medium">Workers</p>
                <p className="text-gray-400 text-xs">3 nodes</p>
              </div>
              <div className="bg-blue-500/10 rounded p-2">
                <p className="text-blue-400 font-medium">Cores</p>
                <p className="text-gray-400 text-xs">6 total</p>
              </div>
              <div className="bg-purple-500/10 rounded p-2">
                <p className="text-purple-400 font-medium">Memory</p>
                <p className="text-gray-400 text-xs">3 GB total</p>
              </div>
            </div>
          </div>
        </div>
      </EducationSection>
    </div>
  )
}
