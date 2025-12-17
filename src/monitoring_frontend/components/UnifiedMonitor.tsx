import { useState, useEffect, useCallback } from 'react'
import useSWR from 'swr'
import { 
  Activity, Server, Cpu, Database, Layers, Shuffle, Grid3X3, Monitor,
  Zap, Network, Box, GitBranch, HardDrive, ChevronDown, ChevronRight,
  Info, RefreshCw, Play, Clock, CheckCircle, XCircle, Loader2, BarChart3,
  ArrowRight, ArrowDown, Settings, FileText, Table, Filter, List
} from 'lucide-react'
import axios from 'axios'

// ═══════════════════════════════════════════════════════════════════════════════
// INTERFACES
// ═══════════════════════════════════════════════════════════════════════════════

interface ClusterStatus {
  url: string
  workers: SparkWorker[]
  aliveworkers: number
  cores: number
  coresused: number
  memory: number
  memoryused: number
  activeapps?: any[]
  completedapps?: any[]
  status: string
}

interface SparkWorker {
  id: string
  host: string
  port: number
  webuiaddress: string
  cores: number
  coresused: number
  coresfree: number
  memory: number
  memoryused: number
  memoryfree: number
  state: string
  lastheartbeat: number
}

interface SparkApplication {
  id: string
  name: string
  attempts?: { completed: boolean; startTime: string; endTime?: string }[]
}

interface SparkJob {
  jobId: number
  name: string
  status: string
  numTasks: number
  numCompletedTasks: number
  numActiveTasks: number
  numFailedTasks: number
  submissionTime: string
}

interface SparkStage {
  stageId: number
  attemptId: number
  name: string
  status: string
  numTasks: number
  numCompleteTasks: number
  numActiveTasks: number
  numFailedTasks: number
  inputBytes: number
  outputBytes: number
  shuffleReadBytes: number
  shuffleWriteBytes: number
  executorRunTime: number
}

interface SparkExecutor {
  id: string
  hostPort: string
  isActive: boolean
  totalCores: number
  activeTasks: number
  completedTasks: number
  failedTasks: number
  memoryUsed: number
  maxMemory: number
  rddBlocks: number
  totalShuffleRead: number
  totalShuffleWrite: number
}

interface Props {
  refreshInterval: number | null
}

// ═══════════════════════════════════════════════════════════════════════════════
// HELPER FUNCTIONS
// ═══════════════════════════════════════════════════════════════════════════════

const fetcher = (url: string) => axios.get(url).then(res => res.data)

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i]
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`
  return `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`
}

// ═══════════════════════════════════════════════════════════════════════════════
// COLLAPSIBLE SECTION COMPONENT
// ═══════════════════════════════════════════════════════════════════════════════

interface SectionProps {
  title: string
  icon: React.ElementType
  color: string
  children: React.ReactNode
  explanation: string
  defaultExpanded?: boolean
  badge?: string
}

function MonitorSection({ title, icon: Icon, color, children, explanation, defaultExpanded = true, badge }: SectionProps) {
  const [isExpanded, setIsExpanded] = useState(defaultExpanded)
  const [showExplanation, setShowExplanation] = useState(false)
  
  return (
    <div className="bg-gray-800/50 border border-gray-700 rounded-lg overflow-hidden mb-6">
      {/* Section Header */}
      <div className="flex items-center justify-between p-4 border-b border-gray-700 bg-gray-800/30">
        <button
          onClick={() => setIsExpanded(!isExpanded)}
          className="flex items-center flex-1 text-left"
        >
          {isExpanded ? (
            <ChevronDown className="h-5 w-5 text-gray-400 mr-2" />
          ) : (
            <ChevronRight className="h-5 w-5 text-gray-400 mr-2" />
          )}
          <Icon className={`h-5 w-5 mr-3 ${color}`} />
          <span className="text-white font-medium">{title}</span>
          {badge && (
            <span className="ml-3 text-xs bg-gray-700 text-gray-300 px-2 py-0.5 rounded">
              {badge}
            </span>
          )}
        </button>
        <button
          onClick={() => setShowExplanation(!showExplanation)}
          className={`p-1.5 rounded-full transition-colors ${showExplanation ? 'bg-blue-500/20 text-blue-400' : 'text-gray-500 hover:text-gray-300'}`}
          title="Show explanation"
        >
          <Info className="h-4 w-4" />
        </button>
      </div>
      
      {/* Explanation Panel */}
      {showExplanation && (
        <div className="bg-blue-900/20 border-b border-blue-700/30 p-4">
          <div className="flex items-start">
            <Info className="h-4 w-4 text-blue-400 mr-2 mt-0.5 flex-shrink-0" />
            <p className="text-sm text-gray-300">{explanation}</p>
          </div>
        </div>
      )}
      
      {/* Section Content */}
      {isExpanded && (
        <div className="p-4">
          {children}
        </div>
      )}
    </div>
  )
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN UNIFIED MONITOR COMPONENT
// ═══════════════════════════════════════════════════════════════════════════════

export default function UnifiedMonitor({ refreshInterval }: Props) {
  // Fetch cluster status
  const { data: cluster, error: clusterError, mutate: mutateCluster } = useSWR<ClusterStatus>(
    '/api/spark/cluster',
    fetcher,
    { refreshInterval: refreshInterval || 0 }
  )
  
  // Fetch applications
  const { data: applications } = useSWR<SparkApplication[]>(
    '/api/spark/applications',
    fetcher,
    { refreshInterval: refreshInterval || 0 }
  )
  
  // State for job execution monitoring
  const [jobs, setJobs] = useState<SparkJob[]>([])
  const [stages, setStages] = useState<SparkStage[]>([])
  const [executors, setExecutors] = useState<SparkExecutor[]>([])
  const [isPolling, setIsPolling] = useState(false)
  
  // Fetch execution details
  const fetchExecutionDetails = useCallback(async () => {
    try {
      if (!applications?.length) return
      
      const appId = applications[0]?.id
      if (!appId) return
      
      const [jobsRes, stagesRes, executorsRes] = await Promise.all([
        fetch(`/api/spark/jobs?appId=${appId}`),
        fetch(`/api/spark/stages?appId=${appId}`),
        fetch(`/api/spark/executors?appId=${appId}`)
      ])
      
      if (jobsRes.ok) setJobs(await jobsRes.json())
      if (stagesRes.ok) setStages(await stagesRes.json())
      if (executorsRes.ok) setExecutors(await executorsRes.json())
    } catch (e) {
      console.error('Failed to fetch execution details:', e)
    }
  }, [applications])
  
  // Poll for execution details
  useEffect(() => {
    if (applications?.length) {
      fetchExecutionDetails()
      const interval = setInterval(fetchExecutionDetails, refreshInterval || 5000)
      return () => clearInterval(interval)
    }
  }, [applications, refreshInterval, fetchExecutionDetails])

  // Calculate metrics
  const totalTasks = jobs.reduce((sum, j) => sum + (j.numTasks || 0), 0)
  const completedTasks = jobs.reduce((sum, j) => sum + (j.numCompletedTasks || 0), 0)
  const activeTasks = jobs.reduce((sum, j) => sum + (j.numActiveTasks || 0), 0)
  const totalShuffleRead = stages.reduce((sum, s) => sum + (s.shuffleReadBytes || 0), 0)
  const totalShuffleWrite = stages.reduce((sum, s) => sum + (s.shuffleWriteBytes || 0), 0)

  if (clusterError) {
    return (
      <div className="bg-red-500/20 border border-red-500/50 rounded-lg p-6 text-center">
        <XCircle className="h-12 w-12 text-red-400 mx-auto mb-4" />
        <h3 className="text-red-300 font-medium mb-2">Failed to Connect to Spark Cluster</h3>
        <p className="text-gray-400 text-sm">Make sure the Spark cluster is running and accessible.</p>
        <button onClick={() => mutateCluster()} className="mt-4 text-spark-orange hover:underline">
          Retry Connection
        </button>
      </div>
    )
  }

  return (
    <div className="space-y-2">
      
      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {/* SECTION 1: THE DRIVER (Your Job's Brain) */}
      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      <MonitorSection
        title="🧠 The Driver - Your Job's Brain"
        icon={Monitor}
        color="text-yellow-400"
        badge="Master Node"
        explanation="The Driver is the main process running your PySpark application. It's like the project manager - it doesn't do heavy lifting itself, but plans everything, coordinates workers, and collects final results. The Driver runs on the spark-master container in this cluster."
      >
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
          {/* Driver Status Card */}
          <div className="bg-yellow-500/10 border border-yellow-500/30 rounded-lg p-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-yellow-400 font-medium">Driver Status</span>
              <span className={`px-2 py-0.5 rounded text-xs ${cluster?.status === 'ALIVE' ? 'bg-green-500/20 text-green-300' : 'bg-red-500/20 text-red-300'}`}>
                {cluster?.status || 'Unknown'}
              </span>
            </div>
            <p className="text-2xl font-bold text-white">{cluster?.url?.split('//')[1] || 'spark-master:7077'}</p>
            <p className="text-xs text-gray-400 mt-1">Spark Master URL</p>
          </div>
          
          {/* Driver Responsibilities */}
          <div className="bg-gray-900/50 rounded-lg p-4 col-span-2">
            <h4 className="text-white font-medium mb-2 text-sm">What the Driver Does:</h4>
            <div className="grid grid-cols-2 gap-2 text-xs">
              <div className="flex items-center text-gray-300">
                <Zap className="h-3 w-3 text-yellow-400 mr-2" />
                Parses your Python/PySpark code
              </div>
              <div className="flex items-center text-gray-300">
                <GitBranch className="h-3 w-3 text-yellow-400 mr-2" />
                Builds the DAG execution plan
              </div>
              <div className="flex items-center text-gray-300">
                <Layers className="h-3 w-3 text-yellow-400 mr-2" />
                Splits work into stages & tasks
              </div>
              <div className="flex items-center text-gray-300">
                <Network className="h-3 w-3 text-yellow-400 mr-2" />
                Coordinates all executors
              </div>
            </div>
          </div>
        </div>
        
        {/* Active Applications from Driver */}
        <div className="bg-gray-900/50 rounded-lg p-4">
          <h4 className="text-white font-medium mb-3 flex items-center text-sm">
            <Activity className="h-4 w-4 mr-2 text-yellow-400" />
            Applications Managed by Driver
            <span className="ml-2 text-xs text-gray-400">({applications?.length || 0} total)</span>
          </h4>
          {applications && applications.length > 0 ? (
            <div className="space-y-2">
              {applications.slice(0, 5).map((app) => (
                <div key={app.id} className="flex items-center justify-between bg-gray-800/50 rounded p-2">
                  <div className="flex items-center">
                    {app.attempts?.[0]?.completed ? (
                      <CheckCircle className="h-4 w-4 text-green-400 mr-2" />
                    ) : (
                      <Loader2 className="h-4 w-4 text-blue-400 mr-2 animate-spin" />
                    )}
                    <span className="text-white text-sm">{app.name}</span>
                    <span className="text-gray-500 text-xs ml-2">{app.id}</span>
                  </div>
                  <span className={`text-xs px-2 py-0.5 rounded ${app.attempts?.[0]?.completed ? 'bg-green-500/20 text-green-300' : 'bg-blue-500/20 text-blue-300'}`}>
                    {app.attempts?.[0]?.completed ? 'Completed' : 'Running'}
                  </span>
                </div>
              ))}
            </div>
          ) : (
            <p className="text-gray-400 text-sm">No applications submitted yet</p>
          )}
        </div>
      </MonitorSection>

      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {/* SECTION 2: WORKER NODES (The Cluster) */}
      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      <MonitorSection
        title="🖥️ Worker Nodes - The Cluster Machines"
        icon={Server}
        color="text-blue-400"
        badge={`${cluster?.aliveworkers || 0} Alive`}
        explanation="Worker Nodes are the physical/virtual machines in your cluster. Each worker can host multiple Executors. In a real cluster, these would be separate servers. In this Docker setup, each spark-worker container simulates a separate machine."
      >
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {cluster?.workers?.map((worker) => (
            <div key={worker.id} className="bg-gray-900/50 border border-gray-700 rounded-lg p-4">
              <div className="flex items-center justify-between mb-3">
                <div className="flex items-center">
                  <Server className="h-4 w-4 text-blue-400 mr-2" />
                  <span className="text-white font-medium text-sm">Worker</span>
                </div>
                <span className={`text-xs px-2 py-0.5 rounded ${worker.state === 'ALIVE' ? 'bg-green-500/20 text-green-300' : 'bg-red-500/20 text-red-300'}`}>
                  {worker.state}
                </span>
              </div>
              
              <p className="text-xs text-gray-400 mb-3 font-mono">{worker.host}:{worker.port}</p>
              
              {/* CPU Cores */}
              <div className="mb-3">
                <div className="flex justify-between text-xs mb-1">
                  <span className="text-gray-400">CPU Cores</span>
                  <span className="text-white">{worker.coresused}/{worker.cores}</span>
                </div>
                <div className="h-2 bg-gray-700 rounded-full overflow-hidden">
                  <div 
                    className="h-full bg-green-500 transition-all"
                    style={{ width: `${worker.cores > 0 ? (worker.coresused / worker.cores) * 100 : 0}%` }}
                  />
                </div>
              </div>
              
              {/* Memory */}
              <div>
                <div className="flex justify-between text-xs mb-1">
                  <span className="text-gray-400">Memory</span>
                  <span className="text-white">{formatBytes(worker.memoryused * 1024 * 1024)}/{formatBytes(worker.memory * 1024 * 1024)}</span>
                </div>
                <div className="h-2 bg-gray-700 rounded-full overflow-hidden">
                  <div 
                    className="h-full bg-purple-500 transition-all"
                    style={{ width: `${worker.memory > 0 ? (worker.memoryused / worker.memory) * 100 : 0}%` }}
                  />
                </div>
              </div>
              
              <p className="text-xs text-gray-500 mt-2">
                Last heartbeat: {new Date(worker.lastheartbeat).toLocaleTimeString()}
              </p>
            </div>
          ))}
        </div>
        
        {/* Worker Node Explanation */}
        <div className="mt-4 bg-blue-500/10 border border-blue-500/30 rounded-lg p-3">
          <p className="text-xs text-gray-300">
            <strong className="text-blue-300">💡 Understanding Workers:</strong> Each worker node contributes CPU cores and memory to the cluster pool. 
            The Cluster Manager (Spark Standalone in this case) tracks worker health via heartbeats and allocates resources to applications.
          </p>
        </div>
      </MonitorSection>

      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {/* SECTION 3: EXECUTORS (The Actual Workers) */}
      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      <MonitorSection
        title="⚙️ Executors - The Actual Workers"
        icon={Cpu}
        color="text-green-400"
        badge={`${executors.filter(e => e.id !== 'driver').length} Active`}
        explanation="Executors are JVM processes that run on Worker Nodes. Each executor has dedicated memory and CPU cores. They execute the actual computations (map, filter, reduce operations) on data partitions. One worker can have multiple executors."
      >
        {executors.filter(e => e.id !== 'driver').length > 0 ? (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {executors.filter(e => e.id !== 'driver').map((executor) => (
              <div key={executor.id} className="bg-gray-900/50 border border-gray-700 rounded-lg p-4">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-white font-medium text-sm">Executor {executor.id}</span>
                  <span className={`text-xs px-2 py-0.5 rounded ${executor.isActive ? 'bg-green-500/20 text-green-300' : 'bg-gray-500/20 text-gray-300'}`}>
                    {executor.isActive ? 'Active' : 'Idle'}
                  </span>
                </div>
                <p className="text-xs text-gray-400 mb-3 font-mono">{executor.hostPort}</p>
                
                <div className="grid grid-cols-3 gap-2 text-center text-xs mb-3">
                  <div className="bg-blue-500/10 rounded p-2">
                    <p className="text-blue-400 font-bold">{executor.activeTasks}</p>
                    <p className="text-gray-500">Active</p>
                  </div>
                  <div className="bg-green-500/10 rounded p-2">
                    <p className="text-green-400 font-bold">{executor.completedTasks}</p>
                    <p className="text-gray-500">Done</p>
                  </div>
                  <div className="bg-red-500/10 rounded p-2">
                    <p className="text-red-400 font-bold">{executor.failedTasks}</p>
                    <p className="text-gray-500">Failed</p>
                  </div>
                </div>
                
                {/* Memory */}
                <div className="mb-2">
                  <div className="flex justify-between text-xs mb-1">
                    <span className="text-gray-400">Memory</span>
                    <span className="text-white">{formatBytes(executor.memoryUsed)}/{formatBytes(executor.maxMemory)}</span>
                  </div>
                  <div className="h-1.5 bg-gray-700 rounded-full overflow-hidden">
                    <div 
                      className="h-full bg-purple-500"
                      style={{ width: `${executor.maxMemory > 0 ? (executor.memoryUsed / executor.maxMemory) * 100 : 0}%` }}
                    />
                  </div>
                </div>
                
                {/* RDD Blocks */}
                <div className="flex justify-between text-xs text-gray-400">
                  <span>RDD Blocks Cached:</span>
                  <span className="text-cyan-400">{executor.rddBlocks || 0}</span>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="text-center py-8 text-gray-400">
            <Cpu className="h-12 w-12 mx-auto mb-3 opacity-50" />
            <p>No executors currently active</p>
            <p className="text-xs mt-1">Executors are launched when applications run</p>
          </div>
        )}
        
        {/* Executor Explanation */}
        <div className="mt-4 bg-green-500/10 border border-green-500/30 rounded-lg p-3">
          <p className="text-xs text-gray-300">
            <strong className="text-green-300">💡 Executor Memory Layout:</strong> Each executor's memory is split into: 
            <span className="text-blue-300"> Execution Memory</span> (shuffles, joins, sorts), 
            <span className="text-purple-300"> Storage Memory</span> (cached RDDs), and 
            <span className="text-yellow-300"> User Memory</span> (your objects). The Unified Memory Manager dynamically balances execution and storage.
          </p>
        </div>
      </MonitorSection>

      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {/* SECTION 4: RDD & PARTITIONS (How Your Data is Split) */}
      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      <MonitorSection
        title="📦 RDD & Partitions - Your Distributed Data"
        icon={Database}
        color="text-cyan-400"
        badge="Data Layer"
        explanation="RDD (Resilient Distributed Dataset) is Spark's core data abstraction. Your data is split into Partitions - chunks that can be processed in parallel. Each partition lives on one executor. More partitions = more parallelism, but also more overhead."
      >
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
          {/* RDD Explanation */}
          <div className="bg-cyan-500/10 border border-cyan-500/30 rounded-lg p-4">
            <h4 className="text-cyan-400 font-medium mb-3 text-sm">What is an RDD?</h4>
            <ul className="space-y-2 text-xs text-gray-300">
              <li className="flex items-start">
                <span className="text-cyan-400 mr-2">R</span>
                <span><strong>Resilient:</strong> Can recover from node failures via lineage</span>
              </li>
              <li className="flex items-start">
                <span className="text-cyan-400 mr-2">D</span>
                <span><strong>Distributed:</strong> Data spread across multiple nodes</span>
              </li>
              <li className="flex items-start">
                <span className="text-cyan-400 mr-2">D</span>
                <span><strong>Dataset:</strong> Collection of records (rows)</span>
              </li>
            </ul>
          </div>
          
          {/* Partition Visualization */}
          <div className="bg-gray-900/50 rounded-lg p-4">
            <h4 className="text-white font-medium mb-3 text-sm">Partition Distribution</h4>
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs text-gray-400">Example: 6 partitions across 3 workers</span>
            </div>
            <div className="grid grid-cols-3 gap-2">
              {[1, 2, 3].map((worker) => (
                <div key={worker} className="bg-gray-800 rounded p-2 text-center">
                  <p className="text-xs text-gray-400 mb-1">Worker {worker}</p>
                  <div className="flex justify-center gap-1">
                    <div className="w-6 h-6 bg-cyan-500/30 border border-cyan-500 rounded text-xs flex items-center justify-center text-cyan-300">P{(worker-1)*2}</div>
                    <div className="w-6 h-6 bg-cyan-500/30 border border-cyan-500 rounded text-xs flex items-center justify-center text-cyan-300">P{(worker-1)*2+1}</div>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
        
        {/* RDD Blocks from Executors */}
        {executors.some(e => e.rddBlocks > 0) && (
          <div className="bg-gray-900/50 rounded-lg p-4">
            <h4 className="text-white font-medium mb-2 text-sm">Cached RDD Blocks</h4>
            <div className="flex flex-wrap gap-2">
              {executors.filter(e => e.id !== 'driver' && e.rddBlocks > 0).map((e) => (
                <div key={e.id} className="bg-cyan-500/10 border border-cyan-500/30 rounded px-3 py-1">
                  <span className="text-xs text-gray-300">Executor {e.id}: </span>
                  <span className="text-cyan-400 font-medium">{e.rddBlocks} blocks</span>
                </div>
              ))}
            </div>
          </div>
        )}
        
        <div className="mt-4 bg-cyan-500/10 border border-cyan-500/30 rounded-lg p-3">
          <p className="text-xs text-gray-300">
            <strong className="text-cyan-300">💡 Partition Tips:</strong> Ideal partition size is 128MB-256MB. 
            Too few partitions = underutilized cores. Too many = excessive overhead. 
            Use <code className="bg-gray-800 px-1 rounded">repartition(n)</code> or <code className="bg-gray-800 px-1 rounded">coalesce(n)</code> to adjust.
          </p>
        </div>
      </MonitorSection>

      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {/* SECTION 5: TASKS (The Smallest Unit of Work) */}
      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      <MonitorSection
        title="📦 Tasks - The Smallest Unit of Work"
        icon={Box}
        color="text-green-400"
        badge={`${completedTasks}/${totalTasks} Done`}
        explanation="A Task is the smallest unit of work in Spark. Each task processes ONE partition of data. Tasks run in parallel across executor cores. If you have 100 partitions and 10 cores, Spark runs 10 tasks at a time until all 100 are complete."
      >
        {/* Task Summary Cards */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4">
          <div className="bg-gray-900/50 rounded-lg p-4 text-center">
            <p className="text-3xl font-bold text-white">{totalTasks}</p>
            <p className="text-xs text-gray-400">Total Tasks</p>
          </div>
          <div className="bg-blue-500/10 rounded-lg p-4 text-center">
            <p className="text-3xl font-bold text-blue-400">{activeTasks}</p>
            <p className="text-xs text-gray-400">Running Now</p>
          </div>
          <div className="bg-green-500/10 rounded-lg p-4 text-center">
            <p className="text-3xl font-bold text-green-400">{completedTasks}</p>
            <p className="text-xs text-gray-400">Completed</p>
          </div>
          <div className="bg-red-500/10 rounded-lg p-4 text-center">
            <p className="text-3xl font-bold text-red-400">{jobs.reduce((sum, j) => sum + (j.numFailedTasks || 0), 0)}</p>
            <p className="text-xs text-gray-400">Failed</p>
          </div>
        </div>
        
        {/* Progress Bar */}
        {totalTasks > 0 && (
          <div className="mb-4">
            <div className="flex justify-between text-sm mb-1">
              <span className="text-gray-400">Overall Progress</span>
              <span className="text-white font-medium">{Math.round((completedTasks / totalTasks) * 100)}%</span>
            </div>
            <div className="h-4 bg-gray-700 rounded-full overflow-hidden">
              <div 
                className="h-full bg-gradient-to-r from-blue-500 to-green-500 transition-all duration-500"
                style={{ width: `${(completedTasks / totalTasks) * 100}%` }}
              />
            </div>
          </div>
        )}
        
        <div className="bg-green-500/10 border border-green-500/30 rounded-lg p-3">
          <p className="text-xs text-gray-300">
            <strong className="text-green-300">💡 Task Execution:</strong> Each task: 
            1) Reads its partition from storage/shuffle, 
            2) Applies transformations (map, filter, etc.), 
            3) Writes output to memory or disk. 
            Failed tasks are automatically retried on other executors.
          </p>
        </div>
      </MonitorSection>

      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {/* SECTION 6: STAGES (Shuffle Boundaries) */}
      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      <MonitorSection
        title="📊 Stages - Shuffle Boundaries"
        icon={Layers}
        color="text-purple-400"
        badge={`${stages.length} Stages`}
        explanation="A Stage is a set of tasks that can run without shuffling data. Spark creates a new stage whenever a 'wide' transformation (groupBy, join, reduceByKey) requires data to move between partitions. Stages run sequentially; tasks within a stage run in parallel."
      >
        {stages.length > 0 ? (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-gray-400 text-xs border-b border-gray-700">
                  <th className="text-left p-2">Stage</th>
                  <th className="text-left p-2">Name</th>
                  <th className="text-center p-2">Tasks</th>
                  <th className="text-center p-2">Input</th>
                  <th className="text-center p-2">Shuffle R/W</th>
                  <th className="text-center p-2">Status</th>
                </tr>
              </thead>
              <tbody>
                {stages.slice(0, 10).map((stage) => (
                  <tr key={`${stage.stageId}-${stage.attemptId}`} className="border-b border-gray-800 hover:bg-gray-800/50">
                    <td className="p-2 text-gray-300">{stage.stageId}</td>
                    <td className="p-2 text-white truncate max-w-xs text-xs">{stage.name}</td>
                    <td className="p-2 text-center">
                      <span className="text-green-400">{stage.numCompleteTasks}</span>
                      <span className="text-gray-500">/{stage.numTasks}</span>
                    </td>
                    <td className="p-2 text-center text-gray-300 text-xs">{formatBytes(stage.inputBytes || 0)}</td>
                    <td className="p-2 text-center text-xs">
                      <span className="text-yellow-300">{formatBytes(stage.shuffleReadBytes || 0)}</span>
                      <span className="text-gray-500"> / </span>
                      <span className="text-orange-300">{formatBytes(stage.shuffleWriteBytes || 0)}</span>
                    </td>
                    <td className="p-2 text-center">
                      <span className={`text-xs px-2 py-0.5 rounded ${
                        stage.status === 'ACTIVE' ? 'bg-blue-500/20 text-blue-300' :
                        stage.status === 'COMPLETE' ? 'bg-green-500/20 text-green-300' :
                        stage.status === 'FAILED' ? 'bg-red-500/20 text-red-300' :
                        'bg-gray-500/20 text-gray-300'
                      }`}>
                        {stage.status}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="text-center py-6 text-gray-400">
            <Layers className="h-10 w-10 mx-auto mb-2 opacity-50" />
            <p>No stages to display</p>
          </div>
        )}
        
        <div className="mt-4 bg-purple-500/10 border border-purple-500/30 rounded-lg p-3">
          <p className="text-xs text-gray-300">
            <strong className="text-purple-300">💡 Stage Optimization:</strong> Fewer stages = less shuffling = faster jobs. 
            Combine operations before groupBy. Use broadcast joins for small tables. 
            Avoid <code className="bg-gray-800 px-1 rounded">groupByKey()</code> - prefer <code className="bg-gray-800 px-1 rounded">reduceByKey()</code>.
          </p>
        </div>
      </MonitorSection>

      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {/* SECTION 7: SHUFFLE (The Most Expensive Operation) */}
      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      <MonitorSection
        title="🔀 Shuffle - Data Movement Between Nodes"
        icon={Shuffle}
        color="text-yellow-400"
        badge={`${formatBytes(totalShuffleRead + totalShuffleWrite)} moved`}
        explanation="Shuffle is when data must be redistributed across the cluster. This happens during groupBy, join, reduceByKey, repartition. It's the most expensive operation because data travels over the network. Minimizing shuffle = faster jobs."
      >
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
          {/* Shuffle Read */}
          <div className="bg-yellow-500/10 border border-yellow-500/30 rounded-lg p-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-yellow-400 font-medium text-sm">Shuffle Read</span>
              <ArrowDown className="h-4 w-4 text-yellow-400" />
            </div>
            <p className="text-2xl font-bold text-white">{formatBytes(totalShuffleRead)}</p>
            <p className="text-xs text-gray-400 mt-1">Data pulled from other executors</p>
          </div>
          
          {/* Shuffle Write */}
          <div className="bg-orange-500/10 border border-orange-500/30 rounded-lg p-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-orange-400 font-medium text-sm">Shuffle Write</span>
              <ArrowRight className="h-4 w-4 text-orange-400" />
            </div>
            <p className="text-2xl font-bold text-white">{formatBytes(totalShuffleWrite)}</p>
            <p className="text-xs text-gray-400 mt-1">Data written for other executors</p>
          </div>
        </div>
        
        {/* Shuffle Flow Diagram */}
        <div className="bg-gray-900/50 rounded-lg p-4">
          <h4 className="text-white font-medium mb-3 text-sm">How Shuffle Works</h4>
          <div className="flex items-center justify-center space-x-2 text-xs">
            <div className="bg-blue-500/20 border border-blue-500/50 rounded px-3 py-2 text-blue-300">
              Mapper Tasks
            </div>
            <ArrowRight className="h-4 w-4 text-yellow-400" />
            <div className="bg-yellow-500/20 border border-yellow-500/50 rounded px-3 py-2 text-yellow-300">
              Shuffle Write<br/>(to disk)
            </div>
            <ArrowRight className="h-4 w-4 text-yellow-400" />
            <div className="bg-orange-500/20 border border-orange-500/50 rounded px-3 py-2 text-orange-300">
              Network Transfer
            </div>
            <ArrowRight className="h-4 w-4 text-yellow-400" />
            <div className="bg-green-500/20 border border-green-500/50 rounded px-3 py-2 text-green-300">
              Shuffle Read<br/>(reducer)
            </div>
          </div>
        </div>
        
        <div className="mt-4 bg-yellow-500/10 border border-yellow-500/30 rounded-lg p-3">
          <p className="text-xs text-gray-300">
            <strong className="text-yellow-300">💡 Reduce Shuffle:</strong> 
            1) Filter data early (before groupBy), 
            2) Use broadcast joins for small tables, 
            3) Increase spark.sql.shuffle.partitions for large data, 
            4) Use coalesce() instead of repartition() when reducing partitions.
          </p>
        </div>
      </MonitorSection>

      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {/* SECTION 8: MAP & TRANSFORMATIONS */}
      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      <MonitorSection
        title="🔧 Map & Transformations - The Computations"
        icon={Filter}
        color="text-green-400"
        badge="Operations"
        explanation="Transformations are operations that create a new RDD/DataFrame from an existing one. 'Narrow' transformations (map, filter) process partitions independently. 'Wide' transformations (groupBy, join) require shuffle. Spark uses lazy evaluation - transformations only execute when an action is called."
      >
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {/* Narrow Transformations */}
          <div className="bg-green-500/10 border border-green-500/30 rounded-lg p-4">
            <h4 className="text-green-400 font-medium mb-3 text-sm">Narrow Transformations (No Shuffle) ✓</h4>
            <div className="space-y-2 text-xs">
              <div className="flex justify-between items-center bg-gray-800/50 rounded p-2">
                <code className="text-green-400">.map()</code>
                <span className="text-gray-400">Transform each row → one output</span>
              </div>
              <div className="flex justify-between items-center bg-gray-800/50 rounded p-2">
                <code className="text-green-400">.filter()</code>
                <span className="text-gray-400">Keep rows matching condition</span>
              </div>
              <div className="flex justify-between items-center bg-gray-800/50 rounded p-2">
                <code className="text-green-400">.flatMap()</code>
                <span className="text-gray-400">One input → multiple outputs</span>
              </div>
              <div className="flex justify-between items-center bg-gray-800/50 rounded p-2">
                <code className="text-green-400">.select()</code>
                <span className="text-gray-400">Choose specific columns</span>
              </div>
            </div>
          </div>
          
          {/* Wide Transformations */}
          <div className="bg-yellow-500/10 border border-yellow-500/30 rounded-lg p-4">
            <h4 className="text-yellow-400 font-medium mb-3 text-sm">Wide Transformations (Require Shuffle) ⚠️</h4>
            <div className="space-y-2 text-xs">
              <div className="flex justify-between items-center bg-gray-800/50 rounded p-2">
                <code className="text-yellow-400">.groupBy()</code>
                <span className="text-gray-400">Group rows by key</span>
              </div>
              <div className="flex justify-between items-center bg-gray-800/50 rounded p-2">
                <code className="text-yellow-400">.join()</code>
                <span className="text-gray-400">Combine two DataFrames</span>
              </div>
              <div className="flex justify-between items-center bg-gray-800/50 rounded p-2">
                <code className="text-yellow-400">.reduceByKey()</code>
                <span className="text-gray-400">Aggregate values by key</span>
              </div>
              <div className="flex justify-between items-center bg-gray-800/50 rounded p-2">
                <code className="text-yellow-400">.repartition()</code>
                <span className="text-gray-400">Change partition count</span>
              </div>
            </div>
          </div>
        </div>
        
        <div className="mt-4 bg-green-500/10 border border-green-500/30 rounded-lg p-3">
          <p className="text-xs text-gray-300">
            <strong className="text-green-300">💡 Lazy Evaluation:</strong> Transformations don't execute immediately. 
            Spark builds a DAG (Directed Acyclic Graph) of operations. Execution only happens when you call an 
            <strong className="text-orange-300"> Action</strong> like <code className="bg-gray-800 px-1 rounded">.collect()</code>, 
            <code className="bg-gray-800 px-1 rounded">.count()</code>, or <code className="bg-gray-800 px-1 rounded">.write()</code>.
          </p>
        </div>
      </MonitorSection>

      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {/* SECTION 9: TASK SCHEDULING & RESOURCE ALLOCATION */}
      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      <MonitorSection
        title="📋 Task Scheduling & Resource Allocation"
        icon={Settings}
        color="text-blue-400"
        badge="Cluster Manager"
        explanation="The DAG Scheduler converts your code into stages. The Task Scheduler assigns tasks to executor slots. The Cluster Manager allocates CPU and memory to executors. This all happens automatically, but understanding it helps you tune performance."
      >
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
          {/* Resource Pool */}
          <div className="bg-gray-900/50 rounded-lg p-4">
            <h4 className="text-white font-medium mb-3 text-sm">Cluster Resource Pool</h4>
            <div className="space-y-3">
              <div>
                <div className="flex justify-between text-sm mb-1">
                  <span className="text-gray-400">Total Cores</span>
                  <span className="text-white">{cluster?.coresused || 0} / {cluster?.cores || 0} used</span>
                </div>
                <div className="h-3 bg-gray-700 rounded-full overflow-hidden">
                  <div 
                    className="h-full bg-green-500"
                    style={{ width: `${cluster?.cores ? (cluster.coresused / cluster.cores) * 100 : 0}%` }}
                  />
                </div>
              </div>
              <div>
                <div className="flex justify-between text-sm mb-1">
                  <span className="text-gray-400">Total Memory</span>
                  <span className="text-white">{formatBytes((cluster?.memoryused || 0) * 1024 * 1024)} / {formatBytes((cluster?.memory || 0) * 1024 * 1024)}</span>
                </div>
                <div className="h-3 bg-gray-700 rounded-full overflow-hidden">
                  <div 
                    className="h-full bg-purple-500"
                    style={{ width: `${cluster?.memory ? (cluster.memoryused / cluster.memory) * 100 : 0}%` }}
                  />
                </div>
              </div>
            </div>
          </div>
          
          {/* Scheduling Flow */}
          <div className="bg-blue-500/10 border border-blue-500/30 rounded-lg p-4">
            <h4 className="text-blue-400 font-medium mb-3 text-sm">Scheduling Pipeline</h4>
            <div className="space-y-2 text-xs">
              <div className="flex items-center">
                <span className="bg-yellow-500/20 text-yellow-400 px-2 py-0.5 rounded mr-2">1</span>
                <span className="text-gray-300">Driver builds DAG of stages</span>
              </div>
              <div className="flex items-center">
                <span className="bg-blue-500/20 text-blue-400 px-2 py-0.5 rounded mr-2">2</span>
                <span className="text-gray-300">DAG Scheduler creates TaskSets</span>
              </div>
              <div className="flex items-center">
                <span className="bg-purple-500/20 text-purple-400 px-2 py-0.5 rounded mr-2">3</span>
                <span className="text-gray-300">Task Scheduler assigns to executors</span>
              </div>
              <div className="flex items-center">
                <span className="bg-green-500/20 text-green-400 px-2 py-0.5 rounded mr-2">4</span>
                <span className="text-gray-300">Executors run tasks on partitions</span>
              </div>
            </div>
          </div>
        </div>
        
        <div className="bg-blue-500/10 border border-blue-500/30 rounded-lg p-3">
          <p className="text-xs text-gray-300">
            <strong className="text-blue-300">💡 Data Locality:</strong> Spark tries to schedule tasks where data already exists (NODE_LOCAL {'>'} RACK_LOCAL {'>'} ANY). 
            This minimizes network transfer. If you see many RACK_LOCAL or ANY tasks, consider increasing partition count or rebalancing data.
          </p>
        </div>
      </MonitorSection>

      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {/* SECTION 10: AGGREGATION & FINAL OUTPUT */}
      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      <MonitorSection
        title="📤 Aggregation & Final Output"
        icon={FileText}
        color="text-orange-400"
        badge="Results"
        explanation="After all transformations, Actions trigger execution and produce output. Results can be collected to the driver (collect), written to storage (write), or aggregated (count, sum). Be careful with collect() on large datasets - it brings all data to the driver!"
      >
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {/* Actions */}
          <div className="bg-orange-500/10 border border-orange-500/30 rounded-lg p-4">
            <h4 className="text-orange-400 font-medium mb-3 text-sm">Common Actions (Trigger Execution)</h4>
            <div className="space-y-2 text-xs">
              <div className="flex justify-between items-center bg-gray-800/50 rounded p-2">
                <code className="text-orange-400">.collect()</code>
                <span className="text-gray-400">Return all data to driver ⚠️</span>
              </div>
              <div className="flex justify-between items-center bg-gray-800/50 rounded p-2">
                <code className="text-orange-400">.count()</code>
                <span className="text-gray-400">Count rows</span>
              </div>
              <div className="flex justify-between items-center bg-gray-800/50 rounded p-2">
                <code className="text-orange-400">.show(n)</code>
                <span className="text-gray-400">Display first n rows</span>
              </div>
              <div className="flex justify-between items-center bg-gray-800/50 rounded p-2">
                <code className="text-orange-400">.write.parquet()</code>
                <span className="text-gray-400">Write to storage ✓</span>
              </div>
            </div>
          </div>
          
          {/* Output Flow */}
          <div className="bg-gray-900/50 rounded-lg p-4">
            <h4 className="text-white font-medium mb-3 text-sm">Result Flow</h4>
            <div className="space-y-3 text-xs">
              <div className="flex items-center justify-between bg-gray-800/50 rounded p-2">
                <span className="text-gray-300">Executor Results</span>
                <ArrowRight className="h-4 w-4 text-orange-400" />
                <span className="text-orange-400">Aggregated at Driver</span>
              </div>
              <div className="flex items-center justify-between bg-gray-800/50 rounded p-2">
                <span className="text-gray-300">Driver Aggregates</span>
                <ArrowRight className="h-4 w-4 text-green-400" />
                <span className="text-green-400">Final Output</span>
              </div>
            </div>
            
            {/* Job Results */}
            {jobs.length > 0 && (
              <div className="mt-3 pt-3 border-t border-gray-700">
                <p className="text-xs text-gray-400 mb-2">Recent Job Results:</p>
                <div className="space-y-1">
                  {jobs.slice(0, 3).map((job) => (
                    <div key={job.jobId} className="flex items-center justify-between text-xs">
                      <span className="text-gray-300">Job {job.jobId}</span>
                      <span className={`px-2 py-0.5 rounded ${
                        job.status === 'SUCCEEDED' ? 'bg-green-500/20 text-green-300' :
                        job.status === 'RUNNING' ? 'bg-blue-500/20 text-blue-300' :
                        'bg-red-500/20 text-red-300'
                      }`}>
                        {job.status}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>
        
        <div className="mt-4 bg-orange-500/10 border border-orange-500/30 rounded-lg p-3">
          <p className="text-xs text-gray-300">
            <strong className="text-orange-300">💡 Output Best Practices:</strong> 
            Use <code className="bg-gray-800 px-1 rounded">.take(n)</code> instead of <code className="bg-gray-800 px-1 rounded">.collect()</code> for sampling. 
            Write large results to Parquet/Delta Lake, not to driver memory. 
            Use <code className="bg-gray-800 px-1 rounded">.cache()</code> if you'll reuse intermediate results.
          </p>
        </div>
      </MonitorSection>

      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {/* SECTION 11: COMPLETE EXECUTION FLOW */}
      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      <MonitorSection
        title="🔄 Complete Execution Flow"
        icon={GitBranch}
        color="text-spark-orange"
        badge="Full Picture"
        explanation="This shows the complete journey of your Spark job from submission to final output. Understanding this flow helps you identify bottlenecks and optimize performance."
        defaultExpanded={false}
      >
        <div className="bg-gray-900/50 rounded-lg p-4">
          <div className="space-y-4 text-sm">
            <div className="flex items-start">
              <span className="bg-yellow-500/20 text-yellow-400 px-2 py-1 rounded text-xs font-mono mr-3 mt-0.5">1</span>
              <div>
                <p className="text-white font-medium">You submit: spark-submit your_script.py</p>
                <p className="text-gray-400 text-xs">Driver process starts on master node</p>
              </div>
            </div>
            <div className="flex items-start">
              <span className="bg-blue-500/20 text-blue-400 px-2 py-1 rounded text-xs font-mono mr-3 mt-0.5">2</span>
              <div>
                <p className="text-white font-medium">Driver parses code → builds DAG</p>
                <p className="text-gray-400 text-xs">Logical plan of transformations created</p>
              </div>
            </div>
            <div className="flex items-start">
              <span className="bg-purple-500/20 text-purple-400 px-2 py-1 rounded text-xs font-mono mr-3 mt-0.5">3</span>
              <div>
                <p className="text-white font-medium">Cluster Manager allocates executors</p>
                <p className="text-gray-400 text-xs">Workers launch JVM processes with allocated memory</p>
              </div>
            </div>
            <div className="flex items-start">
              <span className="bg-cyan-500/20 text-cyan-400 px-2 py-1 rounded text-xs font-mono mr-3 mt-0.5">4</span>
              <div>
                <p className="text-white font-medium">Data loaded into RDD partitions</p>
                <p className="text-gray-400 text-xs">Each partition assigned to an executor</p>
              </div>
            </div>
            <div className="flex items-start">
              <span className="bg-green-500/20 text-green-400 px-2 py-1 rounded text-xs font-mono mr-3 mt-0.5">5</span>
              <div>
                <p className="text-white font-medium">Tasks scheduled to executor cores</p>
                <p className="text-gray-400 text-xs">Driver sends serialized code to workers</p>
              </div>
            </div>
            <div className="flex items-start">
              <span className="bg-yellow-500/20 text-yellow-400 px-2 py-1 rounded text-xs font-mono mr-3 mt-0.5">6</span>
              <div>
                <p className="text-white font-medium">Executors process & shuffle if needed</p>
                <p className="text-gray-400 text-xs">Wide transforms trigger data redistribution</p>
              </div>
            </div>
            <div className="flex items-start">
              <span className="bg-green-500/20 text-green-400 px-2 py-1 rounded text-xs font-mono mr-3 mt-0.5">✓</span>
              <div>
                <p className="text-white font-medium">Results returned to Driver</p>
                <p className="text-gray-400 text-xs">Or written to storage (Parquet, database, etc.)</p>
              </div>
            </div>
          </div>
        </div>
      </MonitorSection>

    </div>
  )
}
