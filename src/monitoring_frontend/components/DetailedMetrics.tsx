import { useState } from 'react'
import useSWR from 'swr'
import { 
  fetchClusterStatus, 
  fetchApplications, 
  fetchJobs, 
  fetchStages, 
  fetchExecutors,
  ClusterStatus,
  SparkApplication,
  SparkJob,
  SparkStage,
  SparkExecutor,
  formatBytes 
} from '../lib/sparkApi'
import { 
  Info, 
  ChevronDown, 
  ChevronRight, 
  Cpu, 
  HardDrive, 
  Clock, 
  Activity,
  Layers,
  Box,
  GitBranch,
  Zap,
  Database,
  AlertCircle,
  CheckCircle,
  XCircle,
  Timer,
  BarChart3
} from 'lucide-react'

interface Props {
  refreshInterval: number | null
}

// Tooltip component for explanations
function InfoTooltip({ text }: { text: string }) {
  return (
    <div className="group relative inline-block ml-1">
      <Info className="h-3.5 w-3.5 text-gray-500 hover:text-gray-300 cursor-help" />
      <div className="absolute z-50 hidden group-hover:block w-64 p-2 text-xs bg-gray-900 border border-gray-600 rounded-lg shadow-xl -translate-x-1/2 left-1/2 mt-1">
        <div className="absolute -top-1 left-1/2 -translate-x-1/2 w-2 h-2 bg-gray-900 border-l border-t border-gray-600 rotate-45"></div>
        {text}
      </div>
    </div>
  )
}

// Section header with collapsible functionality
function SectionHeader({ 
  title, 
  description, 
  isExpanded, 
  onToggle, 
  icon: Icon,
  count 
}: { 
  title: string
  description: string
  isExpanded: boolean
  onToggle: () => void
  icon: React.ElementType
  count?: number
}) {
  return (
    <button 
      onClick={onToggle}
      className="w-full flex items-center justify-between p-4 bg-gray-800/70 hover:bg-gray-800 rounded-t-lg border border-gray-700 transition-colors"
    >
      <div className="flex items-center">
        <Icon className="h-5 w-5 text-spark-orange mr-3" />
        <div className="text-left">
          <h3 className="text-white font-medium flex items-center">
            {title}
            {count !== undefined && (
              <span className="ml-2 px-2 py-0.5 text-xs bg-gray-700 rounded-full">{count}</span>
            )}
          </h3>
          <p className="text-xs text-gray-400 mt-0.5">{description}</p>
        </div>
      </div>
      {isExpanded ? (
        <ChevronDown className="h-5 w-5 text-gray-400" />
      ) : (
        <ChevronRight className="h-5 w-5 text-gray-400" />
      )}
    </button>
  )
}

export default function DetailedMetrics({ refreshInterval }: Props) {
  // Expansion states for collapsible sections
  const [expandedSections, setExpandedSections] = useState({
    cluster: true,
    executors: false,
    jobs: false,
    stages: false,
    memory: false,
  })

  // Fetch data
  const { data: cluster } = useSWR<ClusterStatus>(
    'cluster-status',
    fetchClusterStatus,
    { refreshInterval: refreshInterval || 0 }
  )

  const { data: applications } = useSWR<SparkApplication[]>(
    'applications',
    fetchApplications,
    { refreshInterval: refreshInterval || 0 }
  )

  const { data: jobs } = useSWR<SparkJob[]>(
    'jobs',
    fetchJobs,
    { refreshInterval: refreshInterval || 0 }
  )

  const { data: stages } = useSWR<SparkStage[]>(
    'stages',
    fetchStages,
    { refreshInterval: refreshInterval || 0 }
  )

  const { data: executors } = useSWR<SparkExecutor[]>(
    'executors',
    fetchExecutors,
    { refreshInterval: refreshInterval || 0 }
  )

  const toggleSection = (section: keyof typeof expandedSections) => {
    setExpandedSections(prev => ({ ...prev, [section]: !prev[section] }))
  }

  const workers = cluster?.workers || []
  // Count apps that are currently active (state not FINISHED/KILLED)
  const activeApps = applications?.filter(app => app.state === 'RUNNING' || app.state === 'WAITING') || []

  return (
    <div className="space-y-4">
      {/* Introduction Banner */}
      <div className="bg-gradient-to-r from-blue-900/30 to-purple-900/30 border border-blue-700/50 rounded-lg p-4">
        <div className="flex items-start">
          <Info className="h-5 w-5 text-blue-400 mr-3 mt-0.5 flex-shrink-0" />
          <div>
            <h3 className="text-blue-300 font-medium">Detailed PySpark Monitoring</h3>
            <p className="text-sm text-gray-300 mt-1">
              This view provides in-depth metrics about your Spark cluster, including executor details, 
              job progress, stage breakdown, and memory utilization. Hover over the 
              <Info className="h-3 w-3 inline mx-1 text-gray-400" /> 
              icons for explanations of each metric.
            </p>
          </div>
        </div>
      </div>

      {/* Cluster Architecture Section */}
      <div className="bg-gray-800/50 border border-gray-700 rounded-lg overflow-hidden">
        <SectionHeader
          title="Cluster Architecture"
          description="Overview of your Spark cluster topology and resource allocation"
          isExpanded={expandedSections.cluster}
          onToggle={() => toggleSection('cluster')}
          icon={GitBranch}
          count={workers.length}
        />
        
        {expandedSections.cluster && (
          <div className="p-4 border-t border-gray-700 space-y-4">
            {/* Cluster Topology Explanation */}
            <div className="bg-gray-900/50 rounded-lg p-3 text-sm">
              <p className="text-gray-300">
                <span className="text-spark-orange font-medium">Spark Cluster Architecture:</span> Your cluster consists of 
                a <span className="text-blue-400">Master node</span> that coordinates work distribution and 
                <span className="text-green-400"> {workers.length} Worker nodes</span> that execute tasks.
              </p>
            </div>

            {/* Master Node */}
            <div className="bg-gray-900/30 rounded-lg p-4">
              <div className="flex items-center mb-3">
                <Zap className="h-5 w-5 text-spark-orange mr-2" />
                <h4 className="text-white font-medium">Spark Master</h4>
                <InfoTooltip text="The Master node manages the cluster, schedules applications, and distributes work to Workers. It doesn't execute tasks directly but coordinates all cluster operations." />
              </div>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                <div>
                  <p className="text-gray-400">URL</p>
                  <p className="text-white font-mono text-xs">{cluster?.url || 'spark://spark-master:7077'}</p>
                </div>
                <div>
                  <p className="text-gray-400">Status</p>
                  <p className="text-green-400 flex items-center">
                    <CheckCircle className="h-3 w-3 mr-1" />
                    {cluster?.status || 'ALIVE'}
                  </p>
                </div>
                <div>
                  <p className="text-gray-400">Total Cores</p>
                  <p className="text-white">{cluster?.cores || 0}</p>
                </div>
                <div>
                  <p className="text-gray-400">Total Memory</p>
                  <p className="text-white">{formatBytes((cluster?.memory || 0) * 1024 * 1024)}</p>
                </div>
              </div>
            </div>

            {/* Worker Nodes */}
            <div>
              <div className="flex items-center mb-3">
                <Box className="h-5 w-5 text-blue-400 mr-2" />
                <h4 className="text-white font-medium">Worker Nodes</h4>
                <InfoTooltip text="Worker nodes are the compute engines of the cluster. Each worker can run multiple Executors, which process data partitions in parallel. More workers = more parallelism." />
              </div>
              
              <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                {workers.map((worker: any, idx: number) => (
                  <div key={worker.id || idx} className="bg-gray-900/30 rounded-lg p-3">
                    <div className="flex items-center justify-between mb-2">
                      <span className="text-sm font-medium text-white">Worker {idx + 1}</span>
                      <span className={`text-xs px-2 py-0.5 rounded-full ${
                        worker.state === 'ALIVE' ? 'bg-green-500/20 text-green-400' : 'bg-red-500/20 text-red-400'
                      }`}>
                        {worker.state}
                      </span>
                    </div>
                    <div className="space-y-2 text-xs">
                      <div className="flex justify-between">
                        <span className="text-gray-400">Host:</span>
                        <span className="text-gray-200 font-mono">{worker.host}</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-gray-400">Cores:</span>
                        <span className="text-gray-200">{worker.coresused || 0} / {worker.cores} used</span>
                      </div>
                      <div className="flex justify-between">
                        <span className="text-gray-400">Memory:</span>
                        <span className="text-gray-200">
                          {formatBytes((worker.memoryused || 0) * 1024 * 1024)} / {formatBytes((worker.memory || 0) * 1024 * 1024)}
                        </span>
                      </div>
                      <div className="mt-2">
                        <div className="flex justify-between text-gray-400 mb-1">
                          <span>Utilization</span>
                          <span>{worker.cores > 0 ? Math.round((worker.coresused / worker.cores) * 100) : 0}%</span>
                        </div>
                        <div className="bg-gray-700 rounded-full h-1.5">
                          <div 
                            className="bg-blue-500 h-1.5 rounded-full transition-all"
                            style={{ width: `${worker.cores > 0 ? (worker.coresused / worker.cores) * 100 : 0}%` }}
                          />
                        </div>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Executors Section */}
      <div className="bg-gray-800/50 border border-gray-700 rounded-lg overflow-hidden">
        <SectionHeader
          title="Executors"
          description="JVM processes running on workers that execute tasks and cache data"
          isExpanded={expandedSections.executors}
          onToggle={() => toggleSection('executors')}
          icon={Cpu}
          count={executors?.length || 0}
        />
        
        {expandedSections.executors && (
          <div className="p-4 border-t border-gray-700">
            <div className="bg-gray-900/50 rounded-lg p-3 text-sm mb-4">
              <p className="text-gray-300">
                <span className="text-spark-orange font-medium">What are Executors?</span> Executors are JVM processes 
                launched on worker nodes. Each executor:
              </p>
              <ul className="mt-2 space-y-1 text-gray-400 text-xs ml-4">
                <li>• Runs tasks (smallest unit of work) in multiple threads</li>
                <li>• Stores RDD partitions in memory/disk cache</li>
                <li>• Reports metrics back to the Driver</li>
                <li>• Executors remain alive for the entire application lifecycle</li>
              </ul>
            </div>

            {executors && executors.length > 0 ? (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="bg-gray-700/50">
                    <tr>
                      <th className="px-3 py-2 text-left text-xs text-gray-300">
                        ID
                        <InfoTooltip text="Unique identifier for each executor. 'driver' is the main application process." />
                      </th>
                      <th className="px-3 py-2 text-left text-xs text-gray-300">
                        Host
                        <InfoTooltip text="The worker node where this executor is running." />
                      </th>
                      <th className="px-3 py-2 text-left text-xs text-gray-300">
                        Cores
                        <InfoTooltip text="Number of CPU cores allocated. More cores = more parallel task execution." />
                      </th>
                      <th className="px-3 py-2 text-left text-xs text-gray-300">
                        Memory
                        <InfoTooltip text="JVM heap memory allocated for execution and caching." />
                      </th>
                      <th className="px-3 py-2 text-left text-xs text-gray-300">
                        Active Tasks
                        <InfoTooltip text="Number of tasks currently executing on this executor." />
                      </th>
                      <th className="px-3 py-2 text-left text-xs text-gray-300">
                        Completed
                        <InfoTooltip text="Total tasks successfully completed by this executor." />
                      </th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-700">
                    {executors.map((exec: SparkExecutor) => (
                      <tr key={exec.id} className="hover:bg-gray-700/30">
                        <td className="px-3 py-2 font-mono text-spark-orange">{exec.id}</td>
                        <td className="px-3 py-2 text-gray-300">{exec.hostPort}</td>
                        <td className="px-3 py-2 text-gray-300">{exec.totalCores}</td>
                        <td className="px-3 py-2 text-gray-300">{formatBytes(exec.maxMemory)}</td>
                        <td className="px-3 py-2">
                          <span className={exec.activeTasks > 0 ? 'text-green-400' : 'text-gray-400'}>
                            {exec.activeTasks}
                          </span>
                        </td>
                        <td className="px-3 py-2 text-gray-300">{exec.completedTasks}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="text-center py-8 text-gray-400">
                <Cpu className="h-8 w-8 mx-auto mb-2 opacity-50" />
                <p>No active executors</p>
                <p className="text-xs mt-1">Executors are created when a Spark application runs</p>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Jobs Section */}
      <div className="bg-gray-800/50 border border-gray-700 rounded-lg overflow-hidden">
        <SectionHeader
          title="Jobs"
          description="Spark actions that trigger computation across the cluster"
          isExpanded={expandedSections.jobs}
          onToggle={() => toggleSection('jobs')}
          icon={Activity}
          count={jobs?.length || 0}
        />
        
        {expandedSections.jobs && (
          <div className="p-4 border-t border-gray-700">
            <div className="bg-gray-900/50 rounded-lg p-3 text-sm mb-4">
              <p className="text-gray-300">
                <span className="text-spark-orange font-medium">What are Jobs?</span> A Job is created each time 
                an <span className="text-blue-400">action</span> (like collect(), count(), save()) is called on an RDD or DataFrame.
              </p>
              <ul className="mt-2 space-y-1 text-gray-400 text-xs ml-4">
                <li>• Each job consists of multiple <span className="text-purple-400">Stages</span></li>
                <li>• Stages are separated by shuffle operations (groupBy, join, etc.)</li>
                <li>• Jobs are executed sequentially unless using threading</li>
              </ul>
            </div>

            {jobs && jobs.length > 0 ? (
              <div className="space-y-2">
                {jobs.slice(0, 10).map((job: SparkJob) => (
                  <div key={job.jobId} className="bg-gray-900/30 rounded-lg p-3">
                    <div className="flex items-center justify-between mb-2">
                      <div className="flex items-center">
                        <span className="text-spark-orange font-mono mr-2">Job {job.jobId}</span>
                        <span className={`text-xs px-2 py-0.5 rounded-full ${
                          job.status === 'SUCCEEDED' ? 'bg-green-500/20 text-green-400' :
                          job.status === 'RUNNING' ? 'bg-blue-500/20 text-blue-400' :
                          job.status === 'FAILED' ? 'bg-red-500/20 text-red-400' :
                          'bg-gray-500/20 text-gray-400'
                        }`}>
                          {job.status}
                        </span>
                      </div>
                      <span className="text-xs text-gray-400">
                        {job.numCompletedTasks} / {job.numTasks} tasks
                      </span>
                    </div>
                    <div className="bg-gray-700 rounded-full h-2 mb-2">
                      <div 
                        className={`h-2 rounded-full transition-all ${
                          job.status === 'SUCCEEDED' ? 'bg-green-500' :
                          job.status === 'RUNNING' ? 'bg-blue-500' :
                          job.status === 'FAILED' ? 'bg-red-500' : 'bg-gray-500'
                        }`}
                        style={{ width: `${job.numTasks > 0 ? (job.numCompletedTasks / job.numTasks) * 100 : 0}%` }}
                      />
                    </div>
                    <div className="flex justify-between text-xs text-gray-400">
                      <span>Stages: {job.numCompletedStages} / {job.numStages}</span>
                      <span>Active: {job.numActiveTasks}</span>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="text-center py-8 text-gray-400">
                <Activity className="h-8 w-8 mx-auto mb-2 opacity-50" />
                <p>No jobs recorded</p>
                <p className="text-xs mt-1">Submit a Spark application to see job details</p>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Stages Section */}
      <div className="bg-gray-800/50 border border-gray-700 rounded-lg overflow-hidden">
        <SectionHeader
          title="Stages"
          description="Groups of tasks that can run in parallel without shuffling data"
          isExpanded={expandedSections.stages}
          onToggle={() => toggleSection('stages')}
          icon={Layers}
          count={stages?.length || 0}
        />
        
        {expandedSections.stages && (
          <div className="p-4 border-t border-gray-700">
            <div className="bg-gray-900/50 rounded-lg p-3 text-sm mb-4">
              <p className="text-gray-300">
                <span className="text-spark-orange font-medium">What are Stages?</span> Stages are sets of tasks 
                that can execute in parallel. A new stage begins whenever data needs to be <span className="text-yellow-400">shuffled</span> across partitions.
              </p>
              <ul className="mt-2 space-y-1 text-gray-400 text-xs ml-4">
                <li>• <span className="text-green-400">Narrow transformations</span> (map, filter): Same stage, no shuffle</li>
                <li>• <span className="text-yellow-400">Wide transformations</span> (groupBy, join): New stage, requires shuffle</li>
                <li>• Fewer stages = better performance (less data movement)</li>
              </ul>
            </div>

            {stages && stages.length > 0 ? (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="bg-gray-700/50">
                    <tr>
                      <th className="px-3 py-2 text-left text-xs text-gray-300">Stage ID</th>
                      <th className="px-3 py-2 text-left text-xs text-gray-300">Status</th>
                      <th className="px-3 py-2 text-left text-xs text-gray-300">
                        Tasks
                        <InfoTooltip text="Number of tasks in this stage. Each task processes one data partition." />
                      </th>
                      <th className="px-3 py-2 text-left text-xs text-gray-300">
                        Input
                        <InfoTooltip text="Total bytes read by this stage from storage or previous stages." />
                      </th>
                      <th className="px-3 py-2 text-left text-xs text-gray-300">
                        Shuffle Read
                        <InfoTooltip text="Data received from other executors during shuffle operations." />
                      </th>
                      <th className="px-3 py-2 text-left text-xs text-gray-300">
                        Shuffle Write
                        <InfoTooltip text="Data written to be read by other executors in subsequent stages." />
                      </th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-700">
                    {stages.slice(0, 10).map((stage: SparkStage) => (
                      <tr key={`${stage.stageId}-${stage.attemptId}`} className="hover:bg-gray-700/30">
                        <td className="px-3 py-2 font-mono text-spark-orange">{stage.stageId}</td>
                        <td className="px-3 py-2">
                          <span className={`text-xs px-2 py-0.5 rounded-full ${
                            stage.status === 'COMPLETE' ? 'bg-green-500/20 text-green-400' :
                            stage.status === 'ACTIVE' ? 'bg-blue-500/20 text-blue-400' :
                            stage.status === 'FAILED' ? 'bg-red-500/20 text-red-400' :
                            'bg-gray-500/20 text-gray-400'
                          }`}>
                            {stage.status}
                          </span>
                        </td>
                        <td className="px-3 py-2 text-gray-300">
                          {stage.numCompleteTasks} / {stage.numTasks}
                        </td>
                        <td className="px-3 py-2 text-gray-300">{formatBytes(stage.inputBytes)}</td>
                        <td className="px-3 py-2 text-gray-300">{formatBytes(stage.shuffleReadBytes)}</td>
                        <td className="px-3 py-2 text-gray-300">{formatBytes(stage.shuffleWriteBytes)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="text-center py-8 text-gray-400">
                <Layers className="h-8 w-8 mx-auto mb-2 opacity-50" />
                <p>No stages recorded</p>
                <p className="text-xs mt-1">Stages are created when jobs execute</p>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Memory Analysis Section */}
      <div className="bg-gray-800/50 border border-gray-700 rounded-lg overflow-hidden">
        <SectionHeader
          title="Memory Analysis"
          description="Breakdown of cluster memory allocation and usage patterns"
          isExpanded={expandedSections.memory}
          onToggle={() => toggleSection('memory')}
          icon={Database}
        />
        
        {expandedSections.memory && (
          <div className="p-4 border-t border-gray-700">
            <div className="bg-gray-900/50 rounded-lg p-3 text-sm mb-4">
              <p className="text-gray-300">
                <span className="text-spark-orange font-medium">Spark Memory Model:</span> Each executor divides its memory into regions:
              </p>
              <ul className="mt-2 space-y-1 text-gray-400 text-xs ml-4">
                <li>• <span className="text-blue-400">Execution Memory:</span> Used for shuffles, joins, sorts, aggregations</li>
                <li>• <span className="text-purple-400">Storage Memory:</span> Used for caching RDDs and broadcast variables</li>
                <li>• <span className="text-green-400">User Memory:</span> For user data structures and Spark metadata</li>
                <li>• <span className="text-gray-400">Reserved:</span> ~300MB reserved for system</li>
              </ul>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {/* Total Cluster Memory */}
              <div className="bg-gray-900/30 rounded-lg p-4">
                <h4 className="text-white font-medium mb-3 flex items-center">
                  <HardDrive className="h-4 w-4 mr-2 text-purple-400" />
                  Total Cluster Memory
                </h4>
                <div className="space-y-3">
                  <div>
                    <div className="flex justify-between text-sm mb-1">
                      <span className="text-gray-400">Allocated</span>
                      <span className="text-white">{formatBytes((cluster?.memory || 0) * 1024 * 1024)}</span>
                    </div>
                    <div className="bg-gray-700 rounded-full h-3">
                      <div className="bg-purple-500 h-3 rounded-full" style={{ width: '100%' }} />
                    </div>
                  </div>
                  <div>
                    <div className="flex justify-between text-sm mb-1">
                      <span className="text-gray-400">Used</span>
                      <span className="text-white">{formatBytes((cluster?.memoryused || 0) * 1024 * 1024)}</span>
                    </div>
                    <div className="bg-gray-700 rounded-full h-3">
                      <div 
                        className="bg-green-500 h-3 rounded-full" 
                        style={{ width: `${cluster?.memory ? (cluster.memoryused / cluster.memory) * 100 : 0}%` }} 
                      />
                    </div>
                  </div>
                </div>
              </div>

              {/* Memory Per Worker */}
              <div className="bg-gray-900/30 rounded-lg p-4">
                <h4 className="text-white font-medium mb-3 flex items-center">
                  <BarChart3 className="h-4 w-4 mr-2 text-blue-400" />
                  Memory Per Worker
                </h4>
                <div className="space-y-2">
                  {workers.map((worker: any, idx: number) => (
                    <div key={worker.id || idx}>
                      <div className="flex justify-between text-xs mb-1">
                        <span className="text-gray-400">Worker {idx + 1}</span>
                        <span className="text-gray-300">
                          {formatBytes((worker.memoryused || 0) * 1024 * 1024)} / {formatBytes((worker.memory || 0) * 1024 * 1024)}
                        </span>
                      </div>
                      <div className="bg-gray-700 rounded-full h-2">
                        <div 
                          className="bg-blue-500 h-2 rounded-full"
                          style={{ width: `${worker.memory ? (worker.memoryused / worker.memory) * 100 : 0}%` }}
                        />
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Quick Reference */}
      <div className="bg-gradient-to-r from-gray-800/50 to-gray-900/50 border border-gray-700 rounded-lg p-4">
        <h3 className="text-white font-medium mb-3 flex items-center">
          <AlertCircle className="h-4 w-4 mr-2 text-yellow-400" />
          Quick Reference: Spark Terminology
        </h3>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3 text-xs">
          <div className="bg-gray-900/50 rounded p-2">
            <span className="text-spark-orange font-medium">Driver:</span>
            <span className="text-gray-400 ml-1">Main process that creates SparkContext and coordinates execution</span>
          </div>
          <div className="bg-gray-900/50 rounded p-2">
            <span className="text-blue-400 font-medium">Executor:</span>
            <span className="text-gray-400 ml-1">Worker process that runs tasks and caches data</span>
          </div>
          <div className="bg-gray-900/50 rounded p-2">
            <span className="text-green-400 font-medium">Task:</span>
            <span className="text-gray-400 ml-1">Smallest unit of work, processes one data partition</span>
          </div>
          <div className="bg-gray-900/50 rounded p-2">
            <span className="text-purple-400 font-medium">Stage:</span>
            <span className="text-gray-400 ml-1">Group of tasks separated by shuffle boundaries</span>
          </div>
          <div className="bg-gray-900/50 rounded p-2">
            <span className="text-yellow-400 font-medium">Shuffle:</span>
            <span className="text-gray-400 ml-1">Data redistribution across partitions (expensive!)</span>
          </div>
          <div className="bg-gray-900/50 rounded p-2">
            <span className="text-cyan-400 font-medium">Partition:</span>
            <span className="text-gray-400 ml-1">Chunk of data processed by one task</span>
          </div>
        </div>
      </div>
    </div>
  )
}
