import { useState, useEffect, useCallback } from 'react'
import useSWR from 'swr'
import { 
  Activity, Server, Cpu, Database, Layers, Shuffle, Grid3X3, Monitor,
  Zap, Network, Box, GitBranch, HardDrive, ChevronDown, ChevronRight,
  Info, RefreshCw, Play, Clock, CheckCircle, XCircle, Loader2, BarChart3,
  ArrowRight, ArrowDown, Settings, FileText, Table, Filter, List, BookOpen,
  Terminal, Code, Rocket
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
  details?: string
}

// Helper function to parse stage name and extract meaningful information
function parseStageInfo(stage: SparkStage): { operation: string; location: string; lineNum: string; scalaClass: string } {
  const name = stage.name || ''
  const details = stage.details || ''
  
  // Pattern: "operation at file:line"
  const match = name.match(/^(.+?)\s+at\s+(.+?):(\d+)$/)
  
  if (match) {
    const [, operation, filePath, lineNum] = match
    // Check if it's <unknown> - if so, try to get info from details
    if (filePath === '<unknown>') {
      // Extract Scala class info from details (e.g., "org.apache.spark.sql.Dataset.count(Dataset.scala:3625)")
      const scalaMatch = details.match(/org\.apache\.spark\.sql\.(\w+)\.(\w+)\(([^:]+):(\d+)\)/)
      if (scalaMatch) {
        const [, className, methodName, scalaFile, scalaLine] = scalaMatch
        return {
          operation: operation.trim(),
          location: `${className}.${methodName}`,
          lineNum: scalaLine,
          scalaClass: scalaFile
        }
      }
      // Fallback for <unknown>
      return {
        operation: operation.trim(),
        location: 'PySpark',
        lineNum: '',
        scalaClass: ''
      }
    }
    
    // Extract just the filename from the path
    const fileName = filePath.split('/').pop() || filePath
    return {
      operation: operation.trim(),
      location: fileName,
      lineNum: lineNum,
      scalaClass: ''
    }
  }
  
  // Handle "<unknown>:0" cases - try to infer from details
  if (name.includes('<unknown>')) {
    const operation = name.split(' at ')[0] || name
    
    // Try to extract from details
    const scalaMatch = details.match(/org\.apache\.spark\.sql\.(\w+)\.(\w+)\(([^:]+):(\d+)\)/)
    if (scalaMatch) {
      const [, className, methodName, scalaFile, scalaLine] = scalaMatch
      return {
        operation: operation.trim(),
        location: `${className}.${methodName}`,
        lineNum: scalaLine,
        scalaClass: scalaFile
      }
    }
    
    return {
      operation: operation.trim(),
      location: 'PySpark',
      lineNum: '',
      scalaClass: ''
    }
  }
  
  // Fallback: just return the name as operation
  return {
    operation: name || 'Unknown Operation',
    location: '',
    lineNum: '',
    scalaClass: ''
  }
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

interface JobTemplate {
  id: string
  name: string
  description: string
  command: string
  icon: string
}

interface ExecutionStep {
  step: number
  name: string
  status: 'pending' | 'active' | 'completed'
  description: string
  timestamp?: string
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

// Job Templates
const jobTemplates: JobTemplate[] = [
  {
    id: 'pi',
    name: 'Calculate Pi',
    description: 'Monte Carlo Pi estimation - distributes random point sampling across workers',
    command: '/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/examples/src/main/python/pi.py 100',
    icon: '🥧'
  },
  {
    id: 'wordcount',
    name: 'Word Count',
    description: 'Classic MapReduce - maps words, shuffles by key, reduces to count',
    command: '/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/wordcount.py',
    icon: '📝'
  },
  {
    id: 'long-demo',
    name: 'Long Running Demo',
    description: 'Multi-stage job with sleeps - great for watching execution flow',
    command: '/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/long_running_demo.py',
    icon: '⏱️'
  },
  {
    id: 'etl-demo',
    name: 'ETL Pipeline Demo',
    description: 'Extract-Transform-Load with multiple stages and shuffles',
    command: '/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/etl_demo.py',
    icon: '🔄'
  },
]

// ═══════════════════════════════════════════════════════════════════════════════
// COLLAPSIBLE SECTION COMPONENT
// ═══════════════════════════════════════════════════════════════════════════════

interface SectionProps {
  title: string
  icon: React.ElementType
  color: string
  badge?: string
  defaultExpanded?: boolean
  children: React.ReactNode
}

function MonitorSection({ title, icon: Icon, color, badge, defaultExpanded = true, children }: SectionProps) {
  const [isExpanded, setIsExpanded] = useState(defaultExpanded)
  
  return (
    <div className="bg-gray-800/50 border border-gray-700 rounded-lg overflow-hidden mb-4">
      <button
        onClick={() => setIsExpanded(!isExpanded)}
        className="w-full flex items-center justify-between p-3 border-b border-gray-700 bg-gray-800/30 hover:bg-gray-700/30 transition-colors"
      >
        <div className="flex items-center">
          {isExpanded ? <ChevronDown className="h-5 w-5 text-gray-400 mr-2" /> : <ChevronRight className="h-5 w-5 text-gray-400 mr-2" />}
          <Icon className={`h-5 w-5 mr-3 ${color}`} />
          <span className="text-white font-medium">{title}</span>
          {badge && <span className="ml-3 text-xs bg-gray-700 text-gray-300 px-2 py-0.5 rounded">{badge}</span>}
        </div>
        <span title="Includes learning content"><BookOpen className="h-4 w-4 text-blue-400" /></span>
      </button>
      {isExpanded && <div className="p-4">{children}</div>}
    </div>
  )
}

// Learning Box Component
function LearnBox({ title, children, color = 'blue' }: { title: string; children: React.ReactNode; color?: string }) {
  const colorClasses: Record<string, string> = {
    blue: 'bg-blue-500/10 border-blue-500/30 text-blue-300',
    green: 'bg-green-500/10 border-green-500/30 text-green-300',
    yellow: 'bg-yellow-500/10 border-yellow-500/30 text-yellow-300',
    purple: 'bg-purple-500/10 border-purple-500/30 text-purple-300',
    cyan: 'bg-cyan-500/10 border-cyan-500/30 text-cyan-300',
    orange: 'bg-orange-500/10 border-orange-500/30 text-orange-300',
  }
  return (
    <div className={`${colorClasses[color]} border rounded-lg p-3 mt-4`}>
      <div className="flex items-start">
        <BookOpen className="h-4 w-4 mr-2 mt-0.5 flex-shrink-0" />
        <div><p className="font-medium text-sm mb-1">{title}</p><div className="text-xs text-gray-300">{children}</div></div>
      </div>
    </div>
  )
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN UNIFIED MONITOR COMPONENT
// ═══════════════════════════════════════════════════════════════════════════════

export default function UnifiedMonitor({ refreshInterval }: Props) {
  const { data: cluster, error: clusterError, mutate: mutateCluster } = useSWR<ClusterStatus>('/api/spark/cluster', fetcher, { refreshInterval: refreshInterval || 0 })
  const { data: applications, mutate: mutateApps } = useSWR<SparkApplication[]>('/api/spark/applications', fetcher, { refreshInterval: refreshInterval || 0 })
  
  const [jobs, setJobs] = useState<SparkJob[]>([])
  const [stages, setStages] = useState<SparkStage[]>([])
  const [executors, setExecutors] = useState<SparkExecutor[]>([])
  
  // Run Job State
  const [isRunSectionOpen, setIsRunSectionOpen] = useState(true)
  const [customCommand, setCustomCommand] = useState('')
  const [selectedTemplate, setSelectedTemplate] = useState<JobTemplate | null>(null)
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [submitResult, setSubmitResult] = useState<{ success: boolean; message: string } | null>(null)
  const [currentJobName, setCurrentJobName] = useState<string | null>(null)
  const [executionSteps, setExecutionSteps] = useState<ExecutionStep[]>([])
  const [jobOutput, setJobOutput] = useState<string[]>([])
  
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
  
  useEffect(() => {
    if (applications?.length) {
      fetchExecutionDetails()
      const interval = setInterval(fetchExecutionDetails, refreshInterval || 5000)
      return () => clearInterval(interval)
    }
  }, [applications, refreshInterval, fetchExecutionDetails])

  // Update execution steps based on job state
  useEffect(() => {
    if (!currentJobName) return
    
    const hasApps = applications && applications.length > 0
    const runningApp = applications?.find(a => !a.attempts?.[0]?.completed)
    const hasJobs = jobs.length > 0
    const hasStages = stages.length > 0
    const hasActiveTasks = jobs.some(j => j.numActiveTasks > 0)
    const allComplete = applications?.[0]?.attempts?.[0]?.completed
    
    setExecutionSteps([
      { step: 1, name: 'spark-submit', status: 'completed', description: 'Job submitted to Spark Master', timestamp: new Date().toLocaleTimeString() },
      { step: 2, name: 'Driver Started', status: hasApps ? 'completed' : 'active', description: 'Driver process launched, parsing code' },
      { step: 3, name: 'DAG Created', status: hasJobs ? 'completed' : (hasApps ? 'active' : 'pending'), description: 'Building execution plan from transformations' },
      { step: 4, name: 'Stages Scheduled', status: hasStages ? 'completed' : (hasJobs ? 'active' : 'pending'), description: 'Breaking DAG into stages at shuffle boundaries' },
      { step: 5, name: 'Tasks Running', status: hasActiveTasks ? 'active' : (hasStages ? 'completed' : 'pending'), description: 'Executing tasks on executor cores' },
      { step: 6, name: 'Shuffle/Aggregate', status: stages.some(s => s.shuffleWriteBytes > 0) ? 'completed' : 'pending', description: 'Moving data between stages' },
      { step: 7, name: 'Results Collected', status: allComplete ? 'completed' : 'pending', description: 'Final output returned to driver' },
    ])
  }, [currentJobName, applications, jobs, stages])

  // Submit job function
  const submitJob = async (command: string, name: string) => {
    setIsSubmitting(true)
    setSubmitResult(null)
    setCurrentJobName(name)
    setJobOutput([`[${new Date().toLocaleTimeString()}] Submitting "${name}"...`])
    setExecutionSteps([
      { step: 1, name: 'spark-submit', status: 'active', description: 'Submitting job to Spark Master...' },
      { step: 2, name: 'Driver Started', status: 'pending', description: 'Waiting for driver to start' },
      { step: 3, name: 'DAG Created', status: 'pending', description: 'Waiting for execution plan' },
      { step: 4, name: 'Stages Scheduled', status: 'pending', description: 'Waiting for stage creation' },
      { step: 5, name: 'Tasks Running', status: 'pending', description: 'Waiting for task execution' },
      { step: 6, name: 'Shuffle/Aggregate', status: 'pending', description: 'Waiting for shuffle' },
      { step: 7, name: 'Results Collected', status: 'pending', description: 'Waiting for completion' },
    ])
    
    try {
      const response = await fetch('/api/spark/submit', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ command, name, jobId: `job-${Date.now()}` }),
      })
      const result = await response.json()
      
      if (response.ok) {
        setSubmitResult({ success: true, message: `Job "${name}" submitted! Watch the execution flow below.` })
        setJobOutput(prev => [...prev, `[${new Date().toLocaleTimeString()}] ✓ Job submitted successfully`])
        mutateApps()
      } else {
        setSubmitResult({ success: false, message: result.error || 'Failed to submit job' })
        setJobOutput(prev => [...prev, `[${new Date().toLocaleTimeString()}] ✗ Error: ${result.error}`])
        setCurrentJobName(null)
      }
    } catch (error: any) {
      setSubmitResult({ success: false, message: error.message || 'Network error' })
      setJobOutput(prev => [...prev, `[${new Date().toLocaleTimeString()}] ✗ Network error`])
      setCurrentJobName(null)
    } finally {
      setIsSubmitting(false)
    }
  }

  // Clear/Reset function
  const clearExecution = () => {
    setCurrentJobName(null)
    setExecutionSteps([])
    setSubmitResult(null)
    setSelectedTemplate(null)
    setCustomCommand('')
    setJobOutput([])
  }

  // Select template (populate command, don't run)
  const selectTemplate = (template: JobTemplate) => {
    setSelectedTemplate(template)
    setCustomCommand(template.command)
    setSubmitResult(null)
  }

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
        <p className="text-gray-400 text-sm">Make sure the Spark cluster is running.</p>
        <button onClick={() => mutateCluster()} className="mt-4 text-spark-orange hover:underline">Retry</button>
      </div>
    )
  }

  return (
    <div className="space-y-4">
      
      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {/* RUN JOB SECTION - At the top */}
      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      <div className="bg-gradient-to-r from-spark-orange/20 to-purple-900/20 border border-spark-orange/30 rounded-lg overflow-hidden">
        <button
          onClick={() => setIsRunSectionOpen(!isRunSectionOpen)}
          className="w-full flex items-center justify-between p-4 hover:bg-gray-800/30 transition-colors"
        >
          <div className="flex items-center">
            {isRunSectionOpen ? <ChevronDown className="h-5 w-5 text-spark-orange mr-2" /> : <ChevronRight className="h-5 w-5 text-spark-orange mr-2" />}
            <Rocket className="h-6 w-6 text-spark-orange mr-3" />
            <div className="text-left">
              <span className="text-white font-bold text-lg">🚀 Run a Spark Job</span>
              <p className="text-gray-400 text-xs">Submit a job and watch the complete execution flow in real-time</p>
            </div>
          </div>
          <Play className="h-6 w-6 text-spark-orange" />
        </button>
        
        {isRunSectionOpen && (
          <div className="p-4 border-t border-spark-orange/30">
            {/* Job Templates - Click to populate, Run button to execute */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-3 mb-4">
              {jobTemplates.map((template) => (
                <button
                  key={template.id}
                  onClick={() => {
                    setCustomCommand(template.command)
                    setSelectedTemplate(template)
                  }}
                  className={`bg-gray-800/50 border rounded-lg p-3 hover:border-spark-orange/50 hover:bg-gray-800 transition-all text-left ${
                    selectedTemplate?.id === template.id ? 'border-spark-orange ring-1 ring-spark-orange' : 'border-gray-700'
                  }`}
                >
                  <div className="flex items-center mb-2">
                    <span className="text-2xl mr-2">{template.icon}</span>
                    <span className="text-white font-medium text-sm">{template.name}</span>
                  </div>
                  <p className="text-gray-400 text-xs">{template.description}</p>
                  {selectedTemplate?.id === template.id && (
                    <div className="mt-2 text-xs text-spark-orange flex items-center">
                      <CheckCircle className="h-3 w-3 mr-1" /> Selected
                    </div>
                  )}
                </button>
              ))}
            </div>
            
            {/* Custom Command */}
            <div className="bg-gray-900/50 rounded-lg p-3 mb-4">
              <div className="flex items-center mb-2">
                <Terminal className="h-4 w-4 text-green-400 mr-2" />
                <span className="text-white text-sm font-medium">Custom Command</span>
              </div>
              <div className="flex gap-2">
                <input
                  type="text"
                  value={customCommand}
                  onChange={(e) => setCustomCommand(e.target.value)}
                  placeholder="/opt/spark/bin/spark-submit --master spark://spark-master:7077 /path/to/script.py"
                  className="flex-1 bg-gray-800 border border-gray-600 rounded px-3 py-2 text-white text-sm font-mono placeholder-gray-500 focus:border-spark-orange focus:outline-none"
                />
                <button
                  onClick={() => submitJob(customCommand, selectedTemplate?.name || 'Custom Job')}
                  disabled={isSubmitting || !customCommand.trim()}
                  className="bg-spark-orange hover:bg-spark-orange/80 disabled:bg-gray-600 text-white px-4 py-2 rounded flex items-center text-sm font-medium"
                >
                  {isSubmitting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4 mr-1" />}
                  Run
                </button>
                {(customCommand || selectedTemplate || submitResult || currentJobName) && (
                  <button
                    onClick={() => {
                      setCustomCommand('')
                      setSelectedTemplate(null)
                      setSubmitResult(null)
                      setCurrentJobName(null)
                      setExecutionSteps([])
                    }}
                    className="bg-gray-700 hover:bg-gray-600 text-white px-4 py-2 rounded flex items-center text-sm font-medium"
                  >
                    <XCircle className="h-4 w-4 mr-1" />
                    Clear
                  </button>
                )}
              </div>
            </div>
            
            {/* What spark-submit does */}
            <LearnBox title="📚 What does spark-submit do?" color="orange">
              <p className="mb-2"><code className="bg-gray-800 px-1 rounded">spark-submit</code> is the command that launches your Spark application:</p>
              <ol className="list-decimal list-inside space-y-1 ml-2 text-xs">
                <li><strong>--master spark://host:7077</strong> → Connects to the Spark Master (cluster manager)</li>
                <li><strong>script.py</strong> → Your Python code containing transformations (map, filter, groupBy)</li>
                <li>Master allocates <strong>executors</strong> on worker nodes</li>
                <li>Driver sends <strong>serialized code</strong> to all executors</li>
                <li>Each executor runs tasks on its <strong>data partitions</strong></li>
              </ol>
            </LearnBox>
            
            {/* Submit Result */}
            {submitResult && (
              <div className={`mt-4 p-3 rounded-lg flex items-center ${submitResult.success ? 'bg-green-500/20 border border-green-500/50' : 'bg-red-500/20 border border-red-500/50'}`}>
                {submitResult.success ? <CheckCircle className="h-5 w-5 text-green-400 mr-2" /> : <XCircle className="h-5 w-5 text-red-400 mr-2" />}
                <span className={submitResult.success ? 'text-green-300' : 'text-red-300'}>{submitResult.message}</span>
              </div>
            )}
          </div>
        )}
      </div>

      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {/* EXECUTION FLOW - Shows real-time progress when job is running */}
      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {currentJobName && executionSteps.length > 0 && (
        <div className="bg-gradient-to-r from-blue-900/30 to-purple-900/30 border border-blue-700/50 rounded-lg p-4">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-center">
              <Activity className="h-5 w-5 text-blue-400 mr-2 animate-pulse" />
              <h3 className="text-white font-bold">Execution Flow: {currentJobName}</h3>
            </div>
            <button onClick={() => { setCurrentJobName(null); setExecutionSteps([]) }} className="text-gray-400 hover:text-white text-sm">Clear</button>
          </div>
          
          {/* Step-by-step flow */}
          <div className="relative">
            <div className="absolute left-4 top-0 bottom-0 w-0.5 bg-gray-700"></div>
            <div className="space-y-3">
              {executionSteps.map((step, idx) => (
                <div key={step.step} className="flex items-start ml-0">
                  <div className={`relative z-10 w-8 h-8 rounded-full flex items-center justify-center text-xs font-bold mr-3 ${
                    step.status === 'completed' ? 'bg-green-500 text-white' :
                    step.status === 'active' ? 'bg-blue-500 text-white animate-pulse' :
                    'bg-gray-700 text-gray-400'
                  }`}>
                    {step.status === 'completed' ? '✓' : step.step}
                  </div>
                  <div className="flex-1 bg-gray-800/50 rounded-lg p-3">
                    <div className="flex items-center justify-between">
                      <span className={`font-medium text-sm ${step.status === 'active' ? 'text-blue-300' : step.status === 'completed' ? 'text-green-300' : 'text-gray-400'}`}>
                        {step.name}
                      </span>
                      {step.status === 'active' && <Loader2 className="h-4 w-4 text-blue-400 animate-spin" />}
                      {step.status === 'completed' && <CheckCircle className="h-4 w-4 text-green-400" />}
                    </div>
                    <p className="text-xs text-gray-400 mt-1">{step.description}</p>
                  </div>
                </div>
              ))}
            </div>
          </div>
          
          {/* Live Stats */}
          <div className="grid grid-cols-4 gap-3 mt-4 pt-4 border-t border-gray-700">
            <div className="bg-gray-800/50 rounded p-2 text-center">
              <p className="text-lg font-bold text-white">{jobs.length}</p>
              <p className="text-xs text-gray-400">Jobs</p>
            </div>
            <div className="bg-gray-800/50 rounded p-2 text-center">
              <p className="text-lg font-bold text-white">{stages.length}</p>
              <p className="text-xs text-gray-400">Stages</p>
            </div>
            <div className="bg-gray-800/50 rounded p-2 text-center">
              <p className="text-lg font-bold text-green-400">{completedTasks}<span className="text-gray-500">/{totalTasks}</span></p>
              <p className="text-xs text-gray-400">Tasks</p>
            </div>
            <div className="bg-gray-800/50 rounded p-2 text-center">
              <p className="text-lg font-bold text-yellow-400">{formatBytes(totalShuffleRead + totalShuffleWrite)}</p>
              <p className="text-xs text-gray-400">Shuffle</p>
            </div>
          </div>
        </div>
      )}

      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {/* SECTION 1: THE DRIVER */}
      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      <MonitorSection title="🧠 The Driver - Your Job's Brain" icon={Monitor} color="text-yellow-400" badge="Master Node">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
          <div className="bg-yellow-500/10 border border-yellow-500/30 rounded-lg p-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-yellow-400 font-medium text-sm">Driver Status</span>
              <span className={`px-2 py-0.5 rounded text-xs ${cluster?.status === 'ALIVE' ? 'bg-green-500/20 text-green-300' : 'bg-red-500/20 text-red-300'}`}>
                {cluster?.status || 'Unknown'}
              </span>
            </div>
            <p className="text-xl font-bold text-white">{cluster?.url?.split('//')[1] || 'spark-master:7077'}</p>
            <p className="text-xs text-gray-400 mt-1">Spark Master URL</p>
          </div>
          <div className="bg-gray-900/50 rounded-lg p-4 col-span-2">
            <h4 className="text-white font-medium mb-2 text-sm">Applications ({applications?.length || 0})</h4>
            {applications && applications.length > 0 ? (
              <div className="space-y-1 max-h-24 overflow-y-auto">
                {applications.slice(0, 3).map((app) => (
                  <div key={app.id} className="flex items-center justify-between bg-gray-800/50 rounded p-2 text-xs">
                    <div className="flex items-center">
                      {app.attempts?.[0]?.completed ? <CheckCircle className="h-3 w-3 text-green-400 mr-2" /> : <Loader2 className="h-3 w-3 text-blue-400 mr-2 animate-spin" />}
                      <span className="text-white">{app.name}</span>
                    </div>
                    <span className={`px-2 py-0.5 rounded ${app.attempts?.[0]?.completed ? 'bg-green-500/20 text-green-300' : 'bg-blue-500/20 text-blue-300'}`}>
                      {app.attempts?.[0]?.completed ? 'Done' : 'Running'}
                    </span>
                  </div>
                ))}
              </div>
            ) : <p className="text-gray-400 text-xs">No applications submitted</p>}
          </div>
        </div>
        <LearnBox title="📚 What is the Driver?" color="yellow">
          <p className="mb-2">The <strong>Driver</strong> is the main process running your PySpark application:</p>
          <ul className="list-disc list-inside space-y-1 ml-2">
            <li>Parses your Python/PySpark code</li>
            <li>Builds the DAG (Directed Acyclic Graph) execution plan</li>
            <li>Splits work into stages and tasks</li>
            <li>Coordinates all executors and collects results</li>
            <li>Runs on <code className="bg-gray-800 px-1 rounded">spark-master</code> container in this cluster</li>
          </ul>
        </LearnBox>
      </MonitorSection>

      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {/* SECTION 2: WORKER NODES */}
      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      <MonitorSection title="🖥️ Worker Nodes - The Cluster Machines" icon={Server} color="text-blue-400" badge={`${cluster?.aliveworkers || 0} Alive`}>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {cluster?.workers?.map((worker) => (
            <div key={worker.id} className="bg-gray-900/50 border border-gray-700 rounded-lg p-4">
              <div className="flex items-center justify-between mb-2">
                <span className="text-white font-medium text-sm">Worker</span>
                <span className={`text-xs px-2 py-0.5 rounded ${worker.state === 'ALIVE' ? 'bg-green-500/20 text-green-300' : 'bg-red-500/20 text-red-300'}`}>{worker.state}</span>
              </div>
              <p className="text-xs text-gray-400 font-mono mb-2">{worker.host}:{worker.port}</p>
              <div className="space-y-2">
                <div>
                  <div className="flex justify-between text-xs mb-1"><span className="text-gray-400">CPU</span><span className="text-white">{worker.coresused}/{worker.cores}</span></div>
                  <div className="h-2 bg-gray-700 rounded-full overflow-hidden"><div className="h-full bg-green-500" style={{ width: `${worker.cores > 0 ? (worker.coresused / worker.cores) * 100 : 0}%` }} /></div>
                </div>
                <div>
                  <div className="flex justify-between text-xs mb-1"><span className="text-gray-400">Memory</span><span className="text-white">{formatBytes(worker.memoryused * 1024 * 1024)}/{formatBytes(worker.memory * 1024 * 1024)}</span></div>
                  <div className="h-2 bg-gray-700 rounded-full overflow-hidden"><div className="h-full bg-purple-500" style={{ width: `${worker.memory > 0 ? (worker.memoryused / worker.memory) * 100 : 0}%` }} /></div>
                </div>
              </div>
            </div>
          ))}
        </div>
        <LearnBox title="📚 What are Worker Nodes?" color="blue">
          <p>Worker Nodes are the machines in your cluster. Each worker contributes CPU cores and memory. In Docker, each <code className="bg-gray-800 px-1 rounded">spark-worker-N</code> container simulates a separate machine. Workers host <strong>Executors</strong> which actually run your tasks.</p>
        </LearnBox>
      </MonitorSection>

      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {/* SECTION 3: EXECUTORS */}
      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      <MonitorSection title="⚙️ Executors - The Actual Workers" icon={Cpu} color="text-green-400" badge={`${executors.filter(e => e.id !== 'driver').length} Active`}>
        {executors.filter(e => e.id !== 'driver').length > 0 ? (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            {executors.filter(e => e.id !== 'driver').map((executor) => (
              <div key={executor.id} className="bg-gray-900/50 border border-gray-700 rounded-lg p-3">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-white font-medium text-sm">Executor {executor.id}</span>
                  <span className={`text-xs px-2 py-0.5 rounded ${executor.isActive ? 'bg-green-500/20 text-green-300' : 'bg-gray-500/20 text-gray-300'}`}>{executor.isActive ? 'Active' : 'Idle'}</span>
                </div>
                <div className="grid grid-cols-3 gap-2 text-center text-xs mb-2">
                  <div className="bg-blue-500/10 rounded p-1"><p className="text-blue-400 font-bold">{executor.activeTasks}</p><p className="text-gray-500">Active</p></div>
                  <div className="bg-green-500/10 rounded p-1"><p className="text-green-400 font-bold">{executor.completedTasks}</p><p className="text-gray-500">Done</p></div>
                  <div className="bg-red-500/10 rounded p-1"><p className="text-red-400 font-bold">{executor.failedTasks}</p><p className="text-gray-500">Failed</p></div>
                </div>
                <div className="text-xs"><span className="text-gray-400">Memory:</span> <span className="text-white">{formatBytes(executor.memoryUsed)}/{formatBytes(executor.maxMemory)}</span></div>
                <div className="text-xs"><span className="text-gray-400">RDD Blocks:</span> <span className="text-cyan-400">{executor.rddBlocks}</span></div>
              </div>
            ))}
          </div>
        ) : (
          <div className="text-center py-6 text-gray-400"><Cpu className="h-10 w-10 mx-auto mb-2 opacity-50" /><p>No executors active - run a job to see executors</p></div>
        )}
        <LearnBox title="📚 What are Executors?" color="green">
          <p>Executors are JVM processes on worker nodes that actually run your code. Each has dedicated memory split into: <span className="text-blue-300">Execution Memory</span> (shuffles, joins), <span className="text-purple-300">Storage Memory</span> (cached RDDs), and <span className="text-yellow-300">User Memory</span> (your objects).</p>
        </LearnBox>
      </MonitorSection>

      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {/* SECTION 4: RDD & PARTITIONS */}
      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      <MonitorSection title="📦 RDD & Partitions - Your Distributed Data" icon={Database} color="text-cyan-400" badge="Data Layer">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div className="bg-cyan-500/10 border border-cyan-500/30 rounded-lg p-4">
            <h4 className="text-cyan-400 font-medium mb-2 text-sm">What is RDD?</h4>
            <ul className="space-y-1 text-xs text-gray-300">
              <li><span className="text-cyan-400 font-bold">R</span>esilient - Recovers from failures via lineage</li>
              <li><span className="text-cyan-400 font-bold">D</span>istributed - Spread across cluster nodes</li>
              <li><span className="text-cyan-400 font-bold">D</span>ataset - Collection of records/rows</li>
            </ul>
          </div>
          <div className="bg-gray-900/50 rounded-lg p-4">
            <h4 className="text-white font-medium mb-2 text-sm">RDD Blocks Cached</h4>
            {executors.some(e => e.rddBlocks > 0) ? (
              <div className="flex flex-wrap gap-2">
                {executors.filter(e => e.id !== 'driver' && e.rddBlocks > 0).map(e => (
                  <span key={e.id} className="bg-cyan-500/20 text-cyan-300 px-2 py-1 rounded text-xs">Executor {e.id}: {e.rddBlocks} blocks</span>
                ))}
              </div>
            ) : <p className="text-gray-400 text-xs">No RDD blocks cached</p>}
          </div>
        </div>
        <LearnBox title="📚 Understanding Partitions" color="cyan">
          <p>Your data is split into <strong>Partitions</strong> - chunks processed in parallel. Each partition lives on one executor. More partitions = more parallelism. Ideal size: 128-256MB. Use <code className="bg-gray-800 px-1 rounded">repartition(n)</code> to adjust.</p>
        </LearnBox>
      </MonitorSection>

      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {/* SECTION 5: TASKS */}
      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      <MonitorSection title="📦 Tasks - Smallest Unit of Work" icon={Box} color="text-green-400" badge={`${completedTasks}/${totalTasks}`}>
        <div className="grid grid-cols-4 gap-4 mb-4">
          <div className="bg-gray-900/50 rounded-lg p-3 text-center"><p className="text-2xl font-bold text-white">{totalTasks}</p><p className="text-xs text-gray-400">Total</p></div>
          <div className="bg-blue-500/10 rounded-lg p-3 text-center"><p className="text-2xl font-bold text-blue-400">{activeTasks}</p><p className="text-xs text-gray-400">Active</p></div>
          <div className="bg-green-500/10 rounded-lg p-3 text-center"><p className="text-2xl font-bold text-green-400">{completedTasks}</p><p className="text-xs text-gray-400">Done</p></div>
          <div className="bg-red-500/10 rounded-lg p-3 text-center"><p className="text-2xl font-bold text-red-400">{jobs.reduce((sum, j) => sum + (j.numFailedTasks || 0), 0)}</p><p className="text-xs text-gray-400">Failed</p></div>
        </div>
        {totalTasks > 0 && (
          <div className="mb-4">
            <div className="flex justify-between text-sm mb-1"><span className="text-gray-400">Progress</span><span className="text-white">{Math.round((completedTasks / totalTasks) * 100)}%</span></div>
            <div className="h-4 bg-gray-700 rounded-full overflow-hidden"><div className="h-full bg-gradient-to-r from-blue-500 to-green-500 transition-all" style={{ width: `${(completedTasks / totalTasks) * 100}%` }} /></div>
          </div>
        )}
        <LearnBox title="📚 What are Tasks?" color="green">
          <p>A <strong>Task</strong> processes ONE partition of data. Tasks run in parallel across executor cores. Each task: 1) Reads partition, 2) Applies transformations (map, filter), 3) Writes output. Failed tasks are retried on other executors.</p>
        </LearnBox>
      </MonitorSection>

      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {/* SECTION 6: STAGES */}
      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      <MonitorSection title="📊 Stages - Shuffle Boundaries" icon={Layers} color="text-purple-400" badge={`${stages.length} Stages`}>
        {stages.length > 0 ? (
          <>
            {/* Stage Summary Stats */}
            <div className="grid grid-cols-4 gap-3 mb-4">
              <div className="bg-purple-500/10 border border-purple-500/30 rounded-lg p-3 text-center">
                <p className="text-xl font-bold text-white">{stages.length}</p>
                <p className="text-xs text-gray-400">Total Stages</p>
              </div>
              <div className="bg-green-500/10 border border-green-500/30 rounded-lg p-3 text-center">
                <p className="text-xl font-bold text-green-400">{stages.filter(s => s.status === 'COMPLETE').length}</p>
                <p className="text-xs text-gray-400">Completed</p>
              </div>
              <div className="bg-blue-500/10 border border-blue-500/30 rounded-lg p-3 text-center">
                <p className="text-xl font-bold text-blue-400">{stages.filter(s => s.status === 'ACTIVE').length}</p>
                <p className="text-xs text-gray-400">Running</p>
              </div>
              <div className="bg-gray-500/10 border border-gray-500/30 rounded-lg p-3 text-center">
                <p className="text-xl font-bold text-gray-400">{stages.filter(s => s.status === 'PENDING' || s.status === 'SKIPPED').length}</p>
                <p className="text-xs text-gray-400">Pending/Skipped</p>
              </div>
            </div>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead><tr className="text-gray-400 text-xs border-b border-gray-700"><th className="text-left p-2">Stage</th><th className="text-left p-2">Name</th><th className="text-center p-2">Tasks</th><th className="text-center p-2">Input/Output</th><th className="text-center p-2">Shuffle R/W</th><th className="text-center p-2">Duration</th><th className="text-center p-2">Status</th></tr></thead>
                <tbody>
                  {stages.slice(0, 12).map((stage) => (
                    <tr key={`${stage.stageId}-${stage.attemptId}`} className="border-b border-gray-800 hover:bg-gray-800/30">
                      <td className="p-2 text-gray-300 font-mono">{stage.stageId}</td>
                      <td className="p-2 text-white truncate max-w-xs text-xs">
                        {(() => {
                          const info = parseStageInfo(stage)
                          return (
                            <div>
                              <span className="font-medium text-blue-300">{info.operation}</span>
                              {info.location && (
                                <span className="text-gray-400 ml-1">
                                  @ <span className="font-mono text-green-400">{info.location}</span>
                                  {info.lineNum && <span className="text-gray-500">:{info.lineNum}</span>}
                                </span>
                              )}
                            </div>
                          )
                        })()}
                      </td>
                      <td className="p-2 text-center">
                        <span className="text-green-400">{stage.numCompleteTasks}</span>
                        <span className="text-gray-500">/{stage.numTasks}</span>
                        {stage.numFailedTasks > 0 && <span className="text-red-400 ml-1">({stage.numFailedTasks} failed)</span>}
                      </td>
                      <td className="p-2 text-center text-xs">
                        <span className="text-cyan-300">{formatBytes(stage.inputBytes || 0)}</span>
                        <span className="text-gray-500">/</span>
                        <span className="text-pink-300">{formatBytes(stage.outputBytes || 0)}</span>
                      </td>
                      <td className="p-2 text-center text-xs">
                        <span className="text-yellow-300">{formatBytes(stage.shuffleReadBytes)}</span>
                        <span className="text-gray-500">/</span>
                        <span className="text-orange-300">{formatBytes(stage.shuffleWriteBytes)}</span>
                      </td>
                      <td className="p-2 text-center text-xs text-gray-300">
                        {stage.executorRunTime ? `${(stage.executorRunTime / 1000).toFixed(1)}s` : '-'}
                      </td>
                      <td className="p-2 text-center">
                        <span className={`text-xs px-2 py-0.5 rounded ${
                          stage.status === 'ACTIVE' ? 'bg-blue-500/20 text-blue-300 animate-pulse' : 
                          stage.status === 'COMPLETE' ? 'bg-green-500/20 text-green-300' : 
                          stage.status === 'PENDING' ? 'bg-yellow-500/20 text-yellow-300' :
                          stage.status === 'SKIPPED' ? 'bg-gray-500/20 text-gray-300' :
                          'bg-gray-500/20 text-gray-300'
                        }`}>{stage.status}</span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {stages.length > 12 && <p className="text-gray-400 text-xs text-center mt-2">Showing 12 of {stages.length} stages...</p>}
            </div>
          </>
        ) : (
          <div className="text-center py-6 text-gray-400">
            <Layers className="h-10 w-10 mx-auto mb-2 opacity-50" />
            <p className="mb-2">No stages - run a job to see stages</p>
            <p className="text-xs text-gray-500">Stages appear when a job is running. Each stage contains tasks that can run in parallel.</p>
          </div>
        )}
        <LearnBox title="📚 What are Stages?" color="purple">
          <p className="mb-2">A <strong>Stage</strong> is a set of tasks that can run without shuffling. Spark creates a new stage at each <em>wide</em> transformation (groupBy, join).</p>
          <ul className="list-disc list-inside space-y-1 text-xs ml-2">
            <li><strong>Input/Output</strong>: Data read from source / written to next stage or storage</li>
            <li><strong>Shuffle R/W</strong>: Data exchanged between nodes (expensive!)</li>
            <li><strong>Fewer stages = less shuffling = faster jobs</strong></li>
          </ul>
        </LearnBox>
      </MonitorSection>

      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {/* SECTION 7: SHUFFLE */}
      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      <MonitorSection title="🔀 Shuffle - Data Movement" icon={Shuffle} color="text-yellow-400" badge={formatBytes(totalShuffleRead + totalShuffleWrite)}>
        <div className="grid grid-cols-2 gap-4 mb-4">
          <div className="bg-yellow-500/10 border border-yellow-500/30 rounded-lg p-4 text-center">
            <ArrowDown className="h-6 w-6 text-yellow-400 mx-auto mb-2" />
            <p className="text-2xl font-bold text-white">{formatBytes(totalShuffleRead)}</p>
            <p className="text-xs text-gray-400">Shuffle Read</p>
            <p className="text-xs text-gray-500 mt-1">Data pulled from other executors</p>
          </div>
          <div className="bg-orange-500/10 border border-orange-500/30 rounded-lg p-4 text-center">
            <ArrowRight className="h-6 w-6 text-orange-400 mx-auto mb-2" />
            <p className="text-2xl font-bold text-white">{formatBytes(totalShuffleWrite)}</p>
            <p className="text-xs text-gray-400">Shuffle Write</p>
            <p className="text-xs text-gray-500 mt-1">Data sent to other executors</p>
          </div>
        </div>
        
        {/* Explanation for 0B shuffle */}
        {totalShuffleRead === 0 && totalShuffleWrite === 0 && (
          <div className="bg-gray-800/50 border border-gray-700 rounded-lg p-3 mb-4">
            <div className="flex items-start">
              <Activity className="h-5 w-5 text-gray-400 mr-2 mt-0.5" />
              <div>
                <p className="text-gray-300 text-sm font-medium">Why is shuffle showing 0B?</p>
                <ul className="text-gray-400 text-xs mt-1 space-y-1">
                  <li>• <strong>No job running:</strong> Shuffle only occurs during job execution</li>
                  <li>• <strong>Narrow transformations only:</strong> map(), filter(), flatMap() don't require shuffle</li>
                  <li>• <strong>Single partition:</strong> If data fits in one partition, no shuffle needed</li>
                  <li>• <strong>Cached data:</strong> Reading from cache may skip shuffle</li>
                </ul>
                <p className="text-gray-500 text-xs mt-2 italic">Run a job with groupBy, join, or reduceByKey to see shuffle metrics!</p>
              </div>
            </div>
          </div>
        )}
        
        {/* Shuffle per stage breakdown when data exists */}
        {stages.some(s => s.shuffleReadBytes > 0 || s.shuffleWriteBytes > 0) && (
          <div className="bg-gray-800/50 rounded-lg p-3 mb-4">
            <h4 className="text-white text-sm font-medium mb-2">Shuffle by Stage</h4>
            <div className="space-y-2">
              {stages.filter(s => s.shuffleReadBytes > 0 || s.shuffleWriteBytes > 0).slice(0, 5).map(stage => {
                const info = parseStageInfo(stage)
                return (
                  <div key={stage.stageId} className="flex items-center justify-between text-xs bg-gray-900/50 rounded p-2">
                    <span className="text-gray-300">
                      Stage {stage.stageId}: <span className="text-blue-300">{info.operation}</span>
                      {info.location && info.location !== 'internal' && (
                        <span className="text-gray-500 ml-1">@ {info.location}{info.lineNum && `:${info.lineNum}`}</span>
                      )}
                    </span>
                    <div className="flex gap-3">
                      <span className="text-yellow-300">↓{formatBytes(stage.shuffleReadBytes)}</span>
                      <span className="text-orange-300">↑{formatBytes(stage.shuffleWriteBytes)}</span>
                    </div>
                  </div>
                )
              })}
            </div>
          </div>
        )}
        
        <LearnBox title="📚 What is Shuffle?" color="yellow">
          <p className="mb-2"><strong>Shuffle</strong> is when data moves between nodes - the most expensive operation!</p>
          <div className="grid grid-cols-2 gap-2 text-xs mb-2">
            <div className="bg-gray-800/50 rounded p-2">
              <p className="text-yellow-300 font-medium">Causes Shuffle:</p>
              <ul className="text-gray-400 mt-1">
                <li>• groupBy(), groupByKey()</li>
                <li>• join(), cogroup()</li>
                <li>• reduceByKey(), aggregateByKey()</li>
                <li>• repartition(), coalesce()</li>
              </ul>
            </div>
            <div className="bg-gray-800/50 rounded p-2">
              <p className="text-green-300 font-medium">Minimize Shuffle:</p>
              <ul className="text-gray-400 mt-1">
                <li>• Filter early (less data to move)</li>
                <li>• Broadcast small tables</li>
                <li>• Use reduceByKey over groupByKey</li>
                <li>• Partition by join key</li>
              </ul>
            </div>
          </div>
        </LearnBox>
      </MonitorSection>

      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {/* SECTION 8: MAP & TRANSFORMATIONS */}
      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      <MonitorSection title="🔧 Map & Transformations" icon={Filter} color="text-green-400" badge="Operations" defaultExpanded={false}>
        {/* Live Transformation Output - when job is running */}
        {stages.length > 0 && (
          <div className="bg-gray-800/50 border border-gray-700 rounded-lg p-4 mb-4">
            <h4 className="text-white font-medium mb-3 flex items-center">
              <Activity className="h-4 w-4 text-green-400 mr-2 animate-pulse" />
              Live Transformations from Current Job
            </h4>
            <div className="space-y-2 max-h-48 overflow-y-auto">
              {stages.map((stage, idx) => {
                // Parse transformation from stage name
                const stageName = stage.name || ''
                const transformations: string[] = []
                
                // Detect common transformations
                if (stageName.includes('map') || stageName.includes('Map')) transformations.push('map')
                if (stageName.includes('filter') || stageName.includes('Filter')) transformations.push('filter')
                if (stageName.includes('flatMap') || stageName.includes('FlatMap')) transformations.push('flatMap')
                if (stageName.includes('groupBy') || stageName.includes('GroupBy')) transformations.push('groupBy')
                if (stageName.includes('reduceBy') || stageName.includes('ReduceBy')) transformations.push('reduceByKey')
                if (stageName.includes('join') || stageName.includes('Join')) transformations.push('join')
                if (stageName.includes('agg') || stageName.includes('Agg')) transformations.push('aggregate')
                if (stageName.includes('count') || stageName.includes('Count')) transformations.push('count')
                if (stageName.includes('collect') || stageName.includes('Collect')) transformations.push('collect')
                if (stageName.includes('write') || stageName.includes('Write') || stageName.includes('save')) transformations.push('write')
                if (stageName.includes('show') || stageName.includes('Show')) transformations.push('show')
                if (stageName.includes('take') || stageName.includes('Take')) transformations.push('take')
                if (stageName.includes('distinct') || stageName.includes('Distinct')) transformations.push('distinct')
                if (stageName.includes('union') || stageName.includes('Union')) transformations.push('union')
                if (stageName.includes('sort') || stageName.includes('Sort') || stageName.includes('orderBy')) transformations.push('sort')
                if (stageName.includes('cache') || stageName.includes('Cache') || stageName.includes('persist')) transformations.push('cache')
                
                const isWide = transformations.some(t => ['groupBy', 'reduceByKey', 'join', 'aggregate', 'distinct', 'sort'].includes(t))
                
                return (
                  <div key={`${stage.stageId}-${stage.attemptId}`} className={`flex items-center justify-between p-2 rounded text-xs ${
                    stage.status === 'ACTIVE' ? 'bg-blue-500/20 border border-blue-500/50' : 
                    stage.status === 'COMPLETE' ? 'bg-green-500/10 border border-green-500/30' : 
                    'bg-gray-900/50 border border-gray-700'
                  }`}>
                    <div className="flex items-center">
                      <span className={`w-6 h-6 rounded-full flex items-center justify-center text-xs mr-2 ${
                        stage.status === 'COMPLETE' ? 'bg-green-500 text-white' :
                        stage.status === 'ACTIVE' ? 'bg-blue-500 text-white animate-pulse' :
                        'bg-gray-600 text-gray-300'
                      }`}>
                        {stage.status === 'COMPLETE' ? '✓' : stage.stageId}
                      </span>
                      <div>
                        {(() => {
                          const info = parseStageInfo(stage)
                          return (
                            <span className="truncate max-w-xs block">
                              <span className="text-blue-300 font-medium">{info.operation}</span>
                              {info.location && info.location !== 'internal' && (
                                <span className="text-gray-400 ml-1">
                                  @ <span className="text-green-400">{info.location}</span>
                                  {info.lineNum && <span className="text-gray-500">:{info.lineNum}</span>}
                                </span>
                              )}
                              {info.location === 'internal' && (
                                <span className="text-gray-500 ml-1">(internal)</span>
                              )}
                            </span>
                          )
                        })()}
                        {transformations.length > 0 && (
                          <div className="flex flex-wrap gap-1 mt-1">
                            {transformations.map((t, i) => (
                              <span key={i} className={`px-1.5 py-0.5 rounded text-xs ${
                                ['groupBy', 'reduceByKey', 'join', 'aggregate', 'distinct', 'sort'].includes(t) 
                                  ? 'bg-yellow-500/20 text-yellow-300' 
                                  : 'bg-green-500/20 text-green-300'
                              }`}>
                                {t}{['groupBy', 'reduceByKey', 'join', 'aggregate', 'distinct', 'sort'].includes(t) ? ' ⚠️' : ' ✓'}
                              </span>
                            ))}
                          </div>
                        )}
                      </div>
                    </div>
                    <div className="text-right">
                      <span className={`px-2 py-0.5 rounded text-xs ${
                        stage.status === 'ACTIVE' ? 'bg-blue-500/30 text-blue-300' :
                        stage.status === 'COMPLETE' ? 'bg-green-500/30 text-green-300' :
                        'bg-gray-500/30 text-gray-400'
                      }`}>
                        {stage.status}
                      </span>
                      {isWide && <span className="block text-yellow-400 text-xs mt-1">Shuffle!</span>}
                    </div>
                  </div>
                )
              })}
            </div>
          </div>
        )}
        
        <div className="grid grid-cols-2 gap-4">
          <div className="bg-green-500/10 border border-green-500/30 rounded-lg p-3">
            <h4 className="text-green-400 font-medium mb-2 text-sm">Narrow (No Shuffle) ✓</h4>
            <div className="space-y-1 text-xs">
              <div className="flex justify-between bg-gray-800/50 rounded p-1"><code className="text-green-400">.map()</code><span className="text-gray-400">1 → 1 transform</span></div>
              <div className="flex justify-between bg-gray-800/50 rounded p-1"><code className="text-green-400">.filter()</code><span className="text-gray-400">Keep matching</span></div>
              <div className="flex justify-between bg-gray-800/50 rounded p-1"><code className="text-green-400">.flatMap()</code><span className="text-gray-400">1 → many</span></div>
              <div className="flex justify-between bg-gray-800/50 rounded p-1"><code className="text-green-400">.mapPartitions()</code><span className="text-gray-400">Batch per partition</span></div>
              <div className="flex justify-between bg-gray-800/50 rounded p-1"><code className="text-green-400">.union()</code><span className="text-gray-400">Combine RDDs</span></div>
            </div>
          </div>
          <div className="bg-yellow-500/10 border border-yellow-500/30 rounded-lg p-3">
            <h4 className="text-yellow-400 font-medium mb-2 text-sm">Wide (Shuffle!) ⚠️</h4>
            <div className="space-y-1 text-xs">
              <div className="flex justify-between bg-gray-800/50 rounded p-1"><code className="text-yellow-400">.groupBy()</code><span className="text-gray-400">Group by key</span></div>
              <div className="flex justify-between bg-gray-800/50 rounded p-1"><code className="text-yellow-400">.join()</code><span className="text-gray-400">Combine DFs</span></div>
              <div className="flex justify-between bg-gray-800/50 rounded p-1"><code className="text-yellow-400">.reduceByKey()</code><span className="text-gray-400">Aggregate by key</span></div>
              <div className="flex justify-between bg-gray-800/50 rounded p-1"><code className="text-yellow-400">.distinct()</code><span className="text-gray-400">Remove dups</span></div>
              <div className="flex justify-between bg-gray-800/50 rounded p-1"><code className="text-yellow-400">.orderBy()</code><span className="text-gray-400">Sort globally</span></div>
            </div>
          </div>
        </div>
        <LearnBox title="📚 Lazy Evaluation & DAG" color="green">
          <p className="mb-2">Transformations don't execute immediately! Spark builds a <strong>Directed Acyclic Graph (DAG)</strong> of operations.</p>
          <div className="bg-gray-800/50 rounded p-2 text-xs font-mono">
            <span className="text-gray-400"># Nothing happens yet...</span><br />
            <span className="text-blue-300">df</span> = spark.read.csv(<span className="text-green-300">"data.csv"</span>)<br />
            <span className="text-blue-300">df2</span> = df.filter(col(<span className="text-green-300">"age"</span>) {'>'} 21)<br />
            <span className="text-blue-300">df3</span> = df2.groupBy(<span className="text-green-300">"city"</span>).count()<br />
            <span className="text-gray-400"># NOW execution happens!</span><br />
            df3.<span className="text-yellow-300">show()</span>  <span className="text-gray-400"># Action triggers DAG execution</span>
          </div>
        </LearnBox>
      </MonitorSection>

      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {/* SECTION 9: TASK SCHEDULING */}
      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      <MonitorSection title="📋 Task Scheduling & Resources" icon={Settings} color="text-blue-400" badge="Scheduler" defaultExpanded={false}>
        {/* Live Task Distribution */}
        {jobs.length > 0 && (
          <div className="bg-blue-500/10 border border-blue-500/30 rounded-lg p-4 mb-4">
            <h4 className="text-white font-medium mb-3 flex items-center">
              <Activity className="h-4 w-4 text-blue-400 mr-2 animate-pulse" />
              Live Task Distribution
            </h4>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              <div className="bg-gray-800/50 rounded p-3 text-center">
                <p className="text-2xl font-bold text-white">{totalTasks}</p>
                <p className="text-xs text-gray-400">Total Tasks</p>
              </div>
              <div className="bg-gray-800/50 rounded p-3 text-center">
                <p className="text-2xl font-bold text-blue-400">{jobs.reduce((sum, j) => sum + j.numActiveTasks, 0)}</p>
                <p className="text-xs text-gray-400">Active</p>
              </div>
              <div className="bg-gray-800/50 rounded p-3 text-center">
                <p className="text-2xl font-bold text-green-400">{completedTasks}</p>
                <p className="text-xs text-gray-400">Completed</p>
              </div>
              <div className="bg-gray-800/50 rounded p-3 text-center">
                <p className="text-2xl font-bold text-red-400">{jobs.reduce((sum, j) => sum + j.numFailedTasks, 0)}</p>
                <p className="text-xs text-gray-400">Failed</p>
              </div>
            </div>
            
            {/* Task per Executor */}
            {executors.length > 0 && (
              <div className="mt-4">
                <h5 className="text-gray-300 text-sm mb-2">Tasks per Executor</h5>
                <div className="space-y-2">
                  {executors.filter(e => e.id !== 'driver').map(executor => (
                    <div key={executor.id} className="flex items-center bg-gray-900/50 rounded p-2">
                      <span className="text-gray-400 text-xs w-24 truncate">{executor.id}</span>
                      <div className="flex-1 mx-3 h-4 bg-gray-700 rounded-full overflow-hidden">
                        <div 
                          className="h-full bg-gradient-to-r from-blue-500 to-green-500" 
                          style={{ width: `${executor.totalCores > 0 ? (executor.activeTasks / executor.totalCores) * 100 : 0}%` }}
                        />
                      </div>
                      <span className="text-white text-xs w-20 text-right">
                        {executor.activeTasks || 0}/{executor.totalCores} cores
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}
        
        <div className="grid grid-cols-2 gap-4">
          <div className="bg-gray-900/50 rounded-lg p-4">
            <h4 className="text-white font-medium mb-2 text-sm">Cluster Resources</h4>
            <div className="space-y-2">
              <div>
                <div className="flex justify-between text-xs mb-1">
                  <span className="text-gray-400">Cores</span>
                  <span className="text-white">{cluster?.coresused || 0}/{cluster?.cores || 0} used</span>
                </div>
                <div className="h-2 bg-gray-700 rounded-full">
                  <div className="h-full bg-green-500 rounded-full transition-all duration-500" style={{ width: `${cluster?.cores ? (cluster.coresused / cluster.cores) * 100 : 0}%` }} />
                </div>
              </div>
              <div>
                <div className="flex justify-between text-xs mb-1">
                  <span className="text-gray-400">Memory</span>
                  <span className="text-white">{formatBytes((cluster?.memoryused || 0) * 1024 * 1024)}/{formatBytes((cluster?.memory || 0) * 1024 * 1024)}</span>
                </div>
                <div className="h-2 bg-gray-700 rounded-full">
                  <div className="h-full bg-purple-500 rounded-full transition-all duration-500" style={{ width: `${cluster?.memory ? (cluster.memoryused / cluster.memory) * 100 : 0}%` }} />
                </div>
              </div>
              <div>
                <div className="flex justify-between text-xs mb-1">
                  <span className="text-gray-400">Workers</span>
                  <span className="text-white">{cluster?.aliveworkers || 0} alive</span>
                </div>
              </div>
            </div>
          </div>
          <div className="bg-blue-500/10 border border-blue-500/30 rounded-lg p-4">
            <h4 className="text-blue-400 font-medium mb-2 text-sm">Scheduling Pipeline</h4>
            <ol className="space-y-2 text-xs text-gray-300">
              <li className="flex items-center"><span className="w-5 h-5 rounded-full bg-blue-600 text-white flex items-center justify-center text-xs mr-2">1</span>Driver builds DAG from code</li>
              <li className="flex items-center"><span className="w-5 h-5 rounded-full bg-blue-600 text-white flex items-center justify-center text-xs mr-2">2</span>DAG Scheduler splits at shuffles</li>
              <li className="flex items-center"><span className="w-5 h-5 rounded-full bg-blue-600 text-white flex items-center justify-center text-xs mr-2">3</span>Task Scheduler → executors</li>
              <li className="flex items-center"><span className="w-5 h-5 rounded-full bg-blue-600 text-white flex items-center justify-center text-xs mr-2">4</span>Tasks run on data partitions</li>
            </ol>
          </div>
        </div>
        <LearnBox title="📚 Data Locality" color="blue">
          <p>Spark tries to run tasks where data already exists: <span className="text-green-300">NODE_LOCAL</span> (same node) {'>'} <span className="text-yellow-300">RACK_LOCAL</span> {'>'} <span className="text-red-300">ANY</span>. This minimizes network transfer.</p>
        </LearnBox>
      </MonitorSection>

      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {/* SECTION 10: AGGREGATION & OUTPUT */}
      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      <MonitorSection title="📤 Aggregation & Output" icon={FileText} color="text-orange-400" badge="Results" defaultExpanded={false}>
        <div className="grid grid-cols-2 gap-4">
          <div className="bg-orange-500/10 border border-orange-500/30 rounded-lg p-3">
            <h4 className="text-orange-400 font-medium mb-2 text-sm">Actions (Trigger Execution)</h4>
            <div className="space-y-1 text-xs">
              <div className="flex justify-between bg-gray-800/50 rounded p-1"><code className="text-orange-400">.collect()</code><span className="text-gray-400">All to driver ⚠️</span></div>
              <div className="flex justify-between bg-gray-800/50 rounded p-1"><code className="text-orange-400">.count()</code><span className="text-gray-400">Count rows</span></div>
              <div className="flex justify-between bg-gray-800/50 rounded p-1"><code className="text-orange-400">.write.parquet()</code><span className="text-gray-400">Save to storage ✓</span></div>
            </div>
          </div>
          <div className="bg-gray-900/50 rounded-lg p-3">
            <h4 className="text-white font-medium mb-2 text-sm">Job Results</h4>
            {jobs.length > 0 ? (
              <div className="space-y-1">
                {jobs.slice(0, 3).map(job => (
                  <div key={job.jobId} className="flex justify-between text-xs bg-gray-800/50 rounded p-1">
                    <span className="text-gray-300">Job {job.jobId}</span>
                    <span className={`px-2 rounded ${job.status === 'SUCCEEDED' ? 'bg-green-500/20 text-green-300' : job.status === 'RUNNING' ? 'bg-blue-500/20 text-blue-300' : 'bg-gray-500/20 text-gray-300'}`}>{job.status}</span>
                  </div>
                ))}
              </div>
            ) : <p className="text-gray-400 text-xs">No jobs completed</p>}
          </div>
        </div>
        <LearnBox title="📚 Output Best Practices" color="orange">
          <p>Use <code className="bg-gray-800 px-1 rounded">.take(n)</code> instead of <code className="bg-gray-800 px-1 rounded">.collect()</code> for sampling. Write large results to Parquet. Use <code className="bg-gray-800 px-1 rounded">.cache()</code> if reusing intermediate results.</p>
        </LearnBox>
      </MonitorSection>

    </div>
  )
}
