import { useState, useEffect, useCallback } from 'react'
import { 
  Play, Square, RefreshCw, Upload, FileCode, Clock, CheckCircle, XCircle, 
  Loader2, Trash2, FolderOpen, ChevronDown, ChevronRight, Activity, 
  Cpu, Database, Layers, ArrowRight, Info, BarChart3, Zap, Server
} from 'lucide-react'

interface JobTemplate {
  id: string
  name: string
  description: string
  command: string
  args?: string[]
  icon: string
}

interface RunningJob {
  id: string
  name: string
  status: 'running' | 'completed' | 'failed' | 'pending'
  startTime: string
  duration?: number
  output?: string
  appId?: string  // Spark application ID for tracking
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
  completionTime?: string
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
}

interface ExecutionDetails {
  appId: string
  appName: string
  jobs: SparkJob[]
  stages: SparkStage[]
  executors: SparkExecutor[]
  lastUpdated: string
}

const defaultTemplates: JobTemplate[] = [
  {
    id: 'pi',
    name: 'Calculate Pi',
    description: 'Classic Spark Pi calculation example - demonstrates distributed computation',
    command: '/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/examples/src/main/python/pi.py 100',
    icon: '🥧'
  },
  {
    id: 'wordcount',
    name: 'Word Count',
    description: 'Count words in a sample text file - shows map/reduce pattern',
    command: '/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/wordcount.py',
    icon: '📝'
  },
  {
    id: 'long-demo',
    name: 'Long Running Demo',
    description: 'A demo job that runs for several minutes - good for watching progress',
    command: '/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/long_running_demo.py',
    icon: '⏱️'
  },
  {
    id: 'etl-demo',
    name: 'ETL Pipeline Demo',
    description: 'Sample ETL pipeline with transformations - demonstrates stages',
    command: '/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/etl_demo.py',
    icon: '🔄'
  },
]

interface Props {
  refreshInterval?: number | null
}

// Format bytes to human readable
function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i]
}

// Format duration in ms to readable
function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`
  return `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`
}

export default function RunJobs({ refreshInterval }: Props) {
  const [templates] = useState<JobTemplate[]>(defaultTemplates)
  const [runningJobs, setRunningJobs] = useState<RunningJob[]>([])
  const [customCommand, setCustomCommand] = useState('')
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [submitResult, setSubmitResult] = useState<{ success: boolean; message: string } | null>(null)
  const [activeSection, setActiveSection] = useState<'templates' | 'custom' | 'files'>('templates')
  
  // Execution tracking state
  const [executionDetails, setExecutionDetails] = useState<ExecutionDetails | null>(null)
  const [expandedJobId, setExpandedJobId] = useState<string | null>(null)
  const [isPolling, setIsPolling] = useState(false)

  // Fetch live execution details from Spark API
  const fetchExecutionDetails = useCallback(async () => {
    try {
      // Get applications
      const appsRes = await fetch('/api/spark/applications')
      if (!appsRes.ok) return
      const apps = await appsRes.json()
      
      // Find running application
      const runningApp = apps.find((app: any) => 
        app.attempts?.[0]?.completed === false || 
        (Date.now() - new Date(app.attempts?.[0]?.startTime).getTime()) < 300000 // within 5 min
      )
      
      if (!runningApp) {
        // Check if there's a recently completed app
        const recentApp = apps[0]
        if (recentApp) {
          const appId = recentApp.id
          const [jobsRes, stagesRes, executorsRes] = await Promise.all([
            fetch(`/api/spark/jobs?appId=${appId}`),
            fetch(`/api/spark/stages?appId=${appId}`),
            fetch(`/api/spark/executors?appId=${appId}`)
          ])
          
          const jobs = jobsRes.ok ? await jobsRes.json() : []
          const stages = stagesRes.ok ? await stagesRes.json() : []
          const executors = executorsRes.ok ? await executorsRes.json() : []
          
          setExecutionDetails({
            appId,
            appName: recentApp.name || 'Spark Application',
            jobs: Array.isArray(jobs) ? jobs : [],
            stages: Array.isArray(stages) ? stages : [],
            executors: Array.isArray(executors) ? executors : [],
            lastUpdated: new Date().toISOString()
          })
        }
        return
      }
      
      const appId = runningApp.id
      
      // Fetch jobs, stages, and executors in parallel
      const [jobsRes, stagesRes, executorsRes] = await Promise.all([
        fetch(`/api/spark/jobs?appId=${appId}`),
        fetch(`/api/spark/stages?appId=${appId}`),
        fetch(`/api/spark/executors?appId=${appId}`)
      ])
      
      const jobs = jobsRes.ok ? await jobsRes.json() : []
      const stages = stagesRes.ok ? await stagesRes.json() : []
      const executors = executorsRes.ok ? await executorsRes.json() : []
      
      setExecutionDetails({
        appId,
        appName: runningApp.name || 'Spark Application',
        jobs: Array.isArray(jobs) ? jobs : [],
        stages: Array.isArray(stages) ? stages : [],
        executors: Array.isArray(executors) ? executors : [],
        lastUpdated: new Date().toISOString()
      })
      
    } catch (error) {
      console.error('Failed to fetch execution details:', error)
    }
  }, [])

  // Poll for updates when a job is running
  useEffect(() => {
    const hasRunningJob = runningJobs.some(j => j.status === 'running' || j.status === 'pending')
    
    if (hasRunningJob || isPolling) {
      fetchExecutionDetails()
      const interval = setInterval(fetchExecutionDetails, 2000) // Poll every 2 seconds
      return () => clearInterval(interval)
    }
  }, [runningJobs, isPolling, fetchExecutionDetails])

  // Submit a job
  const submitJob = async (command: string, name: string) => {
    setIsSubmitting(true)
    setSubmitResult(null)
    setIsPolling(true)
    setExpandedJobId(null)
    
    const jobId = `job-${Date.now()}`
    const newJob: RunningJob = {
      id: jobId,
      name: name,
      status: 'pending',
      startTime: new Date().toISOString(),
    }
    
    setRunningJobs(prev => [newJob, ...prev])
    setExpandedJobId(jobId) // Auto-expand the new job
    
    try {
      const response = await fetch('/api/spark/submit', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ command, jobId }),
      })
      
      const result = await response.json()
      
      if (response.ok) {
        setRunningJobs(prev => 
          prev.map(j => j.id === jobId ? { ...j, status: 'running', appId: result.appId } : j)
        )
        setSubmitResult({ success: true, message: `Job "${name}" submitted! Watch the progress below.` })
        
        // Keep polling for a while after submission
        setTimeout(() => setIsPolling(false), 60000)
      } else {
        setRunningJobs(prev => 
          prev.map(j => j.id === jobId ? { ...j, status: 'failed', output: result.error } : j)
        )
        setSubmitResult({ success: false, message: result.error || 'Failed to submit job' })
        setIsPolling(false)
      }
    } catch (error: any) {
      setRunningJobs(prev => 
        prev.map(j => j.id === jobId ? { ...j, status: 'failed', output: error.message } : j)
      )
      setSubmitResult({ success: false, message: error.message || 'Network error' })
      setIsPolling(false)
    } finally {
      setIsSubmitting(false)
    }
  }

  const clearCompletedJobs = () => {
    setRunningJobs(prev => prev.filter(j => j.status === 'running' || j.status === 'pending'))
  }

  // Calculate overall progress
  const getOverallProgress = () => {
    if (!executionDetails?.jobs?.length) return 0
    const totalTasks = executionDetails.jobs.reduce((sum, j) => sum + (j.numTasks || 0), 0)
    const completedTasks = executionDetails.jobs.reduce((sum, j) => sum + (j.numCompletedTasks || 0), 0)
    return totalTasks > 0 ? Math.round((completedTasks / totalTasks) * 100) : 0
  }

  return (
    <div className="space-y-6">
      {/* Section Tabs */}
      <div className="flex space-x-2 border-b border-gray-700 pb-2">
        <button
          onClick={() => setActiveSection('templates')}
          className={`px-4 py-2 rounded-t-lg flex items-center ${
            activeSection === 'templates' 
              ? 'bg-gray-700 text-white' 
              : 'text-gray-400 hover:text-white'
          }`}
        >
          <FileCode className="h-4 w-4 mr-2" />
          Job Templates
        </button>
        <button
          onClick={() => setActiveSection('custom')}
          className={`px-4 py-2 rounded-t-lg flex items-center ${
            activeSection === 'custom' 
              ? 'bg-gray-700 text-white' 
              : 'text-gray-400 hover:text-white'
          }`}
        >
          <Play className="h-4 w-4 mr-2" />
          Custom Command
        </button>
        <button
          onClick={() => setActiveSection('files')}
          className={`px-4 py-2 rounded-t-lg flex items-center ${
            activeSection === 'files' 
              ? 'bg-gray-700 text-white' 
              : 'text-gray-400 hover:text-white'
          }`}
        >
          <FolderOpen className="h-4 w-4 mr-2" />
          Browse Files
        </button>
      </div>

      {/* Submit Result Message */}
      {submitResult && (
        <div className={`p-4 rounded-lg flex items-center ${
          submitResult.success ? 'bg-green-500/20 border border-green-500/50' : 'bg-red-500/20 border border-red-500/50'
        }`}>
          {submitResult.success ? (
            <CheckCircle className="h-5 w-5 text-green-400 mr-2" />
          ) : (
            <XCircle className="h-5 w-5 text-red-400 mr-2" />
          )}
          <span className={submitResult.success ? 'text-green-300' : 'text-red-300'}>
            {submitResult.message}
          </span>
          <button
            onClick={() => setSubmitResult(null)}
            className="ml-auto text-gray-400 hover:text-white"
          >
            ✕
          </button>
        </div>
      )}

      {/* Templates Section */}
      {activeSection === 'templates' && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {templates.map((template) => (
            <div
              key={template.id}
              className="bg-gray-800/50 border border-gray-700 rounded-lg p-4 hover:border-spark-orange/50 transition-colors"
            >
              <div className="flex items-start justify-between mb-3">
                <div className="flex items-center">
                  <span className="text-2xl mr-3">{template.icon}</span>
                  <div>
                    <h3 className="text-white font-medium">{template.name}</h3>
                    <p className="text-gray-400 text-sm">{template.description}</p>
                  </div>
                </div>
              </div>
              
              <div className="bg-gray-900/50 rounded p-2 mb-3 font-mono text-xs text-gray-400 overflow-x-auto">
                {template.command}
              </div>
              
              <button
                onClick={() => submitJob(template.command, template.name)}
                disabled={isSubmitting}
                className="w-full bg-spark-orange hover:bg-spark-orange/80 disabled:bg-gray-600 text-white py-2 px-4 rounded-lg flex items-center justify-center transition-colors"
              >
                {isSubmitting ? (
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                ) : (
                  <Play className="h-4 w-4 mr-2" />
                )}
                Run Job
              </button>
            </div>
          ))}
        </div>
      )}

      {/* Custom Command Section */}
      {activeSection === 'custom' && (
        <div className="bg-gray-800/50 border border-gray-700 rounded-lg p-6">
          <h3 className="text-white font-medium mb-4 flex items-center">
            <Play className="h-5 w-5 mr-2 text-spark-orange" />
            Submit Custom Spark Job
          </h3>
          
          <div className="space-y-4">
            <div>
              <label className="block text-sm text-gray-400 mb-2">
                spark-submit command (runs inside spark-master container)
              </label>
              <textarea
                value={customCommand}
                onChange={(e) => setCustomCommand(e.target.value)}
                placeholder="/opt/spark/bin/spark-submit --master spark://spark-master:7077 /path/to/your/script.py"
                className="w-full bg-gray-900 border border-gray-600 rounded-lg p-3 text-white font-mono text-sm placeholder-gray-500 focus:border-spark-orange focus:ring-1 focus:ring-spark-orange"
                rows={4}
              />
            </div>
            
            <div className="flex space-x-3">
              <button
                onClick={() => submitJob(customCommand, 'Custom Job')}
                disabled={isSubmitting || !customCommand.trim()}
                className="bg-spark-orange hover:bg-spark-orange/80 disabled:bg-gray-600 text-white py-2 px-6 rounded-lg flex items-center transition-colors"
              >
                {isSubmitting ? (
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                ) : (
                  <Play className="h-4 w-4 mr-2" />
                )}
                Submit Job
              </button>
              
              <button
                onClick={() => setCustomCommand('')}
                className="bg-gray-700 hover:bg-gray-600 text-white py-2 px-4 rounded-lg"
              >
                Clear
              </button>
            </div>
          </div>
          
          {/* Quick Examples */}
          <div className="mt-6 pt-4 border-t border-gray-700">
            <h4 className="text-sm text-gray-400 mb-3">Quick Examples:</h4>
            <div className="space-y-2">
              {[
                { label: 'Pi Calculation', cmd: '/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/examples/src/main/python/pi.py 100' },
                { label: 'PySpark Shell', cmd: '/opt/spark/bin/pyspark --master spark://spark-master:7077' },
              ].map((example, idx) => (
                <button
                  key={idx}
                  onClick={() => setCustomCommand(example.cmd)}
                  className="block w-full text-left text-sm text-gray-400 hover:text-white p-2 rounded bg-gray-900/50 hover:bg-gray-900 transition-colors"
                >
                  <span className="text-spark-orange">{example.label}:</span>
                  <code className="ml-2 text-xs">{example.cmd.substring(0, 60)}...</code>
                </button>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Browse Files Section */}
      {activeSection === 'files' && (
        <div className="bg-gray-800/50 border border-gray-700 rounded-lg p-6">
          <h3 className="text-white font-medium mb-4 flex items-center">
            <FolderOpen className="h-5 w-5 mr-2 text-spark-orange" />
            Available Spark Applications
          </h3>
          
          <div className="text-gray-400 text-sm mb-4">
            Files mounted in <code className="bg-gray-900 px-2 py-1 rounded">/opt/spark-apps/</code> inside the Spark container
          </div>
          
          <div className="bg-gray-900/50 rounded-lg p-4">
            <div className="space-y-2">
              {[
                { name: 'long_running_demo.py', desc: 'Demo job with progress tracking', size: '2.4 KB' },
                { name: 'wordcount.py', desc: 'Word count example', size: '1.1 KB' },
                { name: 'etl_demo.py', desc: 'ETL pipeline demonstration', size: '3.2 KB' },
                { name: 'streaming_demo.py', desc: 'Spark Streaming example', size: '2.8 KB' },
              ].map((file, idx) => (
                <div
                  key={idx}
                  className="flex items-center justify-between p-3 rounded hover:bg-gray-800 transition-colors"
                >
                  <div className="flex items-center">
                    <FileCode className="h-4 w-4 text-blue-400 mr-3" />
                    <div>
                      <span className="text-white">{file.name}</span>
                      <p className="text-xs text-gray-500">{file.desc}</p>
                    </div>
                  </div>
                  <div className="flex items-center space-x-3">
                    <span className="text-xs text-gray-500">{file.size}</span>
                    <button
                      onClick={() => submitJob(
                        `/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/${file.name}`,
                        file.name
                      )}
                      disabled={isSubmitting}
                      className="bg-spark-orange/20 hover:bg-spark-orange/40 text-spark-orange py-1 px-3 rounded text-sm flex items-center"
                    >
                      <Play className="h-3 w-3 mr-1" />
                      Run
                    </button>
                  </div>
                </div>
              ))}
            </div>
          </div>
          
          <div className="mt-4 p-3 bg-blue-500/10 border border-blue-500/30 rounded-lg">
            <p className="text-blue-300 text-sm">
              💡 <strong>Tip:</strong> Mount your own Python scripts to <code className="bg-gray-900 px-1 rounded">/opt/spark-apps/</code> 
              by adding them to <code className="bg-gray-900 px-1 rounded">docker/spark-cluster/apps/</code>
            </p>
          </div>
        </div>
      )}

      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {/* LIVE EXECUTION DETAILS - Shows real-time progress when jobs are running */}
      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {executionDetails && (runningJobs.some(j => j.status === 'running' || j.status === 'pending') || isPolling) && (
        <div className="bg-gradient-to-r from-blue-900/30 to-purple-900/30 border border-blue-700/50 rounded-lg overflow-hidden">
          <div className="p-4 border-b border-blue-700/50">
            <div className="flex items-center justify-between">
              <div className="flex items-center">
                <Activity className="h-5 w-5 text-blue-400 mr-2 animate-pulse" />
                <h3 className="text-white font-medium">Live Execution Monitor</h3>
                <span className="ml-3 text-xs text-gray-400">
                  App: {executionDetails.appId}
                </span>
              </div>
              <div className="flex items-center space-x-4">
                <span className="text-xs text-gray-400">
                  Updated: {new Date(executionDetails.lastUpdated).toLocaleTimeString()}
                </span>
                <button
                  onClick={fetchExecutionDetails}
                  className="text-gray-400 hover:text-white"
                >
                  <RefreshCw className="h-4 w-4" />
                </button>
              </div>
            </div>
            
            {/* Overall Progress Bar */}
            <div className="mt-4">
              <div className="flex items-center justify-between mb-1">
                <span className="text-sm text-gray-300">Overall Progress</span>
                <span className="text-sm text-white font-medium">{getOverallProgress()}%</span>
              </div>
              <div className="h-3 bg-gray-700 rounded-full overflow-hidden">
                <div 
                  className="h-full bg-gradient-to-r from-blue-500 to-green-500 transition-all duration-500"
                  style={{ width: `${getOverallProgress()}%` }}
                />
              </div>
            </div>
          </div>

          {/* Summary Cards */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 p-4 border-b border-blue-700/50">
            <div className="bg-gray-800/50 rounded-lg p-3 text-center">
              <div className="flex items-center justify-center mb-1">
                <Layers className="h-4 w-4 text-purple-400 mr-1" />
                <span className="text-xs text-gray-400">Jobs</span>
              </div>
              <p className="text-xl font-bold text-white">{executionDetails.jobs?.length || 0}</p>
              <p className="text-xs text-gray-500">
                {executionDetails.jobs?.filter(j => j.status === 'RUNNING').length || 0} running
              </p>
            </div>
            <div className="bg-gray-800/50 rounded-lg p-3 text-center">
              <div className="flex items-center justify-center mb-1">
                <BarChart3 className="h-4 w-4 text-blue-400 mr-1" />
                <span className="text-xs text-gray-400">Stages</span>
              </div>
              <p className="text-xl font-bold text-white">{executionDetails.stages?.length || 0}</p>
              <p className="text-xs text-gray-500">
                {executionDetails.stages?.filter(s => s.status === 'ACTIVE').length || 0} active
              </p>
            </div>
            <div className="bg-gray-800/50 rounded-lg p-3 text-center">
              <div className="flex items-center justify-center mb-1">
                <Cpu className="h-4 w-4 text-green-400 mr-1" />
                <span className="text-xs text-gray-400">Tasks</span>
              </div>
              <p className="text-xl font-bold text-white">
                {executionDetails.jobs?.reduce((sum, j) => sum + (j.numCompletedTasks || 0), 0) || 0}
                <span className="text-sm text-gray-400">
                  /{executionDetails.jobs?.reduce((sum, j) => sum + (j.numTasks || 0), 0) || 0}
                </span>
              </p>
              <p className="text-xs text-gray-500">completed</p>
            </div>
            <div className="bg-gray-800/50 rounded-lg p-3 text-center">
              <div className="flex items-center justify-center mb-1">
                <Server className="h-4 w-4 text-orange-400 mr-1" />
                <span className="text-xs text-gray-400">Executors</span>
              </div>
              <p className="text-xl font-bold text-white">
                {executionDetails.executors?.filter(e => e.isActive && e.id !== 'driver').length || 0}
              </p>
              <p className="text-xs text-gray-500">active</p>
            </div>
          </div>

          {/* Jobs Detail Table */}
          {executionDetails.jobs?.length > 0 && (
            <div className="p-4 border-b border-blue-700/50">
              <h4 className="text-white font-medium mb-3 flex items-center">
                <Layers className="h-4 w-4 mr-2 text-purple-400" />
                Spark Jobs
                <span className="ml-2 text-xs text-gray-400">(Click for stage details)</span>
              </h4>
              <div className="space-y-2">
                {executionDetails.jobs.slice().reverse().map((job) => (
                  <div key={job.jobId} className="bg-gray-800/50 rounded-lg overflow-hidden">
                    <div 
                      className="p-3 cursor-pointer hover:bg-gray-700/50 transition-colors"
                      onClick={() => setExpandedJobId(expandedJobId === `spark-${job.jobId}` ? null : `spark-${job.jobId}`)}
                    >
                      <div className="flex items-center justify-between">
                        <div className="flex items-center">
                          {expandedJobId === `spark-${job.jobId}` ? (
                            <ChevronDown className="h-4 w-4 text-gray-400 mr-2" />
                          ) : (
                            <ChevronRight className="h-4 w-4 text-gray-400 mr-2" />
                          )}
                          <span className="text-gray-400 text-sm mr-2">Job {job.jobId}</span>
                          <span className="text-white text-sm truncate max-w-md">{job.name || 'Spark Job'}</span>
                        </div>
                        <div className="flex items-center space-x-4">
                          {/* Task Progress */}
                          <div className="flex items-center">
                            <div className="w-24 h-2 bg-gray-700 rounded-full overflow-hidden mr-2">
                              <div 
                                className={`h-full ${job.status === 'SUCCEEDED' ? 'bg-green-500' : job.status === 'FAILED' ? 'bg-red-500' : 'bg-blue-500'}`}
                                style={{ width: `${job.numTasks > 0 ? (job.numCompletedTasks / job.numTasks) * 100 : 0}%` }}
                              />
                            </div>
                            <span className="text-xs text-gray-400 w-16">
                              {job.numCompletedTasks}/{job.numTasks}
                            </span>
                          </div>
                          {/* Status Badge */}
                          <span className={`text-xs px-2 py-1 rounded ${
                            job.status === 'RUNNING' ? 'bg-blue-500/20 text-blue-300' :
                            job.status === 'SUCCEEDED' ? 'bg-green-500/20 text-green-300' :
                            job.status === 'FAILED' ? 'bg-red-500/20 text-red-300' :
                            'bg-gray-500/20 text-gray-300'
                          }`}>
                            {job.status}
                          </span>
                        </div>
                      </div>
                      
                      {/* Expanded Task Breakdown */}
                      {expandedJobId === `spark-${job.jobId}` && (
                        <div className="mt-3 pt-3 border-t border-gray-700 grid grid-cols-4 gap-3 text-center text-xs">
                          <div className="bg-blue-500/10 rounded p-2">
                            <p className="text-blue-400 font-medium">{job.numActiveTasks}</p>
                            <p className="text-gray-500">Active</p>
                          </div>
                          <div className="bg-green-500/10 rounded p-2">
                            <p className="text-green-400 font-medium">{job.numCompletedTasks}</p>
                            <p className="text-gray-500">Completed</p>
                          </div>
                          <div className="bg-red-500/10 rounded p-2">
                            <p className="text-red-400 font-medium">{job.numFailedTasks}</p>
                            <p className="text-gray-500">Failed</p>
                          </div>
                          <div className="bg-gray-500/10 rounded p-2">
                            <p className="text-gray-300 font-medium">{job.numTasks}</p>
                            <p className="text-gray-500">Total</p>
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Stages Detail Table */}
          {executionDetails.stages?.length > 0 && (
            <div className="p-4 border-b border-blue-700/50">
              <h4 className="text-white font-medium mb-3 flex items-center">
                <BarChart3 className="h-4 w-4 mr-2 text-blue-400" />
                Active & Recent Stages
              </h4>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-gray-400 text-xs border-b border-gray-700">
                      <th className="text-left p-2">Stage</th>
                      <th className="text-left p-2">Name</th>
                      <th className="text-center p-2">Tasks</th>
                      <th className="text-center p-2">Input</th>
                      <th className="text-center p-2">Shuffle Read</th>
                      <th className="text-center p-2">Shuffle Write</th>
                      <th className="text-center p-2">Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {executionDetails.stages.slice(0, 10).map((stage) => (
                      <tr key={`${stage.stageId}-${stage.attemptId}`} className="border-b border-gray-800 hover:bg-gray-800/50">
                        <td className="p-2 text-gray-300">{stage.stageId}</td>
                        <td className="p-2 text-white truncate max-w-xs">{stage.name}</td>
                        <td className="p-2 text-center">
                          <span className="text-green-400">{stage.numCompleteTasks}</span>
                          <span className="text-gray-500">/{stage.numTasks}</span>
                        </td>
                        <td className="p-2 text-center text-gray-300">{formatBytes(stage.inputBytes || 0)}</td>
                        <td className="p-2 text-center text-yellow-300">{formatBytes(stage.shuffleReadBytes || 0)}</td>
                        <td className="p-2 text-center text-orange-300">{formatBytes(stage.shuffleWriteBytes || 0)}</td>
                        <td className="p-2 text-center">
                          <span className={`text-xs px-2 py-1 rounded ${
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
            </div>
          )}

          {/* Executor Status */}
          {executionDetails.executors?.filter(e => e.id !== 'driver').length > 0 && (
            <div className="p-4">
              <h4 className="text-white font-medium mb-3 flex items-center">
                <Server className="h-4 w-4 mr-2 text-orange-400" />
                Executor Activity
              </h4>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
                {executionDetails.executors.filter(e => e.id !== 'driver').map((executor) => (
                  <div key={executor.id} className="bg-gray-800/50 rounded-lg p-3">
                    <div className="flex items-center justify-between mb-2">
                      <span className="text-white text-sm font-medium">Executor {executor.id}</span>
                      <span className={`text-xs px-2 py-0.5 rounded ${
                        executor.isActive ? 'bg-green-500/20 text-green-300' : 'bg-gray-500/20 text-gray-300'
                      }`}>
                        {executor.isActive ? 'Active' : 'Idle'}
                      </span>
                    </div>
                    <div className="text-xs text-gray-400 mb-2">{executor.hostPort}</div>
                    <div className="grid grid-cols-3 gap-2 text-center text-xs">
                      <div>
                        <p className="text-blue-400 font-medium">{executor.activeTasks}</p>
                        <p className="text-gray-500">Active</p>
                      </div>
                      <div>
                        <p className="text-green-400 font-medium">{executor.completedTasks}</p>
                        <p className="text-gray-500">Done</p>
                      </div>
                      <div>
                        <p className="text-red-400 font-medium">{executor.failedTasks}</p>
                        <p className="text-gray-500">Failed</p>
                      </div>
                    </div>
                    {/* Memory Usage */}
                    <div className="mt-2">
                      <div className="flex justify-between text-xs text-gray-400 mb-1">
                        <span>Memory</span>
                        <span>{formatBytes(executor.memoryUsed || 0)} / {formatBytes(executor.maxMemory || 0)}</span>
                      </div>
                      <div className="h-1.5 bg-gray-700 rounded-full overflow-hidden">
                        <div 
                          className="h-full bg-purple-500"
                          style={{ width: `${executor.maxMemory > 0 ? (executor.memoryUsed / executor.maxMemory) * 100 : 0}%` }}
                        />
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* What's Happening Explanation */}
          <div className="p-4 bg-gray-800/30 border-t border-blue-700/50">
            <div className="flex items-start">
              <Info className="h-4 w-4 text-blue-400 mr-2 mt-0.5 flex-shrink-0" />
              <div className="text-xs text-gray-400">
                <p className="mb-1">
                  <strong className="text-blue-300">What you're seeing:</strong> Real-time execution metrics from the Spark REST API.
                </p>
                <p>
                  <span className="text-purple-300">Jobs</span> contain multiple <span className="text-blue-300">Stages</span>, 
                  each with <span className="text-green-300">Tasks</span> running on <span className="text-orange-300">Executors</span>. 
                  Shuffle operations move data between stages.
                </p>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {/* JOB HISTORY - Shows submitted jobs and their status */}
      {/* ═══════════════════════════════════════════════════════════════════════════ */}
      {runningJobs.length > 0 && (
        <div className="bg-gray-800/50 border border-gray-700 rounded-lg overflow-hidden">
          <div className="flex items-center justify-between p-4 border-b border-gray-700">
            <h3 className="text-white font-medium flex items-center">
              <Clock className="h-5 w-5 mr-2" />
              Job History ({runningJobs.length})
            </h3>
            <button
              onClick={clearCompletedJobs}
              className="text-gray-400 hover:text-white text-sm flex items-center"
            >
              <Trash2 className="h-4 w-4 mr-1" />
              Clear Completed
            </button>
          </div>
          
          <div className="divide-y divide-gray-700">
            {runningJobs.map((job) => (
              <div key={job.id} className="p-4 flex items-center justify-between">
                <div className="flex items-center">
                  {job.status === 'running' && <Loader2 className="h-4 w-4 text-blue-400 mr-3 animate-spin" />}
                  {job.status === 'pending' && <Clock className="h-4 w-4 text-yellow-400 mr-3" />}
                  {job.status === 'completed' && <CheckCircle className="h-4 w-4 text-green-400 mr-3" />}
                  {job.status === 'failed' && <XCircle className="h-4 w-4 text-red-400 mr-3" />}
                  <div>
                    <p className="text-white">{job.name}</p>
                    <p className="text-xs text-gray-500">
                      Started: {new Date(job.startTime).toLocaleTimeString()}
                    </p>
                  </div>
                </div>
                <span className={`text-sm px-2 py-1 rounded ${
                  job.status === 'running' ? 'bg-blue-500/20 text-blue-300' :
                  job.status === 'pending' ? 'bg-yellow-500/20 text-yellow-300' :
                  job.status === 'completed' ? 'bg-green-500/20 text-green-300' :
                  'bg-red-500/20 text-red-300'
                }`}>
                  {job.status}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* No Jobs Running - Show Refresh Button */}
      {!runningJobs.some(j => j.status === 'running' || j.status === 'pending') && !isPolling && (
        <div className="text-center py-8">
          <p className="text-gray-400 mb-4">Submit a job to see real-time execution details</p>
          <button
            onClick={() => { setIsPolling(true); fetchExecutionDetails(); setTimeout(() => setIsPolling(false), 10000); }}
            className="text-spark-orange hover:text-spark-orange/80 flex items-center justify-center mx-auto"
          >
            <RefreshCw className="h-4 w-4 mr-2" />
            Check for running applications
          </button>
        </div>
      )}
    </div>
  )
}
