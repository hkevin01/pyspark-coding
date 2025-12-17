import { useState, useEffect } from 'react'
import useSWR from 'swr'
import { 
  fetchClusterStatus, 
  fetchApplications,
  fetchJobs,
  fetchStages,
  ClusterStatus,
  SparkApplication,
  SparkJob,
  SparkStage,
  formatBytes,
  formatDuration 
} from '../lib/sparkApi'
import { 
  Table, 
  Info, 
  CheckCircle, 
  XCircle, 
  Clock, 
  Database,
  FileText,
  Download,
  Eye,
  BarChart3,
  Layers,
  Activity,
  RefreshCw,
  AlertCircle
} from 'lucide-react'

interface Props {
  refreshInterval: number | null
}

// Info tooltip
function ResultTooltip({ text }: { text: string }) {
  return (
    <div className="group relative inline-block ml-1">
      <Info className="h-3 w-3 text-gray-500 hover:text-gray-300 cursor-help" />
      <div className="absolute z-50 hidden group-hover:block w-64 p-2 text-xs bg-gray-900 border border-gray-600 rounded-lg shadow-xl -translate-x-1/2 left-1/2 mt-1">
        <div className="absolute -top-1 left-1/2 -translate-x-1/2 w-2 h-2 bg-gray-900 border-l border-t border-gray-600 rotate-45"></div>
        {text}
      </div>
    </div>
  )
}

export default function ResultsViewer({ refreshInterval }: Props) {
  const [selectedApp, setSelectedApp] = useState<string | null>(null)
  const [viewMode, setViewMode] = useState<'jobs' | 'stages' | 'data'>('jobs')

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
    selectedApp ? `jobs-${selectedApp}` : null,
    () => fetchJobs(selectedApp!),
    { refreshInterval: refreshInterval || 0 }
  )

  const { data: stages } = useSWR<SparkStage[]>(
    selectedApp ? `stages-${selectedApp}` : null,
    () => fetchStages(selectedApp!),
    { refreshInterval: refreshInterval || 0 }
  )

  const allApps = [
    ...(cluster?.activeapps || []),
    ...(cluster?.completedapps || []).slice(0, 10)
  ]

  // Calculate job statistics
  const completedJobs = jobs?.filter(j => j.status === 'SUCCEEDED') || []
  const failedJobs = jobs?.filter(j => j.status === 'FAILED') || []
  const runningJobs = jobs?.filter(j => j.status === 'RUNNING') || []

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="bg-gradient-to-r from-green-900/30 to-blue-900/30 border border-green-700/50 rounded-lg p-4">
        <div className="flex items-start">
          <Table className="h-5 w-5 text-green-400 mr-3 mt-0.5 flex-shrink-0" />
          <div>
            <h3 className="text-green-300 font-medium">📊 Results & Output Viewer</h3>
            <p className="text-sm text-gray-300 mt-1">
              View completed job results, execution statistics, and output data. 
              Select an application to see its jobs, stages, and final outputs.
            </p>
          </div>
        </div>
      </div>

      {/* Understanding Results Section */}
      <div className="bg-gray-800/50 border border-gray-700 rounded-lg p-4">
        <h4 className="text-white font-medium mb-3 flex items-center">
          <Info className="h-4 w-4 mr-2 text-blue-400" />
          Understanding Spark Results & Output
        </h4>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
          <div className="bg-gray-900/50 rounded-lg p-3">
            <h5 className="text-green-400 font-medium mb-2">✅ What "Results" Means</h5>
            <p className="text-gray-400 text-xs">
              In Spark, results are produced by <span className="text-white">actions</span> like 
              <code className="bg-gray-800 px-1 rounded mx-1">.collect()</code>,
              <code className="bg-gray-800 px-1 rounded mx-1">.count()</code>,
              <code className="bg-gray-800 px-1 rounded mx-1">.show()</code>, or
              <code className="bg-gray-800 px-1 rounded mx-1">.write()</code>.
              Until an action is called, nothing executes (lazy evaluation).
            </p>
          </div>
          <div className="bg-gray-900/50 rounded-lg p-3">
            <h5 className="text-yellow-400 font-medium mb-2">📤 Where Results Go</h5>
            <ul className="text-gray-400 text-xs space-y-1">
              <li>• <span className="text-white">.collect()</span> → Driver memory (risky for large data!)</li>
              <li>• <span className="text-white">.show()</span> → Prints to console/notebook</li>
              <li>• <span className="text-white">.write.parquet()</span> → Distributed file storage</li>
              <li>• <span className="text-white">.write.jdbc()</span> → Database table</li>
            </ul>
          </div>
          <div className="bg-gray-900/50 rounded-lg p-3">
            <h5 className="text-purple-400 font-medium mb-2">📊 Aggregation Process</h5>
            <p className="text-gray-400 text-xs">
              When you call an action, executors compute partial results on their partitions.
              These are <span className="text-white">aggregated</span> (combined) and sent to the driver.
              For <code className="bg-gray-800 px-1 rounded">.count()</code>, each executor counts its partition, 
              then the driver sums them all.
            </p>
          </div>
        </div>
      </div>

      {/* Application Selector */}
      <div className="bg-gray-800/50 border border-gray-700 rounded-lg p-4">
        <div className="flex items-center justify-between mb-4">
          <h4 className="text-white font-medium flex items-center">
            <Activity className="h-4 w-4 mr-2 text-spark-orange" />
            Select Application
            <ResultTooltip text="Choose a Spark application to view its execution results. Active apps are currently running, completed apps have finished." />
          </h4>
          <span className="text-xs text-gray-400">
            {allApps.length} application{allApps.length !== 1 ? 's' : ''} found
          </span>
        </div>

        {allApps.length === 0 ? (
          <div className="text-center py-8 text-gray-400">
            <Database className="h-10 w-10 mx-auto mb-3 opacity-50" />
            <p>No applications found</p>
            <p className="text-xs mt-1">Submit a Spark job to see results here</p>
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
            {allApps.map((app: any) => {
              const isActive = cluster?.activeapps?.some((a: any) => a.id === app.id)
              const isSelected = selectedApp === app.id
              
              return (
                <button
                  key={app.id}
                  onClick={() => setSelectedApp(app.id)}
                  className={`p-3 rounded-lg border text-left transition-all ${
                    isSelected 
                      ? 'bg-spark-orange/20 border-spark-orange' 
                      : 'bg-gray-900/50 border-gray-700 hover:border-gray-500'
                  }`}
                >
                  <div className="flex items-center justify-between mb-2">
                    <span className="text-white font-medium text-sm truncate">{app.name}</span>
                    <span className={`text-xs px-2 py-0.5 rounded-full ${
                      isActive 
                        ? 'bg-green-500/20 text-green-400' 
                        : 'bg-blue-500/20 text-blue-400'
                    }`}>
                      {isActive ? 'Running' : 'Completed'}
                    </span>
                  </div>
                  <p className="text-xs text-gray-400 font-mono truncate">{app.id}</p>
                  <div className="flex items-center mt-2 text-xs text-gray-500">
                    <Clock className="h-3 w-3 mr-1" />
                    {formatDuration(app.duration || 0)}
                  </div>
                </button>
              )
            })}
          </div>
        )}
      </div>

      {/* Results View */}
      {selectedApp && (
        <div className="space-y-4">
          {/* View Mode Tabs */}
          <div className="flex space-x-2">
            <button
              onClick={() => setViewMode('jobs')}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                viewMode === 'jobs'
                  ? 'bg-spark-orange text-white'
                  : 'bg-gray-800 text-gray-400 hover:text-white'
              }`}
            >
              <BarChart3 className="h-4 w-4 inline mr-2" />
              Jobs ({jobs?.length || 0})
            </button>
            <button
              onClick={() => setViewMode('stages')}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                viewMode === 'stages'
                  ? 'bg-spark-orange text-white'
                  : 'bg-gray-800 text-gray-400 hover:text-white'
              }`}
            >
              <Layers className="h-4 w-4 inline mr-2" />
              Stages ({stages?.length || 0})
            </button>
            <button
              onClick={() => setViewMode('data')}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                viewMode === 'data'
                  ? 'bg-spark-orange text-white'
                  : 'bg-gray-800 text-gray-400 hover:text-white'
              }`}
            >
              <Table className="h-4 w-4 inline mr-2" />
              Data Flow
            </button>
          </div>

          {/* Job Summary Stats */}
          <div className="grid grid-cols-4 gap-4">
            <div className="bg-gray-800/50 border border-gray-700 rounded-lg p-4 text-center">
              <p className="text-2xl font-bold text-white">{jobs?.length || 0}</p>
              <p className="text-xs text-gray-400">Total Jobs</p>
            </div>
            <div className="bg-green-500/10 border border-green-500/30 rounded-lg p-4 text-center">
              <p className="text-2xl font-bold text-green-400">{completedJobs.length}</p>
              <p className="text-xs text-gray-400">Succeeded</p>
            </div>
            <div className="bg-blue-500/10 border border-blue-500/30 rounded-lg p-4 text-center">
              <p className="text-2xl font-bold text-blue-400">{runningJobs.length}</p>
              <p className="text-xs text-gray-400">Running</p>
            </div>
            <div className="bg-red-500/10 border border-red-500/30 rounded-lg p-4 text-center">
              <p className="text-2xl font-bold text-red-400">{failedJobs.length}</p>
              <p className="text-xs text-gray-400">Failed</p>
            </div>
          </div>

          {/* View Content */}
          {viewMode === 'jobs' && (
            <div className="bg-gray-800/50 border border-gray-700 rounded-lg overflow-hidden">
              <div className="p-4 border-b border-gray-700">
                <h4 className="text-white font-medium flex items-center">
                  Job Execution Results
                  <ResultTooltip text="Each job is triggered by an action (like .count() or .show()). Jobs contain multiple stages that run in parallel or sequence." />
                </h4>
              </div>
              
              {jobs && jobs.length > 0 ? (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead className="bg-gray-900/50">
                      <tr>
                        <th className="px-4 py-3 text-left text-xs text-gray-400">Job ID</th>
                        <th className="px-4 py-3 text-left text-xs text-gray-400">Status</th>
                        <th className="px-4 py-3 text-left text-xs text-gray-400">Stages</th>
                        <th className="px-4 py-3 text-left text-xs text-gray-400">Tasks</th>
                        <th className="px-4 py-3 text-left text-xs text-gray-400">Progress</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-700">
                      {jobs.map((job) => (
                        <tr key={job.jobId} className="hover:bg-gray-700/30">
                          <td className="px-4 py-3 font-mono text-spark-orange">Job {job.jobId}</td>
                          <td className="px-4 py-3">
                            <span className={`inline-flex items-center px-2 py-1 rounded-full text-xs ${
                              job.status === 'SUCCEEDED' ? 'bg-green-500/20 text-green-400' :
                              job.status === 'RUNNING' ? 'bg-blue-500/20 text-blue-400' :
                              job.status === 'FAILED' ? 'bg-red-500/20 text-red-400' :
                              'bg-gray-500/20 text-gray-400'
                            }`}>
                              {job.status === 'SUCCEEDED' && <CheckCircle className="h-3 w-3 mr-1" />}
                              {job.status === 'FAILED' && <XCircle className="h-3 w-3 mr-1" />}
                              {job.status === 'RUNNING' && <RefreshCw className="h-3 w-3 mr-1 animate-spin" />}
                              {job.status}
                            </span>
                          </td>
                          <td className="px-4 py-3 text-gray-300">
                            {job.numCompletedStages}/{job.numStages}
                          </td>
                          <td className="px-4 py-3 text-gray-300">
                            {job.numCompletedTasks}/{job.numTasks}
                          </td>
                          <td className="px-4 py-3">
                            <div className="w-full bg-gray-700 rounded-full h-2">
                              <div 
                                className={`h-2 rounded-full ${
                                  job.status === 'SUCCEEDED' ? 'bg-green-500' :
                                  job.status === 'FAILED' ? 'bg-red-500' : 'bg-blue-500'
                                }`}
                                style={{ width: `${job.numTasks > 0 ? (job.numCompletedTasks / job.numTasks) * 100 : 0}%` }}
                              />
                            </div>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="p-8 text-center text-gray-400">
                  <BarChart3 className="h-10 w-10 mx-auto mb-3 opacity-50" />
                  <p>No jobs found for this application</p>
                </div>
              )}
            </div>
          )}

          {viewMode === 'stages' && (
            <div className="bg-gray-800/50 border border-gray-700 rounded-lg overflow-hidden">
              <div className="p-4 border-b border-gray-700">
                <h4 className="text-white font-medium flex items-center">
                  Stage Execution Details
                  <ResultTooltip text="Stages are groups of tasks that can run in parallel. New stages are created at shuffle boundaries (groupBy, join, etc)." />
                </h4>
              </div>
              
              {stages && stages.length > 0 ? (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead className="bg-gray-900/50">
                      <tr>
                        <th className="px-4 py-3 text-left text-xs text-gray-400">Stage</th>
                        <th className="px-4 py-3 text-left text-xs text-gray-400">Status</th>
                        <th className="px-4 py-3 text-left text-xs text-gray-400">Tasks</th>
                        <th className="px-4 py-3 text-left text-xs text-gray-400">Input</th>
                        <th className="px-4 py-3 text-left text-xs text-gray-400">Shuffle Read</th>
                        <th className="px-4 py-3 text-left text-xs text-gray-400">Shuffle Write</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-700">
                      {stages.map((stage) => (
                        <tr key={`${stage.stageId}-${stage.attemptId}`} className="hover:bg-gray-700/30">
                          <td className="px-4 py-3 font-mono text-purple-400">Stage {stage.stageId}</td>
                          <td className="px-4 py-3">
                            <span className={`inline-flex items-center px-2 py-1 rounded-full text-xs ${
                              stage.status === 'COMPLETE' ? 'bg-green-500/20 text-green-400' :
                              stage.status === 'ACTIVE' ? 'bg-blue-500/20 text-blue-400' :
                              stage.status === 'FAILED' ? 'bg-red-500/20 text-red-400' :
                              'bg-gray-500/20 text-gray-400'
                            }`}>
                              {stage.status}
                            </span>
                          </td>
                          <td className="px-4 py-3 text-gray-300">
                            {stage.numCompleteTasks}/{stage.numTasks}
                          </td>
                          <td className="px-4 py-3 text-gray-300">{formatBytes(stage.inputBytes)}</td>
                          <td className="px-4 py-3 text-yellow-400">{formatBytes(stage.shuffleReadBytes)}</td>
                          <td className="px-4 py-3 text-orange-400">{formatBytes(stage.shuffleWriteBytes)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="p-8 text-center text-gray-400">
                  <Layers className="h-10 w-10 mx-auto mb-3 opacity-50" />
                  <p>No stages found for this application</p>
                </div>
              )}
            </div>
          )}

          {viewMode === 'data' && (
            <div className="bg-gray-800/50 border border-gray-700 rounded-lg p-4">
              <h4 className="text-white font-medium mb-4 flex items-center">
                <Table className="h-4 w-4 mr-2 text-green-400" />
                Data Flow Visualization
                <ResultTooltip text="Shows how data moves through your Spark job: input → transformations → shuffle → aggregation → output" />
              </h4>
              
              <div className="space-y-4">
                {/* Data Flow Diagram */}
                <div className="bg-gray-900/50 rounded-lg p-4">
                  <div className="flex items-center justify-between text-sm">
                    <div className="text-center">
                      <div className="bg-blue-500/20 border border-blue-500/30 rounded-lg p-3 mb-2">
                        <Database className="h-8 w-8 text-blue-400 mx-auto" />
                      </div>
                      <p className="text-blue-400 font-medium">Input</p>
                      <p className="text-gray-500 text-xs">Source Data</p>
                    </div>
                    <div className="flex-1 border-t-2 border-dashed border-gray-600 mx-4" />
                    <div className="text-center">
                      <div className="bg-green-500/20 border border-green-500/30 rounded-lg p-3 mb-2">
                        <Activity className="h-8 w-8 text-green-400 mx-auto" />
                      </div>
                      <p className="text-green-400 font-medium">Transform</p>
                      <p className="text-gray-500 text-xs">Map, Filter, etc.</p>
                    </div>
                    <div className="flex-1 border-t-2 border-dashed border-gray-600 mx-4" />
                    <div className="text-center">
                      <div className="bg-yellow-500/20 border border-yellow-500/30 rounded-lg p-3 mb-2">
                        <RefreshCw className="h-8 w-8 text-yellow-400 mx-auto" />
                      </div>
                      <p className="text-yellow-400 font-medium">Shuffle</p>
                      <p className="text-gray-500 text-xs">Redistribute</p>
                    </div>
                    <div className="flex-1 border-t-2 border-dashed border-gray-600 mx-4" />
                    <div className="text-center">
                      <div className="bg-purple-500/20 border border-purple-500/30 rounded-lg p-3 mb-2">
                        <BarChart3 className="h-8 w-8 text-purple-400 mx-auto" />
                      </div>
                      <p className="text-purple-400 font-medium">Aggregate</p>
                      <p className="text-gray-500 text-xs">Combine Results</p>
                    </div>
                    <div className="flex-1 border-t-2 border-dashed border-gray-600 mx-4" />
                    <div className="text-center">
                      <div className="bg-spark-orange/20 border border-spark-orange/30 rounded-lg p-3 mb-2">
                        <FileText className="h-8 w-8 text-spark-orange mx-auto" />
                      </div>
                      <p className="text-spark-orange font-medium">Output</p>
                      <p className="text-gray-500 text-xs">Final Result</p>
                    </div>
                  </div>
                </div>

                {/* Stats Summary */}
                {stages && stages.length > 0 && (
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                    <div className="bg-blue-500/10 border border-blue-500/30 rounded-lg p-4">
                      <h5 className="text-blue-400 font-medium mb-2">📥 Total Input</h5>
                      <p className="text-2xl font-bold text-white">
                        {formatBytes(stages.reduce((sum, s) => sum + (s.inputBytes || 0), 0))}
                      </p>
                      <p className="text-xs text-gray-400 mt-1">Data read from source</p>
                    </div>
                    <div className="bg-yellow-500/10 border border-yellow-500/30 rounded-lg p-4">
                      <h5 className="text-yellow-400 font-medium mb-2">🔀 Total Shuffle</h5>
                      <p className="text-2xl font-bold text-white">
                        {formatBytes(stages.reduce((sum, s) => sum + (s.shuffleReadBytes || 0) + (s.shuffleWriteBytes || 0), 0))}
                      </p>
                      <p className="text-xs text-gray-400 mt-1">Data moved across network</p>
                    </div>
                    <div className="bg-green-500/10 border border-green-500/30 rounded-lg p-4">
                      <h5 className="text-green-400 font-medium mb-2">📤 Total Output</h5>
                      <p className="text-2xl font-bold text-white">
                        {formatBytes(stages.reduce((sum, s) => sum + (s.outputBytes || 0), 0))}
                      </p>
                      <p className="text-xs text-gray-400 mt-1">Data written to destination</p>
                    </div>
                  </div>
                )}

                {/* What the output looks like explanation */}
                <div className="bg-gray-900/50 rounded-lg p-4">
                  <h5 className="text-white font-medium mb-3">📋 Understanding Your Output</h5>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
                    <div>
                      <p className="text-gray-400 mb-2">When you call <code className="bg-gray-800 px-1 rounded">.show()</code>:</p>
                      <pre className="bg-gray-800 p-2 rounded text-xs overflow-x-auto">
{`+-------+-----+
|  city |count|
+-------+-----+
|    NYC| 1234|
| Boston|  567|
+-------+-----+`}
                      </pre>
                    </div>
                    <div>
                      <p className="text-gray-400 mb-2">When you call <code className="bg-gray-800 px-1 rounded">.collect()</code>:</p>
                      <pre className="bg-gray-800 p-2 rounded text-xs overflow-x-auto">
{`[Row(city='NYC', count=1234),
 Row(city='Boston', count=567)]`}
                      </pre>
                      <p className="text-red-400 text-xs mt-2">⚠️ Warning: Collects ALL data to driver!</p>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      )}

      {/* No app selected message */}
      {!selectedApp && allApps.length > 0 && (
        <div className="bg-gray-800/30 border border-gray-700 rounded-lg p-8 text-center">
          <Eye className="h-12 w-12 text-gray-600 mx-auto mb-3" />
          <p className="text-gray-400">Select an application above to view its results</p>
          <p className="text-xs text-gray-500 mt-1">
            You'll see jobs, stages, and data flow for the selected application
          </p>
        </div>
      )}
    </div>
  )
}
