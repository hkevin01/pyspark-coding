import useSWR from 'swr'
import { fetchClusterStatus, ClusterStatus, formatDuration, getStatusBadgeColor } from '../lib/sparkApi'
import { PlayCircle, CheckCircle2, Info, Monitor, Cpu, HardDrive, Clock, ExternalLink } from 'lucide-react'

interface Props {
  refreshInterval: number | null
}

// Tooltip component for explanations
function AppTooltip({ text }: { text: string }) {
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

export default function ApplicationsList({ refreshInterval }: Props) {
  const { data: cluster } = useSWR<ClusterStatus>(
    'cluster-status',
    fetchClusterStatus,
    { refreshInterval: refreshInterval || 0 }
  )

  const applications = [
    ...(cluster?.activeapps || []).map((app: any) => ({ ...app, isActive: true })),
    ...(cluster?.completedapps || []).slice(0, 5).map((app: any) => ({ ...app, isActive: false }))
  ]

  if (applications.length === 0) {
    return (
      <div className="bg-gray-800/50 border border-gray-700 rounded-lg p-6">
        <div className="text-center">
          <Monitor className="h-10 w-10 text-gray-600 mx-auto mb-3" />
          <p className="text-gray-400">No applications found</p>
          <p className="text-xs text-gray-500 mt-2">
            Submit a Spark job to see it here. Each application creates a Driver 
            that coordinates executors across worker nodes.
          </p>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-3">
      {/* Explanation Banner */}
      <div className="bg-gradient-to-r from-orange-900/20 to-yellow-900/20 border border-orange-700/30 rounded-lg p-3">
        <div className="flex items-start">
          <Info className="h-4 w-4 text-orange-400 mr-2 mt-0.5 flex-shrink-0" />
          <div className="text-xs">
            <span className="text-orange-300 font-medium">What is an Application?</span>
            <p className="text-gray-400 mt-1">
              Each application = one SparkContext (e.g., one Jupyter session, one spark-submit job).
              The <span className="text-yellow-400">Driver</span> runs on the master node and coordinates 
              <span className="text-blue-400"> Executors</span> on workers.
            </p>
          </div>
        </div>
      </div>

      <div className="bg-gray-800/50 border border-gray-700 rounded-lg p-4 space-y-3">
        {applications.map((app: any, idx: number) => (
          <div
            key={app.id || idx}
            className="bg-gray-700/30 border border-gray-600 rounded-lg p-4 hover:bg-gray-700/50 transition-colors"
          >
            <div className="flex items-start justify-between mb-3">
              <div className="flex-1">
                <div className="flex items-center">
                  {app.isActive ? (
                    <PlayCircle className="h-4 w-4 text-green-400 mr-2 animate-pulse" />
                  ) : (
                    <CheckCircle2 className="h-4 w-4 text-blue-400 mr-2" />
                  )}
                  <h4 className="text-sm font-medium text-white">{app.name || 'Unknown Application'}</h4>
                  <AppTooltip text="Application name is set via spark.app.name config or SparkSession.builder.appName(). It helps identify your job in the cluster." />
                </div>
                <p className="text-xs text-gray-400 mt-1 font-mono">{app.id}</p>
              </div>
              <span className={`px-2 py-1 text-xs font-medium rounded-full border ${getStatusBadgeColor(app.state || 'UNKNOWN')}`}>
                {app.state || 'UNKNOWN'}
              </span>
            </div>
            
            {/* Driver Info Section */}
            <div className="bg-yellow-500/5 border border-yellow-500/20 rounded-lg p-3 mb-3">
              <div className="flex items-center mb-2">
                <Monitor className="h-4 w-4 text-yellow-400 mr-2" />
                <span className="text-yellow-400 text-xs font-medium">Driver Process</span>
                <AppTooltip text="The Driver is the main process that runs your PySpark code. It creates the SparkContext, builds the execution plan (DAG), and schedules tasks to executors. In client mode, it runs on your machine; in cluster mode, it runs on a worker." />
              </div>
              <div className="grid grid-cols-2 gap-2 text-xs">
                <div>
                  <span className="text-gray-400">Location:</span>
                  <span className="text-white ml-1 font-mono">spark-master:7077</span>
                </div>
                <div>
                  <span className="text-gray-400">Mode:</span>
                  <span className="text-white ml-1">Client Mode</span>
                </div>
              </div>
            </div>

            {/* Resources Grid */}
            <div className="grid grid-cols-4 gap-3 text-xs">
              <div className="bg-gray-800/50 rounded p-2">
                <div className="flex items-center mb-1">
                  <Cpu className="h-3 w-3 text-blue-400 mr-1" />
                  <span className="text-gray-400">Cores</span>
                  <AppTooltip text="Total CPU cores allocated to this application's executors. More cores = more parallel tasks can run simultaneously." />
                </div>
                <p className="text-white font-medium">{app.cores || 0}</p>
                <p className="text-gray-500 text-xs">allocated</p>
              </div>
              <div className="bg-gray-800/50 rounded p-2">
                <div className="flex items-center mb-1">
                  <HardDrive className="h-3 w-3 text-purple-400 mr-1" />
                  <span className="text-gray-400">Memory</span>
                  <AppTooltip text="Memory per executor (set via spark.executor.memory). This is JVM heap memory for task execution and data caching." />
                </div>
                <p className="text-white font-medium">{app.memoryPerExecutorMB || 0} MB</p>
                <p className="text-gray-500 text-xs">per executor</p>
              </div>
              <div className="bg-gray-800/50 rounded p-2">
                <div className="flex items-center mb-1">
                  <Clock className="h-3 w-3 text-green-400 mr-1" />
                  <span className="text-gray-400">Duration</span>
                  <AppTooltip text="Total time since the application started. For streaming apps, this keeps growing. For batch jobs, it shows total processing time." />
                </div>
                <p className="text-white font-medium">{formatDuration(app.duration || 0)}</p>
                <p className="text-gray-500 text-xs">running</p>
              </div>
              <div className="bg-gray-800/50 rounded p-2">
                <div className="flex items-center mb-1">
                  <ExternalLink className="h-3 w-3 text-orange-400 mr-1" />
                  <span className="text-gray-400">UI</span>
                </div>
                {app.isActive ? (
                  <a 
                    href="http://localhost:4040" 
                    target="_blank" 
                    rel="noopener noreferrer"
                    className="text-orange-400 hover:text-orange-300 font-medium"
                  >
                    :4040
                  </a>
                ) : (
                  <span className="text-gray-500">Closed</span>
                )}
                <p className="text-gray-500 text-xs">Spark UI</p>
              </div>
            </div>

            {/* What This App is Doing */}
            {app.isActive && (
              <div className="mt-3 pt-3 border-t border-gray-600">
                <p className="text-xs text-gray-400">
                  <span className="text-green-400">●</span> This application is actively running. 
                  The driver is coordinating tasks across {Math.ceil((app.cores || 1) / 2)} executor(s).
                  Visit <span className="text-orange-400">localhost:4040</span> to see jobs, stages, and executors.
                </p>
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  )
}
