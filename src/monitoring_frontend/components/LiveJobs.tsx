import useSWR from 'swr'
import { fetchClusterStatus, ClusterStatus } from '../lib/sparkApi'
import { Activity, Clock, CheckCircle, Info, Cpu, HardDrive, Monitor, Zap } from 'lucide-react'

interface Props {
  refreshInterval: number | null
}

// Info tooltip
function JobTooltip({ text }: { text: string }) {
  return (
    <div className="group relative inline-block ml-1">
      <Info className="h-3 w-3 text-gray-500 hover:text-gray-300 cursor-help" />
      <div className="absolute z-50 hidden group-hover:block w-56 p-2 text-xs bg-gray-900 border border-gray-600 rounded-lg shadow-xl -translate-x-1/2 left-1/2 mt-1">
        <div className="absolute -top-1 left-1/2 -translate-x-1/2 w-2 h-2 bg-gray-900 border-l border-t border-gray-600 rotate-45"></div>
        {text}
      </div>
    </div>
  )
}

export default function LiveJobs({ refreshInterval }: Props) {
  const { data: cluster } = useSWR<ClusterStatus>(
    'cluster-status',
    fetchClusterStatus,
    { refreshInterval: refreshInterval || 0 }
  )

  const activeApps = cluster?.activeapps || []

  if (activeApps.length === 0) {
    return (
      <div className="bg-gray-800/50 border border-gray-700 rounded-lg p-8">
        <div className="text-center mb-4">
          <Activity className="h-12 w-12 text-gray-600 mx-auto mb-3" />
          <p className="text-gray-400">No active applications running</p>
          <p className="text-sm text-gray-500 mt-1">Submit a Spark job to see live metrics</p>
        </div>
        
        {/* Explanation when idle */}
        <div className="bg-gray-900/50 rounded-lg p-4 text-sm">
          <h4 className="text-gray-300 font-medium mb-2 flex items-center">
            <Info className="h-4 w-4 mr-2 text-blue-400" />
            What appears here?
          </h4>
          <p className="text-gray-400 text-xs">
            When you submit a Spark job (via spark-submit, Jupyter, or the Run Jobs tab), it creates an 
            <span className="text-yellow-400"> Application</span>. Each application has a 
            <span className="text-yellow-400"> Driver</span> (the brain) that runs on spark-master, and 
            <span className="text-blue-400"> Executors</span> (the workers) that run on spark-worker nodes.
            You'll see resource allocation, running time, and a link to the Spark UI at port 4040.
          </p>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-4">
      {/* Active Apps Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center">
          <Zap className="h-5 w-5 text-green-400 mr-2 animate-pulse" />
          <span className="text-green-400 font-medium">{activeApps.length} Active Application{activeApps.length !== 1 ? 's' : ''}</span>
        </div>
        <span className="text-xs text-gray-400">
          Each app = separate SparkContext with its own Driver & Executors
        </span>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 xl:grid-cols-3 gap-4">
        {activeApps.map((app: any) => (
          <div
            key={app.id}
            className="bg-gradient-to-br from-gray-800/80 to-gray-800/40 border-2 border-green-500/50 rounded-lg p-5 hover:border-green-400 transition-all"
          >
            <div className="flex items-start justify-between mb-3">
              <div className="flex-1">
                <h3 className="text-white font-medium text-lg mb-1">{app.name}</h3>
                <p className="text-xs text-gray-400 font-mono">{app.id}</p>
              </div>
              <div className="flex items-center px-2 py-1 bg-green-500/20 border border-green-500/50 rounded-full">
                <div className="h-2 w-2 bg-green-400 rounded-full animate-pulse mr-2"></div>
                <span className="text-xs font-medium text-green-400">RUNNING</span>
              </div>
            </div>

            {/* Driver Info */}
            <div className="bg-yellow-500/10 border border-yellow-500/20 rounded p-2 mb-3">
              <div className="flex items-center text-xs">
                <Monitor className="h-3 w-3 text-yellow-400 mr-1" />
                <span className="text-yellow-400 font-medium">Driver</span>
                <span className="text-gray-400 ml-1">@ spark-master:7077</span>
                <JobTooltip text="The Driver runs your main() function, creates the DAG, and schedules tasks. It's running on the master node in this cluster." />
              </div>
            </div>

            <div className="grid grid-cols-2 gap-3">
              <div className="bg-gray-700/30 rounded p-3">
                <div className="flex items-center mb-1">
                  <Cpu className="h-3 w-3 text-blue-400 mr-1" />
                  <span className="text-xs text-gray-400">Cores</span>
                  <JobTooltip text="CPU cores allocated across all executors. Each core can run one task at a time." />
                </div>
                <p className="text-xl font-bold text-white">{app.coresGranted || app.cores || 0}</p>
                <p className="text-xs text-gray-500">of {app.maxCores || app.cores || 0} max</p>
              </div>
              
              <div className="bg-gray-700/30 rounded p-3">
                <div className="flex items-center mb-1">
                  <HardDrive className="h-3 w-3 text-purple-400 mr-1" />
                  <span className="text-xs text-gray-400">Memory</span>
                  <JobTooltip text="JVM heap memory per executor for task execution and data caching." />
                </div>
                <p className="text-xl font-bold text-white">{app.memoryPerExecutorMB || 0}</p>
                <p className="text-xs text-gray-500">MB per executor</p>
              </div>
            </div>

            <div className="mt-3 pt-3 border-t border-gray-700 flex items-center justify-between text-xs">
              <div className="flex items-center text-gray-400">
                <Clock className="h-3 w-3 mr-1" />
                Running for {Math.floor((app.duration || 0) / 1000)}s
              </div>
              <a
                href={`http://localhost:4040`}
                target="_blank"
                rel="noopener noreferrer"
                className="text-blue-400 hover:text-blue-300 transition-colors flex items-center"
              >
                Spark UI →
                <JobTooltip text="Opens the Spark Web UI (port 4040) showing jobs, stages, executors, and SQL queries for this app." />
              </a>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
