import useSWR from 'swr'
import { fetchClusterStatus, ClusterStatus } from '../lib/sparkApi'
import { Activity, Clock, CheckCircle } from 'lucide-react'

interface Props {
  refreshInterval: number | null
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
      <div className="bg-gray-800/50 border border-gray-700 rounded-lg p-8 text-center">
        <Activity className="h-12 w-12 text-gray-600 mx-auto mb-3" />
        <p className="text-gray-400">No active applications running</p>
        <p className="text-sm text-gray-500 mt-1">Submit a Spark job to see live metrics</p>
      </div>
    )
  }

  return (
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

          <div className="grid grid-cols-2 gap-3">
            <div className="bg-gray-700/30 rounded p-3">
              <p className="text-xs text-gray-400 mb-1">CPU Cores</p>
              <p className="text-xl font-bold text-white">{app.coresGranted || app.cores || 0}</p>
              <p className="text-xs text-gray-500">of {app.maxCores || app.cores || 0} max</p>
            </div>
            
            <div className="bg-gray-700/30 rounded p-3">
              <p className="text-xs text-gray-400 mb-1">Memory</p>
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
              className="text-blue-400 hover:text-blue-300 transition-colors"
            >
              View Details →
            </a>
          </div>
        </div>
      ))}
    </div>
  )
}
