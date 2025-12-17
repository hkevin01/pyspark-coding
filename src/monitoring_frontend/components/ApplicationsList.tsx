import useSWR from 'swr'
import { fetchClusterStatus, ClusterStatus, formatDuration, getStatusBadgeColor } from '../lib/sparkApi'
import { PlayCircle, CheckCircle2 } from 'lucide-react'

interface Props {
  refreshInterval: number | null
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
        <p className="text-gray-400 text-center">No applications found</p>
      </div>
    )
  }

  return (
    <div className="bg-gray-800/50 border border-gray-700 rounded-lg p-4 space-y-3">
      {applications.map((app: any, idx: number) => (
        <div
          key={app.id || idx}
          className="bg-gray-700/30 border border-gray-600 rounded-lg p-4 hover:bg-gray-700/50 transition-colors"
        >
          <div className="flex items-start justify-between mb-2">
            <div className="flex-1">
              <div className="flex items-center">
                {app.isActive ? (
                  <PlayCircle className="h-4 w-4 text-green-400 mr-2" />
                ) : (
                  <CheckCircle2 className="h-4 w-4 text-blue-400 mr-2" />
                )}
                <h4 className="text-sm font-medium text-white">{app.name || 'Unknown Application'}</h4>
              </div>
              <p className="text-xs text-gray-400 mt-1">{app.id}</p>
            </div>
            <span className={`px-2 py-1 text-xs font-medium rounded-full border ${getStatusBadgeColor(app.state || 'UNKNOWN')}`}>
              {app.state || 'UNKNOWN'}
            </span>
          </div>
          
          <div className="grid grid-cols-3 gap-2 text-xs">
            <div>
              <p className="text-gray-400">Cores</p>
              <p className="text-white font-medium">{app.cores || 0}</p>
            </div>
            <div>
              <p className="text-gray-400">Memory</p>
              <p className="text-white font-medium">{app.memoryPerExecutorMB || 0} MB</p>
            </div>
            <div>
              <p className="text-gray-400">Duration</p>
              <p className="text-white font-medium">{formatDuration(app.duration || 0)}</p>
            </div>
          </div>
        </div>
      ))}
    </div>
  )
}
