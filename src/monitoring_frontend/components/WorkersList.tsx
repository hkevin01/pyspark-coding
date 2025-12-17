import useSWR from 'swr'
import { fetchClusterStatus, ClusterStatus, SparkWorker, formatBytes } from '../lib/sparkApi'
import { Server, CheckCircle, XCircle } from 'lucide-react'

interface Props {
  refreshInterval: number | null
}

export default function WorkersList({ refreshInterval }: Props) {
  const { data: cluster } = useSWR<ClusterStatus>(
    'cluster-status',
    fetchClusterStatus,
    { refreshInterval: refreshInterval || 0 }
  )

  const workers = cluster?.workers || []

  if (!Array.isArray(workers) || workers.length === 0) {
    return (
      <div className="bg-gray-800/50 border border-gray-700 rounded-lg p-6">
        <p className="text-gray-400 text-center">No workers found</p>
      </div>
    )
  }

  return (
    <div className="bg-gray-800/50 border border-gray-700 rounded-lg overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead className="bg-gray-700/50">
            <tr>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-300 uppercase">Worker</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-300 uppercase">Status</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-300 uppercase">Cores</th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-300 uppercase">Memory</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-700">
            {workers.map((worker: any, idx: number) => (
              <tr key={worker.id || idx} className="hover:bg-gray-700/30 transition-colors">
                <td className="px-4 py-3">
                  <div className="flex items-center">
                    <Server className="h-4 w-4 text-blue-400 mr-2" />
                    <div>
                      <p className="text-sm font-medium text-white">{worker.host || 'Unknown'}</p>
                      <p className="text-xs text-gray-400">{worker.webuiaddress || 'N/A'}</p>
                    </div>
                  </div>
                </td>
                <td className="px-4 py-3">
                  <div className="flex items-center">
                    {worker.state === 'ALIVE' ? (
                      <CheckCircle className="h-4 w-4 text-green-400 mr-1" />
                    ) : (
                      <XCircle className="h-4 w-4 text-red-400 mr-1" />
                    )}
                    <span className={worker.state === 'ALIVE' ? 'text-green-400' : 'text-red-400'}>
                      {worker.state}
                    </span>
                  </div>
                </td>
                <td className="px-4 py-3">
                  <div className="text-sm text-white">
                    {worker.coresused || 0} / {worker.cores || 0}
                  </div>
                  <div className="mt-1 bg-gray-700 rounded-full h-1.5 w-20">
                    <div
                      className="bg-green-500 h-1.5 rounded-full"
                      style={{ width: `${((worker.coresused || 0) / (worker.cores || 1)) * 100}%` }}
                    ></div>
                  </div>
                </td>
                <td className="px-4 py-3">
                  <div className="text-sm text-white">
                    {formatBytes((worker.memoryused || 0) * 1024 * 1024)}
                  </div>
                  <div className="text-xs text-gray-400">
                    of {formatBytes((worker.memory || 0) * 1024 * 1024)}
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
