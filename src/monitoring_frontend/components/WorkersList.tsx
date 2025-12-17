import useSWR from 'swr'
import { fetchClusterStatus, ClusterStatus, SparkWorker, formatBytes } from '../lib/sparkApi'
import { Server, CheckCircle, XCircle, Info } from 'lucide-react'

interface Props {
  refreshInterval: number | null
}

// Column header tooltip
function ColumnTooltip({ text }: { text: string }) {
  return (
    <div className="group relative inline-block ml-1">
      <Info className="h-3 w-3 text-gray-500 hover:text-gray-300 cursor-help" />
      <div className="absolute z-50 hidden group-hover:block w-48 p-2 text-xs bg-gray-900 border border-gray-600 rounded-lg shadow-xl -translate-x-1/2 left-1/2 mt-1">
        {text}
      </div>
    </div>
  )
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
      {/* Explanation banner */}
      <div className="bg-gray-900/50 px-4 py-2 border-b border-gray-700">
        <p className="text-xs text-gray-400">
          <span className="text-blue-400 font-medium">Workers</span> are physical/virtual machines that execute Spark tasks. 
          Each worker runs multiple <span className="text-green-400">Executors</span> that process data partitions in parallel.
        </p>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead className="bg-gray-700/50">
            <tr>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-300 uppercase">
                Worker
                <ColumnTooltip text="Worker hostname and Web UI address. Click the Web UI link to see detailed worker metrics." />
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-300 uppercase">
                Status
                <ColumnTooltip text="ALIVE means the worker is healthy and accepting tasks. DEAD means connection lost." />
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-300 uppercase">
                Cores
                <ColumnTooltip text="CPU cores allocated to this worker. 'Used' cores are running tasks." />
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium text-gray-300 uppercase">
                Memory
                <ColumnTooltip text="RAM available for executor JVMs. Used memory is allocated to running applications." />
              </th>
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
