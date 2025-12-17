import { useEffect, useState } from 'react'
import useSWR from 'swr'
import { fetchClusterStatus, ClusterStatus, formatBytes } from '../lib/sparkApi'
import { Server, Cpu, HardDrive, Activity, Info } from 'lucide-react'

interface Props {
  refreshInterval: number | null
}

// Tooltip component for metric explanations
function MetricTooltip({ text }: { text: string }) {
  return (
    <div className="group relative inline-block ml-1">
      <Info className="h-3 w-3 text-gray-500 hover:text-gray-300 cursor-help" />
      <div className="absolute z-50 hidden group-hover:block w-48 p-2 text-xs bg-gray-900 border border-gray-600 rounded-lg shadow-xl -translate-x-1/2 left-1/2 mt-1">
        <div className="absolute -top-1 left-1/2 -translate-x-1/2 w-2 h-2 bg-gray-900 border-l border-t border-gray-600 rotate-45"></div>
        {text}
      </div>
    </div>
  )
}

export default function ClusterOverview({ refreshInterval }: Props) {
  const { data: cluster, error } = useSWR<ClusterStatus>(
    'cluster-status',
    fetchClusterStatus,
    { refreshInterval: refreshInterval || 0, revalidateOnFocus: false }
  )

  if (error) {
    return (
      <div className="bg-red-500/10 border border-red-500/50 rounded-lg p-4 mb-8">
        <p className="text-red-400">Failed to load cluster status. Is the Spark cluster running?</p>
      </div>
    )
  }

  if (!cluster) {
    return (
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        {[...Array(4)].map((_, i) => (
          <div key={i} className="bg-gray-800/50 rounded-lg p-6 animate-pulse">
            <div className="h-4 bg-gray-700 rounded w-1/2 mb-4"></div>
            <div className="h-8 bg-gray-700 rounded w-3/4"></div>
          </div>
        ))}
      </div>
    )
  }

  const workers = cluster.aliveworkers || 0
  const cores = cluster.cores || 0
  const coresUsed = cluster.coresused || 0
  const memory = cluster.memory || 0
  const memoryUsed = cluster.memoryused || 0
  const activeApps = cluster.activeapps?.length || 0

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
      {/* Workers Card - Shows number of active compute nodes */}
      <div className="bg-gradient-to-br from-blue-900/50 to-blue-800/30 backdrop-blur-sm border border-blue-700/50 rounded-lg p-6 hover:scale-105 transition-transform">
        <div className="flex items-center justify-between mb-2">
          <h3 className="text-sm font-medium text-blue-300 flex items-center">
            Workers
            <MetricTooltip text="Worker nodes execute tasks. Each worker contributes CPU and memory to the cluster." />
          </h3>
          <Server className="h-5 w-5 text-blue-400" />
        </div>
        <p className="text-3xl font-bold text-white">{workers}</p>
        <p className="text-xs text-blue-300 mt-1">Active compute nodes</p>
      </div>

      {/* CPU Card - Shows core allocation and usage */}
      <div className="bg-gradient-to-br from-green-900/50 to-green-800/30 backdrop-blur-sm border border-green-700/50 rounded-lg p-6 hover:scale-105 transition-transform">
        <div className="flex items-center justify-between mb-2">
          <h3 className="text-sm font-medium text-green-300 flex items-center">
            CPU Cores
            <MetricTooltip text="Total CPU cores available. Tasks run in parallel across cores. More cores = more parallelism." />
          </h3>
          <Cpu className="h-5 w-5 text-green-400" />
        </div>
        <p className="text-3xl font-bold text-white">{coresUsed} / {cores}</p>
        <div className="mt-2 bg-gray-700 rounded-full h-2">
          <div 
            className="bg-green-500 h-2 rounded-full transition-all"
            style={{ width: `${cores > 0 ? (coresUsed / cores) * 100 : 0}%` }}
          ></div>
        </div>
        <p className="text-xs text-green-300 mt-1">
          {cores > 0 ? Math.round((coresUsed / cores) * 100) : 0}% utilized
        </p>
      </div>

      {/* Memory Card - Shows RAM allocation and usage */}
      <div className="bg-gradient-to-br from-purple-900/50 to-purple-800/30 backdrop-blur-sm border border-purple-700/50 rounded-lg p-6 hover:scale-105 transition-transform">
        <div className="flex items-center justify-between mb-2">
          <h3 className="text-sm font-medium text-purple-300 flex items-center">
            Memory
            <MetricTooltip text="Executor memory for computations and caching. High memory enables larger datasets and better caching." />
          </h3>
          <HardDrive className="h-5 w-5 text-purple-400" />
        </div>
        <p className="text-3xl font-bold text-white">{formatBytes(memoryUsed * 1024 * 1024)}</p>
        <div className="mt-2 bg-gray-700 rounded-full h-2">
          <div 
            className="bg-purple-500 h-2 rounded-full transition-all"
            style={{ width: `${memory > 0 ? (memoryUsed / memory) * 100 : 0}%` }}
          ></div>
        </div>
        <p className="text-xs text-purple-300 mt-1">
          of {formatBytes(memory * 1024 * 1024)} ({memory > 0 ? Math.round((memoryUsed / memory) * 100) : 0}%)
        </p>
      </div>

      {/* Active Apps Card - Shows running applications */}
      <div className="bg-gradient-to-br from-orange-900/50 to-orange-800/30 backdrop-blur-sm border border-orange-700/50 rounded-lg p-6 hover:scale-105 transition-transform">
        <div className="flex items-center justify-between mb-2">
          <h3 className="text-sm font-medium text-orange-300 flex items-center">
            Active Apps
            <MetricTooltip text="Spark applications currently running. Each app gets dedicated executors from the cluster." />
          </h3>
          <Activity className={`h-5 w-5 text-orange-400 ${activeApps > 0 ? 'animate-pulse' : ''}`} />
        </div>
        <p className="text-3xl font-bold text-white">{activeApps}</p>
        <p className="text-xs text-orange-300 mt-1">
          {activeApps === 0 ? 'Cluster idle' : `${activeApps} running`}
        </p>
      </div>
    </div>
  )
}
