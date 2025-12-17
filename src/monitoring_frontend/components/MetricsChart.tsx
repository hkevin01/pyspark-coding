import { useState, useEffect } from 'react'
import useSWR from 'swr'
import { LineChart, Line, AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts'
import { fetchClusterStatus, ClusterStatus } from '../lib/sparkApi'
import { TrendingUp, Info, Cpu, HardDrive, Activity } from 'lucide-react'

interface Props {
  refreshInterval: number | null
}

interface MetricData {
  timestamp: string
  cpuUsage: number
  memoryUsage: number
  activeApps: number
}

// Tooltip component for metric explanations
function MetricInfo({ text }: { text: string }) {
  return (
    <div className="group relative inline-block ml-2">
      <Info className="h-4 w-4 text-gray-500 hover:text-gray-300 cursor-help" />
      <div className="absolute z-50 hidden group-hover:block w-72 p-3 text-xs bg-gray-900 border border-gray-600 rounded-lg shadow-xl -translate-x-1/2 left-1/2 mt-1">
        <div className="absolute -top-1 left-1/2 -translate-x-1/2 w-2 h-2 bg-gray-900 border-l border-t border-gray-600 rotate-45"></div>
        {text}
      </div>
    </div>
  )
}

export default function MetricsChart({ refreshInterval }: Props) {
  const [metricsHistory, setMetricsHistory] = useState<MetricData[]>([])
  
  const { data: cluster } = useSWR<ClusterStatus>(
    'cluster-status',
    fetchClusterStatus,
    { refreshInterval: refreshInterval || 0 }
  )

  useEffect(() => {
    if (cluster) {
      const now = new Date()
      const timestamp = `${now.getHours()}:${String(now.getMinutes()).padStart(2, '0')}:${String(now.getSeconds()).padStart(2, '0')}`
      
      const newData: MetricData = {
        timestamp,
        cpuUsage: cluster.cores > 0 ? Math.round((cluster.coresused / cluster.cores) * 100) : 0,
        memoryUsage: cluster.memory > 0 ? Math.round((cluster.memoryused / cluster.memory) * 100) : 0,
        activeApps: cluster.activeapps?.length || 0,
      }

      setMetricsHistory(prev => {
        const updated = [...prev, newData]
        // Keep last 20 data points
        return updated.slice(-20)
      })
    }
  }, [cluster])

  if (metricsHistory.length === 0) {
    return (
      <div className="bg-gray-800/50 border border-gray-700 rounded-lg p-8 text-center">
        <TrendingUp className="h-12 w-12 text-gray-600 mx-auto mb-3" />
        <p className="text-gray-400">Collecting metrics data...</p>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Metrics Explanation Banner */}
      <div className="bg-gradient-to-r from-blue-900/20 to-purple-900/20 border border-blue-700/30 rounded-lg p-4">
        <div className="flex items-start">
          <TrendingUp className="h-5 w-5 text-blue-400 mr-3 mt-0.5 flex-shrink-0" />
          <div>
            <h3 className="text-blue-300 font-medium">📊 Performance Metrics Explained</h3>
            <p className="text-sm text-gray-300 mt-1">
              These charts show <span className="text-green-400">real-time resource usage</span> across your cluster.
              <span className="text-white font-medium"> CPU %</span> shows how many cores are actively processing tasks.
              <span className="text-purple-400 font-medium"> Memory %</span> shows executor memory allocation.
              Spikes indicate job activity; flat lines mean the cluster is idle.
            </p>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* CPU & Memory Usage */}
        <div className="bg-gray-800/50 border border-gray-700 rounded-lg p-6">
          <h3 className="text-white font-medium mb-2 flex items-center">
            <Cpu className="h-5 w-5 mr-2 text-green-400" />
            Resource Utilization
            <MetricInfo text="CPU Usage: Percentage of cluster cores currently executing tasks. 0% means no tasks running, 100% means all cores are busy. Memory Usage: Percentage of executor memory allocated to running applications. High memory usage may indicate need for more resources or memory leaks." />
          </h3>
          <p className="text-xs text-gray-400 mb-4">
            Shows how efficiently your cluster resources are being used over time
          </p>
          <ResponsiveContainer width="100%" height={250}>
            <AreaChart data={metricsHistory}>
              <defs>
                <linearGradient id="colorCpu" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#10b981" stopOpacity={0.8}/>
                  <stop offset="95%" stopColor="#10b981" stopOpacity={0}/>
                </linearGradient>
                <linearGradient id="colorMemory" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#8b5cf6" stopOpacity={0.8}/>
                  <stop offset="95%" stopColor="#8b5cf6" stopOpacity={0}/>
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="timestamp" stroke="#9ca3af" style={{ fontSize: '12px' }} />
              <YAxis stroke="#9ca3af" style={{ fontSize: '12px' }} domain={[0, 100]} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '0.5rem' }}
                labelStyle={{ color: '#f3f4f6' }}
                formatter={(value: number, name: string) => [
                  `${value}%`,
                  name === 'cpuUsage' ? 'CPU (cores in use)' : 'Memory (allocated)'
                ]}
              />
              <Legend />
              <Area type="monotone" dataKey="cpuUsage" stroke="#10b981" fillOpacity={1} fill="url(#colorCpu)" name="CPU %" />
              <Area type="monotone" dataKey="memoryUsage" stroke="#8b5cf6" fillOpacity={1} fill="url(#colorMemory)" name="Memory %" />
            </AreaChart>
          </ResponsiveContainer>
          <div className="mt-3 grid grid-cols-2 gap-2 text-xs">
            <div className="bg-green-500/10 rounded p-2">
              <span className="text-green-400 font-medium">CPU %</span>
              <p className="text-gray-400">coresUsed / totalCores × 100</p>
            </div>
            <div className="bg-purple-500/10 rounded p-2">
              <span className="text-purple-400 font-medium">Memory %</span>
              <p className="text-gray-400">memoryUsed / totalMemory × 100</p>
            </div>
          </div>
        </div>

        {/* Active Applications */}
        <div className="bg-gray-800/50 border border-gray-700 rounded-lg p-6">
          <h3 className="text-white font-medium mb-2 flex items-center">
            <Activity className="h-5 w-5 mr-2 text-orange-400" />
            Active Applications
            <MetricInfo text="Number of Spark applications currently running on the cluster. Each application is a separate SparkContext (like a Jupyter notebook session or a submitted job). Multiple apps share cluster resources. When this drops to 0, the cluster is idle." />
          </h3>
          <p className="text-xs text-gray-400 mb-4">
            Track how many jobs are running concurrently on your cluster
          </p>
          <ResponsiveContainer width="100%" height={250}>
            <LineChart data={metricsHistory}>
              <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
              <XAxis dataKey="timestamp" stroke="#9ca3af" style={{ fontSize: '12px' }} />
              <YAxis stroke="#9ca3af" style={{ fontSize: '12px' }} allowDecimals={false} />
              <Tooltip
                contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '0.5rem' }}
                labelStyle={{ color: '#f3f4f6' }}
                formatter={(value: number) => [`${value} app(s)`, 'Running']}
              />
              <Legend />
              <Line type="monotone" dataKey="activeApps" stroke="#f97316" strokeWidth={3} dot={{ fill: '#f97316', r: 4 }} name="Applications" />
            </LineChart>
          </ResponsiveContainer>
          <div className="mt-3 bg-orange-500/10 rounded p-2 text-xs">
            <span className="text-orange-400 font-medium">💡 Tip:</span>
            <span className="text-gray-400 ml-1">
              Each app = separate SparkContext. Too many concurrent apps can cause resource contention.
            </span>
          </div>
        </div>
      </div>

      {/* What These Metrics Tell You */}
      <div className="bg-gray-800/30 border border-gray-700 rounded-lg p-4">
        <h4 className="text-white font-medium mb-3 flex items-center">
          <Info className="h-4 w-4 mr-2 text-blue-400" />
          How to Read These Metrics
        </h4>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
          <div className="space-y-1">
            <p className="text-green-400 font-medium">📈 High CPU + Low Memory</p>
            <p className="text-gray-400 text-xs">CPU-bound workload (computations). Tasks are running but not caching much data.</p>
          </div>
          <div className="space-y-1">
            <p className="text-purple-400 font-medium">📈 High Memory + Low CPU</p>
            <p className="text-gray-400 text-xs">Data cached but idle, or waiting for I/O. Consider if you need all that cache.</p>
          </div>
          <div className="space-y-1">
            <p className="text-orange-400 font-medium">📈 Spiky Patterns</p>
            <p className="text-gray-400 text-xs">Jobs starting/stopping. Normal for batch processing. Streaming should be smoother.</p>
          </div>
        </div>
      </div>
    </div>
  )
}
