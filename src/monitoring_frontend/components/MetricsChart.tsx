import { useState, useEffect } from 'react'
import useSWR from 'swr'
import { LineChart, Line, AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts'
import { fetchClusterStatus, ClusterStatus } from '../lib/sparkApi'
import { TrendingUp } from 'lucide-react'

interface Props {
  refreshInterval: number | null
}

interface MetricData {
  timestamp: string
  cpuUsage: number
  memoryUsage: number
  activeApps: number
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
        cpuUsage: cluster.cores > 0 ? Math.round((cluster.coresUsed / cluster.cores) * 100) : 0,
        memoryUsage: cluster.memory > 0 ? Math.round((cluster.memoryUsed / cluster.memory) * 100) : 0,
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
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
      {/* CPU & Memory Usage */}
      <div className="bg-gray-800/50 border border-gray-700 rounded-lg p-6">
        <h3 className="text-white font-medium mb-4 flex items-center">
          <TrendingUp className="h-5 w-5 mr-2 text-blue-400" />
          Resource Utilization
        </h3>
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
            <YAxis stroke="#9ca3af" style={{ fontSize: '12px' }} />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '0.5rem' }}
              labelStyle={{ color: '#f3f4f6' }}
            />
            <Legend />
            <Area type="monotone" dataKey="cpuUsage" stroke="#10b981" fillOpacity={1} fill="url(#colorCpu)" name="CPU %" />
            <Area type="monotone" dataKey="memoryUsage" stroke="#8b5cf6" fillOpacity={1} fill="url(#colorMemory)" name="Memory %" />
          </AreaChart>
        </ResponsiveContainer>
      </div>

      {/* Active Applications */}
      <div className="bg-gray-800/50 border border-gray-700 rounded-lg p-6">
        <h3 className="text-white font-medium mb-4 flex items-center">
          <TrendingUp className="h-5 w-5 mr-2 text-orange-400" />
          Active Applications
        </h3>
        <ResponsiveContainer width="100%" height={250}>
          <LineChart data={metricsHistory}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis dataKey="timestamp" stroke="#9ca3af" style={{ fontSize: '12px' }} />
            <YAxis stroke="#9ca3af" style={{ fontSize: '12px' }} allowDecimals={false} />
            <Tooltip
              contentStyle={{ backgroundColor: '#1f2937', border: '1px solid #374151', borderRadius: '0.5rem' }}
              labelStyle={{ color: '#f3f4f6' }}
            />
            <Legend />
            <Line type="monotone" dataKey="activeApps" stroke="#f97316" strokeWidth={3} dot={{ fill: '#f97316', r: 4 }} name="Applications" />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}
