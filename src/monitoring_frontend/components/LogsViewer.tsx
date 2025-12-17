import { useState, useEffect, useRef } from 'react'
import { Terminal, RefreshCw, Download, Trash2, Search, Filter, ChevronDown } from 'lucide-react'

interface LogEntry {
  timestamp: string
  level: 'INFO' | 'WARN' | 'ERROR' | 'DEBUG'
  source: string
  message: string
}

interface Props {
  refreshInterval?: number | null
}

const containers = [
  { id: 'spark-master', name: 'Spark Master', color: 'text-orange-400' },
  { id: 'spark-worker-1', name: 'Worker 1', color: 'text-blue-400' },
  { id: 'spark-worker-2', name: 'Worker 2', color: 'text-green-400' },
  { id: 'spark-worker-3', name: 'Worker 3', color: 'text-purple-400' },
  { id: 'prometheus', name: 'Prometheus', color: 'text-red-400' },
  { id: 'pyspark-monitoring-dashboard', name: 'Dashboard', color: 'text-cyan-400' },
]

export default function LogsViewer({ refreshInterval }: Props) {
  const [selectedContainer, setSelectedContainer] = useState('spark-master')
  const [logs, setLogs] = useState<string[]>([])
  const [isLoading, setIsLoading] = useState(false)
  const [autoScroll, setAutoScroll] = useState(true)
  const [filter, setFilter] = useState('')
  const [levelFilter, setLevelFilter] = useState<string>('all')
  const [tailLines, setTailLines] = useState(100)
  const logContainerRef = useRef<HTMLDivElement>(null)

  const fetchLogs = async () => {
    setIsLoading(true)
    try {
      const response = await fetch(`/api/logs/${selectedContainer}?tail=${tailLines}`)
      if (response.ok) {
        const data = await response.json()
        setLogs(data.logs || [])
      } else {
        setLogs([`Error fetching logs: ${response.statusText}`])
      }
    } catch (error: any) {
      setLogs([`Error: ${error.message}`])
    } finally {
      setIsLoading(false)
    }
  }

  useEffect(() => {
    fetchLogs()
  }, [selectedContainer, tailLines])

  useEffect(() => {
    if (refreshInterval && refreshInterval > 0) {
      const interval = setInterval(fetchLogs, refreshInterval)
      return () => clearInterval(interval)
    }
  }, [refreshInterval, selectedContainer])

  useEffect(() => {
    if (autoScroll && logContainerRef.current) {
      logContainerRef.current.scrollTop = logContainerRef.current.scrollHeight
    }
  }, [logs, autoScroll])

  const filteredLogs = logs.filter(line => {
    if (filter && !line.toLowerCase().includes(filter.toLowerCase())) {
      return false
    }
    if (levelFilter !== 'all') {
      if (levelFilter === 'error' && !line.includes('ERROR') && !line.includes('Exception') && !line.includes('FATAL')) {
        return false
      }
      if (levelFilter === 'warn' && !line.includes('WARN')) {
        return false
      }
      if (levelFilter === 'info' && !line.includes('INFO')) {
        return false
      }
    }
    return true
  })

  const getLineColor = (line: string) => {
    if (line.includes('ERROR') || line.includes('Exception') || line.includes('FATAL')) {
      return 'text-red-400'
    }
    if (line.includes('WARN')) {
      return 'text-yellow-400'
    }
    if (line.includes('INFO')) {
      return 'text-green-400'
    }
    if (line.includes('DEBUG')) {
      return 'text-gray-500'
    }
    return 'text-gray-300'
  }

  const downloadLogs = () => {
    const blob = new Blob([logs.join('\n')], { type: 'text/plain' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `${selectedContainer}-logs-${new Date().toISOString().split('T')[0]}.txt`
    a.click()
    URL.revokeObjectURL(url)
  }

  return (
    <div className="space-y-4">
      {/* Container Selector */}
      <div className="flex flex-wrap gap-2">
        {containers.map((container) => (
          <button
            key={container.id}
            onClick={() => setSelectedContainer(container.id)}
            className={`px-3 py-2 rounded-lg text-sm font-medium transition-colors flex items-center ${
              selectedContainer === container.id
                ? 'bg-gray-700 text-white border border-gray-600'
                : 'bg-gray-800/50 text-gray-400 hover:text-white hover:bg-gray-700/50'
            }`}
          >
            <Terminal className={`h-4 w-4 mr-2 ${container.color}`} />
            {container.name}
          </button>
        ))}
      </div>

      {/* Controls */}
      <div className="flex flex-wrap items-center gap-4 bg-gray-800/50 border border-gray-700 rounded-lg p-4">
        {/* Search */}
        <div className="flex-1 min-w-[200px] relative">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-500" />
          <input
            type="text"
            placeholder="Search logs..."
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            className="w-full bg-gray-900 border border-gray-600 rounded-lg pl-10 pr-4 py-2 text-white text-sm placeholder-gray-500 focus:border-spark-orange focus:ring-1 focus:ring-spark-orange"
          />
        </div>

        {/* Level Filter */}
        <div className="relative">
          <Filter className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-500" />
          <select
            value={levelFilter}
            onChange={(e) => setLevelFilter(e.target.value)}
            className="bg-gray-900 border border-gray-600 rounded-lg pl-10 pr-8 py-2 text-white text-sm appearance-none cursor-pointer"
          >
            <option value="all">All Levels</option>
            <option value="error">Errors Only</option>
            <option value="warn">Warnings</option>
            <option value="info">Info</option>
          </select>
          <ChevronDown className="absolute right-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-500 pointer-events-none" />
        </div>

        {/* Tail Lines */}
        <select
          value={tailLines}
          onChange={(e) => setTailLines(Number(e.target.value))}
          className="bg-gray-900 border border-gray-600 rounded-lg px-3 py-2 text-white text-sm"
        >
          <option value={50}>Last 50 lines</option>
          <option value={100}>Last 100 lines</option>
          <option value={200}>Last 200 lines</option>
          <option value={500}>Last 500 lines</option>
        </select>

        {/* Auto-scroll toggle */}
        <label className="flex items-center cursor-pointer">
          <input
            type="checkbox"
            checked={autoScroll}
            onChange={(e) => setAutoScroll(e.target.checked)}
            className="sr-only peer"
          />
          <div className="relative w-9 h-5 bg-gray-700 peer-focus:outline-none rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:rounded-full after:h-4 after:w-4 after:transition-all peer-checked:bg-green-600"></div>
          <span className="ml-2 text-sm text-gray-400">Auto-scroll</span>
        </label>

        {/* Actions */}
        <button
          onClick={fetchLogs}
          disabled={isLoading}
          className="bg-gray-700 hover:bg-gray-600 text-white py-2 px-3 rounded-lg flex items-center text-sm"
        >
          <RefreshCw className={`h-4 w-4 mr-1 ${isLoading ? 'animate-spin' : ''}`} />
          Refresh
        </button>

        <button
          onClick={downloadLogs}
          className="bg-gray-700 hover:bg-gray-600 text-white py-2 px-3 rounded-lg flex items-center text-sm"
        >
          <Download className="h-4 w-4 mr-1" />
          Download
        </button>
      </div>

      {/* Log Output */}
      <div className="bg-gray-900 border border-gray-700 rounded-lg overflow-hidden">
        <div className="flex items-center justify-between px-4 py-2 bg-gray-800 border-b border-gray-700">
          <span className="text-sm text-gray-400">
            {selectedContainer} — {filteredLogs.length} lines
            {filter && ` (filtered from ${logs.length})`}
          </span>
          <div className="flex items-center space-x-2">
            <span className="text-xs text-gray-500">
              {isLoading ? 'Loading...' : 'Ready'}
            </span>
            {isLoading && <RefreshCw className="h-3 w-3 text-gray-500 animate-spin" />}
          </div>
        </div>
        
        <div
          ref={logContainerRef}
          className="p-4 h-[500px] overflow-auto font-mono text-xs leading-relaxed"
        >
          {filteredLogs.length === 0 ? (
            <div className="text-gray-500 text-center py-8">
              {isLoading ? 'Loading logs...' : 'No logs found'}
            </div>
          ) : (
            filteredLogs.map((line, idx) => (
              <div
                key={idx}
                className={`py-0.5 hover:bg-gray-800/50 ${getLineColor(line)}`}
              >
                <span className="text-gray-600 mr-3 select-none">{String(idx + 1).padStart(4, ' ')}</span>
                {line}
              </div>
            ))
          )}
        </div>
      </div>

      {/* Log Stats */}
      <div className="flex items-center justify-between text-xs text-gray-500">
        <div className="flex items-center space-x-4">
          <span className="flex items-center">
            <span className="w-2 h-2 rounded-full bg-red-400 mr-1"></span>
            Errors: {logs.filter(l => l.includes('ERROR') || l.includes('Exception')).length}
          </span>
          <span className="flex items-center">
            <span className="w-2 h-2 rounded-full bg-yellow-400 mr-1"></span>
            Warnings: {logs.filter(l => l.includes('WARN')).length}
          </span>
          <span className="flex items-center">
            <span className="w-2 h-2 rounded-full bg-green-400 mr-1"></span>
            Info: {logs.filter(l => l.includes('INFO')).length}
          </span>
        </div>
        <span>Total: {logs.length} lines</span>
      </div>
    </div>
  )
}
