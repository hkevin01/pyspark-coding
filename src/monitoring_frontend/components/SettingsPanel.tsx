import { useState } from 'react'
import { Settings, ExternalLink, RefreshCw, Moon, Sun, Bell, Database, Server, Shield } from 'lucide-react'

interface Props {
  refreshInterval: number | null
  onRefreshIntervalChange: (interval: number) => void
  autoRefresh: boolean
  onAutoRefreshChange: (enabled: boolean) => void
}

export default function SettingsPanel({ 
  refreshInterval, 
  onRefreshIntervalChange, 
  autoRefresh, 
  onAutoRefreshChange 
}: Props) {
  const [theme, setTheme] = useState('dark')
  const [notifications, setNotifications] = useState(false)

  const externalLinks = [
    { name: 'Spark Master UI', url: 'http://localhost:9080', icon: Server, color: 'text-orange-400' },
    { name: 'Prometheus', url: 'http://localhost:9090', icon: Database, color: 'text-red-400' },
    { name: 'Worker 1 UI', url: 'http://localhost:8081', icon: Server, color: 'text-blue-400' },
    { name: 'Worker 2 UI', url: 'http://localhost:8082', icon: Server, color: 'text-green-400' },
    { name: 'Worker 3 UI', url: 'http://localhost:8083', icon: Server, color: 'text-purple-400' },
    { name: 'Spark App UI', url: 'http://localhost:4040', icon: Server, color: 'text-cyan-400' },
  ]

  return (
    <div className="space-y-8">
      {/* Refresh Settings */}
      <section className="bg-gray-800/50 border border-gray-700 rounded-lg p-6">
        <h3 className="text-white font-medium mb-4 flex items-center">
          <RefreshCw className="h-5 w-5 mr-2 text-spark-orange" />
          Auto-Refresh Settings
        </h3>
        
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-white">Enable Auto-Refresh</p>
              <p className="text-sm text-gray-400">Automatically fetch new data at regular intervals</p>
            </div>
            <label className="flex items-center cursor-pointer">
              <input
                type="checkbox"
                checked={autoRefresh}
                onChange={(e) => onAutoRefreshChange(e.target.checked)}
                className="sr-only peer"
              />
              <div className="relative w-11 h-6 bg-gray-700 peer-focus:outline-none rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-green-600"></div>
            </label>
          </div>
          
          <div className="flex items-center justify-between">
            <div>
              <p className="text-white">Refresh Interval</p>
              <p className="text-sm text-gray-400">How often to fetch new data</p>
            </div>
            <select
              value={refreshInterval || 5000}
              onChange={(e) => onRefreshIntervalChange(Number(e.target.value))}
              disabled={!autoRefresh}
              className="bg-gray-900 border border-gray-600 rounded-lg px-4 py-2 text-white disabled:opacity-50"
            >
              <option value={1000}>1 second</option>
              <option value={2000}>2 seconds</option>
              <option value={5000}>5 seconds</option>
              <option value={10000}>10 seconds</option>
              <option value={30000}>30 seconds</option>
              <option value={60000}>1 minute</option>
            </select>
          </div>
        </div>
      </section>

      {/* Display Settings */}
      <section className="bg-gray-800/50 border border-gray-700 rounded-lg p-6">
        <h3 className="text-white font-medium mb-4 flex items-center">
          <Moon className="h-5 w-5 mr-2 text-spark-orange" />
          Display Settings
        </h3>
        
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-white">Theme</p>
              <p className="text-sm text-gray-400">Choose your preferred theme</p>
            </div>
            <div className="flex space-x-2">
              <button
                onClick={() => setTheme('dark')}
                className={`px-4 py-2 rounded-lg flex items-center ${
                  theme === 'dark' ? 'bg-gray-700 text-white' : 'bg-gray-900 text-gray-400'
                }`}
              >
                <Moon className="h-4 w-4 mr-2" />
                Dark
              </button>
              <button
                onClick={() => setTheme('light')}
                className={`px-4 py-2 rounded-lg flex items-center ${
                  theme === 'light' ? 'bg-gray-700 text-white' : 'bg-gray-900 text-gray-400'
                }`}
                disabled
              >
                <Sun className="h-4 w-4 mr-2" />
                Light
                <span className="ml-2 text-xs bg-yellow-500/20 text-yellow-400 px-2 py-0.5 rounded">Soon</span>
              </button>
            </div>
          </div>
          
          <div className="flex items-center justify-between">
            <div>
              <p className="text-white">Browser Notifications</p>
              <p className="text-sm text-gray-400">Get notified about job completions and errors</p>
            </div>
            <label className="flex items-center cursor-pointer">
              <input
                type="checkbox"
                checked={notifications}
                onChange={(e) => setNotifications(e.target.checked)}
                className="sr-only peer"
              />
              <div className="relative w-11 h-6 bg-gray-700 peer-focus:outline-none rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-green-600"></div>
            </label>
          </div>
        </div>
      </section>

      {/* Quick Links */}
      <section className="bg-gray-800/50 border border-gray-700 rounded-lg p-6">
        <h3 className="text-white font-medium mb-4 flex items-center">
          <ExternalLink className="h-5 w-5 mr-2 text-spark-orange" />
          External Links
        </h3>
        
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
          {externalLinks.map((link) => {
            const Icon = link.icon
            return (
              <a
                key={link.name}
                href={link.url}
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center justify-between p-3 bg-gray-900/50 rounded-lg hover:bg-gray-900 transition-colors group"
              >
                <div className="flex items-center">
                  <Icon className={`h-4 w-4 mr-3 ${link.color}`} />
                  <span className="text-white text-sm">{link.name}</span>
                </div>
                <ExternalLink className="h-4 w-4 text-gray-500 group-hover:text-white transition-colors" />
              </a>
            )
          })}
        </div>
      </section>

      {/* About */}
      <section className="bg-gray-800/50 border border-gray-700 rounded-lg p-6">
        <h3 className="text-white font-medium mb-4 flex items-center">
          <Shield className="h-5 w-5 mr-2 text-spark-orange" />
          About
        </h3>
        
        <div className="space-y-3 text-sm">
          <div className="flex justify-between">
            <span className="text-gray-400">Dashboard Version</span>
            <span className="text-white">1.0.0</span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-400">Spark Version</span>
            <span className="text-white">3.5.0</span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-400">Next.js Version</span>
            <span className="text-white">14.x</span>
          </div>
          <div className="flex justify-between">
            <span className="text-gray-400">API Status</span>
            <span className="text-green-400 flex items-center">
              <span className="w-2 h-2 rounded-full bg-green-400 mr-2 animate-pulse"></span>
              Connected
            </span>
          </div>
        </div>
        
        <div className="mt-4 pt-4 border-t border-gray-700">
          <p className="text-gray-500 text-xs">
            PySpark Monitoring Dashboard — Built with Next.js, React, and the Spark REST API.
            Designed for real-time cluster monitoring and job management.
          </p>
        </div>
      </section>
    </div>
  )
}
