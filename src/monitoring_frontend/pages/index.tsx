import { useState, useEffect } from 'react'
import Head from 'next/head'
import ClusterOverview from '../components/ClusterOverview'
import ApplicationsList from '../components/ApplicationsList'
import WorkersList from '../components/WorkersList'
import MetricsChart from '../components/MetricsChart'
import LiveJobs from '../components/LiveJobs'
import { Activity, Server, Zap } from 'lucide-react'

export default function Dashboard() {
  const [autoRefresh, setAutoRefresh] = useState(true)
  const [refreshInterval, setRefreshInterval] = useState(5000)

  return (
    <>
      <Head>
        <title>PySpark Monitoring Dashboard</title>
        <meta name="description" content="Real-time PySpark Cluster Monitoring" />
        <link rel="icon" href="/favicon.ico" />
      </Head>

      <div className="min-h-screen bg-gradient-to-br from-gray-900 via-blue-900 to-gray-900">
        {/* Header */}
        <header className="bg-gray-800/50 backdrop-blur-sm border-b border-gray-700 sticky top-0 z-50">
          <div className="container mx-auto px-4 py-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center space-x-3">
                <Zap className="h-8 w-8 text-spark-orange" />
                <h1 className="text-2xl font-bold text-white">
                  PySpark Cluster Monitor
                </h1>
              </div>
              
              <div className="flex items-center space-x-4">
                <div className="flex items-center space-x-2">
                  <Activity className={`h-5 w-5 ${autoRefresh ? 'text-green-400 animate-pulse' : 'text-gray-400'}`} />
                  <label className="flex items-center cursor-pointer">
                    <input
                      type="checkbox"
                      checked={autoRefresh}
                      onChange={(e) => setAutoRefresh(e.target.checked)}
                      className="sr-only peer"
                    />
                    <div className="relative w-11 h-6 bg-gray-700 peer-focus:outline-none rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-green-600"></div>
                  </label>
                  <span className="text-sm text-gray-300">Auto-Refresh</span>
                </div>
                
                <select
                  value={refreshInterval}
                  onChange={(e) => setRefreshInterval(Number(e.target.value))}
                  className="bg-gray-700 text-white rounded-md px-3 py-1 text-sm"
                  disabled={!autoRefresh}
                >
                  <option value={2000}>2s</option>
                  <option value={5000}>5s</option>
                  <option value={10000}>10s</option>
                  <option value={30000}>30s</option>
                </select>
              </div>
            </div>
          </div>
        </header>

        {/* Main Content */}
        <main className="container mx-auto px-4 py-8">
          {/* Cluster Overview Cards */}
          <ClusterOverview refreshInterval={autoRefresh ? refreshInterval : null} />

          {/* Live Jobs Section */}
          <section className="mb-8">
            <h2 className="text-xl font-semibold text-white mb-4 flex items-center">
              <Activity className="mr-2" />
              Live Applications
            </h2>
            <LiveJobs refreshInterval={autoRefresh ? refreshInterval : null} />
          </section>

          {/* Two Column Layout */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
            {/* Applications List */}
            <section>
              <h2 className="text-xl font-semibold text-white mb-4">
                Recent Applications
              </h2>
              <ApplicationsList refreshInterval={autoRefresh ? refreshInterval : null} />
            </section>

            {/* Workers List */}
            <section>
              <h2 className="text-xl font-semibold text-white mb-4 flex items-center">
                <Server className="mr-2" />
                Cluster Workers
              </h2>
              <WorkersList refreshInterval={autoRefresh ? refreshInterval : null} />
            </section>
          </div>

          {/* Metrics Charts */}
          <section>
            <h2 className="text-xl font-semibold text-white mb-4">
              Performance Metrics
            </h2>
            <MetricsChart refreshInterval={autoRefresh ? refreshInterval : null} />
          </section>
        </main>

        {/* Footer */}
        <footer className="bg-gray-800/50 border-t border-gray-700 py-4 mt-12">
          <div className="container mx-auto px-4 text-center text-gray-400 text-sm">
            <p>PySpark Monitoring Dashboard | Built with Next.js & Spark REST API</p>
          </div>
        </footer>
      </div>
    </>
  )
}
