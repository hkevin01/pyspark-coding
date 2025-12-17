import { useState, useEffect } from 'react'
import Head from 'next/head'
import ClusterOverview from '../components/ClusterOverview'
import ApplicationsList from '../components/ApplicationsList'
import WorkersList from '../components/WorkersList'
import MetricsChart from '../components/MetricsChart'
import LiveJobs from '../components/LiveJobs'
import TabNavigation, { TabType } from '../components/TabNavigation'
import RunJobs from '../components/RunJobs'
import LogsViewer from '../components/LogsViewer'
import SettingsPanel from '../components/SettingsPanel'
import DetailedMetrics from '../components/DetailedMetrics'
import { Activity, Server, Zap, Info } from 'lucide-react'

export default function Dashboard() {
  const [activeTab, setActiveTab] = useState<TabType>('monitor')
  const [autoRefresh, setAutoRefresh] = useState(true)
  const [refreshInterval, setRefreshInterval] = useState(5000)

  // Render content based on active tab
  const renderTabContent = () => {
    switch (activeTab) {
      case 'monitor':
        return (
          <>
            {/* Welcome Banner - explains what the dashboard shows */}
            <div className="bg-gradient-to-r from-spark-orange/10 to-blue-900/20 border border-spark-orange/30 rounded-lg p-4 mb-6">
              <div className="flex items-start">
                <Info className="h-5 w-5 text-spark-orange mr-3 mt-0.5 flex-shrink-0" />
                <div>
                  <h3 className="text-spark-orange font-medium">Cluster Overview</h3>
                  <p className="text-sm text-gray-300 mt-1">
                    This dashboard shows real-time metrics from your PySpark cluster. The cards below display 
                    <span className="text-blue-400"> worker count</span>, 
                    <span className="text-green-400"> CPU utilization</span>, 
                    <span className="text-purple-400"> memory usage</span>, and 
                    <span className="text-orange-400"> active applications</span>. 
                    For detailed explanations, switch to the <span className="text-spark-orange font-medium">Details</span> tab.
                  </p>
                </div>
              </div>
            </div>

            {/* Cluster Overview Cards - shows key metrics at a glance */}
            <ClusterOverview refreshInterval={autoRefresh ? refreshInterval : null} />

            {/* Live Jobs Section - displays currently running Spark applications */}
            <section className="mb-8">
              <div className="flex items-center justify-between mb-4">
                <h2 className="text-xl font-semibold text-white flex items-center">
                  <Activity className="mr-2 text-spark-orange" />
                  Live Applications
                </h2>
                <span className="text-xs text-gray-400 bg-gray-800 px-2 py-1 rounded">
                  Shows apps currently executing on the cluster
                </span>
              </div>
              <LiveJobs refreshInterval={autoRefresh ? refreshInterval : null} />
            </section>

            {/* Two Column Layout - Recent apps and worker status */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
              {/* Applications List - history of submitted jobs */}
              <section>
                <div className="flex items-center justify-between mb-4">
                  <h2 className="text-xl font-semibold text-white">Recent Applications</h2>
                  <span className="text-xs text-gray-400 bg-gray-800 px-2 py-1 rounded">
                    Job history & completion status
                  </span>
                </div>
                <ApplicationsList refreshInterval={autoRefresh ? refreshInterval : null} />
              </section>

              {/* Workers List - status of each compute node */}
              <section>
                <div className="flex items-center justify-between mb-4">
                  <h2 className="text-xl font-semibold text-white flex items-center">
                    <Server className="mr-2 text-blue-400" />
                    Cluster Workers
                  </h2>
                  <span className="text-xs text-gray-400 bg-gray-800 px-2 py-1 rounded">
                    Individual node health & resources
                  </span>
                </div>
                <WorkersList refreshInterval={autoRefresh ? refreshInterval : null} />
              </section>
            </div>

            {/* Metrics Charts - time-series performance data */}
            <section>
              <div className="flex items-center justify-between mb-4">
                <h2 className="text-xl font-semibold text-white">Performance Metrics</h2>
                <span className="text-xs text-gray-400 bg-gray-800 px-2 py-1 rounded">
                  Historical CPU & memory trends
                </span>
              </div>
              <MetricsChart refreshInterval={autoRefresh ? refreshInterval : null} />
            </section>
          </>
        )
      case 'details':
        return <DetailedMetrics refreshInterval={autoRefresh ? refreshInterval : null} />
      case 'run':
        return <RunJobs />
      case 'logs':
        return <LogsViewer />
      case 'settings':
        return (
          <SettingsPanel
            autoRefresh={autoRefresh}
            onAutoRefreshChange={setAutoRefresh}
            refreshInterval={refreshInterval}
            onRefreshIntervalChange={setRefreshInterval}
          />
        )
      default:
        return null
    }
  }

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
              
              {/* Show auto-refresh controls only on monitor tab */}
              {activeTab === 'monitor' && (
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
              )}
            </div>
          </div>
        </header>

        {/* Tab Navigation */}
        <TabNavigation activeTab={activeTab} onTabChange={setActiveTab} />

        {/* Main Content */}
        <main className="container mx-auto px-4 py-8">
          {renderTabContent()}
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
