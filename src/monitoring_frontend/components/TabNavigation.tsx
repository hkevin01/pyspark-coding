import { Activity, Play, FileText, Settings, Terminal, BarChart3 } from 'lucide-react'

export type TabType = 'monitor' | 'details' | 'run' | 'logs' | 'settings'

interface TabNavigationProps {
  activeTab: TabType
  onTabChange: (tab: TabType) => void
}

const tabs = [
  { id: 'monitor' as TabType, label: 'Monitor', icon: Activity, description: 'Real-time cluster overview' },
  { id: 'details' as TabType, label: 'Details', icon: BarChart3, description: 'In-depth metrics & explanations' },
  { id: 'run' as TabType, label: 'Run Jobs', icon: Play, description: 'Submit and manage Spark jobs' },
  { id: 'logs' as TabType, label: 'Logs', icon: Terminal, description: 'View container logs' },
  { id: 'settings' as TabType, label: 'Settings', icon: Settings, description: 'Configure dashboard' },
]

export default function TabNavigation({ activeTab, onTabChange }: TabNavigationProps) {
  return (
    <div className="border-b border-gray-700">
      <nav className="flex space-x-1" aria-label="Tabs">
        {tabs.map((tab) => {
          const Icon = tab.icon
          const isActive = activeTab === tab.id
          
          return (
            <button
              key={tab.id}
              onClick={() => onTabChange(tab.id)}
              className={`
                group relative flex items-center px-4 py-3 text-sm font-medium rounded-t-lg transition-all
                ${isActive 
                  ? 'bg-gray-800 text-white border-b-2 border-spark-orange' 
                  : 'text-gray-400 hover:text-white hover:bg-gray-800/50'
                }
              `}
              title={tab.description}
            >
              <Icon className={`h-4 w-4 mr-2 ${isActive ? 'text-spark-orange' : ''}`} />
              {tab.label}
              
              {/* Active indicator dot */}
              {isActive && (
                <span className="absolute bottom-0 left-1/2 transform -translate-x-1/2 translate-y-1/2 w-2 h-2 bg-spark-orange rounded-full" />
              )}
            </button>
          )
        })}
      </nav>
    </div>
  )
}
