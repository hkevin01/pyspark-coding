import { useState, useEffect } from 'react'
import { Play, Square, RefreshCw, Upload, FileCode, Clock, CheckCircle, XCircle, Loader2, Trash2, FolderOpen } from 'lucide-react'

interface JobTemplate {
  id: string
  name: string
  description: string
  command: string
  args?: string[]
  icon: string
}

interface RunningJob {
  id: string
  name: string
  status: 'running' | 'completed' | 'failed' | 'pending'
  startTime: string
  duration?: number
  output?: string
}

const defaultTemplates: JobTemplate[] = [
  {
    id: 'pi',
    name: 'Calculate Pi',
    description: 'Classic Spark Pi calculation example',
    command: '/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/examples/src/main/python/pi.py 100',
    icon: '🥧'
  },
  {
    id: 'wordcount',
    name: 'Word Count',
    description: 'Count words in a sample text file',
    command: '/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/wordcount.py',
    icon: '📝'
  },
  {
    id: 'long-demo',
    name: 'Long Running Demo',
    description: 'A demo job that runs for several minutes',
    command: '/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/long_running_demo.py',
    icon: '⏱️'
  },
  {
    id: 'etl-demo',
    name: 'ETL Pipeline Demo',
    description: 'Sample ETL pipeline with transformations',
    command: '/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/etl_demo.py',
    icon: '🔄'
  },
]

interface Props {
  refreshInterval?: number | null
}

export default function RunJobs({ refreshInterval }: Props) {
  const [templates] = useState<JobTemplate[]>(defaultTemplates)
  const [runningJobs, setRunningJobs] = useState<RunningJob[]>([])
  const [customCommand, setCustomCommand] = useState('')
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [submitResult, setSubmitResult] = useState<{ success: boolean; message: string } | null>(null)
  const [activeSection, setActiveSection] = useState<'templates' | 'custom' | 'files'>('templates')

  // Submit a job
  const submitJob = async (command: string, name: string) => {
    setIsSubmitting(true)
    setSubmitResult(null)
    
    const jobId = `job-${Date.now()}`
    const newJob: RunningJob = {
      id: jobId,
      name: name,
      status: 'pending',
      startTime: new Date().toISOString(),
    }
    
    setRunningJobs(prev => [newJob, ...prev])
    
    try {
      const response = await fetch('/api/spark/submit', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ command, jobId }),
      })
      
      const result = await response.json()
      
      if (response.ok) {
        setRunningJobs(prev => 
          prev.map(j => j.id === jobId ? { ...j, status: 'running' } : j)
        )
        setSubmitResult({ success: true, message: `Job "${name}" submitted successfully!` })
      } else {
        setRunningJobs(prev => 
          prev.map(j => j.id === jobId ? { ...j, status: 'failed', output: result.error } : j)
        )
        setSubmitResult({ success: false, message: result.error || 'Failed to submit job' })
      }
    } catch (error: any) {
      setRunningJobs(prev => 
        prev.map(j => j.id === jobId ? { ...j, status: 'failed', output: error.message } : j)
      )
      setSubmitResult({ success: false, message: error.message || 'Network error' })
    } finally {
      setIsSubmitting(false)
    }
  }

  const clearCompletedJobs = () => {
    setRunningJobs(prev => prev.filter(j => j.status === 'running' || j.status === 'pending'))
  }

  return (
    <div className="space-y-6">
      {/* Section Tabs */}
      <div className="flex space-x-2 border-b border-gray-700 pb-2">
        <button
          onClick={() => setActiveSection('templates')}
          className={`px-4 py-2 rounded-t-lg flex items-center ${
            activeSection === 'templates' 
              ? 'bg-gray-700 text-white' 
              : 'text-gray-400 hover:text-white'
          }`}
        >
          <FileCode className="h-4 w-4 mr-2" />
          Job Templates
        </button>
        <button
          onClick={() => setActiveSection('custom')}
          className={`px-4 py-2 rounded-t-lg flex items-center ${
            activeSection === 'custom' 
              ? 'bg-gray-700 text-white' 
              : 'text-gray-400 hover:text-white'
          }`}
        >
          <Play className="h-4 w-4 mr-2" />
          Custom Command
        </button>
        <button
          onClick={() => setActiveSection('files')}
          className={`px-4 py-2 rounded-t-lg flex items-center ${
            activeSection === 'files' 
              ? 'bg-gray-700 text-white' 
              : 'text-gray-400 hover:text-white'
          }`}
        >
          <FolderOpen className="h-4 w-4 mr-2" />
          Browse Files
        </button>
      </div>

      {/* Submit Result Message */}
      {submitResult && (
        <div className={`p-4 rounded-lg flex items-center ${
          submitResult.success ? 'bg-green-500/20 border border-green-500/50' : 'bg-red-500/20 border border-red-500/50'
        }`}>
          {submitResult.success ? (
            <CheckCircle className="h-5 w-5 text-green-400 mr-2" />
          ) : (
            <XCircle className="h-5 w-5 text-red-400 mr-2" />
          )}
          <span className={submitResult.success ? 'text-green-300' : 'text-red-300'}>
            {submitResult.message}
          </span>
          <button
            onClick={() => setSubmitResult(null)}
            className="ml-auto text-gray-400 hover:text-white"
          >
            ✕
          </button>
        </div>
      )}

      {/* Templates Section */}
      {activeSection === 'templates' && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {templates.map((template) => (
            <div
              key={template.id}
              className="bg-gray-800/50 border border-gray-700 rounded-lg p-4 hover:border-spark-orange/50 transition-colors"
            >
              <div className="flex items-start justify-between mb-3">
                <div className="flex items-center">
                  <span className="text-2xl mr-3">{template.icon}</span>
                  <div>
                    <h3 className="text-white font-medium">{template.name}</h3>
                    <p className="text-gray-400 text-sm">{template.description}</p>
                  </div>
                </div>
              </div>
              
              <div className="bg-gray-900/50 rounded p-2 mb-3 font-mono text-xs text-gray-400 overflow-x-auto">
                {template.command}
              </div>
              
              <button
                onClick={() => submitJob(template.command, template.name)}
                disabled={isSubmitting}
                className="w-full bg-spark-orange hover:bg-spark-orange/80 disabled:bg-gray-600 text-white py-2 px-4 rounded-lg flex items-center justify-center transition-colors"
              >
                {isSubmitting ? (
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                ) : (
                  <Play className="h-4 w-4 mr-2" />
                )}
                Run Job
              </button>
            </div>
          ))}
        </div>
      )}

      {/* Custom Command Section */}
      {activeSection === 'custom' && (
        <div className="bg-gray-800/50 border border-gray-700 rounded-lg p-6">
          <h3 className="text-white font-medium mb-4 flex items-center">
            <Play className="h-5 w-5 mr-2 text-spark-orange" />
            Submit Custom Spark Job
          </h3>
          
          <div className="space-y-4">
            <div>
              <label className="block text-sm text-gray-400 mb-2">
                spark-submit command (runs inside spark-master container)
              </label>
              <textarea
                value={customCommand}
                onChange={(e) => setCustomCommand(e.target.value)}
                placeholder="/opt/spark/bin/spark-submit --master spark://spark-master:7077 /path/to/your/script.py"
                className="w-full bg-gray-900 border border-gray-600 rounded-lg p-3 text-white font-mono text-sm placeholder-gray-500 focus:border-spark-orange focus:ring-1 focus:ring-spark-orange"
                rows={4}
              />
            </div>
            
            <div className="flex space-x-3">
              <button
                onClick={() => submitJob(customCommand, 'Custom Job')}
                disabled={isSubmitting || !customCommand.trim()}
                className="bg-spark-orange hover:bg-spark-orange/80 disabled:bg-gray-600 text-white py-2 px-6 rounded-lg flex items-center transition-colors"
              >
                {isSubmitting ? (
                  <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                ) : (
                  <Play className="h-4 w-4 mr-2" />
                )}
                Submit Job
              </button>
              
              <button
                onClick={() => setCustomCommand('')}
                className="bg-gray-700 hover:bg-gray-600 text-white py-2 px-4 rounded-lg"
              >
                Clear
              </button>
            </div>
          </div>
          
          {/* Quick Examples */}
          <div className="mt-6 pt-4 border-t border-gray-700">
            <h4 className="text-sm text-gray-400 mb-3">Quick Examples:</h4>
            <div className="space-y-2">
              {[
                { label: 'Pi Calculation', cmd: '/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/examples/src/main/python/pi.py 100' },
                { label: 'PySpark Shell', cmd: '/opt/spark/bin/pyspark --master spark://spark-master:7077' },
              ].map((example, idx) => (
                <button
                  key={idx}
                  onClick={() => setCustomCommand(example.cmd)}
                  className="block w-full text-left text-sm text-gray-400 hover:text-white p-2 rounded bg-gray-900/50 hover:bg-gray-900 transition-colors"
                >
                  <span className="text-spark-orange">{example.label}:</span>
                  <code className="ml-2 text-xs">{example.cmd.substring(0, 60)}...</code>
                </button>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Browse Files Section */}
      {activeSection === 'files' && (
        <div className="bg-gray-800/50 border border-gray-700 rounded-lg p-6">
          <h3 className="text-white font-medium mb-4 flex items-center">
            <FolderOpen className="h-5 w-5 mr-2 text-spark-orange" />
            Available Spark Applications
          </h3>
          
          <div className="text-gray-400 text-sm mb-4">
            Files mounted in <code className="bg-gray-900 px-2 py-1 rounded">/opt/spark-apps/</code> inside the Spark container
          </div>
          
          <div className="bg-gray-900/50 rounded-lg p-4">
            <div className="space-y-2">
              {[
                { name: 'long_running_demo.py', desc: 'Demo job with progress tracking', size: '2.4 KB' },
                { name: 'wordcount.py', desc: 'Word count example', size: '1.1 KB' },
                { name: 'etl_demo.py', desc: 'ETL pipeline demonstration', size: '3.2 KB' },
                { name: 'streaming_demo.py', desc: 'Spark Streaming example', size: '2.8 KB' },
              ].map((file, idx) => (
                <div
                  key={idx}
                  className="flex items-center justify-between p-3 rounded hover:bg-gray-800 transition-colors"
                >
                  <div className="flex items-center">
                    <FileCode className="h-4 w-4 text-blue-400 mr-3" />
                    <div>
                      <span className="text-white">{file.name}</span>
                      <p className="text-xs text-gray-500">{file.desc}</p>
                    </div>
                  </div>
                  <div className="flex items-center space-x-3">
                    <span className="text-xs text-gray-500">{file.size}</span>
                    <button
                      onClick={() => submitJob(
                        `/opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-apps/${file.name}`,
                        file.name
                      )}
                      disabled={isSubmitting}
                      className="bg-spark-orange/20 hover:bg-spark-orange/40 text-spark-orange py-1 px-3 rounded text-sm flex items-center"
                    >
                      <Play className="h-3 w-3 mr-1" />
                      Run
                    </button>
                  </div>
                </div>
              ))}
            </div>
          </div>
          
          <div className="mt-4 p-3 bg-blue-500/10 border border-blue-500/30 rounded-lg">
            <p className="text-blue-300 text-sm">
              💡 <strong>Tip:</strong> Mount your own Python scripts to <code className="bg-gray-900 px-1 rounded">/opt/spark-apps/</code> 
              by adding them to <code className="bg-gray-900 px-1 rounded">docker/spark-cluster/apps/</code>
            </p>
          </div>
        </div>
      )}

      {/* Running Jobs History */}
      {runningJobs.length > 0 && (
        <div className="bg-gray-800/50 border border-gray-700 rounded-lg overflow-hidden">
          <div className="flex items-center justify-between p-4 border-b border-gray-700">
            <h3 className="text-white font-medium flex items-center">
              <Clock className="h-5 w-5 mr-2" />
              Job History ({runningJobs.length})
            </h3>
            <button
              onClick={clearCompletedJobs}
              className="text-gray-400 hover:text-white text-sm flex items-center"
            >
              <Trash2 className="h-4 w-4 mr-1" />
              Clear Completed
            </button>
          </div>
          
          <div className="divide-y divide-gray-700">
            {runningJobs.map((job) => (
              <div key={job.id} className="p-4 flex items-center justify-between">
                <div className="flex items-center">
                  {job.status === 'running' && <Loader2 className="h-4 w-4 text-blue-400 mr-3 animate-spin" />}
                  {job.status === 'pending' && <Clock className="h-4 w-4 text-yellow-400 mr-3" />}
                  {job.status === 'completed' && <CheckCircle className="h-4 w-4 text-green-400 mr-3" />}
                  {job.status === 'failed' && <XCircle className="h-4 w-4 text-red-400 mr-3" />}
                  <div>
                    <p className="text-white">{job.name}</p>
                    <p className="text-xs text-gray-500">
                      Started: {new Date(job.startTime).toLocaleTimeString()}
                    </p>
                  </div>
                </div>
                <span className={`text-sm px-2 py-1 rounded ${
                  job.status === 'running' ? 'bg-blue-500/20 text-blue-300' :
                  job.status === 'pending' ? 'bg-yellow-500/20 text-yellow-300' :
                  job.status === 'completed' ? 'bg-green-500/20 text-green-300' :
                  'bg-red-500/20 text-red-300'
                }`}>
                  {job.status}
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
