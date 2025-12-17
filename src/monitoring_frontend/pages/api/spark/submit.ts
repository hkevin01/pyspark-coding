import type { NextApiRequest, NextApiResponse } from 'next'

// Store job history in memory (in production, use a database)
const jobHistory: Array<{
  id: string
  name: string
  command: string
  status: 'submitted' | 'running' | 'completed' | 'failed'
  submittedAt: string
  output?: string
  error?: string
}> = []

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  // GET - return job history
  if (req.method === 'GET') {
    return res.status(200).json({ 
      success: true,
      jobs: jobHistory.slice(-50)
    })
  }
  
  // POST - submit and execute a Spark job
  if (req.method === 'POST') {
    try {
      const { command, name, jobId } = req.body

      if (!command) {
        return res.status(400).json({ error: 'Command is required' })
      }

      // Security: Only allow spark-submit commands
      if (!command.includes('spark-submit') && !command.includes('pyspark')) {
        return res.status(400).json({ error: 'Only spark-submit and pyspark commands are allowed' })
      }

      const job = {
        id: jobId || `job-${Date.now()}`,
        name: name || 'Custom Job',
        command,
        status: 'submitted' as const,
        submittedAt: new Date().toISOString(),
      }
      
      jobHistory.unshift(job)
      
      // Keep only last 100 jobs in memory
      if (jobHistory.length > 100) {
        jobHistory.pop()
      }

      // ═══════════════════════════════════════════════════════════════════════
      // EXECUTE SPARK-SUBMIT VIA DOCKER EXEC
      // ═══════════════════════════════════════════════════════════════════════
      // We'll use node's child_process to execute docker exec on spark-master
      // This runs the spark-submit command asynchronously in the background
      
      const { exec } = require('child_process')
      
      // Build the docker exec command
      // Using detach mode (-d) so it runs in background and doesn't block the API
      const dockerCommand = `docker exec -d spark-master ${command}`
      
      console.log(`[Submit API] Executing: ${dockerCommand}`)
      
      // Execute asynchronously - don't wait for completion
      exec(dockerCommand, { timeout: 5000 }, (error: any, stdout: string, stderr: string) => {
        if (error) {
          console.error(`[Submit API] Error starting job: ${error.message}`)
          // Update job status in history
          const jobIndex = jobHistory.findIndex(j => j.id === job.id)
          if (jobIndex >= 0) {
            jobHistory[jobIndex].status = 'failed'
            jobHistory[jobIndex].error = error.message
          }
        } else {
          console.log(`[Submit API] Job started successfully`)
          // Update job status to running
          const jobIndex = jobHistory.findIndex(j => j.id === job.id)
          if (jobIndex >= 0) {
            jobHistory[jobIndex].status = 'running'
          }
        }
      })

      // Return immediately - job runs in background
      return res.status(200).json({ 
        success: true, 
        message: `Job "${job.name}" submitted! The spark-submit command is now running on the Spark cluster.`,
        job,
        note: 'Job is executing in the background. Watch the Execution Flow panel for real-time updates.',
        monitorUrls: {
          sparkMaster: 'http://localhost:9080',
          sparkApp: 'http://localhost:4040',
          dashboard: 'http://localhost:3000'
        }
      })
    } catch (error: any) {
      console.error('Error submitting job:', error)
      return res.status(500).json({ 
        error: error.message || 'Failed to submit job' 
      })
    }
  }
  
  return res.status(405).json({ error: 'Method not allowed' })
}
