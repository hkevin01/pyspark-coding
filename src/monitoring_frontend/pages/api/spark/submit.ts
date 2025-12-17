import type { NextApiRequest, NextApiResponse } from 'next'

// Store job history in memory (in production, use a database)
const jobHistory: Array<{
  id: string
  name: string
  command: string
  status: 'submitted' | 'running' | 'completed' | 'failed'
  submittedAt: string
  note?: string
}> = []

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  // GET - return job history
  if (req.method === 'GET') {
    return res.status(200).json({ 
      success: true,
      jobs: jobHistory.slice(-50) // Return last 50 jobs
    })
  }
  
  // POST - record a job submission
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

      // Record the job
      const job = {
        id: jobId || `job-${Date.now()}`,
        name: name || 'Custom Job',
        command,
        status: 'submitted' as const,
        submittedAt: new Date().toISOString(),
        note: 'Job command recorded. To execute, run this command in the Spark master container.'
      }
      
      jobHistory.unshift(job)
      
      // Keep only last 100 jobs in memory
      if (jobHistory.length > 100) {
        jobHistory.pop()
      }

      // Try to submit via Spark REST API if available
      // Note: This requires Spark to be configured with REST submission enabled
      const sparkMasterUrl = process.env.SPARK_MASTER_INTERNAL_URL || 'http://spark-master:9080'
      
      // For now, return success with instructions
      return res.status(200).json({ 
        success: true, 
        message: 'Job recorded successfully',
        job,
        instructions: [
          'To run this job, you can:',
          '1. Access the Spark master container: docker exec -it spark-master bash',
          `2. Run the command: ${command}`,
          '3. Monitor the job at http://localhost:4040'
        ],
        sparkMasterUrl: 'http://localhost:9080',
        sparkAppUrl: 'http://localhost:4040'
      })
    } catch (error: any) {
      console.error('Error recording job:', error)
      return res.status(500).json({ 
        error: error.message || 'Failed to record job' 
      })
    }
  }
  
  return res.status(405).json({ error: 'Method not allowed' })
}
