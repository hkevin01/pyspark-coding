import type { NextApiRequest, NextApiResponse } from 'next'
import { exec } from 'child_process'
import { promisify } from 'util'

const execAsync = promisify(exec)

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' })
  }

  try {
    console.log('[Start Cluster] Checking if Spark cluster is already running...')
    
    // Check if containers are already running
    const { stdout: checkOutput } = await execAsync('docker ps --filter "name=spark-master" --format "{{.Names}}"')
    
    if (checkOutput.trim().includes('spark-master')) {
      console.log('[Start Cluster] Spark cluster is already running')
      return res.status(200).json({ 
        success: true, 
        message: 'Spark cluster is already running',
        alreadyRunning: true
      })
    }

    console.log('[Start Cluster] Starting Spark cluster with docker compose...')
    
    // Find the docker-compose.yml location
    const composeDir = process.env.SPARK_COMPOSE_DIR || '/home/kevin/Projects/pyspark-coding/docker/spark-cluster'
    
    // Start the cluster
    const { stdout, stderr } = await execAsync(`cd ${composeDir} && docker compose up -d`, {
      timeout: 30000 // 30 second timeout
    })
    
    console.log('[Start Cluster] Docker compose output:', stdout)
    if (stderr) console.log('[Start Cluster] Docker compose stderr:', stderr)
    
    // Wait a moment for containers to start
    await new Promise(resolve => setTimeout(resolve, 3000))
    
    // Verify containers started
    const { stdout: verifyOutput } = await execAsync('docker ps --filter "name=spark" --format "{{.Names}}"')
    const runningContainers = verifyOutput.trim().split('\n').filter(Boolean)
    
    console.log('[Start Cluster] Running containers:', runningContainers)
    
    if (runningContainers.length >= 4) {
      return res.status(200).json({ 
        success: true, 
        message: 'Spark cluster started successfully',
        containers: runningContainers,
        urls: {
          sparkMaster: 'http://localhost:9080',
          sparkApp: 'http://localhost:4040',
        }
      })
    } else {
      throw new Error(`Only ${runningContainers.length} containers started, expected 4`)
    }
    
  } catch (error: any) {
    console.error('[Start Cluster] Error:', error)
    return res.status(500).json({ 
      success: false,
      error: error.message || 'Failed to start Spark cluster',
      details: error.stderr || error.stdout
    })
  }
}
