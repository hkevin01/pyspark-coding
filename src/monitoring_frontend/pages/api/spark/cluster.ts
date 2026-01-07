import type { NextApiRequest, NextApiResponse } from 'next'

// Mock data for when Spark cluster is not available
const getMockClusterData = () => ({
  url: 'spark://localhost:7077',
  workers: [
    {
      id: 'worker-1',
      host: '192.168.1.101',
      port: 8081,
      cores: 8,
      coresUsed: 4,
      coresFree: 4,
      memory: 16384,
      memoryUsed: 8192,
      memoryFree: 8192,
      state: 'ALIVE',
      lastHeartbeat: Date.now()
    },
    {
      id: 'worker-2',
      host: '192.168.1.102',
      port: 8082,
      cores: 8,
      coresUsed: 6,
      coresFree: 2,
      memory: 16384,
      memoryUsed: 12288,
      memoryFree: 4096,
      state: 'ALIVE',
      lastHeartbeat: Date.now()
    },
    {
      id: 'worker-3',
      host: '192.168.1.103',
      port: 8083,
      cores: 4,
      coresUsed: 2,
      coresFree: 2,
      memory: 8192,
      memoryUsed: 4096,
      memoryFree: 4096,
      state: 'ALIVE',
      lastHeartbeat: Date.now()
    }
  ],
  aliveworkers: 3,
  cores: 20,
  coresused: 12,
  memory: 40960,
  memoryused: 24576,
  activeapps: [
    {
      id: 'app-20260104-001',
      name: 'PySpark ETL Job',
      cores: 8,
      memoryperslave: 4096,
      submitdate: new Date().toISOString(),
      state: 'RUNNING',
      duration: 125000
    }
  ],
  completedapps: [
    {
      id: 'app-20260103-005',
      name: 'Daily Aggregation',
      cores: 4,
      memoryperslave: 2048,
      submitdate: new Date(Date.now() - 3600000).toISOString(),
      state: 'FINISHED',
      duration: 45000
    }
  ],
  activedrivers: [],
  completeddrivers: [],
  status: 'ALIVE'
})

// This API route proxies requests to Spark Master to avoid CORS issues
export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  // Determine the Spark Master URL based on environment
  // In Docker, use the service name; locally use localhost
  const sparkMasterUrl = process.env.SPARK_MASTER_INTERNAL_URL ||
    process.env.NEXT_PUBLIC_SPARK_MASTER_URL ||
    'http://localhost:8080'

  const useMockData = process.env.USE_MOCK_DATA === 'true'

  // Set CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*')
  res.setHeader('Access-Control-Allow-Methods', 'GET')
  res.setHeader('Content-Type', 'application/json')

  // Return mock data if configured or if Spark is unavailable
  if (useMockData) {
    return res.status(200).json(getMockClusterData())
  }

  try {
    const response = await fetch(`${sparkMasterUrl}/json/`, {
      method: 'GET',
      headers: {
        'Accept': 'application/json',
      },
      signal: AbortSignal.timeout(3000) // 3 second timeout
    })

    if (!response.ok) {
      throw new Error(`Spark Master returned ${response.status}`)
    }

    const data = await response.json()
    res.status(200).json(data)
  } catch (error) {
    // If Spark is unavailable, try to start the cluster automatically
    console.log('Spark Master not available, attempting to auto-start cluster...')

    try {
      const { exec } = require('child_process')
      const { promisify } = require('util')
      const execAsync = promisify(exec)

      // Check if cluster is running
      const { stdout: checkOutput } = await execAsync('docker ps --filter "name=spark-master" --format "{{.Names}}"')

      if (!checkOutput.trim().includes('spark-master')) {
        console.log('Starting Spark cluster automatically...')
        const composeDir = process.env.SPARK_COMPOSE_DIR || '/home/kevin/Projects/pyspark-coding/docker/spark-cluster'
        await execAsync(`cd ${composeDir} && docker compose up -d`, { timeout: 30000 })

        // Wait for cluster to initialize
        await new Promise(resolve => setTimeout(resolve, 5000))

        // Retry fetching cluster data
        const retryResponse = await fetch(`${sparkMasterUrl}/json/`, {
          method: 'GET',
          headers: { 'Accept': 'application/json' },
          signal: AbortSignal.timeout(3000)
        })

        if (retryResponse.ok) {
          const data = await retryResponse.json()
          console.log('Cluster started successfully!')
          return res.status(200).json(data)
        }
      }
    } catch (startError) {
      console.error('Failed to auto-start cluster:', startError)
    }

    // If auto-start failed, return mock data
    console.log('Returning mock data')
    res.status(200).json(getMockClusterData())
  }
}
