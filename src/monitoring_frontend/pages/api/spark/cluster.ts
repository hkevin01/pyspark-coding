import type { NextApiRequest, NextApiResponse } from 'next'

// This API route proxies requests to Spark Master to avoid CORS issues
export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  // Determine the Spark Master URL based on environment
  // In Docker, use the service name; locally use localhost
  const sparkMasterUrl = process.env.SPARK_MASTER_INTERNAL_URL || 
                         process.env.NEXT_PUBLIC_SPARK_MASTER_URL || 
                         'http://spark-master:9080'

  try {
    const response = await fetch(`${sparkMasterUrl}/json/`, {
      method: 'GET',
      headers: {
        'Accept': 'application/json',
      },
    })

    if (!response.ok) {
      throw new Error(`Spark Master returned ${response.status}`)
    }

    const data = await response.json()
    
    // Set CORS headers
    res.setHeader('Access-Control-Allow-Origin', '*')
    res.setHeader('Access-Control-Allow-Methods', 'GET')
    res.setHeader('Content-Type', 'application/json')
    
    res.status(200).json(data)
  } catch (error) {
    console.error('Error fetching from Spark Master:', error)
    res.status(500).json({ 
      error: 'Failed to fetch cluster status',
      message: error instanceof Error ? error.message : 'Unknown error',
      sparkUrl: sparkMasterUrl
    })
  }
}
