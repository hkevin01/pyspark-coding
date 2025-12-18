import type { NextApiRequest, NextApiResponse } from 'next'

// This API route proxies requests to Spark Application UI
export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  const sparkAppUrl = process.env.SPARK_APP_INTERNAL_URL || 
                      process.env.NEXT_PUBLIC_SPARK_APP_URL || 
                      'http://spark-master:4040'
  
  const { appId } = req.query

  try {
    const url = appId 
      ? `${sparkAppUrl}/api/v1/applications/${appId}`
      : `${sparkAppUrl}/api/v1/applications`
    
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), 3000)
    
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'Accept': 'application/json',
      },
      signal: controller.signal,
    })
    clearTimeout(timeout)

    if (!response.ok) {
      // Application UI might not be available if no jobs running
      if (response.status === 404) {
        res.status(200).json([])
        return
      }
      throw new Error(`Spark App UI returned ${response.status}`)
    }

    const data = await response.json()
    
    res.setHeader('Access-Control-Allow-Origin', '*')
    res.setHeader('Content-Type', 'application/json')
    
    res.status(200).json(data)
  } catch {
    // Silently return empty array when Spark app is not running (port 4040 not available)
    res.status(200).json([])
  }
}
