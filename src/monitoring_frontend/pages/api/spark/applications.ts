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
    
    const response = await fetch(url, {
      method: 'GET',
      headers: {
        'Accept': 'application/json',
      },
    })

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
  } catch (error) {
    console.error('Error fetching applications:', error)
    // Return empty array instead of error for graceful degradation
    res.status(200).json([])
  }
}
