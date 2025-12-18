import type { NextApiRequest, NextApiResponse } from 'next'

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  const sparkAppUrl = process.env.SPARK_APP_INTERNAL_URL || 
                      process.env.NEXT_PUBLIC_SPARK_APP_URL || 
                      'http://spark-master:4040'
  
  const { appId } = req.query

  if (!appId) {
    res.status(400).json({ error: 'appId is required' })
    return
  }

  try {
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), 3000)
    
    const response = await fetch(`${sparkAppUrl}/api/v1/applications/${appId}/jobs`, {
      method: 'GET',
      headers: { 'Accept': 'application/json' },
      signal: controller.signal,
    })
    clearTimeout(timeout)

    if (!response.ok) {
      if (response.status === 404) {
        res.status(200).json([])
        return
      }
      throw new Error(`Spark returned ${response.status}`)
    }

    const data = await response.json()
    res.setHeader('Content-Type', 'application/json')
    res.status(200).json(data)
  } catch {
    // Silently return empty array when Spark app is not running (port 4040 not available)
    res.status(200).json([])
  }
}
