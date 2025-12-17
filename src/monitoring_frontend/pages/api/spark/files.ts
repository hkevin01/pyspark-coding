import type { NextApiRequest, NextApiResponse } from 'next'

// Predefined list of common Spark example files
const SPARK_EXAMPLES = [
  { name: 'pi.py', type: 'file', size: '1.2K', date: 'built-in', path: '/opt/spark/examples/src/main/python/pi.py' },
  { name: 'wordcount.py', type: 'file', size: '2.1K', date: 'built-in', path: '/opt/spark/examples/src/main/python/wordcount.py' },
  { name: 'sql/basic.py', type: 'file', size: '3.4K', date: 'built-in', path: '/opt/spark/examples/src/main/python/sql/basic.py' },
  { name: 'ml/random_forest_example.py', type: 'file', size: '4.5K', date: 'built-in', path: '/opt/spark/examples/src/main/python/ml/random_forest_example.py' },
  { name: 'streaming/network_wordcount.py', type: 'file', size: '1.8K', date: 'built-in', path: '/opt/spark/examples/src/main/python/streaming/network_wordcount.py' },
]

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse
) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' })
  }

  try {
    const { path = '/opt/spark/examples' } = req.query
    const targetPath = typeof path === 'string' ? path : '/opt/spark/examples'

    // Return predefined example files
    // In a production setup, you could mount a shared volume or use an API
    return res.status(200).json({ 
      success: true,
      path: targetPath,
      files: SPARK_EXAMPLES,
      note: 'These are common Spark examples available in the container. Mount a shared volume for custom apps.'
    })
  } catch (error: any) {
    console.error('Error listing files:', error)
    return res.status(500).json({ 
      error: error.message || 'Failed to list files' 
    })
  }
}
