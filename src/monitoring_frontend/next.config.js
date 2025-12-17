/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  env: {
    SPARK_MASTER_URL: process.env.SPARK_MASTER_URL || 'http://localhost:9080',
    SPARK_APP_URL: process.env.SPARK_APP_URL || 'http://localhost:4040',
    PROMETHEUS_URL: process.env.PROMETHEUS_URL || 'http://localhost:9090',
  },
  async rewrites() {
    return [
      {
        source: '/api/spark/:path*',
        destination: 'http://localhost:9080/:path*',
      },
      {
        source: '/api/prometheus/:path*',
        destination: 'http://localhost:9090/:path*',
      },
    ];
  },
}

module.exports = nextConfig
