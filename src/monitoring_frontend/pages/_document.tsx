import { Html, Head, Main, NextScript } from 'next/document'

export default function Document() {
  return (
    <Html lang="en">
      <Head>
        <meta charSet="utf-8" />
        <meta name="description" content="Real-time PySpark Cluster Monitoring Dashboard" />
        <meta name="theme-color" content="#1f2937" />
      </Head>
      <body>
        <Main />
        <NextScript />
      </body>
    </Html>
  )
}
