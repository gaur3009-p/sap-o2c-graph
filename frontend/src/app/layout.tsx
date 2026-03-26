import type { Metadata } from 'next'
import './globals.css'

export const metadata: Metadata = {
  title: 'SAP O2C Graph Intelligence',
  description: 'Natural language query interface over the SAP Order-to-Cash graph',
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className="dark">
      <body className="bg-[#060810] text-slate-200 h-screen overflow-hidden">
        {children}
      </body>
    </html>
  )
}
