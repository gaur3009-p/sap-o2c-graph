'use client';
import { useEffect, useState } from 'react'
import { Activity, Github, BookOpen } from 'lucide-react'
import { api } from '@/lib/api'
import type { HealthResponse } from '@/types'
import clsx from 'clsx'

export function Header() {
  const [health, setHealth] = useState<HealthResponse | null>(null)

  useEffect(() => {
    api.health()
      .then(setHealth)
      .catch(() => setHealth(null))

    // Ping every 30s
    const id = setInterval(() => {
      api.health().then(setHealth).catch(() => setHealth(null))
    }, 30_000)
    return () => clearInterval(id)
  }, [])

  const statusColor =
    health?.status === 'ok'       ? '#00ff88' :
    health?.status === 'degraded' ? '#f59e0b' :
    health === null                ? '#475569' : '#f87171'

  return (
    <header className="h-12 border-b border-[#1a2433] bg-[#0c1118]/80 backdrop-blur-sm flex items-center justify-between px-5 shrink-0 z-20">
      {/* Logo + title */}
      <div className="flex items-center gap-3">
        <div className="flex items-center gap-1.5">
          {/* Stacked circles logo */}
          <div className="relative w-6 h-6">
            <div className="absolute top-0 left-0 w-4 h-4 rounded-full border border-[#6366f1] bg-[#1e1b4b]" />
            <div className="absolute bottom-0 right-0 w-4 h-4 rounded-full border border-[#00cc6a] bg-[#001a0d]" />
          </div>
        </div>
        <div>
          <span className="font-display font-semibold text-sm text-slate-100 tracking-wide">
            O2C Graph
          </span>
          <span className="ml-2 text-[10px] font-mono text-slate-600 hidden sm:inline">
            SAP Order-to-Cash Intelligence
          </span>
        </div>
      </div>

      {/* Right side */}
      <div className="flex items-center gap-4">
        {/* Health indicator */}
        <div className="flex items-center gap-1.5 text-[11px] font-mono">
          <div
            className="w-1.5 h-1.5 rounded-full"
            style={{
              background: statusColor,
              boxShadow: health?.status === 'ok' ? `0 0 6px ${statusColor}` : 'none',
            }}
          />
          <span style={{ color: statusColor }}>
            {health === null ? 'connecting…' : health.status}
          </span>
          {health?.details?.database && (
            <span className="text-slate-700 hidden sm:inline">· {health.details.database}</span>
          )}
        </div>

        {/* Divider */}
        <div className="w-px h-4 bg-[#1a2433]" />

        {/* Model badge */}
        {health?.details?.model && (
          <div className="hidden sm:flex items-center gap-1.5 text-[10px] font-mono text-slate-600">
            <Activity size={9} />
            <span>{health.details.model.replace('llama-', 'LLaMA-').replace('-versatile', '')}</span>
          </div>
        )}

        {/* Links */}
        <a
          href="http://localhost:8000/docs"
          target="_blank"
          rel="noopener noreferrer"
          className="text-slate-600 hover:text-slate-300 transition-colors"
          title="API Docs"
        >
          <BookOpen size={14} />
        </a>
        <a
          href="https://github.com"
          target="_blank"
          rel="noopener noreferrer"
          className="text-slate-600 hover:text-slate-300 transition-colors"
          title="GitHub"
        >
          <Github size={14} />
        </a>
      </div>
    </header>
  )
}
