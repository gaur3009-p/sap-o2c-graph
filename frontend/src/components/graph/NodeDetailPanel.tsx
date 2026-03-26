'use client'
import { X, Network, ArrowRight } from 'lucide-react'
import { getNodeConfig, formatPropValue } from '@/lib/nodeConfig'
import { EDGE_COLORS } from '@/lib/nodeConfig'
import type { NodeDetail } from '@/types'

interface Props {
  detail: NodeDetail | null
  loading: boolean
  onClose: () => void
  onNavigate: (id: string) => void
}

export function NodeDetailPanel({ detail, loading, onClose, onNavigate }: Props) {
  if (!detail && !loading) return null

  const cfg = detail ? getNodeConfig(detail.label) : null

  return (
    <div className="panel absolute right-0 top-0 bottom-0 w-72 z-10 flex flex-col animate-fade-in overflow-hidden border-l border-[#1a2433]">
      {/* Header */}
      <div className="flex items-center justify-between p-4 border-b border-[#1a2433]">
        {detail && cfg ? (
          <div className="flex items-center gap-2.5">
            <div
              className="w-8 h-8 rounded-full flex items-center justify-center font-mono text-[10px] font-medium border"
              style={{ background: cfg.bg, borderColor: cfg.border, color: cfg.color }}
            >
              {cfg.short}
            </div>
            <div>
              <div className="text-xs font-medium" style={{ color: cfg.color }}>{detail.label}</div>
              <div className="text-[11px] text-slate-500 font-mono truncate max-w-[160px]">{detail.id}</div>
            </div>
          </div>
        ) : (
          <div className="shimmer h-8 w-40 rounded" />
        )}
        <button
          onClick={onClose}
          className="text-slate-600 hover:text-slate-300 transition-colors p-1"
        >
          <X size={14} />
        </button>
      </div>

      {loading ? (
        <div className="p-4 space-y-2">
          {[...Array(6)].map((_, i) => (
            <div key={i} className="shimmer h-5 rounded" style={{ width: `${60 + (i % 3) * 15}%` }} />
          ))}
        </div>
      ) : detail ? (
        <div className="flex-1 overflow-y-auto">
          {/* Properties */}
          <div className="p-4 border-b border-[#1a2433]">
            <div className="text-[10px] font-mono text-slate-600 uppercase tracking-widest mb-3">Properties</div>
            <div className="space-y-1.5">
              {Object.entries(detail.properties).map(([key, val]) => {
                const display = formatPropValue(val)
                if (display === '—') return null
                return (
                  <div key={key} className="flex gap-2 text-[11px]">
                    <span className="text-slate-600 font-mono shrink-0 w-28 truncate">{key}</span>
                    <span className="text-slate-300 break-all">{display}</span>
                  </div>
                )
              })}
            </div>
          </div>

          {/* Neighbours */}
          {detail.neighbours.length > 0 && (
            <div className="p-4">
              <div className="text-[10px] font-mono text-slate-600 uppercase tracking-widest mb-3 flex items-center gap-1.5">
                <Network size={10} /> Connections ({detail.neighbours.length})
              </div>
              <div className="space-y-1">
                {detail.neighbours.map((nb, i) => {
                  const nbCfg = getNodeConfig(nb.label)
                  const edgeColor = EDGE_COLORS[nb.relationship] ?? '#475569'
                  return (
                    <button
                      key={i}
                      onClick={() => onNavigate(nb.id)}
                      className="w-full flex items-center gap-2 p-2 rounded hover:bg-[#1a2433] transition-colors text-left group"
                    >
                      <div
                        className="w-6 h-6 rounded-full flex items-center justify-center font-mono text-[9px] border shrink-0"
                        style={{ background: nbCfg.bg, borderColor: nbCfg.border, color: nbCfg.color }}
                      >
                        {nbCfg.short}
                      </div>
                      <div className="flex-1 min-w-0">
                        <div className="text-[10px] font-mono" style={{ color: edgeColor }}>
                          {nb.relationship}
                        </div>
                        <div className="text-[11px] text-slate-400 truncate">{nb.id}</div>
                      </div>
                      <ArrowRight size={10} className="text-slate-700 group-hover:text-slate-400 transition-colors shrink-0" />
                    </button>
                  )
                })}
              </div>
            </div>
          )}
        </div>
      ) : null}
    </div>
  )
}
