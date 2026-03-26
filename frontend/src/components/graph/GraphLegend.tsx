'use client'
import { NODE_CONFIG } from '@/lib/nodeConfig'
import type { NodeLabel } from '@/types'

interface Props {
  visibleLabels: Set<NodeLabel>
  onToggle: (label: NodeLabel) => void
  nodeCount: number
  edgeCount: number
}

const ALL_LABELS = Object.keys(NODE_CONFIG) as NodeLabel[]

export function GraphLegend({ visibleLabels, onToggle, nodeCount, edgeCount }: Props) {
  return (
    <div className="absolute bottom-4 left-4 z-10 panel rounded-lg p-3 flex flex-col gap-2.5">
      {/* Stats */}
      <div className="flex gap-3 text-[10px] font-mono text-slate-600 border-b border-[#1a2433] pb-2">
        <span><span className="text-slate-300">{nodeCount}</span> nodes</span>
        <span><span className="text-slate-300">{edgeCount}</span> edges</span>
      </div>

      {/* Label toggles */}
      <div className="grid grid-cols-2 gap-x-4 gap-y-1">
        {ALL_LABELS.filter(l => !['SalesOrderItem', 'Address'].includes(l)).map(label => {
          const cfg = NODE_CONFIG[label]
          const active = visibleLabels.has(label)
          return (
            <button
              key={label}
              onClick={() => onToggle(label)}
              className="flex items-center gap-1.5 text-[10px] font-mono transition-opacity"
              style={{ opacity: active ? 1 : 0.35 }}
            >
              <div
                className="w-2.5 h-2.5 rounded-full border flex-shrink-0"
                style={{ background: active ? cfg.bg : 'transparent', borderColor: cfg.border }}
              />
              <span style={{ color: active ? cfg.color : '#475569' }}>{label.replace('Outbound', '').replace('Document', 'Doc')}</span>
            </button>
          )
        })}
      </div>
    </div>
  )
}
