'use client';
import { memo } from 'react'
import { Handle, Position, type NodeProps } from 'reactflow'
import { getNodeConfig, formatNodeId } from '@/lib/nodeConfig'
import type { NodeLabel } from '@/types'

interface NodeData {
  label: NodeLabel
  nodeId: string
  properties: Record<string, string | number | boolean | null>
  selected?: boolean
  dimmed?: boolean
}

export const GraphNodeComponent = memo(function GraphNodeComponent({ data, selected }: NodeProps<NodeData>) {
  const cfg = getNodeConfig(data.label)

  return (
    <div
      className="relative flex flex-col items-center cursor-pointer transition-all duration-150"
      style={{ opacity: data.dimmed ? 0.25 : 1 }}
    >
      {/* Glow ring when selected */}
      {selected && (
        <div
          className="absolute inset-0 rounded-full pointer-events-none"
          style={{
            boxShadow: `0 0 0 3px ${cfg.border}, 0 0 24px ${cfg.glow}`,
            borderRadius: '50%',
            width: 52,
            height: 52,
            top: -2,
            left: -2,
          }}
        />
      )}

      {/* Node circle */}
      <div
        className="flex items-center justify-center rounded-full border-2 font-mono font-medium text-[10px] tracking-wider transition-all duration-150"
        style={{
          width: 48,
          height: 48,
          background: cfg.bg,
          borderColor: selected ? cfg.color : cfg.border,
          color: cfg.color,
          boxShadow: selected ? `0 0 16px ${cfg.glow}` : `0 0 0 1px ${cfg.glow}`,
        }}
      >
        {cfg.short}
      </div>

      {/* ID label below */}
      <div
        className="mt-1 text-[9px] font-mono text-center max-w-[64px] truncate"
        style={{ color: selected ? cfg.color : '#475569' }}
      >
        {formatNodeId(data.nodeId)}
      </div>

      {/* Hidden handles for React Flow edge routing */}
      <Handle type="target" position={Position.Top}    style={{ opacity: 0, pointerEvents: 'none' }} />
      <Handle type="source" position={Position.Bottom} style={{ opacity: 0, pointerEvents: 'none' }} />
    </div>
  )
})
