'use client';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import ReactFlow, {
  Background,
  BackgroundVariant,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  addEdge,
  type Node,
  type Edge,
  type NodeMouseHandler,
  type FitViewOptions,
  MarkerType,
} from 'reactflow'
import 'reactflow/dist/style.css'
import { GraphNodeComponent } from './GraphNode'
import { NodeDetailPanel } from './NodeDetailPanel'
import { GraphLegend } from './GraphLegend'
import { api } from '@/lib/api'
import { getNodeConfig, EDGE_COLORS } from '@/lib/nodeConfig'
import type { NodeLabel } from '@/types'
import type { NodeDetail } from '@/types'
import { RefreshCw, Maximize2 } from 'lucide-react'

const NODE_TYPES = { graphNode: GraphNodeComponent }

const FIT_OPTIONS: FitViewOptions = { padding: 0.15, duration: 600 }

// ── Layout: force-directed approximation using a simple grid + grouping ───────
function layoutNodes(
  rawNodes: Array<{ id: string; label: NodeLabel; properties: Record<string, unknown> }>,
  rawEdges: Array<{ source: string; target: string; relationship: string }>
): Node[] {
  // Group nodes by label into a defined vertical tier
  const TIER_ORDER: NodeLabel[] = [
    'Customer',
    'SalesOrder',
    'Product',
    'OutboundDelivery',
    'BillingDocument',
    'JournalEntry',
    'Payment',
    'SalesOrderItem',
    'Address',
  ]

  const byLabel = new Map<NodeLabel, typeof rawNodes>()
  for (const n of rawNodes) {
    if (!byLabel.has(n.label)) byLabel.set(n.label, [])
    byLabel.get(n.label)!.push(n)
  }

  const placed: Node[] = []
  const H_GAP = 90
  const V_GAP = 160

  TIER_ORDER.forEach((label, tierIdx) => {
    const group = byLabel.get(label) ?? []
    const y = tierIdx * V_GAP
    const totalW = group.length * H_GAP
    const startX = -totalW / 2

    group.forEach((n, i) => {
      placed.push({
        id: n.id,
        type: 'graphNode',
        position: { x: startX + i * H_GAP, y },
        data: {
          label: n.label,
          nodeId: n.id,
          properties: n.properties,
          dimmed: false,
        },
        draggable: true,
      })
    })
  })

  return placed
}

interface Props {
  highlightIds?: string[]        // nodes to highlight (from chat results)
  onNodeSelect?: (id: string) => void
}

export function GraphCanvas({ highlightIds = [], onNodeSelect }: Props) {
  const [nodes, setNodes, onNodesChange] = useNodesState([])
  const [edges, setEdges, onEdgesChange] = useEdgesState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null)
  const [nodeDetail, setNodeDetail] = useState<NodeDetail | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)

  const [visibleLabels, setVisibleLabels] = useState<Set<NodeLabel>>(
    new Set(['Customer', 'SalesOrder', 'Product', 'OutboundDelivery', 'BillingDocument', 'JournalEntry', 'Payment'])
  )

  const rfInstance = useRef<ReturnType<typeof import('reactflow').useReactFlow> | null>(null)

  // ── Load graph data ─────────────────────────────────────────────────────────
  const loadGraph = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const [rawNodes, rawEdges] = await Promise.all([
        api.graphNodes(undefined, 400),
        api.graphEdges(undefined, 1000),
      ])

      const laidOut = layoutNodes(rawNodes, rawEdges)
      setNodes(laidOut)

      const flowEdges: Edge[] = rawEdges.map((e, i) => ({
        id: `e-${i}`,
        source: e.source,
        target: e.target,
        label: e.relationship,
        labelStyle: { fontSize: 8, fill: '#475569', fontFamily: 'JetBrains Mono' },
        labelBgStyle: { fill: '#0c1118', fillOpacity: 0.85 },
        labelBgPadding: [3, 4] as [number, number],
        style: {
          stroke: EDGE_COLORS[e.relationship] ?? '#334155',
          strokeWidth: 1,
          opacity: 0.55,
        },
        markerEnd: {
          type: MarkerType.ArrowClosed,
          color: EDGE_COLORS[e.relationship] ?? '#334155',
          width: 10,
          height: 10,
        },
        animated: e.relationship === 'SETTLED_BY',
      }))
      setEdges(flowEdges)
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load graph')
    } finally {
      setLoading(false)
    }
  }, [setNodes, setEdges])

  useEffect(() => { loadGraph() }, [loadGraph])

  // ── Highlight nodes from chat results ───────────────────────────────────────
  useEffect(() => {
    if (!nodes.length) return
    const hlSet = new Set(highlightIds)
    setNodes(nds =>
      nds.map(n => ({
        ...n,
        data: {
          ...n.data,
          dimmed: hlSet.size > 0 && !hlSet.has(n.id),
        },
      }))
    )
  }, [highlightIds, setNodes, nodes.length])

  // ── Label visibility filter ─────────────────────────────────────────────────
  const visibleNodes = useMemo(
    () => nodes.filter(n => visibleLabels.has(n.data.label as NodeLabel)),
    [nodes, visibleLabels]
  )

  const visibleEdgeSet = useMemo(() => {
    const ids = new Set(visibleNodes.map(n => n.id))
    return edges.filter(e => ids.has(e.source) && ids.has(e.target))
  }, [edges, visibleNodes])

  // ── Node click → load detail ────────────────────────────────────────────────
  const handleNodeClick: NodeMouseHandler = useCallback(async (_, node) => {
    const id = node.id
    setSelectedNodeId(id)
    onNodeSelect?.(id)
    setDetailLoading(true)
    setNodeDetail(null)
    try {
      const detail = await api.nodeDetail(id)
      setNodeDetail(detail)
    } catch {
      setNodeDetail(null)
    } finally {
      setDetailLoading(false)
    }
  }, [onNodeSelect])

  const handlePaneClick = useCallback(() => {
    setSelectedNodeId(null)
    setNodeDetail(null)
    // Un-dim all
    setNodes(nds => nds.map(n => ({ ...n, data: { ...n.data, dimmed: false } })))
  }, [setNodes])

  const toggleLabel = useCallback((label: NodeLabel) => {
    setVisibleLabels(prev => {
      const next = new Set(prev)
      if (next.has(label)) next.delete(label)
      else next.add(label)
      return next
    })
  }, [])

  // ── Highlight selected node ─────────────────────────────────────────────────
  const displayNodes = useMemo(
    () => visibleNodes.map(n => ({
      ...n,
      selected: n.id === selectedNodeId,
    })),
    [visibleNodes, selectedNodeId]
  )

  if (error) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center gap-4 text-slate-500">
        <div className="text-sm font-mono text-red-400">{error}</div>
        <button
          onClick={loadGraph}
          className="text-xs font-mono border border-[#1a2433] px-4 py-2 rounded hover:border-[#243044] hover:text-slate-300 transition-colors flex items-center gap-2"
        >
          <RefreshCw size={12} /> Retry
        </button>
      </div>
    )
  }

  return (
    <div className="relative flex-1 h-full">
      {/* Loading overlay */}
      {loading && (
        <div className="absolute inset-0 z-20 flex items-center justify-center bg-[#060810]/80 backdrop-blur-sm">
          <div className="flex flex-col items-center gap-3">
            <div className="flex gap-1.5">
              {[0, 1, 2].map(i => (
                <div
                  key={i}
                  className="w-1.5 h-1.5 rounded-full bg-[#00ff88] animate-pulse-slow"
                  style={{ animationDelay: `${i * 200}ms` }}
                />
              ))}
            </div>
            <span className="text-[11px] font-mono text-slate-600">Loading graph…</span>
          </div>
        </div>
      )}

      <ReactFlow
        nodes={displayNodes}
        edges={visibleEdgeSet}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onNodeClick={handleNodeClick}
        onPaneClick={handlePaneClick}
        nodeTypes={NODE_TYPES}
        fitView
        fitViewOptions={FIT_OPTIONS}
        minZoom={0.05}
        maxZoom={3}
        attributionPosition="bottom-left"
        proOptions={{ hideAttribution: true }}
      >
        <Background
          variant={BackgroundVariant.Dots}
          gap={28}
          size={1}
          color="#1a2433"
        />
        <Controls showInteractive={false} />
        <MiniMap
          nodeColor={n => {
            const cfg = getNodeConfig(n.data?.label as NodeLabel)
            return cfg?.border ?? '#334155'
          }}
          maskColor="rgba(6,8,16,0.85)"
          style={{ width: 140, height: 90 }}
        />
      </ReactFlow>

      {/* Legend */}
      <GraphLegend
        visibleLabels={visibleLabels}
        onToggle={toggleLabel}
        nodeCount={visibleNodes.length}
        edgeCount={visibleEdgeSet.length}
      />

      {/* Refresh button */}
      <button
        onClick={loadGraph}
        className="absolute top-3 right-3 z-10 text-slate-600 hover:text-[#00ff88] transition-colors p-2 rounded panel"
        title="Reload graph"
      >
        <RefreshCw size={13} />
      </button>

      {/* Node detail panel */}
      <NodeDetailPanel
        detail={nodeDetail}
        loading={detailLoading}
        onClose={() => { setSelectedNodeId(null); setNodeDetail(null) }}
        onNavigate={id => {
          const target = nodes.find(n => n.id === id)
          if (target) handleNodeClick({} as React.MouseEvent, target as Node)
        }}
      />
    </div>
  )
}
