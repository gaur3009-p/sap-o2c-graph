'use client'
import { useCallback, useRef, useState } from 'react'
import { Header } from '@/components/ui/Header'
import { GraphCanvas } from '@/components/graph/GraphCanvas'
import { ChatPanel } from '@/components/chat/ChatPanel'
import { GripVertical, ChevronLeft, ChevronRight } from 'lucide-react'

const MIN_GRAPH_PCT = 30   // graph panel minimum width %
const MAX_GRAPH_PCT = 80   // graph panel maximum width %
const DEFAULT_GRAPH_PCT = 62

export default function Home() {
  const [splitPct, setSplitPct] = useState(DEFAULT_GRAPH_PCT)
  const [dragging, setDragging] = useState(false)
  const [graphHighlights, setGraphHighlights] = useState<string[]>([])
  const [chatCollapsed, setChatCollapsed] = useState(false)
  const containerRef = useRef<HTMLDivElement>(null)

  // ── Drag-to-resize divider ──────────────────────────────────────────────────
  const startDrag = useCallback((e: React.MouseEvent) => {
    e.preventDefault()
    setDragging(true)

    const onMove = (ev: MouseEvent) => {
      if (!containerRef.current) return
      const rect = containerRef.current.getBoundingClientRect()
      const pct = ((ev.clientX - rect.left) / rect.width) * 100
      setSplitPct(Math.min(MAX_GRAPH_PCT, Math.max(MIN_GRAPH_PCT, pct)))
    }
    const onUp = () => {
      setDragging(false)
      window.removeEventListener('mousemove', onMove)
      window.removeEventListener('mouseup', onUp)
    }
    window.addEventListener('mousemove', onMove)
    window.addEventListener('mouseup', onUp)
  }, [])

  const effectiveSplit = chatCollapsed ? 100 : splitPct

  return (
    <div className="flex flex-col h-screen overflow-hidden">
      <Header />

      {/* Main split-pane area */}
      <div
        ref={containerRef}
        className="flex flex-1 overflow-hidden relative"
        style={{ cursor: dragging ? 'col-resize' : 'default' }}
      >
        {/* ── Graph panel ──────────────────────────────────────────────────── */}
        <div
          className="relative flex flex-col overflow-hidden transition-[width] duration-0"
          style={{ width: `${effectiveSplit}%` }}
        >
          {/* Panel label */}
          <div className="absolute top-3 left-4 z-10 text-[10px] font-mono text-slate-700 uppercase tracking-widest pointer-events-none select-none">
            Graph View
          </div>

          <GraphCanvas
            highlightIds={graphHighlights}
            onNodeSelect={id => {
              // Selecting a node clears any chat-driven highlights
              if (graphHighlights.length > 0) setGraphHighlights([])
            }}
          />
        </div>

        {/* ── Drag divider ─────────────────────────────────────────────────── */}
        {!chatCollapsed && (
          <div
            className="relative flex items-center justify-center w-1 shrink-0 group cursor-col-resize z-10"
            onMouseDown={startDrag}
          >
            {/* Track */}
            <div className="absolute inset-y-0 w-px bg-[#1a2433] group-hover:bg-[#243044] transition-colors" />

            {/* Handle pill */}
            <div className="relative z-10 flex flex-col items-center justify-center w-4 h-10 rounded-full bg-[#111827] border border-[#1a2433] group-hover:border-[#243044] transition-colors">
              <GripVertical size={10} className="text-slate-700 group-hover:text-slate-500 transition-colors" />
            </div>
          </div>
        )}

        {/* ── Chat panel ───────────────────────────────────────────────────── */}
        <div
          className="flex flex-col border-l border-[#1a2433] bg-[#060810] overflow-hidden transition-[width] duration-200"
          style={{ width: chatCollapsed ? 0 : `${100 - effectiveSplit}%` }}
        >
          {!chatCollapsed && (
            <ChatPanel onResultNodes={setGraphHighlights} />
          )}
        </div>

        {/* ── Collapse/expand toggle ────────────────────────────────────────── */}
        <button
          onClick={() => setChatCollapsed(v => !v)}
          className="absolute right-0 top-1/2 -translate-y-1/2 z-20 flex items-center justify-center w-5 h-10 bg-[#111827] border border-[#1a2433] rounded-l-lg hover:border-[#243044] hover:text-slate-300 transition-all text-slate-600"
          title={chatCollapsed ? 'Show chat' : 'Hide chat'}
          style={{ right: chatCollapsed ? 0 : `calc(${100 - splitPct}% - 1px)` }}
        >
          {chatCollapsed
            ? <ChevronLeft size={11} />
            : <ChevronRight size={11} />
          }
        </button>
      </div>
    </div>
  )
}
