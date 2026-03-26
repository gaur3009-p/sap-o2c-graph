'use client'
import { useState } from 'react'
import { ChevronDown, ChevronUp, Table2 } from 'lucide-react'

interface Props {
  columns: string[]
  rows: Array<Array<string | number | boolean | null>>
  rowCount: number
}

export function ResultTable({ columns, rows, rowCount }: Props) {
  const [expanded, setExpanded] = useState(false)
  if (!columns.length || !rows.length) return null

  const displayRows = expanded ? rows : rows.slice(0, 5)
  const hasMore = rows.length > 5

  return (
    <div className="mt-2 rounded-lg overflow-hidden border border-[#1a2433] animate-slide-up">
      {/* Table header bar */}
      <div className="flex items-center justify-between px-3 py-1.5 bg-[#0c1118] border-b border-[#1a2433]">
        <div className="flex items-center gap-1.5 text-[10px] font-mono text-slate-500">
          <Table2 size={10} />
          <span>{rowCount} row{rowCount !== 1 ? 's' : ''}</span>
          <span className="text-slate-700">·</span>
          <span>{columns.length} col{columns.length !== 1 ? 's' : ''}</span>
        </div>
        {hasMore && (
          <button
            onClick={() => setExpanded(v => !v)}
            className="text-[10px] font-mono text-slate-600 hover:text-slate-300 transition-colors flex items-center gap-1"
          >
            {expanded ? <><ChevronUp size={10} /> collapse</> : <><ChevronDown size={10} /> show all</>}
          </button>
        )}
      </div>

      {/* Scrollable table */}
      <div className="overflow-x-auto">
        <table className="w-full text-[11px] font-mono">
          <thead>
            <tr className="bg-[#111827]">
              {columns.map(col => (
                <th
                  key={col}
                  className="text-left px-3 py-1.5 text-slate-500 font-medium whitespace-nowrap border-b border-[#1a2433]"
                >
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {displayRows.map((row, ri) => (
              <tr key={ri} className="border-b border-[#1a2433]/50 hover:bg-[#1a2433]/30 transition-colors">
                {row.map((cell, ci) => (
                  <td
                    key={ci}
                    className="px-3 py-1.5 text-slate-300 whitespace-nowrap max-w-[200px] overflow-hidden text-ellipsis"
                    title={String(cell ?? '')}
                  >
                    {cell === null || cell === undefined ? (
                      <span className="text-slate-700">null</span>
                    ) : typeof cell === 'boolean' ? (
                      <span className={cell ? 'text-[#00ff88]' : 'text-red-400'}>{cell ? 'true' : 'false'}</span>
                    ) : typeof cell === 'number' ? (
                      <span className="text-amber-400">{cell.toLocaleString()}</span>
                    ) : (
                      String(cell)
                    )}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Collapsed indicator */}
      {!expanded && hasMore && (
        <div className="px-3 py-1.5 text-[10px] font-mono text-slate-700 bg-[#0c1118] border-t border-[#1a2433]">
          +{rows.length - 5} more rows — click "show all" to expand
        </div>
      )}
    </div>
  )
}
