'use client'
import { useState } from 'react'
import { Copy, Check, ChevronDown, ChevronUp, Code2 } from 'lucide-react'

interface Props {
  query: string
  queryType: string
}

// Minimal keyword highlighter — no external deps
function highlight(sql: string): React.ReactNode[] {
  const SQL_KW = /\b(SELECT|FROM|WHERE|JOIN|LEFT|INNER|OUTER|ON|AND|OR|NOT|IN|IS|NULL|GROUP BY|ORDER BY|HAVING|LIMIT|OFFSET|COUNT|SUM|AVG|MIN|MAX|DISTINCT|AS|WITH|UNION|ALL|CASE|WHEN|THEN|ELSE|END|BY|DESC|ASC)\b/gi
  const parts = sql.split(SQL_KW)
  return parts.map((part, i) => {
    if (SQL_KW.test(part)) {
      return <span key={i} className="text-[#38bdf8] font-medium">{part}</span>
    }
    // Strings
    if (/^'[^']*'$/.test(part)) {
      return <span key={i} className="text-[#fbbf24]">{part}</span>
    }
    // Numbers
    const numHighlighted = part.replace(/\b(\d+\.?\d*)\b/g, '<NUM>$1</NUM>')
    if (numHighlighted.includes('<NUM>')) {
      return <span key={i} dangerouslySetInnerHTML={{
        __html: numHighlighted
          .replace(/<NUM>/g, '<span style="color:#34d399">')
          .replace(/<\/NUM>/g, '</span>')
      }} />
    }
    return <span key={i} className="text-slate-300">{part}</span>
  })
}

export function QueryDisplay({ query, queryType }: Props) {
  const [open, setOpen] = useState(false)
  const [copied, setCopied] = useState(false)

  const copy = async () => {
    await navigator.clipboard.writeText(query)
    setCopied(true)
    setTimeout(() => setCopied(false), 1500)
  }

  return (
    <div className="mt-2 rounded-lg border border-[#1a2433] overflow-hidden animate-slide-up">
      <button
        onClick={() => setOpen(v => !v)}
        className="w-full flex items-center justify-between px-3 py-1.5 bg-[#0c1118] hover:bg-[#111827] transition-colors"
      >
        <div className="flex items-center gap-1.5 text-[10px] font-mono text-slate-600">
          <Code2 size={10} />
          <span className="uppercase tracking-widest">{queryType}</span>
          <span className="text-slate-700">·</span>
          <span>{query.trim().split('\n').length} lines</span>
        </div>
        <div className="flex items-center gap-2">
          {open && (
            <button
              onClick={e => { e.stopPropagation(); copy() }}
              className="text-slate-600 hover:text-slate-300 transition-colors"
            >
              {copied ? <Check size={10} className="text-[#00ff88]" /> : <Copy size={10} />}
            </button>
          )}
          {open ? <ChevronUp size={10} className="text-slate-600" /> : <ChevronDown size={10} className="text-slate-600" />}
        </div>
      </button>

      {open && (
        <div className="p-3 overflow-x-auto bg-[#060810]">
          <pre className="text-[11px] font-mono leading-relaxed whitespace-pre-wrap break-all">
            {highlight(query)}
          </pre>
        </div>
      )}
    </div>
  )
}
