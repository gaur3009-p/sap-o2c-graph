'use client';
import { useCallback, useEffect, useRef, useState } from 'react'
import { Send, Sparkles, AlertTriangle, Bot } from 'lucide-react'
import { ResultTable } from './ResultTable'
import { QueryDisplay } from './QueryDisplay'
import { api } from '@/lib/api'
import type { ChatMessage } from '@/types'
import clsx from 'clsx'

const SUGGESTED = [
  'Which products appear in the most billing documents?',
  'Show me the top 5 customers by total order value',
  'List all sales orders that were never delivered',
  'Trace the full O2C flow for billing document 90504248',
  'Which deliveries were never billed?',
  'What is the total revenue across all billing documents?',
]

interface Props {
  onResultNodes?: (ids: string[]) => void
}

export function ChatPanel({ onResultNodes }: Props) {
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const bottomRef = useRef<HTMLDivElement>(null)
  const inputRef  = useRef<HTMLTextAreaElement>(null)

  // Auto-scroll on new messages
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  const sendMessage = useCallback(async (question: string) => {
    if (!question.trim() || loading) return

    const userMsg: ChatMessage = {
      id: crypto.randomUUID(),
      role: 'user',
      content: question.trim(),
      timestamp: new Date(),
    }
    const placeholderMsg: ChatMessage = {
      id: crypto.randomUUID(),
      role: 'assistant',
      content: '',
      timestamp: new Date(),
      loading: true,
    }

    setMessages(prev => [...prev, userMsg, placeholderMsg])
    setInput('')
    setLoading(true)

    try {
      const result = await api.chat(question.trim())

      const assistantMsg: ChatMessage = {
        id: placeholderMsg.id,
        role: 'assistant',
        content: result.answer,
        query: result.query,
        query_type: result.query_type,
        columns: result.columns,
        rows: result.rows,
        row_count: result.row_count,
        error: result.error,
        timestamp: new Date(),
        loading: false,
      }

      setMessages(prev => prev.map(m => m.id === placeholderMsg.id ? assistantMsg : m))

      // Highlight matching nodes in graph if we have IDs in the results
      if (result.columns.length && result.rows.length) {
        const idCols = result.columns.findIndex(c =>
          /id|order|document|delivery|payment|customer/i.test(c)
        )
        if (idCols >= 0) {
          const ids = result.rows.map(r => String(r[idCols])).filter(Boolean)
          onResultNodes?.(ids)
        }
      }
    } catch (err) {
      const errMsg: ChatMessage = {
        id: placeholderMsg.id,
        role: 'assistant',
        content: 'Failed to reach the API. Is the backend running on port 8000?',
        error: err instanceof Error ? err.message : 'Unknown error',
        timestamp: new Date(),
        loading: false,
      }
      setMessages(prev => prev.map(m => m.id === placeholderMsg.id ? errMsg : m))
    } finally {
      setLoading(false)
      inputRef.current?.focus()
    }
  }, [loading, onResultNodes])

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      sendMessage(input)
    }
  }

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="px-4 py-3 border-b border-[#1a2433] flex items-center gap-2.5 shrink-0">
        <div className="w-6 h-6 rounded-full bg-[#001a0d] border border-[#00cc6a] flex items-center justify-center">
          <Sparkles size={11} className="text-[#00ff88]" />
        </div>
        <div>
          <div className="text-xs font-display font-semibold text-slate-200 tracking-wide">Graph Intelligence</div>
          <div className="text-[10px] font-mono text-slate-600">Ask anything about the O2C dataset</div>
        </div>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto px-4 py-4 space-y-5">
        {messages.length === 0 ? (
          <EmptyState onSuggest={sendMessage} />
        ) : (
          messages.map((msg, i) => (
            <MessageBubble key={msg.id} message={msg} index={i} />
          ))
        )}
        <div ref={bottomRef} />
      </div>

      {/* Input */}
      <div className="px-4 pb-4 pt-2 border-t border-[#1a2433] shrink-0">
        <div className={clsx(
          'flex items-end gap-2 rounded-xl border bg-[#0c1118] transition-colors duration-150 px-3 py-2',
          loading ? 'border-[#1a2433]' : 'border-[#1a2433] focus-within:border-[#243044]'
        )}>
          <textarea
            ref={inputRef}
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask about orders, deliveries, billing…"
            rows={1}
            disabled={loading}
            className="flex-1 bg-transparent text-sm text-slate-200 placeholder-slate-700 outline-none resize-none font-sans leading-relaxed py-0.5 max-h-32"
            style={{ minHeight: 24 }}
          />
          <button
            onClick={() => sendMessage(input)}
            disabled={!input.trim() || loading}
            className={clsx(
              'rounded-lg p-1.5 transition-all duration-150 shrink-0',
              input.trim() && !loading
                ? 'bg-[#001a0d] text-[#00ff88] border border-[#00cc6a] hover:bg-[#002d17] glow-acid'
                : 'text-slate-700 cursor-not-allowed'
            )}
          >
            {loading ? (
              <div className="w-4 h-4 flex items-center justify-center gap-0.5">
                {[0,1,2].map(i => (
                  <div key={i} className="w-1 h-1 rounded-full bg-[#00ff88] animate-pulse-slow"
                    style={{ animationDelay: `${i * 150}ms` }} />
                ))}
              </div>
            ) : (
              <Send size={14} />
            )}
          </button>
        </div>
        <div className="mt-1.5 text-[10px] font-mono text-slate-700 text-right">
          Enter to send · Shift+Enter for new line
        </div>
      </div>
    </div>
  )
}

// ── Empty state with suggested questions ─────────────────────────────────────
function EmptyState({ onSuggest }: { onSuggest: (q: string) => void }) {
  return (
    <div className="flex flex-col gap-5 py-2 animate-fade-in">
      <div className="flex flex-col items-center text-center gap-2 py-4">
        <div className="w-12 h-12 rounded-full bg-[#001a0d] border border-[#00cc6a]/40 flex items-center justify-center mb-1">
          <Bot size={20} className="text-[#00ff88]/60" />
        </div>
        <div className="text-sm font-display font-semibold text-slate-300">Ready to query</div>
        <div className="text-[11px] font-mono text-slate-600 max-w-[220px] leading-relaxed">
          Ask in plain English — I&apos;ll generate and run the SQL query for you
        </div>
      </div>

      <div>
        <div className="text-[10px] font-mono text-slate-600 uppercase tracking-widest mb-2">Suggested</div>
        <div className="space-y-1.5">
          {SUGGESTED.map(q => (
            <button
              key={q}
              onClick={() => onSuggest(q)}
              className="w-full text-left text-[11px] text-slate-400 hover:text-slate-200 bg-[#0c1118] hover:bg-[#111827] border border-[#1a2433] hover:border-[#243044] rounded-lg px-3 py-2 transition-all duration-100 font-sans leading-snug"
            >
              {q}
            </button>
          ))}
        </div>
      </div>
    </div>
  )
}

// ── Individual message bubble ─────────────────────────────────────────────────
function MessageBubble({ message, index }: { message: ChatMessage; index: number }) {
  const isUser = message.role === 'user'

  if (isUser) {
    return (
      <div className="flex justify-end animate-slide-up chat-message" style={{ animationDelay: `${index * 20}ms` }}>
        <div className="max-w-[85%] bg-[#111827] border border-[#1a2433] rounded-2xl rounded-tr-sm px-3.5 py-2.5">
          <p className="text-sm text-slate-200 leading-relaxed">{message.content}</p>
        </div>
      </div>
    )
  }

  // Assistant message
  return (
    <div className="flex gap-2.5 animate-slide-up chat-message" style={{ animationDelay: `${index * 20}ms` }}>
      {/* Avatar */}
      <div className="w-6 h-6 rounded-full bg-[#001a0d] border border-[#00cc6a]/40 flex items-center justify-center shrink-0 mt-0.5">
        <Sparkles size={10} className="text-[#00ff88]/70" />
      </div>

      <div className="flex-1 min-w-0">
        {message.loading ? (
          <LoadingDots />
        ) : (
          <>
            {/* Error badge */}
            {message.error && (
              <div className="flex items-center gap-1.5 text-[11px] font-mono text-red-400 mb-2 bg-red-950/30 border border-red-900/40 rounded-lg px-3 py-1.5">
                <AlertTriangle size={10} />
                <span>{message.error}</span>
              </div>
            )}

            {/* Answer text */}
            <div className="text-sm text-slate-300 leading-relaxed whitespace-pre-wrap">
              {message.content}
            </div>

            {/* Query */}
            {message.query && message.query_type !== 'none' && (
              <QueryDisplay query={message.query} queryType={message.query_type ?? 'sql'} />
            )}

            {/* Results table */}
            {message.columns && message.rows && message.columns.length > 0 && message.rows.length > 0 && (
              <ResultTable
                columns={message.columns}
                rows={message.rows}
                rowCount={message.row_count ?? message.rows.length}
              />
            )}

            {/* Timestamp */}
            <div className="mt-1.5 text-[10px] font-mono text-slate-700">
              {message.timestamp.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })}
            </div>
          </>
        )}
      </div>
    </div>
  )
}

function LoadingDots() {
  return (
    <div className="flex items-center gap-1.5 py-2">
      {[0, 1, 2].map(i => (
        <div
          key={i}
          className="w-1.5 h-1.5 rounded-full bg-[#00ff88]/50 animate-pulse-slow"
          style={{ animationDelay: `${i * 200}ms` }}
        />
      ))}
      <span className="text-[11px] font-mono text-slate-600 ml-1">Thinking…</span>
    </div>
  )
}
