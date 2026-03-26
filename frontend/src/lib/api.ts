import type { GraphNode, GraphEdge, NodeDetail, ChatResponse, HealthResponse } from '@/types'

const BASE = process.env.NEXT_PUBLIC_API_URL

async function apiFetch<T>(path: string, opts?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...opts,
  })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(`API ${res.status}: ${text}`)
  }
  return res.json()
}

export const api = {
  health: () =>
    apiFetch<HealthResponse>('/api/health'),

  graphNodes: (label?: string, limit = 300) => {
    const params = new URLSearchParams({ limit: String(limit) })
    if (label) params.set('label', label)
    return apiFetch<GraphNode[]>(`/api/graph/nodes?${params}`)
  },

  graphEdges: (relationship?: string, limit = 800) => {
    const params = new URLSearchParams({ limit: String(limit) })
    if (relationship) params.set('relationship', relationship)
    return apiFetch<GraphEdge[]>(`/api/graph/edges?${params}`)
  },

  nodeDetail: (id: string) =>
    apiFetch<NodeDetail>(`/api/graph/node/${encodeURIComponent(id)}`),

  chat: (question: string) =>
    apiFetch<ChatResponse>('/api/chat', {
      method: 'POST',
      body: JSON.stringify({ question }),
    }),
}
