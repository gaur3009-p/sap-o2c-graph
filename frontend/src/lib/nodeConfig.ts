import type { NodeLabel } from '@/types'

export const NODE_CONFIG: Record<NodeLabel, { color: string; bg: string; border: string; glow: string; short: string }> = {
  Customer:         { color: '#818cf8', bg: '#1e1b4b', border: '#6366f1', glow: '#6366f140', short: 'CUST' },
  SalesOrder:       { color: '#38bdf8', bg: '#0c1a2e', border: '#0ea5e9', glow: '#0ea5e940', short: 'SO'   },
  Product:          { color: '#fbbf24', bg: '#1c1505', border: '#f59e0b', glow: '#f59e0b40', short: 'PROD' },
  OutboundDelivery: { color: '#34d399', bg: '#052e16', border: '#10b981', glow: '#10b98140', short: 'DEL'  },
  BillingDocument:  { color: '#fb923c', bg: '#1c0a00', border: '#f97316', glow: '#f9731640', short: 'BILL' },
  JournalEntry:     { color: '#c084fc', bg: '#1a0533', border: '#a78bfa', glow: '#a78bfa40', short: 'JE'   },
  Payment:          { color: '#00ff88', bg: '#001a0d', border: '#00cc6a', glow: '#00ff8840', short: 'PAY'  },
  SalesOrderItem:   { color: '#67e8f9', bg: '#0a1a1f', border: '#22d3ee', glow: '#22d3ee40', short: 'ITEM' },
  Address:          { color: '#94a3b8', bg: '#111827', border: '#64748b', glow: '#64748b40', short: 'ADDR' },
}

export const EDGE_COLORS: Record<string, string> = {
  PLACED:      '#6366f1',
  HAS_ADDRESS: '#64748b',
  CONTAINS:    '#0ea5e9',
  REFERENCES:  '#f59e0b',
  HAS_DELIVERY:'#10b981',
  BILLED_IN:   '#f97316',
  RECORDED_IN: '#a78bfa',
  SETTLED_BY:  '#00cc6a',
}

export function getNodeConfig(label: NodeLabel) {
  return NODE_CONFIG[label] ?? NODE_CONFIG['Customer']
}

export function formatNodeId(id: string): string {
  // Truncate long composite IDs like "9400000220_1"
  if (id.length > 12) return id.slice(0, 10) + '…'
  return id
}

export function formatPropValue(val: unknown): string {
  if (val === null || val === undefined) return '—'
  if (typeof val === 'boolean') return val ? 'Yes' : 'No'
  if (typeof val === 'number') return val.toLocaleString()
  const s = String(val)
  if (s.length > 60) return s.slice(0, 58) + '…'
  return s
}
