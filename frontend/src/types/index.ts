// Types that mirror the backend Pydantic models exactly

export type NodeLabel =
  | 'Customer'
  | 'SalesOrder'
  | 'Product'
  | 'OutboundDelivery'
  | 'BillingDocument'
  | 'JournalEntry'
  | 'Payment'
  | 'SalesOrderItem'
  | 'Address'

export type RelationshipType =
  | 'PLACED'
  | 'HAS_ADDRESS'
  | 'CONTAINS'
  | 'REFERENCES'
  | 'HAS_DELIVERY'
  | 'BILLED_IN'
  | 'RECORDED_IN'
  | 'SETTLED_BY'

export interface GraphNode {
  id: string
  label: NodeLabel
  properties: Record<string, string | number | boolean | null>
}

export interface GraphEdge {
  source: string
  target: string
  relationship: RelationshipType
}

export interface NodeDetail {
  id: string
  label: NodeLabel
  properties: Record<string, unknown>
  neighbours: Array<{
    relationship: string
    id: string
    label: NodeLabel
    name: string
  }>
}

export interface ChatResponse {
  answer: string
  query: string | null
  query_type: 'sql' | 'cypher' | 'none'
  columns: string[]
  rows: Array<Array<string | number | boolean | null>>
  row_count: number
  error: string | null
}

export interface HealthResponse {
  status: 'ok' | 'degraded' | 'error'
  database: boolean
  llm_configured: boolean
  details: Record<string, string>
}

// Chat message for the UI (not the backend model)
export interface ChatMessage {
  id: string
  role: 'user' | 'assistant'
  content: string
  query?: string | null
  query_type?: string
  columns?: string[]
  rows?: Array<Array<string | number | boolean | null>>
  row_count?: number
  error?: string | null
  timestamp: Date
  loading?: boolean
}
