import type {
  ApiResponse,
  Holding,
  MarginData,
  Order,
  OrderStats,
  PlaceOrderRequest,
  PortfolioStats,
  Position,
  Trade,
} from '@/types/trading'
import { apiClient, webClient } from './client'

export interface QuotesData {
  ask: number
  bid: number
  high: number
  low: number
  ltp: number
  oi: number
  open: number
  prev_close: number
  volume: number
}

export interface DepthLevel {
  price: number
  quantity: number
}

export interface DepthData {
  asks: DepthLevel[]
  bids: DepthLevel[]
  high: number
  low: number
  ltp: number
  ltq: number
  oi: number
  open: number
  prev_close: number
  totalbuyqty: number
  totalsellqty: number
  volume: number
}

export interface MultiQuotesSymbol {
  symbol: string
  exchange: string
}

export interface MultiQuotesResult {
  symbol: string
  exchange: string
  data: QuotesData
}

// MultiQuotes API has a different response structure (results at root, not in data)
export interface MultiQuotesApiResponse {
  status: 'success' | 'error'
  results?: MultiQuotesResult[]
  message?: string
}

async function tryLocalThenLive<T>(
  localCall: () => Promise<ApiResponse<T>>,
  liveCall: () => Promise<ApiResponse<T>>,
  hasLocalData: (response: ApiResponse<T>) => boolean
): Promise<ApiResponse<T>> {
  try {
    const localResponse = await localCall()
    if (localResponse.status === 'success' && hasLocalData(localResponse)) {
      return localResponse
    }
  } catch {
    // Fall back to the live broker path below.
  }

  return liveCall()
}

function hasArrayData<T>(response: ApiResponse<T[]>) {
  return Array.isArray(response.data) && response.data.length > 0
}

export const tradingApi = {
  /**
   * Get real-time quotes for a symbol
   */
  getQuotes: async (
    apiKey: string,
    symbol: string,
    exchange: string
  ): Promise<ApiResponse<QuotesData>> => {
    const response = await apiClient.post<ApiResponse<QuotesData>>('/quotes', {
      apikey: apiKey,
      symbol,
      exchange,
    })
    return response.data
  },

  /**
   * Get real-time quotes for multiple symbols
   */
  getMultiQuotes: async (
    apiKey: string,
    symbols: MultiQuotesSymbol[]
  ): Promise<MultiQuotesApiResponse> => {
    const response = await apiClient.post<MultiQuotesApiResponse>('/multiquotes', {
      apikey: apiKey,
      symbols,
    })
    return response.data
  },

  /**
   * Get market depth for a symbol (5-level order book)
   */
  getDepth: async (
    apiKey: string,
    symbol: string,
    exchange: string
  ): Promise<ApiResponse<DepthData>> => {
    const response = await apiClient.post<ApiResponse<DepthData>>('/depth', {
      apikey: apiKey,
      symbol,
      exchange,
    })
    return response.data
  },

  /**
   * Get margin/funds data
   */
  getFunds: async (apiKey: string): Promise<ApiResponse<MarginData>> => {
    const response = await apiClient.post<ApiResponse<MarginData>>('/funds', {
      apikey: apiKey,
    })
    return response.data
  },

  /**
   * Get positions
   */
  getPositions: async (apiKey: string): Promise<ApiResponse<Position[]>> => {
    return tryLocalThenLive(
      async () => {
        const response = await apiClient.post<ApiResponse<Position[]>>('/localpositionbook', {
          apikey: apiKey,
        })
        return response.data
      },
      async () => {
        const response = await apiClient.post<ApiResponse<Position[]>>('/positionbook', {
          apikey: apiKey,
        })
        return response.data
      },
      hasArrayData
    )
  },

  /**
   * Get order book
   */
  getOrders: async (
    apiKey: string
  ): Promise<ApiResponse<{ orders: Order[]; statistics: OrderStats }>> => {
    return tryLocalThenLive(
      async () => {
        const response = await apiClient.get<ApiResponse<{ orders: Order[]; statistics: OrderStats }>>(
          '/localorderbook',
          {
            params: {
              apikey: apiKey,
            },
          }
        )
        return response.data
      },
      async () => {
        const response = await apiClient.post<
          ApiResponse<{ orders: Order[]; statistics: OrderStats }>
        >('/orderbook', {
          apikey: apiKey,
        })
        return response.data
      },
      (response) => Array.isArray(response.data?.orders) && response.data.orders.length > 0
    )
  },

  /**
   * Get trade book
   */
  getTrades: async (apiKey: string): Promise<ApiResponse<Trade[]>> => {
    return tryLocalThenLive(
      async () => {
        const response = await apiClient.post<ApiResponse<Trade[]>>('/localtradebook', {
          apikey: apiKey,
        })
        return response.data
      },
      async () => {
        const response = await apiClient.post<ApiResponse<Trade[]>>('/tradebook', {
          apikey: apiKey,
        })
        return response.data
      },
      hasArrayData
    )
  },

  /**
   * Get holdings
   */
  getHoldings: async (
    apiKey: string
  ): Promise<ApiResponse<{ holdings: Holding[]; statistics: PortfolioStats }>> => {
    return tryLocalThenLive(
      async () => {
        const response = await apiClient.post<
          ApiResponse<{ holdings: Holding[]; statistics: PortfolioStats }>
        >('/localholdings', {
          apikey: apiKey,
        })
        return response.data
      },
      async () => {
        const response = await apiClient.post<
          ApiResponse<{ holdings: Holding[]; statistics: PortfolioStats }>
        >('/holdings', {
          apikey: apiKey,
        })
        return response.data
      },
      (response) => Array.isArray(response.data?.holdings) && response.data.holdings.length > 0
    )
  },

  /**
   * Place order
   */
  placeOrder: async (order: PlaceOrderRequest): Promise<ApiResponse<{ orderid: string }>> => {
    const response = await apiClient.post<ApiResponse<{ orderid: string }>>('/placeorder', order)
    return response.data
  },

  /**
   * Modify order (uses session auth with CSRF)
   */
  modifyOrder: async (
    orderid: string,
    orderData: {
      symbol: string
      exchange: string
      action: string
      product: string
      pricetype: string
      price: number
      quantity: number
      trigger_price?: number
      disclosed_quantity?: number
    }
  ): Promise<ApiResponse<{ orderid: string }>> => {
    const response = await webClient.post<ApiResponse<{ orderid: string }>>('/modify_order', {
      orderid,
      ...orderData,
    })
    return response.data
  },

  /**
   * Cancel order (uses session auth with CSRF)
   */
  cancelOrder: async (orderid: string): Promise<ApiResponse<{ orderid: string }>> => {
    const response = await webClient.post<ApiResponse<{ orderid: string }>>('/cancel_order', {
      orderid,
    })
    return response.data
  },

  /**
   * Close a specific position (uses session auth with CSRF)
   */
  closePosition: async (
    symbol: string,
    exchange: string,
    product: string
  ): Promise<ApiResponse<void>> => {
    // Uses the web route which handles session-based auth with CSRF
    const response = await webClient.post<ApiResponse<void>>('/close_position', {
      symbol,
      exchange,
      product,
    })
    return response.data
  },

  /**
   * Close all positions (uses session auth with CSRF)
   */
  closeAllPositions: async (): Promise<ApiResponse<void>> => {
    const response = await webClient.post<ApiResponse<void>>('/close_all_positions', {})
    return response.data
  },

  /**
   * Cancel all orders (uses session auth with CSRF)
   */
  cancelAllOrders: async (): Promise<ApiResponse<void>> => {
    const response = await webClient.post<ApiResponse<void>>('/cancel_all_orders', {})
    return response.data
  },
}
