// =============================================================================
// Argus AI — gRPC Client Layer Barrel Export
// =============================================================================

export { GrpcWebTransport } from './transport'
export type {
  TransportConfig,
  TransportInterceptor,
  TransportRequest,
  TransportResponse
} from './transport'
export { TransportError, TransportErrorCode } from './transport'

export { EventCollectorClient } from './client'
export type { IngestOptions, BatchConfig } from './client'

export {
  createAuthInterceptor,
  createLoggingInterceptor,
  createMetricsInterceptor
} from './interceptors'
export type {
  TokenProvider,
  LogLevel,
  TransportMetrics
} from './interceptors'
