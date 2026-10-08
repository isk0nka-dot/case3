// =============================================================================
// Argus AI — Proto Types Barrel Export
// =============================================================================

export {
  EventType,
  Severity,
  EventSource,
  TelemetryMode,
  EVENT_TYPE_LABELS,
  SEVERITY_LABELS,
  SEVERITY_COLORS,
  SEVERITY_BADGE_VARIANTS,
  EVENT_SOURCE_LABELS,
  CRITICAL_EVENT_TYPES,
  TELEMETRY_EVENT_TYPES,
  isTelemetryEvent,
  isCriticalEvent,
  getEventCategory
} from './types'

export type {
  GazeDeviationPayload,
  FaceDetectionPayload,
  ObjectDetectionPayload,
  AudioPayload,
  BrowserPayload,
  SystemPayload,
  PsychometryPayload,
  NetworkPayload,
  KernelPayload,
  EventPayload,
  ClientMeta,
  ProctoringEvent,
  IngestEventRequest,
  IngestEventResponse,
  IngestBatchRequest,
  IngestBatchResponse,
  StreamAck,
  HeartbeatRequest,
  HeartbeatResponse,
  SessionDirective
} from './types'

export {
  encodeProctoringEvent,
  encodeIngestEventRequest,
  encodeIngestBatchRequest,
  encodeHeartbeatRequest,
  decodeResponse,
  keysToSnakeCase,
  keysToCamelCase
} from './codec'
