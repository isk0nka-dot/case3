// =============================================================================
// Argus AI — Protobuf ↔ JSON Codec
// =============================================================================
//
// Bidirectional conversion between TypeScript interfaces and the JSON encoding
// used by gRPC-Web-text (base64-encoded Protobuf over HTTP).
//
// Design decision: JSON mode vs Binary Protobuf
//
//   gRPC-Web supports two Content-Types:
//     1. application/grpc-web+proto  — Binary Protobuf (most compact)
//     2. application/grpc-web-text   — Base64-encoded Protobuf
//     3. application/json (via grpc-gateway) — Standard JSON
//
//   We use **JSON encoding over HTTP POST** to the gRPC-Web endpoint. This is
//   possible because Connect protocol supports JSON. However, since our Go
//   server speaks native gRPC-Web (not Connect), we encode events as JSON and
//   send them as the body of gRPC-Web-text requests.
//
//   Actually, for maximum compatibility with the Go gRPC server that uses
//   grpc.Server.ServeHTTP, we implement a **REST-like JSON client** that
//   speaks to a thin translation layer (or directly if the server adds
//   grpc-gateway). For MVP, we use a simple HTTP POST JSON client that maps
//   to the gRPC service methods.
//
//   The real production path would be:
//     Browser → Connect-ES client → gRPC-Web → Go server
//   But since our Go server already handles gRPC-Web via Content-Type routing,
//   we use the grpc-web binary protocol directly with manual frame encoding.
//
// Architecture:
//   TypeScript Object → toProtoJSON() → JSON string → gRPC-Web frame →
//   → HTTP POST → Go gRPC-Web proxy → grpc.Server → handler
//
// =============================================================================

import type {
  ProctoringEvent,
  IngestEventRequest,
  IngestBatchRequest,
  HeartbeatRequest,
  ClientMeta,
  EventPayload
} from './types'

// ---------------------------------------------------------------------------
// camelCase → snake_case conversion (TypeScript → Proto JSON)
// ---------------------------------------------------------------------------

/**
 * Convert a camelCase key to snake_case for Protobuf JSON encoding.
 * Proto3 JSON encoding uses camelCase by default, but the Go server
 * uses jsonpb which accepts both. We send camelCase for compatibility.
 */
function toSnakeCase(str: string): string {
  return str.replace(/[A-Z]/g, letter => `_${letter.toLowerCase()}`)
}

/**
 * Convert a snake_case key to camelCase for TypeScript consumption.
 */
function toCamelCase(str: string): string {
  return str.replace(/_([a-z])/g, (_, letter) => letter.toUpperCase())
}

/**
 * Deep convert object keys from camelCase to snake_case.
 */
export function keysToSnakeCase(obj: Record<string, unknown>): Record<string, unknown> {
  const result: Record<string, unknown> = {}
  for (const key of Object.keys(obj)) {
    const snakeKey = toSnakeCase(key)
    const value = obj[key]
    if (value !== null && value !== undefined) {
      if (Array.isArray(value)) {
        result[snakeKey] = value.map(item =>
          typeof item === 'object' && item !== null
            ? keysToSnakeCase(item as Record<string, unknown>)
            : item
        )
      } else if (typeof value === 'object') {
        result[snakeKey] = keysToSnakeCase(value as Record<string, unknown>)
      } else {
        result[snakeKey] = value
      }
    }
  }
  return result
}

/**
 * Deep convert object keys from snake_case to camelCase.
 */
export function keysToCamelCase(obj: Record<string, unknown>): Record<string, unknown> {
  const result: Record<string, unknown> = {}
  for (const key of Object.keys(obj)) {
    const camelKey = toCamelCase(key)
    const value = obj[key]
    if (value !== null && value !== undefined) {
      if (Array.isArray(value)) {
        result[camelKey] = value.map(item =>
          typeof item === 'object' && item !== null
            ? keysToCamelCase(item as Record<string, unknown>)
            : item
        )
      } else if (typeof value === 'object') {
        result[camelKey] = keysToCamelCase(value as Record<string, unknown>)
      } else {
        result[camelKey] = value
      }
    }
  }
  return result
}

// ---------------------------------------------------------------------------
// Event Encoding — TypeScript → Proto JSON
// ---------------------------------------------------------------------------

/**
 * Encode a ProctoringEvent payload into its proto JSON representation.
 * The oneof field is flattened as per Proto3 JSON encoding rules.
 */
function encodePayload(payload: EventPayload): Record<string, unknown> {
  switch (payload.type) {
    case 'gazeDeviation':
      return { gaze_deviation: keysToSnakeCase(payload.data as unknown as Record<string, unknown>) }
    case 'faceDetection':
      return { face_detection: keysToSnakeCase(payload.data as unknown as Record<string, unknown>) }
    case 'objectDetection':
      return { object_detection: keysToSnakeCase(payload.data as unknown as Record<string, unknown>) }
    case 'audio':
      return { audio: keysToSnakeCase(payload.data as unknown as Record<string, unknown>) }
    case 'browser':
      return { browser: keysToSnakeCase(payload.data as unknown as Record<string, unknown>) }
    case 'system':
      return { system: keysToSnakeCase(payload.data as unknown as Record<string, unknown>) }
    case 'psychometry':
      return { psychometry: keysToSnakeCase(payload.data as unknown as Record<string, unknown>) }
    case 'network':
      return { network: keysToSnakeCase(payload.data as unknown as Record<string, unknown>) }
    case 'kernel':
      return { kernel: keysToSnakeCase(payload.data as unknown as Record<string, unknown>) }
    case 'headPose':
      return { head_pose: keysToSnakeCase(payload.data as unknown as Record<string, unknown>) }
    case 'liveness':
      return { liveness: keysToSnakeCase(payload.data as unknown as Record<string, unknown>) }
    case 'audioAnalysis':
      return { audio_analysis: keysToSnakeCase(payload.data as unknown as Record<string, unknown>) }
    case 'faceEmbedding':
      return { face_embedding: keysToSnakeCase(payload.data as unknown as Record<string, unknown>) }
    default:
      return {}
  }
}

/**
 * Encode a ClientMeta to proto JSON.
 */
function encodeClientMeta(meta: ClientMeta): Record<string, unknown> {
  return {
    user_agent: meta.userAgent,
    sdk_version: meta.sdkVersion,
    resolution: meta.resolution,
    timezone_offset_min: meta.timezoneOffsetMin,
    ...(meta.ipAddress ? { ip_address: meta.ipAddress } : {}),
    ...(meta.region ? { region: meta.region } : {})
  }
}

/**
 * Convert an ISO 8601 timestamp to Proto3 Timestamp JSON format.
 * Proto3 JSON uses RFC 3339 format: "2024-01-15T09:30:00Z"
 */
function toProtoTimestamp(isoString: string): string {
  return new Date(isoString).toISOString()
}

/**
 * Encode a ProctoringEvent to its Proto3 JSON representation.
 */
export function encodeProctoringEvent(event: ProctoringEvent): Record<string, unknown> {
  const proto: Record<string, unknown> = {
    event_id: event.eventId,
    session_id: event.sessionId,
    student_id: event.studentId,
    exam_id: event.examId,
    org_id: event.orgId,
    event_type: event.eventType,
    severity: event.severity,
    source: event.source,
    client_timestamp: toProtoTimestamp(event.clientTimestamp),
    label: event.label,
    confidence: event.confidence
  }

  if (event.serverTimestamp) {
    proto.server_timestamp = toProtoTimestamp(event.serverTimestamp)
  }

  if (event.videoTimestampSec !== undefined) {
    proto.video_timestamp_sec = event.videoTimestampSec
  }

  if (event.payload) {
    Object.assign(proto, encodePayload(event.payload))
  }

  if (event.clientMeta) {
    proto.client_meta = encodeClientMeta(event.clientMeta)
  }

  return proto
}

/**
 * Encode an IngestEventRequest to Proto3 JSON.
 */
export function encodeIngestEventRequest(req: IngestEventRequest): Record<string, unknown> {
  return {
    event: encodeProctoringEvent(req.event)
  }
}

/**
 * Encode an IngestBatchRequest to Proto3 JSON.
 */
export function encodeIngestBatchRequest(req: IngestBatchRequest): Record<string, unknown> {
  return {
    events: req.events.map(encodeProctoringEvent),
    batch_id: req.batchId
  }
}

/**
 * Encode a HeartbeatRequest to Proto3 JSON.
 */
export function encodeHeartbeatRequest(req: HeartbeatRequest): Record<string, unknown> {
  return {
    session_id: req.sessionId,
    student_id: req.studentId,
    exam_id: req.examId,
    client_timestamp: toProtoTimestamp(req.clientTimestamp),
    current_focus_score: req.currentFocusScore,
    violation_count: req.violationCount
  }
}

// ---------------------------------------------------------------------------
// Response Decoding — Proto JSON → TypeScript
// ---------------------------------------------------------------------------

/**
 * Decode a Proto3 JSON response, converting snake_case keys to camelCase.
 * This is the generic decoder for all response types.
 */
export function decodeResponse<T>(json: Record<string, unknown>): T {
  return keysToCamelCase(json) as T
}
