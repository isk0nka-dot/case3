// =============================================================================
// Argus AI — Telemetry Web Worker
// =============================================================================
//
// Background thread for processing high-frequency telemetry (60Hz mouse,
// 30Hz gaze) without blocking the main Vue.js render thread or being
// throttled to 1Hz when the user alt-tabs or minimizes the exam window.
//
// =============================================================================

// Types matching useTelemetryStore.ts
export interface GazePoint {
  readonly x: number
  readonly y: number
  readonly timestamp: number
}

export interface HeatmapCell {
  readonly gridX: number
  readonly gridY: number
  readonly count: number
  readonly intensity: number
}

// ---------------------------------------------------------------------------
// Internal Buffers optimized for Web Workers
// ---------------------------------------------------------------------------

class GazeBuffer {
  private buffer: Float32Array
  private head = 0
  private _size = 0
  readonly capacity: number

  constructor(capacity: number = 256) {
    this.capacity = capacity
    this.buffer = new Float32Array(capacity * 3)
  }

  push(x: number, y: number, timestampOffset: number): void {
    const idx = this.head * 3
    this.buffer[idx] = x
    this.buffer[idx + 1] = y
    this.buffer[idx + 2] = timestampOffset
    this.head = (this.head + 1) % this.capacity
    if (this._size < this.capacity) this._size++
  }

  recent(count: number): GazePoint[] {
    const n = Math.min(count, this._size)
    const result: GazePoint[] = []
    for (let i = 0; i < n; i++) {
      const idx = ((this.head - 1 - i + this.capacity) % this.capacity) * 3
      result.push({
        x: this.buffer[idx]!,
        y: this.buffer[idx + 1]!,
        timestamp: this.buffer[idx + 2]!
      })
    }
    return result
  }

  clear(): void {
    this.buffer.fill(0)
    this.head = 0
    this._size = 0
  }
}

class HeatmapAccumulator {
  private readonly gridSize: number
  private counts: Uint16Array

  constructor(gridSize: number = 10) {
    this.gridSize = gridSize
    this.counts = new Uint16Array(gridSize * gridSize)
  }

  record(x: number, y: number): void {
    const gx = Math.min(this.gridSize - 1, Math.floor(x * this.gridSize))
    const gy = Math.min(this.gridSize - 1, Math.floor(y * this.gridSize))
    this.counts[gy * this.gridSize + gx]!++
  }

  toGrid(): HeatmapCell[] {
    let maxCount = 0
    for (let i = 0; i < this.counts.length; i++) {
      if (this.counts[i]! > maxCount) maxCount = this.counts[i]!
    }

    const cells: HeatmapCell[] = []
    for (let gy = 0; gy < this.gridSize; gy++) {
      for (let gx = 0; gx < this.gridSize; gx++) {
        const count = this.counts[gy * this.gridSize + gx]!
        cells.push({
          gridX: gx,
          gridY: gy,
          count,
          intensity: maxCount > 0 ? count / maxCount : 0
        })
      }
    }
    return cells
  }

  clear(): void {
    this.counts.fill(0)
  }
}

// ---------------------------------------------------------------------------
// Worker State
// ---------------------------------------------------------------------------

const gazeBuffers = new Map<string, GazeBuffer>()
const heatmapAccumulators = new Map<string, HeatmapAccumulator>()
const mousePositions = new Map<string, { x: number, y: number }>()
const focusHistories = new Map<string, number[]>()
const typingHistories = new Map<string, number[]>()
const sampleCounts = new Map<string, { gaze: number, mouse: number, keyboard: number }>()
const sessionStartTimes = new Map<string, number>()
const activeSessionIds = new Set<string>()

// ---------------------------------------------------------------------------
// Message Handling
// ---------------------------------------------------------------------------

self.addEventListener('message', (e: MessageEvent) => {
  const { type, payload } = e.data

  switch (type) {
    case 'START_SESSION': {
      const { sessionId } = payload
      if (!gazeBuffers.has(sessionId)) {
        gazeBuffers.set(sessionId, new GazeBuffer(256))
        heatmapAccumulators.set(sessionId, new HeatmapAccumulator(10))
        mousePositions.set(sessionId, { x: 0.5, y: 0.5 })
        focusHistories.set(sessionId, [])
        typingHistories.set(sessionId, [])
        sampleCounts.set(sessionId, { gaze: 0, mouse: 0, keyboard: 0 })
        sessionStartTimes.set(sessionId, Date.now())
        activeSessionIds.add(sessionId)
      }
      break
    }

    case 'STOP_SESSION': {
      const { sessionId } = payload
      gazeBuffers.delete(sessionId)
      heatmapAccumulators.delete(sessionId)
      mousePositions.delete(sessionId)
      focusHistories.delete(sessionId)
      typingHistories.delete(sessionId)
      sampleCounts.delete(sessionId)
      sessionStartTimes.delete(sessionId)
      activeSessionIds.delete(sessionId)
      break
    }

    case 'RECORD_GAZE': {
      const { sessionId, x, y } = payload
      const buffer = gazeBuffers.get(sessionId)
      if (buffer) {
        const startTime = sessionStartTimes.get(sessionId) ?? Date.now()
        buffer.push(x, y, Date.now() - startTime)
        heatmapAccumulators.get(sessionId)?.record(x, y)
        const counts = sampleCounts.get(sessionId)
        if (counts) counts.gaze++
      }
      break
    }

    case 'RECORD_MOUSE': {
      const { sessionId, x, y } = payload
      const pos = mousePositions.get(sessionId)
      if (pos) { pos.x = x; pos.y = y }
      const counts = sampleCounts.get(sessionId)
      if (counts) counts.mouse++
      break
    }

    case 'RECORD_KEYBOARD': {
      const { sessionId, wpm } = payload
      const history = typingHistories.get(sessionId)
      if (history) {
        history.push(wpm)
        if (history.length > 120) history.shift()
      }
      const counts = sampleCounts.get(sessionId)
      if (counts) counts.keyboard++
      break
    }

    case 'RECORD_FOCUS': {
      const { sessionId, score } = payload
      const history = focusHistories.get(sessionId)
      if (history) {
        history.push(score)
        if (history.length > 120) history.shift()
      }
      break
    }

    case 'GET_STATE': {
      // Main thread requested 2Hz state sync
      const now = Date.now()
      const syncData: any = {
        sessions: {},
        globalStats: { totalSessions: activeSessionIds.size, totalGazeSamples: 0, totalMouseSamples: 0, totalKeyboardEvents: 0 }
      }

      for (const sessionId of activeSessionIds) {
        const gazeBuffer = gazeBuffers.get(sessionId)
        const heatmap = heatmapAccumulators.get(sessionId)
        const mouse = mousePositions.get(sessionId)
        const focus = focusHistories.get(sessionId)
        const typing = typingHistories.get(sessionId)
        const counts = sampleCounts.get(sessionId)

        if (!gazeBuffer || !heatmap || !counts) continue

        syncData.globalStats.totalGazeSamples += counts.gaze
        syncData.globalStats.totalMouseSamples += counts.mouse
        syncData.globalStats.totalKeyboardEvents += counts.keyboard

        const gazeTrail = gazeBuffer.recent(60)

        syncData.sessions[sessionId] = {
          gazeTrail,
          heatmap: heatmap.toGrid(),
          currentGaze: gazeTrail[0] ? { x: gazeTrail[0].x, y: gazeTrail[0].y } : null,
          currentMouse: mouse ? { ...mouse } : null,
          focusHistory: focus ? [...focus] : [],
          typingSpeedHistory: typing ? [...typing] : [],
          lastUpdate: now,
          totalGazeSamples: counts.gaze,
          totalMouseSamples: counts.mouse,
          totalKeyboardEvents: counts.keyboard
        }
      }

      self.postMessage({ type: 'SYNC_STATE', payload: syncData })
      break
    }
  }
})
