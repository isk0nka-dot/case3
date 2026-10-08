export type GazeDirection = 'center' | 'left' | 'right' | 'up' | 'down'

export interface RealtimeVisionFrame {
  timestamp: number
  faceCount: number
  gaze: {
    x: number
    y: number
    direction: GazeDirection
    angleDegrees: number
  }
  headPose: {
    yaw: number
    pitch: number
    roll: number
  }
  livenessScore: number
}

export interface RealtimeAIRuleThresholds {
  noFaceFrames: number
  eventCooldownMs: number
  gazeDeviationMinMs: number
  livenessThreshold: number
  headPoseYawDeg: number
  headPosePitchDeg: number
  headPoseRollDeg: number
}

export type RealtimeAudioClass = 'silence' | 'speech' | 'whisper' | 'music' | 'keyboard' | 'ambient'

export interface RealtimeAudioFrame {
  timestamp: number
  rmsDb: number
  peakDb: number
  vadActive: boolean
  vadConfidence: number
  speechRatio?: number
  speakerCount: number
  classification: RealtimeAudioClass
  classificationConfidence: number
}

export interface RealtimeAudioRuleThresholds {
  audioEventCooldownMs: number
  vadConfidenceThreshold: number
  noiseRmsDbThreshold: number
  audioClassificationConfidenceThreshold: number
}

export interface RealtimeAIRuleState {
  consecutiveNoFace: number
  gazeDeviationStartedAt: number | null
  lastNoFaceEventAt: number
  lastMultiFaceEventAt: number
  lastHeadPoseEventAt: number
  lastLivenessEventAt: number
  lastVoiceActivityEventAt: number
  lastAudioAnomalyEventAt: number
  lastWhisperEventAt: number
  lastSecondSpeakerEventAt: number
}

export type RealtimeAIEventDecision
  = | {
    kind: 'gaze_telemetry'
    gazeX: number
    gazeY: number
  }
  | {
    kind: 'gaze_deviation'
    direction: Exclude<GazeDirection, 'center'>
    durationMs: number
    angleDegrees: number
    gazeX: number
    gazeY: number
  }
  | {
    kind: 'face_not_detected'
    consecutiveFrames: number
  }
  | {
    kind: 'multiple_persons'
    faceCount: number
  }
  | {
    kind: 'head_pose_anomaly'
    yaw: number
    pitch: number
    roll: number
  }
  | {
    kind: 'liveness_failed'
    livenessScore: number
  }
  | {
    kind: 'audio_level_telemetry'
    rmsDb: number
    peakDb: number
    vadActive: boolean
    classification: RealtimeAudioClass
  }
  | {
    kind: 'voice_activity'
    vadConfidence: number
    rmsDb: number
  }
  | {
    kind: 'audio_anomaly'
    rmsDb: number
    classification: RealtimeAudioClass
    confidence: number
  }
  | {
    kind: 'whisper_detected'
    confidence: number
    rmsDb: number
  }
  | {
    kind: 'second_speaker_detected'
    speakerCount: number
    confidence: number
  }

export function createDefaultRealtimeAIRuleState(): RealtimeAIRuleState {
  return {
    consecutiveNoFace: 0,
    gazeDeviationStartedAt: null,
    lastNoFaceEventAt: Number.NEGATIVE_INFINITY,
    lastMultiFaceEventAt: Number.NEGATIVE_INFINITY,
    lastHeadPoseEventAt: Number.NEGATIVE_INFINITY,
    lastLivenessEventAt: Number.NEGATIVE_INFINITY,
    lastVoiceActivityEventAt: Number.NEGATIVE_INFINITY,
    lastAudioAnomalyEventAt: Number.NEGATIVE_INFINITY,
    lastWhisperEventAt: Number.NEGATIVE_INFINITY,
    lastSecondSpeakerEventAt: Number.NEGATIVE_INFINITY
  }
}

export function createDefaultRealtimeAIThresholds(): RealtimeAIRuleThresholds {
  return {
    noFaceFrames: 3,
    eventCooldownMs: 5_000,
    gazeDeviationMinMs: 2_000,
    livenessThreshold: 0.4,
    headPoseYawDeg: 25,
    headPosePitchDeg: 20,
    headPoseRollDeg: 15
  }
}

export function createDefaultRealtimeAudioThresholds(): RealtimeAudioRuleThresholds {
  return {
    audioEventCooldownMs: 5_000,
    vadConfidenceThreshold: 0.65,
    noiseRmsDbThreshold: -25,
    audioClassificationConfidenceThreshold: 0.7
  }
}

export function evaluateVisionFrame(
  frame: RealtimeVisionFrame,
  thresholds: RealtimeAIRuleThresholds,
  state: RealtimeAIRuleState,
  nowMs = frame.timestamp
): RealtimeAIEventDecision[] {
  const decisions: RealtimeAIEventDecision[] = [
    {
      kind: 'gaze_telemetry',
      gazeX: frame.gaze.x,
      gazeY: frame.gaze.y
    }
  ]

  if (frame.faceCount === 0) {
    state.consecutiveNoFace++
  } else {
    state.consecutiveNoFace = 0
  }

  if (
    state.consecutiveNoFace >= thresholds.noFaceFrames
    && nowMs - state.lastNoFaceEventAt >= thresholds.eventCooldownMs
  ) {
    decisions.push({
      kind: 'face_not_detected',
      consecutiveFrames: state.consecutiveNoFace
    })
    state.lastNoFaceEventAt = nowMs
  }

  if (
    frame.faceCount > 1
    && nowMs - state.lastMultiFaceEventAt >= thresholds.eventCooldownMs
  ) {
    decisions.push({
      kind: 'multiple_persons',
      faceCount: frame.faceCount
    })
    state.lastMultiFaceEventAt = nowMs
  }

  if (frame.gaze.direction === 'center') {
    state.gazeDeviationStartedAt = null
  } else {
    state.gazeDeviationStartedAt ??= nowMs
    const durationMs = nowMs - state.gazeDeviationStartedAt
    if (durationMs >= thresholds.gazeDeviationMinMs) {
      decisions.push({
        kind: 'gaze_deviation',
        direction: frame.gaze.direction,
        durationMs,
        angleDegrees: frame.gaze.angleDegrees,
        gazeX: frame.gaze.x,
        gazeY: frame.gaze.y
      })
    }
  }

  if (
    (Math.abs(frame.headPose.yaw) > thresholds.headPoseYawDeg
      || Math.abs(frame.headPose.pitch) > thresholds.headPosePitchDeg
      || Math.abs(frame.headPose.roll) > thresholds.headPoseRollDeg)
    && nowMs - state.lastHeadPoseEventAt >= thresholds.eventCooldownMs
  ) {
    decisions.push({
      kind: 'head_pose_anomaly',
      yaw: frame.headPose.yaw,
      pitch: frame.headPose.pitch,
      roll: frame.headPose.roll
    })
    state.lastHeadPoseEventAt = nowMs
  }

  if (
    frame.faceCount > 0
    && frame.livenessScore < thresholds.livenessThreshold
    && nowMs - state.lastLivenessEventAt >= thresholds.eventCooldownMs
  ) {
    decisions.push({
      kind: 'liveness_failed',
      livenessScore: frame.livenessScore
    })
    state.lastLivenessEventAt = nowMs
  }

  return decisions
}

export function evaluateAudioFrame(
  frame: RealtimeAudioFrame,
  thresholds: RealtimeAudioRuleThresholds,
  state: RealtimeAIRuleState,
  nowMs = frame.timestamp
): RealtimeAIEventDecision[] {
  const decisions: RealtimeAIEventDecision[] = [
    {
      kind: 'audio_level_telemetry',
      rmsDb: frame.rmsDb,
      peakDb: frame.peakDb,
      vadActive: frame.vadActive,
      classification: frame.classification
    }
  ]

  if (
    frame.vadActive
    && frame.vadConfidence >= thresholds.vadConfidenceThreshold
    && nowMs - state.lastVoiceActivityEventAt >= thresholds.audioEventCooldownMs
  ) {
    decisions.push({
      kind: 'voice_activity',
      vadConfidence: frame.vadConfidence,
      rmsDb: frame.rmsDb
    })
    state.lastVoiceActivityEventAt = nowMs
  }

  const isNoiseAnomaly = frame.rmsDb >= thresholds.noiseRmsDbThreshold
    || (
      (frame.classification === 'music' || frame.classification === 'keyboard')
      && frame.classificationConfidence >= thresholds.audioClassificationConfidenceThreshold
    )

  if (
    isNoiseAnomaly
    && nowMs - state.lastAudioAnomalyEventAt >= thresholds.audioEventCooldownMs
  ) {
    decisions.push({
      kind: 'audio_anomaly',
      rmsDb: frame.rmsDb,
      classification: frame.classification,
      confidence: Math.max(frame.classificationConfidence, frame.vadConfidence)
    })
    state.lastAudioAnomalyEventAt = nowMs
  }

  if (
    frame.classification === 'whisper'
    && frame.classificationConfidence >= thresholds.audioClassificationConfidenceThreshold
    && nowMs - state.lastWhisperEventAt >= thresholds.audioEventCooldownMs
  ) {
    decisions.push({
      kind: 'whisper_detected',
      confidence: frame.classificationConfidence,
      rmsDb: frame.rmsDb
    })
    state.lastWhisperEventAt = nowMs
  }

  if (
    frame.speakerCount > 1
    && nowMs - state.lastSecondSpeakerEventAt >= thresholds.audioEventCooldownMs
  ) {
    decisions.push({
      kind: 'second_speaker_detected',
      speakerCount: frame.speakerCount,
      confidence: Math.max(frame.vadConfidence, frame.classificationConfidence)
    })
    state.lastSecondSpeakerEventAt = nowMs
  }

  return decisions
}
