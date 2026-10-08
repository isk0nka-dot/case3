// =============================================================================
// Argus AI SDK — Main Entry Point
// =============================================================================
//
// Plug-and-play exam proctoring SDK for third-party integrations.
//
// Usage:
//   const proctoring = new ArgusSDK({
//     sessionToken: 'eyJ...',
//     serverUrl: 'https://argusai.kz',
//   });
//   await proctoring.startPreflight();
//   await proctoring.startSession();
//   await proctoring.endSession();
//   proctoring.destroy();
// =============================================================================

import type {
  ArgusSDKConfig,
  SessionStatus,
  PreflightCheckResult,
  PreflightResult,
  PreflightStep,
  ViolationEvent,
  SDKError,
  HealthMetrics,
  ResilienceTier,
  TierConfig,
  VisionFrame,
  AudioFrame,
  EventPayload,
  DeliveryState,
  LiveKitPublishingState,
} from './types';

import { GrpcWebTransport } from './core/transport';
import { SessionManager } from './core/session';
import { EventEmitter } from './core/event-emitter';
import { HealthGovernor } from './health/governor';
import { VisionEngine } from './media/vision';
import { AudioEngine } from './media/audio';
import { LiveKitPublisher } from './media/livekit';
import { BrowserIntegrityMonitor } from './security/browser';
import { CameraWidget } from './ui/widget';
import { PreflightChecker } from './ui/preflight';
import { EventSource, EventType, Severity } from './types';

// Re-export public types and classes.
export {
  // Types
  type ArgusSDKConfig,
  type SessionStatus,
  type PreflightCheckResult,
  type PreflightResult,
  type PreflightStep,
  type ViolationEvent,
  type SDKError,
  type HealthMetrics,
  type ResilienceTier,
  type TierConfig,
  type VisionFrame,
  type AudioFrame,
  type EventPayload,
  type DeliveryState,
  type LiveKitPublishingState,
} from './types';

export { EventType, Severity, EventSource } from './types';
export { GrpcWebTransport } from './core/transport';
export { SessionManager } from './core/session';
export { EVENT_COLLECTOR_SERVICE_PATHS } from './core/service-paths';
export { HealthGovernor } from './health/governor';
export { VisionEngine } from './media/vision';
export { AudioEngine } from './media/audio';
export { LiveKitPublisher } from './media/livekit';
export { BrowserIntegrityMonitor } from './security/browser';
export { CameraWidget } from './ui/widget';
export { PreflightChecker } from './ui/preflight';

// ---------------------------------------------------------------------------
// SDK Events
// ---------------------------------------------------------------------------

interface SDKEvents {
  ready: void;
  statusChange: SessionStatus;
  violation: ViolationEvent;
  error: SDKError;
  preflightStep: PreflightStep;
  preflightCheck: PreflightCheckResult;
  preflightComplete: PreflightResult;
  deliveryUpdate: DeliveryState;
  liveKitStateChange: LiveKitPublishingState;
  healthUpdate: HealthMetrics;
  tierChange: { from: ResilienceTier; to: ResilienceTier; config: TierConfig };
}

// ---------------------------------------------------------------------------
// Main SDK Class
// ---------------------------------------------------------------------------

/**
 * ArgusSDK — plug-and-play exam proctoring.
 *
 * Orchestrates the full proctoring lifecycle:
 * 1. Preflight checks (camera, face detection, system)
 * 2. Session start (event streaming, heartbeat)
 * 3. AI monitoring (face detection, audio VAD, browser integrity)
 * 4. Health monitoring (FPS, RTT, adaptive tier switching)
 * 5. Session end (verdict, cleanup)
 *
 * @example
 * ```html
 * <script src="https://cdn.argusai.kz/sdk/v1/argus-sdk.umd.js"></script>
 * <script>
 *   const proctoring = new ArgusSDK({
 *     sessionToken: 'eyJ...',
 *     serverUrl: 'https://argusai.kz',
 *     onViolation: (e) => console.warn('Violation:', e),
 *   });
 *   await proctoring.startPreflight();
 *   await proctoring.startSession();
 * </script>
 * ```
 */
/**
 * Maps integrity events reported by the Argus browser extension (see
 * `argus-sdk/extension`) to proctoring EventTypes. The extension relays these
 * via `window.postMessage({ argusExt: { type, payload, severity } })`.
 */
const EXTENSION_EVENT_MAP: Record<string, { eventType: EventType; severity: Severity; label: string }> = {
  TAB_SWITCH: { eventType: EventType.TAB_SWITCH, severity: Severity.WARNING, label: 'Switched away from exam tab' },
  TAB_OPENED: { eventType: EventType.TAB_SWITCH, severity: Severity.WARNING, label: 'New tab opened during exam' },
  WINDOW_BLUR: { eventType: EventType.FOCUS_LOSS_DETECTED, severity: Severity.WARNING, label: 'Browser window lost focus' },
  NAVIGATION_AWAY: { eventType: EventType.TAB_SWITCH, severity: Severity.CRITICAL, label: 'Navigated away from exam origin' },
  EXTERNAL_DISPLAY_DETECTED: { eventType: EventType.EXTERNAL_DISPLAY_DETECTED, severity: Severity.CRITICAL, label: 'Second monitor detected' },
};

const EXTENSION_SEVERITY_MAP: Record<string, Severity> = {
  INFO: Severity.INFO,
  WARNING: Severity.WARNING,
  CRITICAL: Severity.CRITICAL,
};

export class ArgusSDK extends EventEmitter<SDKEvents> {
  private readonly _config: ArgusSDKConfig;

  // Core modules.
  private _transport: GrpcWebTransport | null = null;
  private _session: SessionManager | null = null;
  private _governor: HealthGovernor | null = null;
  private _vision: VisionEngine | null = null;
  private _audio: AudioEngine | null = null;
  private _liveKit: LiveKitPublisher | null = null;
  private _security: BrowserIntegrityMonitor | null = null;
  private _widget: CameraWidget | null = null;
  private _preflight: PreflightChecker | null = null;
  private _aiSnapshotTimer: ReturnType<typeof setInterval> | null = null;
  private _aiSnapshotInFlight = false;
  private _sessionStartedAtMs = 0;
  private _lastEvidenceSnapshotMs = 0;

  // State.
  private _status: SessionStatus = 'idle';
  private _videoStream: MediaStream | null = null;
  private _lastPreflightResult: PreflightResult | null = null;

  // Browser-extension bridge state.
  private _extensionPresent = false;
  private _extLastBeatMs = 0;
  private _extWatchdog: ReturnType<typeof setInterval> | null = null;
  private _extMessageHandler: ((e: MessageEvent) => void) | null = null;

  constructor(config: ArgusSDKConfig) {
    super();
    this._config = config;

    // Wire external callbacks.
    if (config.onReady) this.on('ready', config.onReady);
    if (config.onError) this.on('error', config.onError);
    if (config.onViolation) this.on('violation', config.onViolation);
    if (config.onStatusChange) this.on('statusChange', config.onStatusChange);
    if (config.onPreflightStep) this.on('preflightStep', config.onPreflightStep);
    if (config.onPreflightCheck) this.on('preflightCheck', config.onPreflightCheck);
    if (config.onPreflightComplete) this.on('preflightComplete', config.onPreflightComplete);
    if (config.onDeliveryUpdate) this.on('deliveryUpdate', config.onDeliveryUpdate);
    if (config.onLiveKitStateChange) this.on('liveKitStateChange', config.onLiveKitStateChange);

    // Listen for the Argus browser extension and probe for its presence early,
    // so a strict `requireExtension` gate can be evaluated before session start.
    this._setupExtensionBridge();
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /** Current session status. */
  get status(): SessionStatus { return this._status; }

  /** Health metrics (null before session start). */
  get health(): HealthMetrics | null { return this._governor?.getMetrics() ?? null; }

  /** Current vision frame (null before session start). */
  get visionFrame(): VisionFrame | null { return this._vision?.currentFrame ?? null; }

  /** Current audio frame (null before session start). */
  get audioFrame(): AudioFrame | null { return this._audio?.currentFrame ?? null; }

  /** Violation count (0 before session start). */
  get violationCount(): number { return this._session?.violationCount ?? 0; }

  /** Buffered event queue depth. */
  get eventQueueDepth(): number { return this._session?.queueDepth ?? 0; }

  /** Number of events dropped due to bounded queue pressure. */
  get droppedEventCount(): number { return this._session?.droppedEventCount ?? 0; }

  /** Current LiveKit publishing state. */
  get liveKitState(): LiveKitPublishingState | null { return this._liveKit?.currentState ?? null; }

  /** Last structured preflight result (null before startPreflight()). */
  get lastPreflightResult(): PreflightResult | null { return this._lastPreflightResult; }

  /** Whether the Argus browser extension has announced itself. */
  get extensionPresent(): boolean { return this._extensionPresent; }

  /**
   * Run preflight checks.
   *
   * Must be called before startSession(). Checks:
   * 1. Camera + microphone access
   * 2. Face detection (single person, centered)
   * 3. System compatibility
   *
   * @returns true if all checks passed.
   */
  async startPreflight(): Promise<boolean> {
    this._setStatus('preflight');

    this._ensureTransportAndSession();
    const consentAccepted = this._config.preflight?.consentAccepted ?? false;
    if (consentAccepted) {
      void this._sendPreflightLifecycleEvent('PREFLIGHT_STARTED', 'Preflight started');
    }

    this._preflight = new PreflightChecker({
      locale: this._config.locale,
      consentAccepted,
      serverUrl: this._config.serverUrl,
      networkCheckUrl: this._config.preflight?.networkCheckUrl,
      networkTimeoutMs: this._config.preflight?.networkTimeoutMs,
      requireScreenCapture: this._config.preflight?.requireScreenCapture,
      mediapipeBasePath: this._config.preflight?.mediapipeBasePath ?? this._config.mediapipeBasePath,
      mediapipeModelAssetPath: this._config.preflight?.mediapipeModelAssetPath,
      allowFaceCheckFallback: this._config.preflight?.allowFaceCheckFallback,
      requireLivenessChallenge: this._config.preflight?.requireLivenessChallenge,
      requireDesktopAgent: this._config.preflight?.requireDesktopAgent,
      desktopAgentPort: this._config.preflight?.desktopAgentPort,
    });

    this._preflight.on('stepUpdate', (step) => {
      this.emit('preflightStep', step);
    });
    this._preflight.on('checkUpdate', (check) => {
      this.emit('preflightCheck', check);
    });
    this._preflight.on('complete', (result) => {
      this._lastPreflightResult = result;
      this.emit('preflightComplete', result);
    });

    const result = await this._preflight.runDetailed();
    const passed = result.allPassed;
    this._lastPreflightResult = result;
    if (consentAccepted) {
      await this._sendPreflightLifecycleEvent(
        passed ? 'PREFLIGHT_PASSED' : 'PREFLIGHT_FAILED',
        passed ? 'Preflight passed' : 'Preflight failed',
        result,
      );
    }

    if (passed) {
      this._videoStream = this._preflight.videoStream;
      this._setStatus('ready');
    } else {
      this._setStatus('error');
      this.emit('error', {
        code: 'PREFLIGHT_FAILED',
        message: this._formatPreflightFailure(result),
        recoverable: true,
      });
    }

    return passed;
  }

  /**
   * Start the proctoring session.
   *
   * Begins event streaming, AI monitoring, and health tracking.
   * Preflight must pass before calling this.
   */
  async startSession(): Promise<void> {
    if (this._status !== 'ready') {
      throw new Error('Cannot start session: preflight not completed. Call startPreflight() first.');
    }

    // Strict mode: the browser extension must be installed and active.
    if (this._config.preflight?.requireExtension && !this._extensionPresent) {
      this._setStatus('error');
      const message = 'Argus proctoring extension is required but was not detected. Please install/enable it and retry.';
      this.emit('error', { code: 'EXTENSION_REQUIRED', message, recoverable: false });
      throw new Error(message);
    }

    this._setStatus('initializing');

    try {
      this._ensureTransportAndSession();

      // Initialize camera widget.
      this._widget = new CameraWidget({
        containerId: this._config.containerId,
        locale: this._config.locale,
      });

      if (this._videoStream) {
        this._widget.setStream(this._videoStream);
      }

      if (this._config.liveKit?.enabled) {
        await this._startLiveKitPublishing();
      }

      // Initialize health governor.
      this._governor = new HealthGovernor({
        serverUrl: this._config.serverUrl,
        sampleIntervalMs: 5000,
      });

      this._governor.setTransportMetrics(() => {
        const h = this._transport!.getHealth();
        return { totalRequests: h.totalRequests, totalFailures: h.totalFailures };
      });

      this._governor.on('tierChange', (change) => {
        this.emit('tierChange', change);
        // Adjust vision engine inference rate based on tier.
        if (this._vision) {
          this._vision.setInferenceInterval(change.config.ai.intervalMs || 500);
        }
      });

      this._governor.on('healthUpdate', (metrics) => {
        this.emit('healthUpdate', metrics);
        if (this._session) {
          this._session.updateFocusScore(metrics.score);
        }
      });

      // Initialize vision engine.
      this._vision = new VisionEngine({
        mediapipeBasePath: this._config.mediapipeBasePath,
        inferenceIntervalMs: 100,
      });

      this._vision.on('frame', (frame) => {
        if (!frame.faceDetected && this._session) {
          this._session.sendEvent(
            3, // FACE_NOT_DETECTED
            2, // WARNING
            1, // WEBCAM
            'Face not detected',
            0.9,
          );
        } else if (frame.faceCount > 1 && this._session) {
          this._session.sendEvent(
            5, // MULTIPLE_PERSONS
            2, // WARNING
            1, // WEBCAM
            `Multiple persons detected (${frame.faceCount})`,
            0.9,
          );
        }
      });

      // Initialize audio engine.
      this._audio = new AudioEngine({ analysisIntervalMs: 100 });

      this._audio.on('voiceActivity', (va) => {
        if (va.detected && this._session) {
          this._session.sendEvent(
            20, // VOICE_ACTIVITY
            1,  // INFO
            1,  // WEBCAM
            'Voice activity detected',
            0.8,
            { type: 'audio', data: { speakerCount: va.speakerCount } },
          );
        }
      });

      // Initialize browser security.
      this._security = new BrowserIntegrityMonitor();
      this._security.on('violation', (v) => {
        if (this._session) {
          this._session.sendEvent(v.eventType, v.severity, v.source, v.label, v.confidence);
        }
        // Capture a webcam snapshot as visual evidence of the violation
        // (throttled internally). Works even when server-side video recording
        // (LiveKit egress) is unavailable.
        void this._captureAndSendEvidenceSnapshot(v.label || String(v.eventType));
      });

      // Start everything.
      this._session!.start();
      this._sessionStartedAtMs = Date.now();
      this._governor.start();
      this._security.start();

      // Start vision (needs video element from widget).
      await this._vision.start(this._widget.videoElement);

      // Start audio.
      if (this._preflight?.audioStream) {
        await this._audio.start(this._preflight.audioStream);
      }

      this._startAISnapshotSampling();

      this._widget.setStatus('active');
      this._setStatus('active');
      this.emit('ready', undefined as unknown as void);

      // Notify browser extension (if installed) to start monitoring this tab.
      this._notifyExtensionStart();
      this._startExtensionWatchdog();
    } catch (err) {
      this._setStatus('error');
      this.emit('error', {
        code: 'SESSION_START_FAILED',
        message: err instanceof Error ? err.message : 'Failed to start session',
        recoverable: false,
      });
      throw err;
    }
  }

  /**
   * End the proctoring session.
   *
   * Flushes remaining events, stops monitoring, and cleans up resources.
   */
  async endSession(): Promise<void> {
    if (this._status !== 'active' && this._status !== 'paused') return;

    // Stop monitoring.
    this._stopAISnapshotSampling();
    this._vision?.stop();
    this._audio?.stop();
    this._liveKit?.stop();
    this._security?.stop();
    this._governor?.stop();

    // Stop session (flushes events).
    if (this._session) {
      await this._session.stop();
    }

    this._widget?.setStatus('completed');
    this._setStatus('completed');
    this._notifyExtensionStop();
  }

  /**
   * Destroy the SDK instance — releases all resources.
   *
   * Must be called when the partner page navigates away or unmounts.
   */
  destroy(): void {
    // Stop all modules.
    this._vision?.destroy();
    this._stopAISnapshotSampling();
    this._audio?.destroy();
    this._liveKit?.destroy();
    this._security?.destroy();
    this._governor?.destroy();
    this._session?.destroy();
    this._widget?.destroy();
    this._transport?.destroy();
    this._preflight?.destroy();

    // Tear down the extension bridge.
    this._stopExtensionWatchdog();
    if (this._extMessageHandler && typeof window !== 'undefined') {
      window.removeEventListener('message', this._extMessageHandler);
      this._extMessageHandler = null;
    }

    // Stop video stream.
    if (this._videoStream) {
      this._videoStream.getTracks().forEach(t => t.stop());
      this._videoStream = null;
    }

    this.removeAllListeners();
    this._setStatus('idle');
  }

  // ---------------------------------------------------------------------------
  // Private
  // ---------------------------------------------------------------------------

  private _setStatus(status: SessionStatus): void {
    if (this._status === status) return;
    this._status = status;
    this.emit('statusChange', status);
    this._widget?.setStatus(status);
  }

  private async _startLiveKitPublishing(): Promise<void> {
    if (!this._videoStream || !this._session) return;

    try {
      this._liveKit = new LiveKitPublisher({
        serverUrl: this._config.serverUrl,
        sessionId: this._session.sessionId,
        sessionToken: this._config.sessionToken,
        config: this._config.liveKit,
      });
      const state = await this._liveKit.start(this._videoStream);
      this.emit('liveKitStateChange', state);
    } catch (err) {
      const error: SDKError = {
        code: 'LIVEKIT_PUBLISH_FAILED',
        message: err instanceof Error ? err.message : 'Failed to publish LiveKit media',
        recoverable: !this._config.liveKit?.required,
      };
      this.emit('error', error);
      if (this._config.liveKit?.required) {
        throw err;
      }
    }
  }

  private _startAISnapshotSampling(): void {
    const cfg = this._config.aiSnapshots;
    if (cfg?.enabled === false || !this._session || !this._widget?.videoElement) return;

    const intervalMs = Math.max(5000, cfg?.intervalMs ?? 15000);
    this._captureAndSendAIFrame().catch(() => {/* best-effort */});
    this._aiSnapshotTimer = setInterval(() => {
      this._captureAndSendAIFrame().catch(() => {/* best-effort */});
    }, intervalMs);
  }

  private _stopAISnapshotSampling(): void {
    if (this._aiSnapshotTimer) {
      clearInterval(this._aiSnapshotTimer);
      this._aiSnapshotTimer = null;
    }
    this._aiSnapshotInFlight = false;
  }

  private async _captureAndSendAIFrame(): Promise<void> {
    if (this._aiSnapshotInFlight || !this._session || !this._widget?.videoElement) return;
    const video = this._widget.videoElement;
    if (video.readyState < HTMLMediaElement.HAVE_CURRENT_DATA || video.videoWidth <= 0 || video.videoHeight <= 0) {
      return;
    }

    this._aiSnapshotInFlight = true;
    try {
      const cfg = this._config.aiSnapshots;
      const maxWidth = Math.max(160, cfg?.maxWidth ?? 640);
      const scale = Math.min(1, maxWidth / video.videoWidth);
      const width = Math.max(1, Math.round(video.videoWidth * scale));
      const height = Math.max(1, Math.round(video.videoHeight * scale));

      const canvas = document.createElement('canvas');
      canvas.width = width;
      canvas.height = height;
      const ctx = canvas.getContext('2d');
      if (!ctx) return;
      ctx.drawImage(video, 0, 0, width, height);

      const quality = Math.max(0.4, Math.min(0.92, cfg?.quality ?? 0.72));
      const blob = await new Promise<Blob | null>((resolve) => canvas.toBlob(resolve, 'image/jpeg', quality));
      if (!blob) return;

      const form = new FormData();
      form.append('frame', blob, `argus-${this._session.sessionId}-${Date.now()}.jpg`);
      form.append('contentType', 'image/jpeg');
      const ts = this._sessionStartedAtMs > 0 ? (Date.now() - this._sessionStartedAtMs) / 1000 : 0;
      form.append('videoTimestampSec', ts.toFixed(3));

      const base = this._config.serverUrl.replace(/\/+$/, '');
      const response = await fetch(`${base}/api/v1/external/sessions/${encodeURIComponent(this._session.sessionId)}/ai-frame`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${this._config.sessionToken}`,
        },
        body: form,
        keepalive: false,
      });

      if (!response.ok && response.status >= 500) {
        this.emit('error', {
          code: 'AI_FRAME_UPLOAD_FAILED',
          message: `Backend AI frame upload failed: HTTP ${response.status}`,
          recoverable: true,
        });
      }
    } catch (err) {
      this.emit('error', {
        code: 'AI_FRAME_UPLOAD_FAILED',
        message: err instanceof Error ? err.message : 'Backend AI frame upload failed',
        recoverable: true,
      });
    } finally {
      this._aiSnapshotInFlight = false;
    }
  }

  // Capture a single webcam frame at the moment of a violation and upload it as
  // an evidence fragment (stored in MinIO + forensic ledger by the backend).
  // Throttled to at most one snapshot per 3s so rapid-fire violations do not
  // flood the backend. Best-effort: failures are swallowed.
  private async _captureAndSendEvidenceSnapshot(eventType: string): Promise<void> {
    if (!this._session || !this._widget?.videoElement) return;
    const now = Date.now();
    if (now - this._lastEvidenceSnapshotMs < 3000) return;
    const video = this._widget.videoElement;
    if (video.readyState < HTMLMediaElement.HAVE_CURRENT_DATA || video.videoWidth <= 0 || video.videoHeight <= 0) {
      return;
    }
    this._lastEvidenceSnapshotMs = now;

    try {
      const maxWidth = 640;
      const scale = Math.min(1, maxWidth / video.videoWidth);
      const width = Math.max(1, Math.round(video.videoWidth * scale));
      const height = Math.max(1, Math.round(video.videoHeight * scale));

      const canvas = document.createElement('canvas');
      canvas.width = width;
      canvas.height = height;
      const ctx = canvas.getContext('2d');
      if (!ctx) return;
      ctx.drawImage(video, 0, 0, width, height);

      const blob = await new Promise<Blob | null>((resolve) => canvas.toBlob(resolve, 'image/jpeg', 0.75));
      if (!blob) return;

      const form = new FormData();
      form.append('frame', blob, `evidence-${this._session.sessionId}-${now}.jpg`);
      form.append('contentType', 'image/jpeg');
      form.append('eventType', eventType.slice(0, 120));

      const base = this._config.serverUrl.replace(/\/+$/, '');
      await fetch(`${base}/api/v1/external/sessions/${encodeURIComponent(this._session.sessionId)}/evidence-snapshot`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${this._config.sessionToken}`,
        },
        body: form,
        keepalive: false,
      });
    } catch {
      // Best-effort — evidence snapshot upload failures are non-fatal.
    }
  }

  private _ensureTransportAndSession(): void {
    if (!this._transport) {
      this._transport = new GrpcWebTransport({
        baseUrl: this._config.serverUrl,
        timeout: 10_000,
        maxRetries: 3,
      });
    }

    if (!this._session) {
      this._session = new SessionManager(this._transport, this._config.sessionToken);
      this._session.on('violation', (v) => this.emit('violation', v));
      this._session.on('error', (e) => this.emit('error', e));
      this._session.on('statusChange', (s) => this._setStatus(s));
      this._session.on('delivery', (state) => this.emit('deliveryUpdate', state));
    }
  }

  private async _sendPreflightLifecycleEvent(
    code: 'PREFLIGHT_STARTED' | 'PREFLIGHT_PASSED' | 'PREFLIGHT_FAILED',
    label: string,
    result?: PreflightResult,
  ): Promise<void> {
    if (!this._session) return;

    const payload: EventPayload = {
      type: 'system',
      data: {
        category: code.toLowerCase(),
        terminated: false,
      },
    };

    this._session.sendLifecycleEvent(
      EventType.FOCUS_SCORE_UPDATE,
      code === 'PREFLIGHT_FAILED' ? Severity.WARNING : Severity.INFO,
      EventSource.SYSTEM,
      label,
      result?.allPassed === false ? 0.99 : 1,
      payload,
    );

    if (result) {
      this._session.sendLifecycleEvent(
        EventType.FOCUS_SCORE_UPDATE,
        Severity.INFO,
        EventSource.SYSTEM,
        `Preflight summary: ${result.summary.passed} passed, ${result.summary.requiredFailed} blocking failures`,
        1,
        {
          type: 'system',
          data: {
            category: 'preflight_summary',
            terminated: false,
          },
        },
      );
    }

    await this._session.flushNow();
  }

  /** Notify Argus browser extension (if installed) that a session has started. */
  private _notifyExtensionStart(): void {
    try {
      if (typeof window !== 'undefined') {
        window.postMessage({
          argus: {
            type: 'SESSION_START',
            payload: {
              sessionToken: this._config.sessionToken,
              serverUrl: this._config.serverUrl,
            },
          },
        }, '*');
      }
    } catch { /* Extension may not be installed — non-fatal */ }
  }

  /** Notify Argus browser extension that the session has ended. */
  private _notifyExtensionStop(): void {
    this._stopExtensionWatchdog();
    try {
      if (typeof window !== 'undefined') {
        window.postMessage({ argus: { type: 'SESSION_END' } }, '*');
      }
    } catch { /* non-fatal */ }
  }

  // ---------------------------------------------------------------------------
  // Browser-extension bridge
  // ---------------------------------------------------------------------------

  /** Start listening for the extension and probe for its presence. */
  private _setupExtensionBridge(): void {
    if (typeof window === 'undefined') return;

    const handler = (event: MessageEvent): void => {
      if (event.source !== window) return;
      const data = event.data as { argusExt?: { type?: string; payload?: Record<string, unknown>; severity?: string } } | null;
      const msg = data?.argusExt;
      if (!msg || typeof msg.type !== 'string') return;
      this._handleExtensionMessage(msg.type, msg.payload ?? {}, msg.severity ?? 'WARNING');
    };
    this._extMessageHandler = handler;
    window.addEventListener('message', handler);

    // Ask any installed extension to announce itself before the session starts.
    try {
      window.postMessage({ argus: { type: 'PING' } }, '*');
    } catch { /* non-fatal */ }
  }

  /** Handle a single message relayed by the extension. */
  private _handleExtensionMessage(type: string, payload: Record<string, unknown>, severityStr: string): void {
    if (type === 'EXTENSION_PRESENT') {
      this._extensionPresent = true;
      this._extLastBeatMs = Date.now();
      return;
    }
    if (type === 'EXTENSION_HEARTBEAT') {
      this._extLastBeatMs = Date.now();
      return;
    }

    // Ingest integrity events only while a session is active.
    const mapped = EXTENSION_EVENT_MAP[type];
    if (!mapped || !this._session) return;

    const severity = EXTENSION_SEVERITY_MAP[severityStr.toUpperCase()] ?? mapped.severity;
    const confidence = typeof payload.confidence === 'number' ? payload.confidence : 0.95;
    this._session.sendEvent(
      mapped.eventType,
      severity,
      EventSource.SYSTEM,
      mapped.label,
      confidence,
      { type: 'system', data: { source: 'extension', extType: type, ...payload } },
    );
  }

  /**
   * In strict mode, watch the extension heartbeat. A gap longer than 10s while
   * the exam is running is treated as the extension being disabled/removed
   * (a tamper signal) and raised as a CRITICAL event.
   */
  private _startExtensionWatchdog(): void {
    if (!this._config.preflight?.requireExtension) return;
    this._stopExtensionWatchdog();
    this._extWatchdog = setInterval(() => {
      if (!this._session || this._extLastBeatMs === 0) return;
      if (Date.now() - this._extLastBeatMs > 10000) {
        this._session.sendLifecycleEvent(
          EventType.TAB_SWITCH,
          Severity.CRITICAL,
          EventSource.SYSTEM,
          'Argus extension stopped responding (possible tamper)',
          0.99,
          { type: 'system', data: { category: 'extension_tamper' } },
        );
        // Reset so the alert fires again only after another sustained gap.
        this._extLastBeatMs = Date.now();
      }
    }, 5000);
  }

  private _stopExtensionWatchdog(): void {
    if (this._extWatchdog) {
      clearInterval(this._extWatchdog);
      this._extWatchdog = null;
    }
  }

  private _formatPreflightFailure(result: PreflightResult): string {
    const failures = result.checks
      .filter(check => check.required && check.status !== 'passed')
      .map(check => check.message);
    if (failures.length === 0) return 'One or more preflight checks failed';
    return failures.join('; ');
  }
}

// UMD global export.
if (typeof window !== 'undefined') {
  (window as unknown as Record<string, unknown>).ArgusSDK = ArgusSDK;
}

// Default export for ESM.
export default ArgusSDK;
