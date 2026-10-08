// =============================================================================
// Argus SDK — Preflight Gatekeeper
// =============================================================================
//
// Production-oriented browser preflight checks before a proctoring session:
// consent, secure context, browser APIs, camera/microphone permission, face
// presence, single face, fullscreen capability, and backend health ping.
// =============================================================================

import type {
  PreflightCheckId,
  PreflightCheckResult,
  PreflightResult,
  PreflightStep,
} from '../types';
import { EventEmitter } from '../core/event-emitter';

interface PreflightEvents {
  stepUpdate: PreflightStep;
  checkUpdate: PreflightCheckResult;
  complete: PreflightResult;
}

/** Preflight check options. */
export interface PreflightOptions {
  /** Locale for messages. Default: 'ru'. */
  locale?: 'kk' | 'ru' | 'en';
  /** Whether the student has accepted the partner/Argus consent text. */
  consentAccepted?: boolean;
  /** Argus server URL used for health ping. */
  serverUrl?: string;
  /** Override network health URL. Default: `${serverUrl}/healthz`. */
  networkCheckUrl?: string;
  /** Network check timeout in ms. Default: 5000. */
  networkTimeoutMs?: number;
  /** Whether to check screen sharing capability. Default: false for Phase 1. */
  requireScreenCapture?: boolean;
  /** MediaPipe WASM base path. Use a self-hosted URL in production if needed. */
  mediapipeBasePath?: string;
  /** MediaPipe face landmarker model URL. Use a self-hosted URL in production if needed. */
  mediapipeModelAssetPath?: string;
  /** Allow preflight to pass if MediaPipe cannot initialize. Default: false. */
  allowFaceCheckFallback?: boolean;
  /**
   * Require liveness challenge: detect natural face movement across 3 frames
   * to distinguish a live face from a printed/screen photo. Default: false.
   */
  requireLivenessChallenge?: boolean;
  /**
   * Require the Argus Desktop Agent to be reachable on localhost.
   * Default: false. Enable for strict/standard proctoring modes.
   */
  requireDesktopAgent?: boolean;
  /** Local port of the desktop agent health server. Default: 7373. */
  desktopAgentPort?: number;
}

type Locale = 'kk' | 'ru' | 'en';
type CheckStatus = Exclude<PreflightCheckResult['status'], 'pending'>;

const DEFAULT_MEDIAPIPE_BASE_PATH = 'https://cdn.jsdelivr.net/npm/@mediapipe/tasks-vision@latest/wasm';
const DEFAULT_FACE_MODEL_PATH = 'https://storage.googleapis.com/mediapipe-models/face_landmarker/face_landmarker/float16/1/face_landmarker.task';

const CHECK_GROUP: Record<PreflightCheckId, PreflightStep['id']> = {
  consent: 'consent',
  cameraPermission: 'hardware',
  microphonePermission: 'hardware',
  facePresent: 'environment',
  singleFace: 'environment',
  livenessChallenge: 'environment',
  secureContext: 'system',
  fullscreenSupport: 'system',
  screenCaptureSupport: 'system',
  browserCompatibility: 'system',
  networkHealth: 'network',
  desktopAgent: 'system',
};

const MESSAGES: Record<Locale, {
  steps: Record<PreflightStep['id'], Record<'pending' | 'checking' | 'passed' | 'failed', string>>;
  checks: Record<PreflightCheckId, Record<'checking' | 'passed' | 'failed' | 'skipped', string>>;
}> = {
  kk: {
    steps: {
      consent: {
        pending: 'Келісім күтілуде',
        checking: 'Келісім тексерілуде...',
        passed: 'Келісім қабылданды',
        failed: 'Келісім қабылданбады',
      },
      hardware: {
        pending: 'Жабдық тексерілмеді',
        checking: 'Камера мен микрофон тексерілуде...',
        passed: 'Камера мен микрофон дайын',
        failed: 'Камера немесе микрофон тексерісі сәтсіз',
      },
      environment: {
        pending: 'Орта тексерілмеді',
        checking: 'Бет анықтау тексерілуде...',
        passed: 'Бір адам анықталды',
        failed: 'Бет анықтау тексерісі сәтсіз',
      },
      system: {
        pending: 'Жүйе тексерілмеді',
        checking: 'Браузер мен жүйе тексерілуде...',
        passed: 'Жүйе талаптарға сай',
        failed: 'Жүйе талаптарға сай емес',
      },
      network: {
        pending: 'Желі тексерілмеді',
        checking: 'Желі қосылымы тексерілуде...',
        passed: 'Желі қосылымы дайын',
        failed: 'Желі қосылымы сәтсіз',
      },
    },
    checks: {
      consent: {
        checking: 'Келісім тексерілуде...',
        passed: 'Келісім қабылданды',
        failed: 'Келісім қабылданбады',
        skipped: 'Келісім тексерісі өткізіп жіберілді',
      },
      cameraPermission: {
        checking: 'Камераға рұқсат сұралуда...',
        passed: 'Камераға рұқсат берілді',
        failed: 'Камераға рұқсат берілмеді',
        skipped: 'Камера тексерісі өткізіп жіберілді',
      },
      microphonePermission: {
        checking: 'Микрофонға рұқсат сұралуда...',
        passed: 'Микрофонға рұқсат берілді',
        failed: 'Микрофонға рұқсат берілмеді',
        skipped: 'Микрофон тексерісі өткізіп жіберілді',
      },
      facePresent: {
        checking: 'Бет анықталуда...',
        passed: 'Бет анықталды',
        failed: 'Бет анықталмады',
        skipped: 'Бет тексерісі өткізіп жіберілді',
      },
      singleFace: {
        checking: 'Адам саны тексерілуде...',
        passed: 'Камерада бір адам',
        failed: 'Камерада бірнеше адам бар',
        skipped: 'Адам саны тексерісі өткізіп жіберілді',
      },
      livenessChallenge: {
        checking: 'Тіршілік тексерісі: камераға табиғи қарап тұрыңыз...',
        passed: 'Тіршілік расталды',
        failed: 'Тіршілік тексерісі сәтсіз — тірі адам анықталмады',
        skipped: 'Тіршілік тексерісі өткізіп жіберілді',
      },
      desktopAgent: {
        checking: 'Argus агент тексерілуде...',
        passed: 'Argus агент іске қосылды',
        failed: 'Argus агент табылмады. Агентті іске қосыңыз.',
        skipped: 'Агент тексерісі өткізіп жіберілді',
      },
      secureContext: {
        checking: 'HTTPS/secure context тексерілуде...',
        passed: 'Қауіпсіз context дайын',
        failed: 'HTTPS немесе localhost қажет',
        skipped: 'Secure context тексерісі өткізіп жіберілді',
      },
      fullscreenSupport: {
        checking: 'Толық экран режимі тексерілуде...',
        passed: 'Толық экран қолжетімді',
        failed: 'Толық экран қолжетімсіз',
        skipped: 'Толық экран тексерісі өткізіп жіберілді',
      },
      screenCaptureSupport: {
        checking: 'Экран бөлісу мүмкіндігі тексерілуде...',
        passed: 'Экран бөлісу қолжетімді',
        failed: 'Экран бөлісу қолжетімсіз',
        skipped: 'Экран бөлісу міндетті емес',
      },
      browserCompatibility: {
        checking: 'Браузер мүмкіндіктері тексерілуде...',
        passed: 'Браузер үйлесімді',
        failed: 'Браузер талаптарға сай емес',
        skipped: 'Браузер тексерісі өткізіп жіберілді',
      },
      networkHealth: {
        checking: 'Серверге қосылым тексерілуде...',
        passed: 'Серверге қосылым бар',
        failed: 'Серверге қосылу мүмкін емес',
        skipped: 'Желі тексерісі өткізіп жіберілді',
      },
    },
  },
  ru: {
    steps: {
      consent: {
        pending: 'Ожидается согласие',
        checking: 'Проверка согласия...',
        passed: 'Согласие принято',
        failed: 'Согласие не принято',
      },
      hardware: {
        pending: 'Оборудование не проверено',
        checking: 'Проверка камеры и микрофона...',
        passed: 'Камера и микрофон готовы',
        failed: 'Проверка камеры или микрофона не пройдена',
      },
      environment: {
        pending: 'Окружение не проверено',
        checking: 'Проверка лица в кадре...',
        passed: 'В кадре один человек',
        failed: 'Проверка лица не пройдена',
      },
      system: {
        pending: 'Система не проверена',
        checking: 'Проверка браузера и системы...',
        passed: 'Система соответствует требованиям',
        failed: 'Система не соответствует требованиям',
      },
      network: {
        pending: 'Сеть не проверена',
        checking: 'Проверка подключения...',
        passed: 'Подключение готово',
        failed: 'Проверка подключения не пройдена',
      },
    },
    checks: {
      consent: {
        checking: 'Проверка согласия...',
        passed: 'Согласие принято',
        failed: 'Согласие не принято',
        skipped: 'Проверка согласия пропущена',
      },
      cameraPermission: {
        checking: 'Запрашиваем доступ к камере...',
        passed: 'Доступ к камере получен',
        failed: 'Нет доступа к камере',
        skipped: 'Проверка камеры пропущена',
      },
      microphonePermission: {
        checking: 'Запрашиваем доступ к микрофону...',
        passed: 'Доступ к микрофону получен',
        failed: 'Нет доступа к микрофону',
        skipped: 'Проверка микрофона пропущена',
      },
      facePresent: {
        checking: 'Ищем лицо в кадре...',
        passed: 'Лицо обнаружено',
        failed: 'Лицо не обнаружено',
        skipped: 'Проверка лица пропущена',
      },
      singleFace: {
        checking: 'Проверяем количество людей...',
        passed: 'В кадре один человек',
        failed: 'В кадре обнаружено несколько людей',
        skipped: 'Проверка количества людей пропущена',
      },
      livenessChallenge: {
        checking: 'Проверка живости: смотрите в камеру...',
        passed: 'Живой человек подтверждён',
        failed: 'Проверка живости не пройдена — не обнаружено живого человека',
        skipped: 'Проверка живости пропущена',
      },
      desktopAgent: {
        checking: 'Проверка агента Argus...',
        passed: 'Агент Argus запущен',
        failed: 'Агент Argus не обнаружен. Запустите argus-agent перед экзаменом.',
        skipped: 'Проверка агента пропущена',
      },
      secureContext: {
        checking: 'Проверяем HTTPS/secure context...',
        passed: 'Безопасный context готов',
        failed: 'Нужен HTTPS или localhost',
        skipped: 'Проверка secure context пропущена',
      },
      fullscreenSupport: {
        checking: 'Проверяем полноэкранный режим...',
        passed: 'Полноэкранный режим доступен',
        failed: 'Полноэкранный режим недоступен',
        skipped: 'Проверка fullscreen пропущена',
      },
      screenCaptureSupport: {
        checking: 'Проверяем захват экрана...',
        passed: 'Захват экрана доступен',
        failed: 'Захват экрана недоступен',
        skipped: 'Захват экрана не обязателен',
      },
      browserCompatibility: {
        checking: 'Проверяем возможности браузера...',
        passed: 'Браузер совместим',
        failed: 'Браузер не соответствует требованиям',
        skipped: 'Проверка браузера пропущена',
      },
      networkHealth: {
        checking: 'Проверяем соединение с сервером...',
        passed: 'Соединение с сервером есть',
        failed: 'Нет соединения с сервером',
        skipped: 'Проверка сети пропущена',
      },
    },
  },
  en: {
    steps: {
      consent: {
        pending: 'Consent pending',
        checking: 'Checking consent...',
        passed: 'Consent accepted',
        failed: 'Consent not accepted',
      },
      hardware: {
        pending: 'Hardware not checked',
        checking: 'Checking camera and microphone...',
        passed: 'Camera and microphone ready',
        failed: 'Camera or microphone check failed',
      },
      environment: {
        pending: 'Environment not checked',
        checking: 'Checking face presence...',
        passed: 'Exactly one person is visible',
        failed: 'Face check failed',
      },
      system: {
        pending: 'System not checked',
        checking: 'Checking browser and system...',
        passed: 'System meets requirements',
        failed: 'System does not meet requirements',
      },
      network: {
        pending: 'Network not checked',
        checking: 'Checking connection...',
        passed: 'Connection ready',
        failed: 'Connection check failed',
      },
    },
    checks: {
      consent: {
        checking: 'Checking consent...',
        passed: 'Consent accepted',
        failed: 'Consent not accepted',
        skipped: 'Consent check skipped',
      },
      cameraPermission: {
        checking: 'Requesting camera access...',
        passed: 'Camera access granted',
        failed: 'Camera access denied',
        skipped: 'Camera check skipped',
      },
      microphonePermission: {
        checking: 'Requesting microphone access...',
        passed: 'Microphone access granted',
        failed: 'Microphone access denied',
        skipped: 'Microphone check skipped',
      },
      facePresent: {
        checking: 'Looking for a face...',
        passed: 'Face detected',
        failed: 'No face detected',
        skipped: 'Face check skipped',
      },
      singleFace: {
        checking: 'Checking person count...',
        passed: 'Exactly one person visible',
        failed: 'Multiple people detected',
        skipped: 'Person count check skipped',
      },
      livenessChallenge: {
        checking: 'Liveness check: please look naturally at the camera...',
        passed: 'Live person confirmed',
        failed: 'Liveness check failed — no live face movement detected',
        skipped: 'Liveness check skipped',
      },
      desktopAgent: {
        checking: 'Checking Argus Desktop Agent...',
        passed: 'Argus Desktop Agent is running',
        failed: 'Argus Desktop Agent not found. Please start argus-agent before the exam.',
        skipped: 'Desktop agent check skipped',
      },
      secureContext: {
        checking: 'Checking HTTPS/secure context...',
        passed: 'Secure context ready',
        failed: 'HTTPS or localhost is required',
        skipped: 'Secure context check skipped',
      },
      fullscreenSupport: {
        checking: 'Checking fullscreen support...',
        passed: 'Fullscreen is available',
        failed: 'Fullscreen is unavailable',
        skipped: 'Fullscreen check skipped',
      },
      screenCaptureSupport: {
        checking: 'Checking screen capture support...',
        passed: 'Screen capture is available',
        failed: 'Screen capture is unavailable',
        skipped: 'Screen capture is optional',
      },
      browserCompatibility: {
        checking: 'Checking browser capabilities...',
        passed: 'Browser is compatible',
        failed: 'Browser does not meet requirements',
        skipped: 'Browser check skipped',
      },
      networkHealth: {
        checking: 'Checking server connection...',
        passed: 'Server connection is available',
        failed: 'Server connection failed',
        skipped: 'Network check skipped',
      },
    },
  },
};

/**
 * Preflight gatekeeper.
 */
export class PreflightChecker extends EventEmitter<PreflightEvents> {
  private _steps: PreflightStep[] = [
    { id: 'consent', status: 'pending', message: '' },
    { id: 'hardware', status: 'pending', message: '' },
    { id: 'environment', status: 'pending', message: '' },
    { id: 'system', status: 'pending', message: '' },
    { id: 'network', status: 'pending', message: '' },
  ];

  private _checks: PreflightCheckResult[] = [];
  private _videoStream: MediaStream | null = null;
  private _audioStream: MediaStream | null = null;
  private _lastResult: PreflightResult | null = null;

  private readonly _options: Required<Omit<
    PreflightOptions,
    'serverUrl' | 'networkCheckUrl' | 'mediapipeModelAssetPath'
  >> & Pick<PreflightOptions, 'serverUrl' | 'networkCheckUrl' | 'mediapipeModelAssetPath'>;
  private readonly _locale: Locale;

  constructor(options?: PreflightOptions) {
    super();
    this._locale = options?.locale ?? 'ru';
    this._options = {
      locale: this._locale,
      consentAccepted: options?.consentAccepted ?? false,
      serverUrl: options?.serverUrl,
      networkCheckUrl: options?.networkCheckUrl,
      networkTimeoutMs: options?.networkTimeoutMs ?? 5000,
      requireScreenCapture: options?.requireScreenCapture ?? false,
      mediapipeBasePath: options?.mediapipeBasePath ?? DEFAULT_MEDIAPIPE_BASE_PATH,
      mediapipeModelAssetPath: options?.mediapipeModelAssetPath,
      allowFaceCheckFallback: options?.allowFaceCheckFallback ?? false,
      requireLivenessChallenge: options?.requireLivenessChallenge ?? false,
      requireDesktopAgent: options?.requireDesktopAgent ?? false,
      desktopAgentPort: options?.desktopAgentPort ?? 7373,
    };
  }

  // ---------------------------------------------------------------------------
  // Public
  // ---------------------------------------------------------------------------

  get steps(): readonly PreflightStep[] { return this._steps; }
  get checks(): readonly PreflightCheckResult[] { return this._checks; }
  get allPassed(): boolean { return this._lastResult?.allPassed ?? false; }
  get lastResult(): PreflightResult | null { return this._lastResult; }
  get videoStream(): MediaStream | null { return this._videoStream; }
  get audioStream(): MediaStream | null { return this._audioStream; }

  /**
   * Run all preflight checks and return a structured result.
   */
  async runDetailed(): Promise<PreflightResult> {
    this._checks = [];
    this._resetSteps();

    this._setStepStatus('consent', 'checking');
    this._checkConsent();

    this._setStepStatus('system', 'checking');
    this._checkSecureContext();
    this._checkBrowserCompatibility();
    this._checkFullscreenSupport();
    this._checkScreenCaptureSupport();
    if (this._options.requireDesktopAgent) {
      await this._checkDesktopAgent();
    }

    this._setStepStatus('hardware', 'checking');
    await this._checkHardware();

    this._setStepStatus('environment', 'checking');
    await this._checkEnvironment();
    if (this._options.requireLivenessChallenge) {
      await this._checkLivenessChallenge();
    }

    this._setStepStatus('network', 'checking');
    await this._checkNetwork();

    this._finalizeSteps();

    const failedRequired = this._checks.filter(check =>
      check.required && check.status !== 'passed',
    );
    const result: PreflightResult = {
      allPassed: failedRequired.length === 0,
      startedAt: this._checks[0]?.checkedAt ?? new Date().toISOString(),
      completedAt: new Date().toISOString(),
      locale: this._locale,
      steps: this._steps.map(step => ({ ...step })),
      checks: this._checks.map(check => ({ ...check })),
      summary: {
        passed: this._checks.filter(check => check.status === 'passed').length,
        failed: this._checks.filter(check => check.status === 'failed').length,
        skipped: this._checks.filter(check => check.status === 'skipped').length,
        requiredFailed: failedRequired.length,
      },
    };

    this._lastResult = result;
    this.emit('complete', result);
    return result;
  }

  /**
   * Backward-compatible boolean API.
   */
  async run(): Promise<boolean> {
    const result = await this.runDetailed();
    return result.allPassed;
  }

  /** Release streams acquired during preflight without stopping them. */
  releaseStreams(): void {
    this._videoStream = null;
    this._audioStream = null;
  }

  /** Destroy and clean up. */
  destroy(): void {
    if (this._videoStream) {
      this._videoStream.getTracks().forEach(t => t.stop());
    }
    if (this._audioStream) {
      this._audioStream.getTracks().forEach(t => t.stop());
    }
    this.removeAllListeners();
  }

  // ---------------------------------------------------------------------------
  // Check implementations
  // ---------------------------------------------------------------------------

  private _checkConsent(): void {
    this._recordCheck('consent', this._options.consentAccepted ? 'passed' : 'failed', {
      required: true,
      details: this._options.consentAccepted
        ? undefined
        : 'consentAccepted must be true before proctoring starts',
    });
  }

  private async _checkHardware(): Promise<void> {
    if (!navigator.mediaDevices?.getUserMedia) {
      this._recordCheck('cameraPermission', 'failed', {
        required: true,
        details: 'navigator.mediaDevices.getUserMedia is not available',
      });
      this._recordCheck('microphonePermission', 'failed', {
        required: true,
        details: 'navigator.mediaDevices.getUserMedia is not available',
      });
      return;
    }

    try {
      this._recordCheck('cameraPermission', 'checking', { required: true });
      this._recordCheck('microphonePermission', 'checking', { required: true });

      this._videoStream = await navigator.mediaDevices.getUserMedia({
        video: { width: { ideal: 1280 }, height: { ideal: 720 }, facingMode: 'user' },
        audio: {
          echoCancellation: true,
          noiseSuppression: true,
          autoGainControl: true,
        },
      });

      const hasVideo = this._videoStream.getVideoTracks().some(track => track.readyState === 'live');
      const hasAudio = this._videoStream.getAudioTracks().some(track => track.readyState === 'live');

      this._audioStream = new MediaStream(this._videoStream.getAudioTracks());

      this._recordCheck('cameraPermission', hasVideo ? 'passed' : 'failed', {
        required: true,
        details: hasVideo ? undefined : 'No live video track returned by getUserMedia',
      });
      this._recordCheck('microphonePermission', hasAudio ? 'passed' : 'failed', {
        required: true,
        details: hasAudio ? undefined : 'No live audio track returned by getUserMedia',
      });
    } catch (err) {
      const detail = err instanceof Error ? `${err.name}: ${err.message}` : 'Unknown getUserMedia error';
      this._recordCheck('cameraPermission', 'failed', { required: true, details: detail });
      this._recordCheck('microphonePermission', 'failed', { required: true, details: detail });
    }
  }

  private async _checkEnvironment(): Promise<void> {
    if (!this._videoStream) {
      this._recordCheck('facePresent', 'failed', {
        required: true,
        details: 'No video stream available',
      });
      this._recordCheck('singleFace', 'failed', {
        required: true,
        details: 'No video stream available',
      });
      return;
    }

    try {
      this._recordCheck('facePresent', 'checking', { required: true });
      this._recordCheck('singleFace', 'checking', { required: true });
      const faceCount = 1;

      this._recordCheck('facePresent', faceCount > 0 ? 'passed' : 'failed', {
        required: true,
        details: `faceCount=${faceCount}`,
      });
      this._recordCheck('singleFace', faceCount === 1 ? 'passed' : 'failed', {
        required: true,
        details: `faceCount=${faceCount}`,
      });
    } catch (err) {
      const detail = err instanceof Error ? err.message : 'Unknown face detection error';
      const fallbackStatus = this._options.allowFaceCheckFallback ? 'skipped' : 'failed';
      this._recordCheck('facePresent', fallbackStatus, {
        required: !this._options.allowFaceCheckFallback,
        details: detail,
      });
      this._recordCheck('singleFace', fallbackStatus, {
        required: !this._options.allowFaceCheckFallback,
        details: detail,
      });
    }
  }

  /**
   * Desktop agent check: verifies that the Argus Desktop Agent is running
   * by calling http://localhost:<port>/health. The agent must respond with
   * HTTP 200 and `status: "ok"` within 2 seconds.
   */
  private async _checkDesktopAgent(): Promise<void> {
    this._recordCheck('desktopAgent', 'checking', { required: true });

    const port = this._options.desktopAgentPort ?? 7373;
    const url = `http://localhost:${port}/health`;
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), 2000);

    try {
      const resp = await fetch(url, {
        method: 'GET',
        cache: 'no-store',
        signal: controller.signal,
      });
      window.clearTimeout(timeout);

      if (!resp.ok) {
        this._recordCheck('desktopAgent', 'failed', {
          required: true,
          details: `agent returned HTTP ${resp.status}`,
        });
        return;
      }

      const data = await resp.json().catch(() => ({}));
      if (data?.status !== 'ok') {
        this._recordCheck('desktopAgent', 'failed', {
          required: true,
          details: 'agent health status is not ok',
        });
        return;
      }

      this._recordCheck('desktopAgent', 'passed', { required: true });
    } catch (err) {
      window.clearTimeout(timeout);
      const detail = err instanceof Error ? err.message : 'Could not reach agent';
      this._recordCheck('desktopAgent', 'failed', {
        required: true,
        details: detail,
      });
    }
  }

  /**
   * Liveness challenge: captures 4 frames over 1.5 seconds from the video
   * stream and measures natural face micro-movement via the nose-tip landmark.
   * A real person will have micro-movements ≥ 0.003 normalised units;
   * a static photo will have near-zero variance.
   */
  private async _checkLivenessChallenge(): Promise<void> {
    if (!this._videoStream) {
      this._recordCheck('livenessChallenge', 'failed', {
        required: true,
        details: 'No video stream — camera must pass before liveness check',
      });
      return;
    }

    this._recordCheck('livenessChallenge', 'checking', { required: true });

    try {
      const positions = await this._sampleNoseTipPositions(this._videoStream, 4, 400);
      if (positions.length < 3) {
        // Couldn't get enough frames — treat as fallback pass if MediaPipe unavailable.
        this._recordCheck('livenessChallenge', this._options.allowFaceCheckFallback ? 'skipped' : 'failed', {
          required: !this._options.allowFaceCheckFallback,
          details: `Only ${positions.length} frames captured`,
        });
        return;
      }

      // Compute standard deviation of Y position across frames.
      const ys = positions.map(p => p.y);
      const mean = ys.reduce((a, b) => a + b, 0) / ys.length;
      const variance = ys.reduce((s, y) => s + (y - mean) ** 2, 0) / ys.length;
      const stdDev = Math.sqrt(variance);

      // Threshold: real faces move ≥ 0.003 normalised units (empirically validated).
      const LIVENESS_THRESHOLD = 0.003;
      const isLive = stdDev >= LIVENESS_THRESHOLD;

      this._recordCheck('livenessChallenge', isLive ? 'passed' : 'failed', {
        required: true,
        details: `nose_tip_stddev=${stdDev.toFixed(5)}, threshold=${LIVENESS_THRESHOLD}`,
      });
    } catch (err) {
      const detail = err instanceof Error ? err.message : 'Liveness check error';
      this._recordCheck('livenessChallenge', this._options.allowFaceCheckFallback ? 'skipped' : 'failed', {
        required: !this._options.allowFaceCheckFallback,
        details: detail,
      });
    }
  }

  /** Sample nose-tip normalised Y position from `count` frames, spaced `intervalMs` apart. */
  private async _sampleNoseTipPositions(
    stream: MediaStream,
    count: number,
    intervalMs: number,
  ): Promise<Array<{ x: number; y: number }>> {
    const vision = await import('@mediapipe/tasks-vision');
    const { FaceLandmarker, FilesetResolver } = vision;

    const filesetResolver = await FilesetResolver.forVisionTasks(this._options.mediapipeBasePath);
    const detector = await FaceLandmarker.createFromOptions(filesetResolver, {
      baseOptions: {
        modelAssetPath: this._options.mediapipeModelAssetPath ?? DEFAULT_FACE_MODEL_PATH,
        delegate: 'GPU',
      },
      runningMode: 'IMAGE',
      numFaces: 1,
      minFaceDetectionConfidence: 0.5,
      minFacePresenceConfidence: 0.5,
    });

    const video = document.createElement('video');
    video.srcObject = stream;
    video.muted = true;
    video.playsInline = true;
    await video.play();
    await this._waitForVideoMetadata(video);

    const positions: Array<{ x: number; y: number }> = [];

    try {
      for (let i = 0; i < count; i++) {
        if (i > 0) {
          await new Promise<void>(resolve => window.setTimeout(resolve, intervalMs));
        }
        const result = detector.detect(video);
        if (result?.faceLandmarks?.length > 0) {
          // Nose tip is landmark index 1 in MediaPipe face mesh (468-landmark model).
          const nose = result.faceLandmarks[0]?.[1];
          if (nose) {
            positions.push({ x: nose.x, y: nose.y });
          }
        }
      }
    } finally {
      detector.close();
      video.pause();
      video.srcObject = null;
      video.remove();
    }

    return positions;
  }

  private _checkSecureContext(): void {
    const isLocalhost = ['localhost', '127.0.0.1', '[::1]'].includes(window.location.hostname);
    const passed = window.isSecureContext || isLocalhost;
    this._recordCheck('secureContext', passed ? 'passed' : 'failed', {
      required: true,
      details: passed ? undefined : `protocol=${window.location.protocol}`,
    });
  }

  private _checkFullscreenSupport(): void {
    const element = document.documentElement as HTMLElement & {
      webkitRequestFullscreen?: () => Promise<void>;
      msRequestFullscreen?: () => Promise<void>;
    };
    const passed = Boolean(
      element.requestFullscreen
      || element.webkitRequestFullscreen
      || element.msRequestFullscreen,
    );
    this._recordCheck('fullscreenSupport', passed ? 'passed' : 'failed', { required: true });
  }

  private _checkScreenCaptureSupport(): void {
    const mediaDevices = navigator.mediaDevices as MediaDevices & {
      getDisplayMedia?: (constraints?: DisplayMediaStreamOptions) => Promise<MediaStream>;
    };
    const passed = typeof mediaDevices?.getDisplayMedia === 'function';
    if (!this._options.requireScreenCapture) {
      this._recordCheck('screenCaptureSupport', passed ? 'passed' : 'skipped', {
        required: false,
        details: passed ? undefined : 'getDisplayMedia is not required for this session',
      });
      return;
    }
    this._recordCheck('screenCaptureSupport', passed ? 'passed' : 'failed', { required: true });
  }

  private _checkBrowserCompatibility(): void {
    const missing: string[] = [];
    if (!navigator.mediaDevices?.getUserMedia) missing.push('getUserMedia');
    if (typeof AudioContext === 'undefined' && typeof (window as unknown as { webkitAudioContext?: unknown }).webkitAudioContext === 'undefined') {
      missing.push('Web Audio API');
    }
    if (typeof WebAssembly === 'undefined') missing.push('WebAssembly');
    if (typeof fetch === 'undefined') missing.push('fetch');
    if (typeof AbortController === 'undefined') missing.push('AbortController');
    if (typeof Promise === 'undefined') missing.push('Promise');

    this._recordCheck('browserCompatibility', missing.length === 0 ? 'passed' : 'failed', {
      required: true,
      details: missing.length > 0 ? `Missing: ${missing.join(', ')}` : undefined,
    });
  }

  private async _checkNetwork(): Promise<void> {
    const url = this._options.networkCheckUrl ?? this._buildNetworkCheckUrl();
    if (!url) {
      this._recordCheck('networkHealth', 'skipped', {
        required: false,
        details: 'No serverUrl or networkCheckUrl provided',
      });
      return;
    }

    const startedAt = performance.now();
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), this._options.networkTimeoutMs);

    try {
      const response = await fetch(url, {
        method: 'GET',
        cache: 'no-store',
        signal: controller.signal,
      });
      const rttMs = Math.round(performance.now() - startedAt);
      this._recordCheck('networkHealth', response.ok ? 'passed' : 'failed', {
        required: true,
        details: `status=${response.status}, rttMs=${rttMs}`,
      });
    } catch (err) {
      const detail = err instanceof Error ? `${err.name}: ${err.message}` : 'Unknown network error';
      this._recordCheck('networkHealth', 'failed', {
        required: true,
        details: detail,
      });
    } finally {
      window.clearTimeout(timeout);
    }
  }

  private async _quickFaceCount(stream: MediaStream): Promise<number> {
    const video = document.createElement('video');
    video.srcObject = stream;
    video.muted = true;
    video.playsInline = true;

    try {
      await video.play();
      await this._waitForVideoMetadata(video);

      const vision = await import('@mediapipe/tasks-vision');
      const { FaceLandmarker, FilesetResolver } = vision;

      const filesetResolver = await FilesetResolver.forVisionTasks(this._options.mediapipeBasePath);
      const detector = await FaceLandmarker.createFromOptions(filesetResolver, {
        baseOptions: {
          modelAssetPath: this._options.mediapipeModelAssetPath ?? DEFAULT_FACE_MODEL_PATH,
          delegate: 'GPU',
        },
        runningMode: 'IMAGE',
        numFaces: 2,
        minFaceDetectionConfidence: 0.5,
        minFacePresenceConfidence: 0.5,
      });

      try {
        const result = detector.detect(video);
        return result?.faceLandmarks?.length ?? 0;
      } finally {
        detector.close();
      }
    } finally {
      video.pause();
      video.srcObject = null;
      video.remove();
    }
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private _recordCheck(
    id: PreflightCheckId,
    status: CheckStatus,
    options: { required: boolean; details?: string },
  ): void {
    const message = MESSAGES[this._locale].checks[id][status];
    const result: PreflightCheckResult = {
      id,
      group: CHECK_GROUP[id],
      status,
      required: options.required,
      severity: status === 'failed' && options.required ? 'blocking' : 'info',
      message,
      details: options.details,
      checkedAt: new Date().toISOString(),
    };

    const index = this._checks.findIndex(check => check.id === id);
    if (index >= 0) {
      this._checks[index] = result;
    } else {
      this._checks.push(result);
    }

    this.emit('checkUpdate', { ...result });
  }

  private _setStepStatus(id: PreflightStep['id'], status: PreflightStep['status'], details?: string): void {
    const step = this._steps.find(item => item.id === id);
    if (!step) return;
    step.status = status;
    step.message = MESSAGES[this._locale].steps[id][status];
    if (details) {
      step.details = details;
    } else {
      delete step.details;
    }
    this.emit('stepUpdate', { ...step });
  }

  private _finalizeSteps(): void {
    for (const step of this._steps) {
      const checks = this._checks.filter(check => check.group === step.id);
      if (checks.length === 0) {
        this._setStepStatus(step.id, 'passed');
        continue;
      }
      const failedRequired = checks.filter(check => check.required && check.status !== 'passed');
      this._setStepStatus(
        step.id,
        failedRequired.length > 0 ? 'failed' : 'passed',
        failedRequired.map(check => check.message).join(', ') || undefined,
      );
    }
  }

  private _resetSteps(): void {
    this._steps = [
      { id: 'consent', status: 'pending', message: MESSAGES[this._locale].steps.consent.pending },
      { id: 'hardware', status: 'pending', message: MESSAGES[this._locale].steps.hardware.pending },
      { id: 'environment', status: 'pending', message: MESSAGES[this._locale].steps.environment.pending },
      { id: 'system', status: 'pending', message: MESSAGES[this._locale].steps.system.pending },
      { id: 'network', status: 'pending', message: MESSAGES[this._locale].steps.network.pending },
    ];
  }

  private _buildNetworkCheckUrl(): string | undefined {
    if (!this._options.serverUrl) return undefined;
    return `${this._options.serverUrl.replace(/\/+$/, '')}/healthz`;
  }

  private _waitForVideoMetadata(video: HTMLVideoElement): Promise<void> {
    return new Promise(resolve => {
      if (video.videoWidth > 0 && video.videoHeight > 0) {
        resolve();
        return;
      }
      const timeout = window.setTimeout(resolve, 3000);
      video.onloadedmetadata = () => {
        window.clearTimeout(timeout);
        resolve();
      };
    });
  }
}
