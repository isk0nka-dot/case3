// Package config provides YAML-based configuration with environment variable
// overrides for the Event-Collector service.
//
// Configuration is loaded in two phases:
//  1. Read and parse the YAML file at the given path.
//  2. Apply environment variable overrides for values that must differ between
//     environments (CI, staging, production) without touching the YAML file.
//
// Every field has a sensible default so the service can start with a minimal
// (or even empty) config file during local development.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// ---------------------------------------------------------------------------
// Top-level configuration
// ---------------------------------------------------------------------------

// Config is the root configuration structure for the Event-Collector service.
// It aggregates all sub-system configs into a single, strongly-typed tree that
// is loaded once at startup and treated as immutable for the process lifetime.
type Config struct {
	Server      ServerConfig      `yaml:"server"`
	Kafka       KafkaConfig       `yaml:"kafka"`
	ClickHouse  ClickHouseConfig  `yaml:"clickhouse"`
	Postgres    PostgresConfig    `yaml:"postgres"`
	MinIO       MinIOConfig       `yaml:"minio"`
	Logger      LoggerConfig      `yaml:"logger"`
	RateLimit   RateLimitConfig   `yaml:"rate_limit"`
	WorkerPool  WorkerPoolConfig  `yaml:"worker_pool"`
	Auth        AuthConfig        `yaml:"auth"`
	Session     SessionConfig     `yaml:"session"`
	CORS        CORSConfig        `yaml:"cors"`
	Chunk       ChunkConfig       `yaml:"chunk"`
	Export      ExportConfig      `yaml:"export"`
	Telegram    TelegramConfig    `yaml:"telegram"`
	DLQ         DLQConfig         `yaml:"dlq"`
	Redis       RedisConfig       `yaml:"redis"`
	Inference   InferenceConfig   `yaml:"inference"`
	AudioBridge AudioBridgeConfig `yaml:"audio_bridge"`
	CryptoErase CryptoEraseConfig `yaml:"crypto_erasure"`
	Backfiller  BackfillerConfig  `yaml:"backfiller"`
	Cleanup     CleanupConfig     `yaml:"cleanup"`
	Webhook     WebhookConfig     `yaml:"webhook"`
}

// CleanupConfig holds org lifecycle cleanup settings.
// When enabled, a background job periodically purges data for
// organizations that have been in the "purged" lifecycle state
// beyond the configured grace period. GDPR right-to-erasure.
type CleanupConfig struct {
	// Enabled controls whether the background cleanup job runs. Default: false.
	Enabled bool `yaml:"enabled"`

	// RunInterval is how often the cleanup job runs. Default: 24h.
	RunInterval time.Duration `yaml:"run_interval"`

	// GracePeriodDays is the number of days after "purged" state before data is deleted.
	// Default: 30 days.
	GracePeriodDays int `yaml:"grace_period_days"`

	// ClickHousePurge controls whether ClickHouse data is also purged.
	// ALTER TABLE DELETE is resource-intensive. Default: false.
	ClickHousePurge bool `yaml:"clickhouse_purge"`
}

// BackfillerConfig holds Kafka-to-ClickHouse backfiller settings (ADR-007).
// When enabled, a Kafka consumer group replays events into ClickHouse,
// ensuring zero data loss even if the primary ClickHouse writer drops events.
type BackfillerConfig struct {
	// Enabled controls whether the backfiller consumer starts. Default: false.
	Enabled bool `yaml:"enabled"`

	// BatchSize is events per ClickHouse batch insert. Default: 500.
	BatchSize int `yaml:"batch_size"`

	// FlushInterval is the maximum time between batch flushes. Default: 5s.
	FlushInterval time.Duration `yaml:"flush_interval"`

	// PauseOnError is the sleep after a ClickHouse insert failure. Default: 10s.
	PauseOnError time.Duration `yaml:"pause_on_error"`
}

// CryptoEraseConfig holds GDPR cryptographic erasure settings.
// When enabled, all evidence and sensitive event fields are encrypted with
// per-student envelope encryption (AES-256-GCM). Destroying a student's
// KEK renders all their data irrecoverable.
type CryptoEraseConfig struct {
	// Enabled activates envelope encryption for evidence storage. Default: false.
	Enabled bool `yaml:"enabled"`

	// KEKHex is the system-wide Key Encryption Key, hex-encoded (64 hex chars = 32 bytes).
	// CRITICAL: Must be loaded from a secrets manager in production.
	// Set via ARGUS_CRYPTO_KEK environment variable.
	KEKHex string `yaml:"kek_hex"`
}

// WebhookConfig holds configuration for the SaaS webhook delivery system.
// When enabled, a background dispatcher polls the webhook_deliveries table
// and delivers payloads to partner endpoints with HMAC-SHA256 signatures.
type WebhookConfig struct {
	// Enabled controls whether the webhook dispatcher runs. Default: true.
	Enabled bool `yaml:"enabled"`

	// PollInterval is how often the dispatcher checks for pending deliveries. Default: 1s.
	PollInterval time.Duration `yaml:"poll_interval"`

	// BatchSize is the maximum number of deliveries to process per poll. Default: 50.
	BatchSize int `yaml:"batch_size"`

	// MaxRetries is the maximum delivery attempts before marking as failed. Default: 5.
	MaxRetries int `yaml:"max_retries"`

	// TimeoutSec is the HTTP request timeout for each delivery attempt. Default: 30.
	TimeoutSec int `yaml:"timeout_sec"`

	// MaxConcurrent limits parallel delivery goroutines. Default: 10.
	MaxConcurrent int `yaml:"max_concurrent"`
}

// TelegramConfig holds Telegram Bot API alerting credentials.
type TelegramConfig struct {
	BotToken string `yaml:"bot_token"`
	ChatID   string `yaml:"chat_id"`
}

// DLQConfig holds configuration for the BadgerDB dead-letter queue.
// When enabled, Kafka write failures are caught and persisted locally,
// then automatically reclaimed when Kafka recovers.
type DLQConfig struct {
	// Enabled controls whether the DLQ is active. Default: true.
	Enabled bool `yaml:"enabled"`
	// DataDir is the filesystem path for BadgerDB data files. Default: "/tmp/argus-dlq".
	DataDir string `yaml:"data_dir"`
	// GCInterval is the BadgerDB value log GC cycle. Default: 5m.
	GCInterval time.Duration `yaml:"gc_interval"`
	// GCDiscardRatio is the ratio of discardable data to trigger GC. Default: 0.5.
	GCDiscardRatio float64 `yaml:"gc_discard_ratio"`
	// MaxEntries is a safety cap on DLQ size. Default: 1,000,000.
	MaxEntries int64 `yaml:"max_entries"`
	// SyncWrites enables fsync on every write. Default: true.
	SyncWrites bool `yaml:"sync_writes"`
	// ReclamationInterval is how often the DLQ is drained back to Kafka. Default: 30s.
	ReclamationInterval time.Duration `yaml:"reclamation_interval"`
	// ReclamationBatchSize is events per drain cycle. Default: 100.
	ReclamationBatchSize int `yaml:"reclamation_batch_size"`
	// HeartbeatInterval is how often a degraded-mode heartbeat is sent to Telegram. Default: 5m.
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`
	// CircuitBreakerFailureThreshold trips breaker after N consecutive Kafka failures. Default: 5.
	CircuitBreakerFailureThreshold uint32 `yaml:"cb_failure_threshold"`
	// CircuitBreakerTimeout is how long the breaker stays open. Default: 30s.
	CircuitBreakerTimeout time.Duration `yaml:"cb_timeout"`
}

// RedisConfig holds Redis connection settings for the asynq job queue.
// When Redis is configured, heavy background jobs (video export, forensic PDF)
// are dispatched via asynq instead of PostgreSQL polling.
type RedisConfig struct {
	// Addr is the Redis server address (host:port). Default: "localhost:6379".
	Addr string `yaml:"addr"`

	// Password is the Redis AUTH password. Default: "" (no auth).
	Password string `yaml:"password"`

	// DB is the Redis database number. Default: 0.
	DB int `yaml:"db"`

	// WorkerConcurrency is the number of concurrent asynq worker goroutines
	// for non-inference queues (critical, default, low).
	// Only used by cmd/worker. Default: 10.
	WorkerConcurrency int `yaml:"worker_concurrency"`

	// InferenceConcurrency is the number of concurrent inference-queue
	// worker goroutines. This isolates GPU-bound AI analysis from lightweight
	// tasks (export, forensic PDF, alerts) preventing resource starvation.
	// Only used by cmd/worker. Default: 2.
	InferenceConcurrency int `yaml:"inference_concurrency"`
}

// InferenceConfig controls the backend AI inference service.
// Used by cmd/inference (standalone GPU gateway) and cmd/worker (gRPC client).
type InferenceConfig struct {
	// GRPCHost is the hostname for the inference gRPC server. Default: "localhost".
	// Override via EVENT_COLLECTOR_INFERENCE_GRPC_HOST for Docker/Kubernetes.
	GRPCHost string `yaml:"grpc_host"`

	// GRPCPort is the port the inference gRPC server listens on. Default: 50061.
	GRPCPort int `yaml:"grpc_port"`

	// EngineType selects the AI inference backend. Default: "python_bridge".
	// Options: "stub" (synthetic local dev only), "python_bridge" (Python ONNX sidecar).
	EngineType string `yaml:"engine_type"`

	// AllowStub must be explicitly enabled for local development. Production
	// inference fails fast when engine_type=stub and allow_stub=false.
	AllowStub bool `yaml:"allow_stub"`

	// ModelDir is the filesystem path to ONNX model files. Default: "".
	ModelDir string `yaml:"model_dir"`

	// PythonBridgeURL is the HTTP endpoint for the Python ONNX sidecar.
	PythonBridgeURL string `yaml:"python_bridge_url"`

	// BridgeTimeoutSec limits startup health checks and per-frame sidecar calls.
	BridgeTimeoutSec int `yaml:"bridge_timeout_sec"`

	// MaxFrameBytes is the maximum allowed size for a single frame. Default: 10 MiB.
	MaxFrameBytes int `yaml:"max_frame_bytes"`

	// MaxVideoDurSec is the maximum video segment duration in seconds. Default: 300 (5 min).
	MaxVideoDurSec int `yaml:"max_video_dur_sec"`

	// FrameSampleRate controls frame sampling for deep scan: analyze every Nth frame. Default: 2.
	FrameSampleRate int `yaml:"frame_sample_rate"`

	// FrameSampleIntervalSec limits expensive video frame extraction. Default: 5.
	FrameSampleIntervalSec int `yaml:"frame_sample_interval_sec"`

	// Configurable anomaly thresholds. Defaults are conservative and can be
	// tuned without code changes as false-positive rates change.
	FaceMismatchThreshold     float32 `yaml:"face_mismatch_threshold"`
	LivenessThreshold         float32 `yaml:"liveness_threshold"`
	ObjectConfidenceThreshold float32 `yaml:"object_confidence_threshold"`
	SpoofConfidenceThreshold  float32 `yaml:"spoof_confidence_threshold"`

	// EnableBackendFaceRules controls whether backend AI emits MULTIPLE_PERSONS
	// and FACE_NOT_DETECTED events. Since the frontend MediaPipe already emits these,
	// this should default to false to avoid duplicates.
	EnableBackendFaceRules bool `yaml:"enable_backend_face_rules"`

	// Concurrency is the number of parallel inference workers. Default: 4.
	Concurrency int `yaml:"concurrency"`
}

// GRPCAddr returns the inference gRPC server address as "{host}:{port}".
// Configurable via EVENT_COLLECTOR_INFERENCE_GRPC_HOST + EVENT_COLLECTOR_INFERENCE_GRPC_PORT.
func (c InferenceConfig) GRPCAddr() string {
	host := c.GRPCHost
	if host == "" {
		host = "localhost"
	}
	return fmt.Sprintf("%s:%d", host, c.GRPCPort)
}

// AudioBridgeConfig controls backend-side aggregation of browser-local audio
// telemetry into derived ClickHouse anomaly events. It never handles raw audio.
type AudioBridgeConfig struct {
	// Enabled controls whether AudioLevelTelemetry can derive audio_anomaly rows.
	Enabled bool `yaml:"enabled"`

	// NoiseThresholdDb is the RMS dB threshold for sustained noise. Default: -35.
	NoiseThresholdDb float32 `yaml:"noise_threshold_db"`

	// VADConfidenceThreshold is the browser VAD confidence cutoff. Default: 0.70.
	VADConfidenceThreshold float32 `yaml:"vad_confidence_threshold"`

	// ConsecutiveEvents is how many high telemetry frames are required before
	// deriving a backend audio_anomaly. Default: 3.
	ConsecutiveEvents int `yaml:"consecutive_events"`

	// CooldownEvents suppresses duplicate derived anomalies after one fires.
	// Default: 12, roughly 3 seconds at the 4 Hz frontend default.
	CooldownEvents int `yaml:"cooldown_events"`

	// DerivedEventConfidence is the fallback confidence for derived anomalies.
	// Default: 0.85.
	DerivedEventConfidence float32 `yaml:"derived_event_confidence"`
}

// ---------------------------------------------------------------------------
// Sub-system configs
// ---------------------------------------------------------------------------

// ServerConfig controls the gRPC and HTTP listener addresses as well as the
// graceful shutdown budget.
type ServerConfig struct {
	// GRPCPort is the TCP port for the main gRPC event ingestion endpoint.
	GRPCPort int `yaml:"grpc_port"`

	// HTTPPort is the TCP port for the HTTP server that exposes health-check,
	// gRPC-Web proxy, and Prometheus metrics endpoints.
	HTTPPort int `yaml:"http_port"`

	// ShutdownTimeout is the maximum duration the process waits for in-flight
	// requests to drain before forcing a hard stop.
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`

	// TLS enables the Strict-Transport-Security response header.
	// Set to true only when this server terminates TLS directly (not via Nginx).
	// In production, TLS is typically terminated at Nginx, so this defaults to false.
	TLS bool `yaml:"tls"`

	// EnablePprof exposes Go runtime profiling endpoints at /debug/pprof/*.
	// MUST be false in production — only enable for debugging under supervision.
	EnablePprof bool `yaml:"enable_pprof"`
}

// KafkaConfig maps 1:1 to kafka.ProducerConfig so the infrastructure layer
// can be initialised directly from this struct.
type KafkaConfig struct {
	// Brokers is the list of Kafka bootstrap servers (host:port).
	Brokers []string `yaml:"brokers"`

	// RequiredAcks controls durability: -1 = all ISR, 1 = leader only, 0 = fire-and-forget.
	RequiredAcks int `yaml:"required_acks"`

	// MaxMessageBytes caps the size of a single Kafka message (including headers).
	MaxMessageBytes int `yaml:"max_message_bytes"`

	// FlushFrequency is the maximum time the async producer waits before flushing
	// a batch of messages to the broker.
	FlushFrequency time.Duration `yaml:"flush_frequency"`

	// FlushMessages is the maximum number of messages buffered before an
	// immediate flush is triggered.
	FlushMessages int `yaml:"flush_messages"`

	// RetryMax is the number of times a failed produce request is retried.
	RetryMax int `yaml:"retry_max"`

	// CompressionCodec selects the wire-compression algorithm.
	// Accepted values: "zstd", "snappy", "lz4", "none".
	CompressionCodec string `yaml:"compression_codec"`

	// IdempotentEnabled activates the Kafka idempotent producer for exactly-once
	// semantics within a single producer session.
	IdempotentEnabled bool `yaml:"idempotent_enabled"`

	// AutoCreateTopics enables automatic topic creation at startup.
	// When true, the service ensures all required topics exist with the
	// configured partition count and replication factor.
	AutoCreateTopics bool `yaml:"auto_create_topics"`

	// TopicPartitions is the base partition count for auto-created topics.
	// Higher values enable more consumer parallelism.
	// Default: 6. Standard and telemetry topics get 2× this value.
	TopicPartitions int `yaml:"topic_partitions"`

	// TopicReplication is the replication factor for auto-created topics.
	// Must not exceed the number of brokers in the cluster.
	// Default: 1 (dev). Set to 3 for production HA.
	TopicReplication int `yaml:"topic_replication"`
}

// ClickHouseConfig maps 1:1 to clickhouse.WriterConfig.
type ClickHouseConfig struct {
	// Addrs is the list of ClickHouse native-protocol addresses (host:port).
	Addrs []string `yaml:"addrs"`

	// Database is the target ClickHouse database.
	Database string `yaml:"database"`

	// Username for ClickHouse authentication.
	Username string `yaml:"username"`

	// Password for ClickHouse authentication.
	Password string `yaml:"password"`

	// BatchSize is the number of events buffered before an immediate flush.
	BatchSize int `yaml:"batch_size"`

	// FlushInterval is the maximum time between periodic batch flushes.
	FlushInterval time.Duration `yaml:"flush_interval"`

	// MaxRetries is the number of times a failed batch insert is retried
	// with exponential backoff.
	MaxRetries int `yaml:"max_retries"`
}

// LoggerConfig controls the structured logger (zap).
type LoggerConfig struct {
	// Level is the minimum enabled log level ("debug", "info", "warn", "error").
	Level string `yaml:"level"`

	// Encoding selects the output format: "json" for production, "console" for
	// human-readable local development.
	Encoding string `yaml:"encoding"`

	// Development enables DPanic-level behaviour and more verbose stack traces.
	Development bool `yaml:"development"`
}

// RateLimitConfig governs the token-bucket rate limiters that protect the
// service from burst traffic and noisy neighbours.
type RateLimitConfig struct {
	// GlobalRPS is the maximum sustained requests-per-second across all clients.
	GlobalRPS float64 `yaml:"global_rps"`

	// GlobalBurst is the token-bucket burst size for the global limiter.
	GlobalBurst int `yaml:"global_burst"`

	// PerSessionRPS is the maximum sustained RPS for a single proctoring session.
	PerSessionRPS float64 `yaml:"per_session_rps"`

	// PerSessionBurst is the burst allowance per session.
	PerSessionBurst int `yaml:"per_session_burst"`

	// HTTPGlobalRPS is the maximum sustained RPS for the admin HTTP API.
	// This is separate from the gRPC global limiter because admin API endpoints
	// have different throughput requirements than event ingestion.
	HTTPGlobalRPS float64 `yaml:"http_global_rps"`

	// HTTPGlobalBurst is the burst allowance for the admin HTTP API.
	HTTPGlobalBurst int `yaml:"http_global_burst"`

	// HTTPPerIPRPS is the maximum sustained RPS from a single client IP
	// on the admin HTTP API.
	HTTPPerIPRPS float64 `yaml:"http_per_ip_rps"`

	// HTTPPerIPBurst is the burst allowance per client IP.
	HTTPPerIPBurst int `yaml:"http_per_ip_burst"`

	// ReconnectBurstRPS is the aggregate RPS cap applied to the first batch
	// of events from sessions that have been offline. This prevents a
	// thundering herd when thousands of clients reconnect simultaneously
	// after a network partition and drain their offline queues in unison.
	// Default: 5000.
	ReconnectBurstRPS float64 `yaml:"reconnect_burst_rps"`

	// ReconnectBurstSize is the token bucket burst for reconnect admission.
	// Default: 10000.
	ReconnectBurstSize int `yaml:"reconnect_burst_size"`
}

// WorkerPoolConfig sizes the goroutine pool that processes incoming events
// off the gRPC stream.
type WorkerPoolConfig struct {
	// Size is the number of persistent worker goroutines.
	Size int `yaml:"size"`

	// QueueSize is the capacity of the buffered channel that feeds the pool.
	// A larger queue absorbs short traffic spikes but increases memory usage.
	QueueSize int `yaml:"queue_size"`
}

// AuthConfig holds JWT authentication settings for the Event Collector.
type AuthConfig struct {
	// Enabled controls whether JWT authentication is enforced. Set to false
	// for local development without the Eduser system. Default: true.
	Enabled bool `yaml:"enabled"`

	// Algorithm selects the JWT signing/verification algorithm.
	// "RS256" (default, recommended): RSA-SHA256 asymmetric — private key signs,
	//   public key verifies. A compromised worker CANNOT forge admin tokens.
	// "HS256" (legacy): HMAC-SHA256 symmetric — same shared secret signs and
	//   verifies. A compromised worker CAN forge admin tokens.
	Algorithm string `yaml:"algorithm"`

	// ── RSA-256 fields (recommended) ─────────────────────────────────

	// PrivateKeyPath is the filesystem path to the PEM-encoded RSA private key.
	// Only the API server needs this. Workers MUST NOT have access.
	// Set via EVENT_COLLECTOR_JWT_PRIVATE_KEY_PATH environment variable.
	PrivateKeyPath string `yaml:"private_key_path"`

	// PublicKeyPath is the filesystem path to the PEM-encoded RSA public key.
	// Both API server and workers need this for token verification.
	// Set via EVENT_COLLECTOR_JWT_PUBLIC_KEY_PATH environment variable.
	PublicKeyPath string `yaml:"public_key_path"`

	// ── HMAC-SHA256 fields (legacy, deprecated) ──────────────────────

	// SigningKey is the HMAC-SHA256 shared secret for JWT verification.
	// DEPRECATED: Use RS256 with PrivateKeyPath/PublicKeyPath instead.
	// In production, set via EVENT_COLLECTOR_JWT_SIGNING_KEY environment variable.
	// Must be at least 32 bytes for HMAC-SHA256 security.
	SigningKey string `yaml:"signing_key"`

	// ── Common fields ────────────────────────────────────────────────

	// Issuer is the expected JWT "iss" claim. Must match the value set by
	// the Eduser system when issuing tokens. Default: "eduser".
	Issuer string `yaml:"issuer"`

	// Audience is the expected JWT "aud" claim. Default: "argus-event-collector".
	Audience string `yaml:"audience"`

	// ClockSkew is the maximum clock drift tolerance for JWT expiration checks.
	// Compensates for NTP drift between Eduser and Event Collector servers.
	// Default: 30 seconds.
	ClockSkew time.Duration `yaml:"clock_skew"`

	// RequireSessionSecret enables per-session HMAC validation in addition
	// to JWT verification. When true, the SessionValidator is called after
	// JWT verification to confirm the session is active in Eduser.
	// Default: false (enable in production).
	RequireSessionSecret bool `yaml:"require_session_secret"`
}

// SessionConfig holds Eduser session validation settings.
type SessionConfig struct {
	// CacheTTL is how long a validated session stays in the in-memory cache
	// before re-validation against Eduser. Default: 5 minutes.
	CacheTTL time.Duration `yaml:"cache_ttl"`

	// NegativeCacheTTL is how long an invalid session stays cached to prevent
	// repeated calls for known-bad sessions. Default: 30 seconds.
	NegativeCacheTTL time.Duration `yaml:"negative_cache_ttl"`

	// CleanupInterval is how often the background goroutine evicts expired
	// cache entries. Default: 1 minute.
	CleanupInterval time.Duration `yaml:"cleanup_interval"`

	// EduserEndpoint is the gRPC or HTTP endpoint of the Eduser session
	// validation API. If empty, noop mode is used (all sessions valid).
	// Example: "eduser-service:50051"
	EduserEndpoint string `yaml:"eduser_endpoint"`

	// EduserTimeout is the per-call timeout for Eduser API requests.
	// Default: 3 seconds.
	EduserTimeout time.Duration `yaml:"eduser_timeout"`
}

// PostgresConfig holds PostgreSQL connection settings for the SaaS admin layer
// (organizations, users, API keys). PostgreSQL handles transactional CRUD
// while ClickHouse handles analytical event storage.
type PostgresConfig struct {
	// Host is the PostgreSQL server hostname. Default: "localhost".
	Host string `yaml:"host"`

	// Port is the PostgreSQL server port. Default: 5432.
	Port int `yaml:"port"`

	// Database is the target database name. Default: "argus".
	Database string `yaml:"database"`

	// Username for PostgreSQL authentication. Default: "argus".
	Username string `yaml:"username"`

	// Password for PostgreSQL authentication. Default: "argus".
	Password string `yaml:"password"`

	// SSLMode controls TLS for the connection. Default: "disable" (dev).
	// Production: "require" or "verify-full".
	SSLMode string `yaml:"ssl_mode"`

	// MaxOpenConns limits the total number of open connections. Default: 25.
	MaxOpenConns int `yaml:"max_open_conns"`

	// MaxIdleConns limits the idle connection pool size. Default: 10.
	MaxIdleConns int `yaml:"max_idle_conns"`

	// ConnMaxLifetime is the maximum time a connection can be reused. Default: 5m.
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
}

// CORSConfig holds CORS middleware settings for gRPC-Web browser access.
type CORSConfig struct {
	// AllowedOrigins is the list of origins permitted for cross-origin requests.
	// Example: ["https://app.argus.ai", "https://staging.argus.ai"]
	// Use ["*"] for development only. Default: ["*"].
	AllowedOrigins []string `yaml:"allowed_origins"`

	// MaxAge is the preflight cache duration in seconds. Default: "86400" (24h).
	MaxAge string `yaml:"max_age"`

	// AllowCredentials enables sending Authorization headers cross-origin.
	// Must be true for JWT authentication. Default: true.
	AllowCredentials bool `yaml:"allow_credentials"`
}

// ChunkConfig holds configuration for the chunked evidence upload endpoint.
// Browser clients operating in degraded (Tier B/C) network conditions split
// large binary evidence into 256KB chunks and upload them sequentially.
type ChunkConfig struct {
	// MaxChunkSize is the maximum allowed size for a single chunk (bytes).
	// Default: 256KB (262144).
	MaxChunkSize int `yaml:"max_chunk_size"`

	// MaxFragmentSize is the maximum total size for all chunks of a fragment (bytes).
	// Default: 50MB (52428800).
	MaxFragmentSize int64 `yaml:"max_fragment_size"`

	// ChunkTTL is the time-to-live for incomplete fragments before cleanup.
	// Default: 5 minutes.
	ChunkTTL time.Duration `yaml:"chunk_ttl"`

	// MaxConcurrentAssemblies limits parallel fragment assemblies.
	// Default: 100.
	MaxConcurrentAssemblies int `yaml:"max_concurrent_assemblies"`
}

// ExportConfig holds configuration for the bulk evidence export worker.
// Controls archive generation, presigned URL TTL, and worker concurrency.
type ExportConfig struct {
	// MaxSessionsPerExport is the maximum number of sessions in a single export.
	// Default: 50.
	MaxSessionsPerExport int `yaml:"max_sessions_per_export"`

	// PresignTTL is the time-to-live for presigned download URLs.
	// Default: 24 hours.
	PresignTTL time.Duration `yaml:"presign_ttl"`

	// MaxArchiveSize is the maximum archive size in bytes.
	// Default: 5GB.
	MaxArchiveSize int64 `yaml:"max_archive_size"`

	// PollInterval is how often the worker polls for pending jobs.
	// Default: 5 seconds.
	PollInterval time.Duration `yaml:"poll_interval"`

	// WorkerCount is the number of concurrent export workers.
	// Default: 2.
	WorkerCount int `yaml:"worker_count"`

	// ExportBucket is the MinIO bucket for completed export archives.
	// Default: "argus-exports".
	ExportBucket string `yaml:"export_bucket"`
}

// MinIOConfig holds S3-compatible object storage settings for evidence fragments.
// MinIO stores immutable video evidence with S3 Object Lock (WORM) for legal
// compliance and chain-of-custody requirements.
type MinIOConfig struct {
	// Endpoint is the MinIO server address (host:port, no protocol).
	// Default: "localhost:9002" (mapped from container port 9000).
	Endpoint string `yaml:"endpoint"`

	// AccessKey is the MinIO access key (username). Default: "argus-minio-admin".
	AccessKey string `yaml:"access_key"`

	// SecretKey is the MinIO secret key (password). Default: "argus-minio-secret-change-in-production".
	SecretKey string `yaml:"secret_key"`

	// Bucket is the S3 bucket for evidence storage. Default: "argus-evidence".
	Bucket string `yaml:"bucket"`

	// Region is the S3 region. Default: "us-east-1" (MinIO default).
	Region string `yaml:"region"`

	// UseSSL enables TLS for the MinIO connection. Default: false (dev).
	// Production: true (always use TLS for evidence transport).
	UseSSL bool `yaml:"use_ssl"`

	// PresignTTL is the time-to-live for presigned download URLs.
	// Evidence viewers receive short-lived URLs to prevent link sharing.
	// Default: 5 minutes.
	PresignTTL time.Duration `yaml:"presign_ttl"`

	// UploadTimeout is the maximum time allowed for a single evidence upload.
	// Default: 30 seconds (sufficient for ~50MB fragments on 100Mbps).
	UploadTimeout time.Duration `yaml:"upload_timeout"`
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

// Load reads a YAML configuration file from path and returns a validated
// Config with defaults applied. Environment variables are NOT consulted.
// Use LoadWithEnv if you need environment-variable overrides.
func Load(path string) (*Config, error) {
	cfg := &Config{}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: failed to read file %q: %w", path, err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("config: failed to parse YAML from %q: %w", path, err)
	}

	applyDefaults(cfg)

	if err := validate(cfg); err != nil {
		return nil, fmt.Errorf("config: validation failed: %w", err)
	}

	return cfg, nil
}

// LoadWithEnv loads a YAML configuration file, applies sensible defaults, and
// then overlays environment variable overrides. This is the recommended entry
// point for production use.
//
// Environment variable mapping:
//
//	EVENT_COLLECTOR_GRPC_PORT        -> Server.GRPCPort
//	EVENT_COLLECTOR_HTTP_PORT        -> Server.HTTPPort
//	EVENT_COLLECTOR_KAFKA_BROKERS    -> Kafka.Brokers  (comma-separated)
//	EVENT_COLLECTOR_CH_ADDRS         -> ClickHouse.Addrs (comma-separated)
//	EVENT_COLLECTOR_CH_DATABASE      -> ClickHouse.Database
//	EVENT_COLLECTOR_CH_USERNAME      -> ClickHouse.Username
//	EVENT_COLLECTOR_CH_PASSWORD      -> ClickHouse.Password
//	EVENT_COLLECTOR_LOG_LEVEL        -> Logger.Level
//	EVENT_COLLECTOR_WORKER_POOL_SIZE -> WorkerPool.Size
//	EVENT_COLLECTOR_JWT_SIGNING_KEY  -> Auth.SigningKey
//	EVENT_COLLECTOR_JWT_ISSUER       -> Auth.Issuer
//	EVENT_COLLECTOR_EDUSER_ENDPOINT  -> Session.EduserEndpoint
//	EVENT_COLLECTOR_CORS_ORIGINS     -> CORS.AllowedOrigins (comma-separated)
//	EVENT_COLLECTOR_MINIO_ENDPOINT  -> MinIO.Endpoint
//	EVENT_COLLECTOR_MINIO_ACCESS_KEY -> MinIO.AccessKey
//	EVENT_COLLECTOR_MINIO_SECRET_KEY -> MinIO.SecretKey
//	EVENT_COLLECTOR_MINIO_BUCKET    -> MinIO.Bucket
//	EVENT_COLLECTOR_MINIO_REGION    -> MinIO.Region
//	EVENT_COLLECTOR_MINIO_USE_SSL   -> MinIO.UseSSL
func LoadWithEnv(path string) (*Config, error) {
	cfg, err := Load(path)
	if err != nil {
		return nil, err
	}

	applyEnvOverrides(cfg)

	// Re-validate after environment overrides have been applied, because an
	// environment variable could have introduced an invalid value.
	if err := validate(cfg); err != nil {
		return nil, fmt.Errorf("config: validation failed after env overrides: %w", err)
	}

	return cfg, nil
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

// applyDefaults fills in zero-valued fields with production-sensible defaults.
// This function is idempotent — calling it multiple times produces the same result.
func applyDefaults(cfg *Config) {
	// --- Server ---
	if cfg.Server.GRPCPort == 0 {
		cfg.Server.GRPCPort = 50051
	}
	if cfg.Server.HTTPPort == 0 {
		cfg.Server.HTTPPort = 8080
	}
	if cfg.Server.ShutdownTimeout == 0 {
		cfg.Server.ShutdownTimeout = 30 * time.Second
	}

	// --- Kafka ---
	if len(cfg.Kafka.Brokers) == 0 {
		cfg.Kafka.Brokers = []string{"localhost:9092"}
	}
	if cfg.Kafka.RequiredAcks == 0 {
		cfg.Kafka.RequiredAcks = -1 // WaitForAll — safest default.
	}
	if cfg.Kafka.MaxMessageBytes == 0 {
		cfg.Kafka.MaxMessageBytes = 1_048_576 // 1 MiB.
	}
	if cfg.Kafka.FlushFrequency == 0 {
		cfg.Kafka.FlushFrequency = 100 * time.Millisecond
	}
	if cfg.Kafka.FlushMessages == 0 {
		cfg.Kafka.FlushMessages = 100
	}
	if cfg.Kafka.RetryMax == 0 {
		cfg.Kafka.RetryMax = 3
	}
	if cfg.Kafka.CompressionCodec == "" {
		cfg.Kafka.CompressionCodec = "zstd"
	}

	// --- ClickHouse ---
	if len(cfg.ClickHouse.Addrs) == 0 {
		cfg.ClickHouse.Addrs = []string{"localhost:9000"}
	}
	if cfg.ClickHouse.Database == "" {
		cfg.ClickHouse.Database = "argus"
	}
	if cfg.ClickHouse.Username == "" {
		cfg.ClickHouse.Username = "default"
	}
	if cfg.ClickHouse.BatchSize == 0 {
		cfg.ClickHouse.BatchSize = 1000
	}
	if cfg.ClickHouse.FlushInterval == 0 {
		cfg.ClickHouse.FlushInterval = 5 * time.Second
	}
	if cfg.ClickHouse.MaxRetries == 0 {
		cfg.ClickHouse.MaxRetries = 3
	}

	// --- PostgreSQL ---
	if cfg.Postgres.Host == "" {
		cfg.Postgres.Host = "localhost"
	}
	if cfg.Postgres.Port == 0 {
		cfg.Postgres.Port = 5432
	}
	if cfg.Postgres.Database == "" {
		cfg.Postgres.Database = "argus"
	}
	if cfg.Postgres.Username == "" {
		cfg.Postgres.Username = "argus"
	}
	if cfg.Postgres.Password == "" {
		cfg.Postgres.Password = "argus"
	}
	if cfg.Postgres.SSLMode == "" {
		cfg.Postgres.SSLMode = "disable"
	}
	if cfg.Postgres.MaxOpenConns == 0 {
		cfg.Postgres.MaxOpenConns = 25
	}
	if cfg.Postgres.MaxIdleConns == 0 {
		cfg.Postgres.MaxIdleConns = 10
	}
	if cfg.Postgres.ConnMaxLifetime == 0 {
		cfg.Postgres.ConnMaxLifetime = 5 * time.Minute
	}

	// --- MinIO ---
	if cfg.MinIO.Endpoint == "" {
		cfg.MinIO.Endpoint = "localhost:9002"
	}
	if cfg.MinIO.AccessKey == "" {
		cfg.MinIO.AccessKey = "argus-minio-admin"
	}
	if cfg.MinIO.SecretKey == "" {
		cfg.MinIO.SecretKey = "argus-minio-secret-change-in-production"
	}
	if cfg.MinIO.Bucket == "" {
		cfg.MinIO.Bucket = "argus-evidence"
	}
	if cfg.MinIO.Region == "" {
		cfg.MinIO.Region = "us-east-1"
	}
	if cfg.MinIO.PresignTTL == 0 {
		cfg.MinIO.PresignTTL = 5 * time.Minute
	}
	if cfg.MinIO.UploadTimeout == 0 {
		cfg.MinIO.UploadTimeout = 30 * time.Second
	}

	// --- Logger ---
	if cfg.Logger.Level == "" {
		cfg.Logger.Level = "info"
	}
	if cfg.Logger.Encoding == "" {
		cfg.Logger.Encoding = "json"
	}

	// --- Rate Limit ---
	if cfg.RateLimit.GlobalRPS == 0 {
		cfg.RateLimit.GlobalRPS = 50_000
	}
	if cfg.RateLimit.GlobalBurst == 0 {
		cfg.RateLimit.GlobalBurst = 100_000
	}
	if cfg.RateLimit.PerSessionRPS == 0 {
		cfg.RateLimit.PerSessionRPS = 100
	}
	if cfg.RateLimit.PerSessionBurst == 0 {
		cfg.RateLimit.PerSessionBurst = 200
	}
	if cfg.RateLimit.HTTPGlobalRPS == 0 {
		cfg.RateLimit.HTTPGlobalRPS = 1000
	}
	if cfg.RateLimit.HTTPGlobalBurst == 0 {
		cfg.RateLimit.HTTPGlobalBurst = 2000
	}
	if cfg.RateLimit.HTTPPerIPRPS == 0 {
		cfg.RateLimit.HTTPPerIPRPS = 100
	}
	if cfg.RateLimit.HTTPPerIPBurst == 0 {
		cfg.RateLimit.HTTPPerIPBurst = 200
	}
	if cfg.RateLimit.ReconnectBurstRPS == 0 {
		cfg.RateLimit.ReconnectBurstRPS = 5000
	}
	if cfg.RateLimit.ReconnectBurstSize == 0 {
		cfg.RateLimit.ReconnectBurstSize = 10000
	}

	// --- Worker Pool ---
	if cfg.WorkerPool.Size == 0 {
		cfg.WorkerPool.Size = 256
	}
	if cfg.WorkerPool.QueueSize == 0 {
		cfg.WorkerPool.QueueSize = 10_000
	}

	// --- Auth ---
	// Auth.Enabled defaults to false (safe for development).
	// In production, set auth.enabled: true in config or via env.
	if cfg.Auth.Issuer == "" {
		cfg.Auth.Issuer = "eduser"
	}
	if cfg.Auth.Audience == "" {
		cfg.Auth.Audience = "argus-event-collector"
	}
	if cfg.Auth.ClockSkew == 0 {
		cfg.Auth.ClockSkew = 30 * time.Second
	}

	// --- Session ---
	if cfg.Session.CacheTTL == 0 {
		cfg.Session.CacheTTL = 5 * time.Minute
	}
	if cfg.Session.NegativeCacheTTL == 0 {
		cfg.Session.NegativeCacheTTL = 30 * time.Second
	}
	if cfg.Session.CleanupInterval == 0 {
		cfg.Session.CleanupInterval = 1 * time.Minute
	}
	if cfg.Session.EduserTimeout == 0 {
		cfg.Session.EduserTimeout = 3 * time.Second
	}

	// --- CORS ---
	if len(cfg.CORS.AllowedOrigins) == 0 {
		cfg.CORS.AllowedOrigins = []string{"*"}
	}
	if cfg.CORS.MaxAge == "" {
		cfg.CORS.MaxAge = "86400"
	}
	// AllowCredentials defaults to false; set explicitly in config.

	// --- Export ---
	if cfg.Export.MaxSessionsPerExport == 0 {
		cfg.Export.MaxSessionsPerExport = 50
	}
	if cfg.Export.PresignTTL == 0 {
		cfg.Export.PresignTTL = 24 * time.Hour
	}
	if cfg.Export.MaxArchiveSize == 0 {
		cfg.Export.MaxArchiveSize = 5 * 1024 * 1024 * 1024 // 5GB
	}
	if cfg.Export.PollInterval == 0 {
		cfg.Export.PollInterval = 5 * time.Second
	}
	if cfg.Export.WorkerCount == 0 {
		cfg.Export.WorkerCount = 2
	}
	if cfg.Export.ExportBucket == "" {
		cfg.Export.ExportBucket = "argus-exports"
	}

	// --- Chunk ---
	if cfg.Chunk.MaxChunkSize == 0 {
		cfg.Chunk.MaxChunkSize = 256 * 1024 // 256KB
	}
	if cfg.Chunk.MaxFragmentSize == 0 {
		cfg.Chunk.MaxFragmentSize = 50 * 1024 * 1024 // 50MB
	}
	if cfg.Chunk.ChunkTTL == 0 {
		cfg.Chunk.ChunkTTL = 5 * time.Minute
	}
	if cfg.Chunk.MaxConcurrentAssemblies == 0 {
		cfg.Chunk.MaxConcurrentAssemblies = 100
	}

	// --- DLQ (BadgerDB dead-letter queue) ---
	// Enabled defaults to true (struct zero-value is false, so we use a separate check).
	if cfg.DLQ.DataDir == "" {
		cfg.DLQ.DataDir = "/tmp/argus-dlq"
	}
	if cfg.DLQ.GCInterval == 0 {
		cfg.DLQ.GCInterval = 5 * time.Minute
	}
	if cfg.DLQ.GCDiscardRatio == 0 {
		cfg.DLQ.GCDiscardRatio = 0.5
	}
	if cfg.DLQ.MaxEntries == 0 {
		cfg.DLQ.MaxEntries = 1_000_000
	}
	if cfg.DLQ.ReclamationInterval == 0 {
		cfg.DLQ.ReclamationInterval = 30 * time.Second
	}
	if cfg.DLQ.ReclamationBatchSize == 0 {
		cfg.DLQ.ReclamationBatchSize = 100
	}
	if cfg.DLQ.HeartbeatInterval == 0 {
		cfg.DLQ.HeartbeatInterval = 5 * time.Minute
	}
	if cfg.DLQ.CircuitBreakerFailureThreshold == 0 {
		cfg.DLQ.CircuitBreakerFailureThreshold = 5
	}
	if cfg.DLQ.CircuitBreakerTimeout == 0 {
		cfg.DLQ.CircuitBreakerTimeout = 30 * time.Second
	}

	// --- Redis (asynq job queue) ---
	if cfg.Redis.Addr == "" {
		cfg.Redis.Addr = "localhost:6379"
	}
	if cfg.Redis.WorkerConcurrency == 0 {
		cfg.Redis.WorkerConcurrency = 10
	}
	if cfg.Redis.InferenceConcurrency == 0 {
		cfg.Redis.InferenceConcurrency = 2
	}

	// --- Backfiller (Kafka-to-ClickHouse replay, ADR-007) ---
	if cfg.Backfiller.BatchSize == 0 {
		cfg.Backfiller.BatchSize = 500
	}
	if cfg.Backfiller.FlushInterval == 0 {
		cfg.Backfiller.FlushInterval = 5 * time.Second
	}
	if cfg.Backfiller.PauseOnError == 0 {
		cfg.Backfiller.PauseOnError = 10 * time.Second
	}

	// --- Inference (backend AI gateway) ---
	if cfg.Inference.GRPCHost == "" {
		cfg.Inference.GRPCHost = "localhost"
	}
	if cfg.Inference.GRPCPort == 0 {
		cfg.Inference.GRPCPort = 50061
	}
	if cfg.Inference.EngineType == "" {
		cfg.Inference.EngineType = "python_bridge"
	}
	if cfg.Inference.PythonBridgeURL == "" {
		cfg.Inference.PythonBridgeURL = "http://localhost:8091"
	}
	if cfg.Inference.BridgeTimeoutSec == 0 {
		cfg.Inference.BridgeTimeoutSec = 5
	}
	if cfg.Inference.MaxFrameBytes == 0 {
		cfg.Inference.MaxFrameBytes = 10 * 1024 * 1024 // 10 MiB
	}
	if cfg.Inference.MaxVideoDurSec == 0 {
		cfg.Inference.MaxVideoDurSec = 300 // 5 minutes
	}
	if cfg.Inference.FrameSampleRate == 0 {
		cfg.Inference.FrameSampleRate = 2
	}
	if cfg.Inference.FrameSampleIntervalSec == 0 {
		cfg.Inference.FrameSampleIntervalSec = 5
	}
	if cfg.Inference.FaceMismatchThreshold == 0 {
		cfg.Inference.FaceMismatchThreshold = 0.6
	}
	if cfg.Inference.LivenessThreshold == 0 {
		cfg.Inference.LivenessThreshold = 0.3
	}
	if cfg.Inference.ObjectConfidenceThreshold == 0 {
		cfg.Inference.ObjectConfidenceThreshold = 0.7
	}
	if cfg.Inference.SpoofConfidenceThreshold == 0 {
		cfg.Inference.SpoofConfidenceThreshold = 0.7
	}
	if cfg.Inference.Concurrency == 0 {
		cfg.Inference.Concurrency = 4
	}

	// --- Audio bridge (browser audio telemetry aggregation) ---
	if cfg.AudioBridge.NoiseThresholdDb == 0 {
		cfg.AudioBridge.NoiseThresholdDb = -35
	}
	if cfg.AudioBridge.VADConfidenceThreshold == 0 {
		cfg.AudioBridge.VADConfidenceThreshold = 0.7
	}
	if cfg.AudioBridge.ConsecutiveEvents == 0 {
		cfg.AudioBridge.ConsecutiveEvents = 3
	}
	if cfg.AudioBridge.CooldownEvents == 0 {
		cfg.AudioBridge.CooldownEvents = 12
	}
	if cfg.AudioBridge.DerivedEventConfidence == 0 {
		cfg.AudioBridge.DerivedEventConfidence = 0.85
	}

	// --- Webhook dispatcher ---
	if cfg.Webhook.PollInterval == 0 {
		cfg.Webhook.PollInterval = 1 * time.Second
	}
	if cfg.Webhook.BatchSize == 0 {
		cfg.Webhook.BatchSize = 50
	}
	if cfg.Webhook.MaxRetries == 0 {
		cfg.Webhook.MaxRetries = 5
	}
	if cfg.Webhook.TimeoutSec == 0 {
		cfg.Webhook.TimeoutSec = 30
	}
	if cfg.Webhook.MaxConcurrent == 0 {
		cfg.Webhook.MaxConcurrent = 10
	}
}

// ---------------------------------------------------------------------------
// Environment variable overrides
// ---------------------------------------------------------------------------

// applyEnvOverrides reads well-known environment variables and writes their
// values into cfg, overriding whatever was loaded from the YAML file. Only
// non-empty environment variables trigger an override.
func applyEnvOverrides(cfg *Config) {
	// Server ports.
	if v := os.Getenv("EVENT_COLLECTOR_GRPC_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.Server.GRPCPort = port
		}
	}
	if v := os.Getenv("EVENT_COLLECTOR_HTTP_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.Server.HTTPPort = port
		}
	}

	// Kafka brokers (comma-separated, e.g. "broker1:9092,broker2:9092").
	if v := os.Getenv("EVENT_COLLECTOR_KAFKA_BROKERS"); v != "" {
		brokers := splitAndTrim(v)
		if len(brokers) > 0 {
			cfg.Kafka.Brokers = brokers
		}
	}
	if v := os.Getenv("EVENT_COLLECTOR_KAFKA_AUTO_CREATE_TOPICS"); v != "" {
		cfg.Kafka.AutoCreateTopics = v == "true" || v == "1"
	}
	if v := os.Getenv("EVENT_COLLECTOR_KAFKA_TOPIC_PARTITIONS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Kafka.TopicPartitions = n
		}
	}
	if v := os.Getenv("EVENT_COLLECTOR_KAFKA_TOPIC_REPLICATION"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Kafka.TopicReplication = n
		}
	}

	// ClickHouse addresses (comma-separated).
	if v := os.Getenv("EVENT_COLLECTOR_CH_ADDRS"); v != "" {
		addrs := splitAndTrim(v)
		if len(addrs) > 0 {
			cfg.ClickHouse.Addrs = addrs
		}
	}

	// ClickHouse credentials and database.
	if v := os.Getenv("EVENT_COLLECTOR_CH_DATABASE"); v != "" {
		cfg.ClickHouse.Database = v
	}
	if v := os.Getenv("EVENT_COLLECTOR_CH_USERNAME"); v != "" {
		cfg.ClickHouse.Username = v
	}
	if v := os.Getenv("EVENT_COLLECTOR_CH_PASSWORD"); v != "" {
		cfg.ClickHouse.Password = v
	}

	// PostgreSQL connection.
	if v := os.Getenv("EVENT_COLLECTOR_PG_HOST"); v != "" {
		cfg.Postgres.Host = v
	}
	if v := os.Getenv("EVENT_COLLECTOR_PG_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.Postgres.Port = port
		}
	}
	if v := os.Getenv("EVENT_COLLECTOR_PG_DATABASE"); v != "" {
		cfg.Postgres.Database = v
	}
	if v := os.Getenv("EVENT_COLLECTOR_PG_USERNAME"); v != "" {
		cfg.Postgres.Username = v
	}
	if v := os.Getenv("EVENT_COLLECTOR_PG_PASSWORD"); v != "" {
		cfg.Postgres.Password = v
	}
	if v := os.Getenv("EVENT_COLLECTOR_PG_SSL_MODE"); v != "" {
		cfg.Postgres.SSLMode = v
	}

	// MinIO (S3-compatible evidence storage).
	if v := os.Getenv("EVENT_COLLECTOR_MINIO_ENDPOINT"); v != "" {
		cfg.MinIO.Endpoint = v
	}
	if v := os.Getenv("EVENT_COLLECTOR_MINIO_ACCESS_KEY"); v != "" {
		cfg.MinIO.AccessKey = v
	}
	if v := os.Getenv("EVENT_COLLECTOR_MINIO_SECRET_KEY"); v != "" {
		cfg.MinIO.SecretKey = v
	}
	if v := os.Getenv("EVENT_COLLECTOR_MINIO_BUCKET"); v != "" {
		cfg.MinIO.Bucket = v
	}
	if v := os.Getenv("EVENT_COLLECTOR_MINIO_REGION"); v != "" {
		cfg.MinIO.Region = v
	}
	if v := os.Getenv("EVENT_COLLECTOR_MINIO_USE_SSL"); v != "" {
		cfg.MinIO.UseSSL = v == "true" || v == "1"
	}

	// Logger level.
	if v := os.Getenv("EVENT_COLLECTOR_LOG_LEVEL"); v != "" {
		cfg.Logger.Level = v
	}

	// Worker pool size.
	if v := os.Getenv("EVENT_COLLECTOR_WORKER_POOL_SIZE"); v != "" {
		if size, err := strconv.Atoi(v); err == nil {
			cfg.WorkerPool.Size = size
		}
	}
	if v := os.Getenv("EVENT_COLLECTOR_WORKER_POOL_QUEUE_SIZE"); v != "" {
		if qs, err := strconv.Atoi(v); err == nil {
			cfg.WorkerPool.QueueSize = qs
		}
	}

	// Auth — JWT configuration (CRITICAL: always set via env in production).
	if v := os.Getenv("EVENT_COLLECTOR_JWT_ALGORITHM"); v != "" {
		cfg.Auth.Algorithm = v
	}
	if v := os.Getenv("EVENT_COLLECTOR_JWT_PRIVATE_KEY_PATH"); v != "" {
		cfg.Auth.PrivateKeyPath = v
		cfg.Auth.Enabled = true // Auto-enable auth when key path is provided.
	}
	if v := os.Getenv("EVENT_COLLECTOR_JWT_PUBLIC_KEY_PATH"); v != "" {
		cfg.Auth.PublicKeyPath = v
		cfg.Auth.Enabled = true // Auto-enable auth when key path is provided.
	}
	if v := os.Getenv("EVENT_COLLECTOR_JWT_SIGNING_KEY"); v != "" {
		cfg.Auth.SigningKey = v
		cfg.Auth.Enabled = true // Auto-enable auth when key is provided.
	}
	if v := os.Getenv("EVENT_COLLECTOR_JWT_ISSUER"); v != "" {
		cfg.Auth.Issuer = v
	}
	if v := os.Getenv("EVENT_COLLECTOR_JWT_AUDIENCE"); v != "" {
		cfg.Auth.Audience = v
	}

	// Session — Eduser endpoint.
	if v := os.Getenv("EVENT_COLLECTOR_EDUSER_ENDPOINT"); v != "" {
		cfg.Session.EduserEndpoint = v
	}

	// CORS — allowed origins (comma-separated).
	if v := os.Getenv("EVENT_COLLECTOR_CORS_ORIGINS"); v != "" {
		origins := splitAndTrim(v)
		if len(origins) > 0 {
			cfg.CORS.AllowedOrigins = origins
		}
	}

	// Telegram — alerting bot credentials.
	if v := os.Getenv("TELEGRAM_BOT_TOKEN"); v != "" {
		cfg.Telegram.BotToken = v
	}
	if v := os.Getenv("TELEGRAM_CHAT_ID"); v != "" {
		cfg.Telegram.ChatID = v
	}

	// DLQ (BadgerDB dead-letter queue).
	if v := os.Getenv("EVENT_COLLECTOR_DLQ_ENABLED"); v != "" {
		cfg.DLQ.Enabled = v == "true" || v == "1"
	}
	if v := os.Getenv("EVENT_COLLECTOR_DLQ_DATA_DIR"); v != "" {
		cfg.DLQ.DataDir = v
	}

	// Redis (asynq job queue).
	if v := os.Getenv("EVENT_COLLECTOR_REDIS_ADDR"); v != "" {
		cfg.Redis.Addr = v
	}
	if v := os.Getenv("EVENT_COLLECTOR_REDIS_PASSWORD"); v != "" {
		cfg.Redis.Password = v
	}
	if v := os.Getenv("EVENT_COLLECTOR_REDIS_DB"); v != "" {
		if db, err := strconv.Atoi(v); err == nil {
			cfg.Redis.DB = db
		}
	}
	if v := os.Getenv("EVENT_COLLECTOR_REDIS_WORKER_CONCURRENCY"); v != "" {
		if c, err := strconv.Atoi(v); err == nil {
			cfg.Redis.WorkerConcurrency = c
		}
	}
	if v := os.Getenv("EVENT_COLLECTOR_REDIS_INFERENCE_CONCURRENCY"); v != "" {
		if c, err := strconv.Atoi(v); err == nil {
			cfg.Redis.InferenceConcurrency = c
		}
	}

	// Inference (backend AI gateway).
	if v := os.Getenv("EVENT_COLLECTOR_INFERENCE_GRPC_HOST"); v != "" {
		cfg.Inference.GRPCHost = v
	}
	if v := os.Getenv("EVENT_COLLECTOR_INFERENCE_GRPC_PORT"); v != "" {
		if port, err := strconv.Atoi(v); err == nil {
			cfg.Inference.GRPCPort = port
		}
	}
	if v := os.Getenv("EVENT_COLLECTOR_INFERENCE_ENGINE_TYPE"); v != "" {
		cfg.Inference.EngineType = v
	}
	if v := os.Getenv("EVENT_COLLECTOR_INFERENCE_ALLOW_STUB"); v != "" {
		cfg.Inference.AllowStub = v == "true" || v == "1"
	}
	if v := os.Getenv("EVENT_COLLECTOR_INFERENCE_MODEL_DIR"); v != "" {
		cfg.Inference.ModelDir = v
	}
	if v := os.Getenv("EVENT_COLLECTOR_INFERENCE_PYTHON_BRIDGE_URL"); v != "" {
		cfg.Inference.PythonBridgeURL = v
	}
	if v := os.Getenv("EVENT_COLLECTOR_INFERENCE_BRIDGE_TIMEOUT_SEC"); v != "" {
		if seconds, err := strconv.Atoi(v); err == nil {
			cfg.Inference.BridgeTimeoutSec = seconds
		}
	}
	if v := os.Getenv("EVENT_COLLECTOR_INFERENCE_FRAME_SAMPLE_INTERVAL_SEC"); v != "" {
		if seconds, err := strconv.Atoi(v); err == nil {
			cfg.Inference.FrameSampleIntervalSec = seconds
		}
	}
	if v := os.Getenv("EVENT_COLLECTOR_INFERENCE_MAX_VIDEO_DUR_SEC"); v != "" {
		if seconds, err := strconv.Atoi(v); err == nil {
			cfg.Inference.MaxVideoDurSec = seconds
		}
	}
	if v := os.Getenv("EVENT_COLLECTOR_INFERENCE_MAX_FRAME_BYTES"); v != "" {
		if bytes, err := strconv.Atoi(v); err == nil {
			cfg.Inference.MaxFrameBytes = bytes
		}
	}
	if v := os.Getenv("EVENT_COLLECTOR_INFERENCE_FACE_MISMATCH_THRESHOLD"); v != "" {
		if threshold, err := strconv.ParseFloat(v, 32); err == nil {
			cfg.Inference.FaceMismatchThreshold = float32(threshold)
		}
	}
	if v := os.Getenv("EVENT_COLLECTOR_INFERENCE_LIVENESS_THRESHOLD"); v != "" {
		if threshold, err := strconv.ParseFloat(v, 32); err == nil {
			cfg.Inference.LivenessThreshold = float32(threshold)
		}
	}
	if v := os.Getenv("EVENT_COLLECTOR_INFERENCE_OBJECT_CONFIDENCE_THRESHOLD"); v != "" {
		if threshold, err := strconv.ParseFloat(v, 32); err == nil {
			cfg.Inference.ObjectConfidenceThreshold = float32(threshold)
		}
	}
	if v := os.Getenv("EVENT_COLLECTOR_INFERENCE_SPOOF_CONFIDENCE_THRESHOLD"); v != "" {
		if threshold, err := strconv.ParseFloat(v, 32); err == nil {
			cfg.Inference.SpoofConfidenceThreshold = float32(threshold)
		}
	}
	if v := os.Getenv("EVENT_COLLECTOR_INFERENCE_CONCURRENCY"); v != "" {
		if c, err := strconv.Atoi(v); err == nil {
			cfg.Inference.Concurrency = c
		}
	}

	// Audio bridge (browser audio telemetry aggregation).
	if v := os.Getenv("EVENT_COLLECTOR_AUDIO_BRIDGE_ENABLED"); v != "" {
		cfg.AudioBridge.Enabled = v == "true" || v == "1"
	}
	if v := os.Getenv("EVENT_COLLECTOR_AUDIO_NOISE_THRESHOLD_DB"); v != "" {
		if threshold, err := strconv.ParseFloat(v, 32); err == nil {
			cfg.AudioBridge.NoiseThresholdDb = float32(threshold)
		}
	}
	if v := os.Getenv("EVENT_COLLECTOR_AUDIO_VAD_CONFIDENCE_THRESHOLD"); v != "" {
		if threshold, err := strconv.ParseFloat(v, 32); err == nil {
			cfg.AudioBridge.VADConfidenceThreshold = float32(threshold)
		}
	}
	if v := os.Getenv("EVENT_COLLECTOR_AUDIO_CONSECUTIVE_EVENTS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.AudioBridge.ConsecutiveEvents = n
		}
	}
	if v := os.Getenv("EVENT_COLLECTOR_AUDIO_COOLDOWN_EVENTS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.AudioBridge.CooldownEvents = n
		}
	}
	if v := os.Getenv("EVENT_COLLECTOR_AUDIO_DERIVED_EVENT_CONFIDENCE"); v != "" {
		if confidence, err := strconv.ParseFloat(v, 32); err == nil {
			cfg.AudioBridge.DerivedEventConfidence = float32(confidence)
		}
	}

	// Backfiller (Kafka-to-ClickHouse replay, ADR-007).
	if v := os.Getenv("EVENT_COLLECTOR_BACKFILLER_ENABLED"); v != "" {
		cfg.Backfiller.Enabled = v == "true" || v == "1"
	}

	// Crypto-erasure (GDPR envelope encryption).
	if v := os.Getenv("ARGUS_CRYPTO_ENABLED"); v != "" {
		cfg.CryptoErase.Enabled = v == "true" || v == "1"
	}
	if v := os.Getenv("ARGUS_CRYPTO_KEK"); v != "" {
		cfg.CryptoErase.KEKHex = v
	}

	// Webhook dispatcher.
	if v := os.Getenv("EVENT_COLLECTOR_WEBHOOK_ENABLED"); v != "" {
		cfg.Webhook.Enabled = v == "true" || v == "1"
	}
	if v := os.Getenv("EVENT_COLLECTOR_WEBHOOK_MAX_RETRIES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Webhook.MaxRetries = n
		}
	}
	if v := os.Getenv("EVENT_COLLECTOR_WEBHOOK_TIMEOUT_SEC"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Webhook.TimeoutSec = n
		}
	}
	if v := os.Getenv("EVENT_COLLECTOR_WEBHOOK_MAX_CONCURRENT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.Webhook.MaxConcurrent = n
		}
	}
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

// validate checks that the configuration is internally consistent and that
// all required fields are populated. It returns a descriptive error on the
// first violation found.
func validate(cfg *Config) error {
	// --- Server ---
	if cfg.Server.GRPCPort < 1 || cfg.Server.GRPCPort > 65535 {
		return fmt.Errorf("server.grpc_port must be between 1 and 65535, got %d", cfg.Server.GRPCPort)
	}
	if cfg.Server.HTTPPort < 1 || cfg.Server.HTTPPort > 65535 {
		return fmt.Errorf("server.http_port must be between 1 and 65535, got %d", cfg.Server.HTTPPort)
	}
	if cfg.Server.GRPCPort == cfg.Server.HTTPPort {
		return fmt.Errorf("server.grpc_port and server.http_port must differ (both are %d)", cfg.Server.GRPCPort)
	}
	if cfg.Server.ShutdownTimeout < 1*time.Second {
		return fmt.Errorf("server.shutdown_timeout must be at least 1s, got %s", cfg.Server.ShutdownTimeout)
	}

	// --- Kafka ---
	if len(cfg.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka.brokers must contain at least one broker address")
	}
	for i, b := range cfg.Kafka.Brokers {
		if b == "" {
			return fmt.Errorf("kafka.brokers[%d] is empty", i)
		}
	}
	validAcks := map[int]bool{-1: true, 0: true, 1: true}
	if !validAcks[cfg.Kafka.RequiredAcks] {
		return fmt.Errorf("kafka.required_acks must be -1, 0, or 1, got %d", cfg.Kafka.RequiredAcks)
	}
	validCodecs := map[string]bool{"zstd": true, "snappy": true, "lz4": true, "none": true}
	if !validCodecs[cfg.Kafka.CompressionCodec] {
		return fmt.Errorf("kafka.compression_codec must be one of zstd/snappy/lz4/none, got %q", cfg.Kafka.CompressionCodec)
	}

	// --- ClickHouse ---
	if len(cfg.ClickHouse.Addrs) == 0 {
		return fmt.Errorf("clickhouse.addrs must contain at least one address")
	}
	for i, a := range cfg.ClickHouse.Addrs {
		if a == "" {
			return fmt.Errorf("clickhouse.addrs[%d] is empty", i)
		}
	}
	if cfg.ClickHouse.Database == "" {
		return fmt.Errorf("clickhouse.database must not be empty")
	}
	if cfg.ClickHouse.BatchSize < 1 {
		return fmt.Errorf("clickhouse.batch_size must be at least 1, got %d", cfg.ClickHouse.BatchSize)
	}
	if cfg.ClickHouse.FlushInterval < 100*time.Millisecond {
		return fmt.Errorf("clickhouse.flush_interval must be at least 100ms, got %s", cfg.ClickHouse.FlushInterval)
	}

	// --- Logger ---
	validLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLevels[cfg.Logger.Level] {
		return fmt.Errorf("logger.level must be one of debug/info/warn/error, got %q", cfg.Logger.Level)
	}
	validEncodings := map[string]bool{"json": true, "console": true}
	if !validEncodings[cfg.Logger.Encoding] {
		return fmt.Errorf("logger.encoding must be json or console, got %q", cfg.Logger.Encoding)
	}

	// --- Rate Limit ---
	if cfg.RateLimit.GlobalRPS <= 0 {
		return fmt.Errorf("rate_limit.global_rps must be positive, got %f", cfg.RateLimit.GlobalRPS)
	}
	if cfg.RateLimit.GlobalBurst < 1 {
		return fmt.Errorf("rate_limit.global_burst must be at least 1, got %d", cfg.RateLimit.GlobalBurst)
	}
	if cfg.RateLimit.PerSessionRPS <= 0 {
		return fmt.Errorf("rate_limit.per_session_rps must be positive, got %f", cfg.RateLimit.PerSessionRPS)
	}
	if cfg.RateLimit.PerSessionBurst < 1 {
		return fmt.Errorf("rate_limit.per_session_burst must be at least 1, got %d", cfg.RateLimit.PerSessionBurst)
	}

	// --- Worker Pool ---
	if cfg.WorkerPool.Size < 1 {
		return fmt.Errorf("worker_pool.size must be at least 1, got %d", cfg.WorkerPool.Size)
	}
	if cfg.WorkerPool.QueueSize < 1 {
		return fmt.Errorf("worker_pool.queue_size must be at least 1, got %d", cfg.WorkerPool.QueueSize)
	}

	// --- Inference ---
	if cfg.Inference.GRPCPort < 1 || cfg.Inference.GRPCPort > 65535 {
		return fmt.Errorf("inference.grpc_port must be between 1 and 65535, got %d", cfg.Inference.GRPCPort)
	}
	if cfg.Inference.EngineType == "python_bridge" && strings.TrimSpace(cfg.Inference.PythonBridgeURL) == "" {
		return fmt.Errorf("inference.python_bridge_url is required when engine_type=python_bridge")
	}
	if cfg.Inference.BridgeTimeoutSec < 1 {
		return fmt.Errorf("inference.bridge_timeout_sec must be at least 1, got %d", cfg.Inference.BridgeTimeoutSec)
	}
	if cfg.Inference.MaxFrameBytes < 1 {
		return fmt.Errorf("inference.max_frame_bytes must be positive, got %d", cfg.Inference.MaxFrameBytes)
	}
	if cfg.Inference.FrameSampleRate < 1 {
		return fmt.Errorf("inference.frame_sample_rate must be at least 1, got %d", cfg.Inference.FrameSampleRate)
	}
	if cfg.Inference.FrameSampleIntervalSec < 1 {
		return fmt.Errorf("inference.frame_sample_interval_sec must be at least 1, got %d", cfg.Inference.FrameSampleIntervalSec)
	}
	if cfg.Inference.Concurrency < 1 {
		return fmt.Errorf("inference.concurrency must be at least 1, got %d", cfg.Inference.Concurrency)
	}
	if err := validateUnitThreshold("inference.face_mismatch_threshold", cfg.Inference.FaceMismatchThreshold); err != nil {
		return err
	}
	if err := validateUnitThreshold("inference.liveness_threshold", cfg.Inference.LivenessThreshold); err != nil {
		return err
	}
	if err := validateUnitThreshold("inference.object_confidence_threshold", cfg.Inference.ObjectConfidenceThreshold); err != nil {
		return err
	}
	if err := validateUnitThreshold("inference.spoof_confidence_threshold", cfg.Inference.SpoofConfidenceThreshold); err != nil {
		return err
	}

	// --- Audio Bridge ---
	if cfg.AudioBridge.Enabled {
		if cfg.AudioBridge.NoiseThresholdDb >= 0 {
			return fmt.Errorf("audio_bridge.noise_threshold_db must be negative dB, got %f", cfg.AudioBridge.NoiseThresholdDb)
		}
		if err := validateUnitThreshold("audio_bridge.vad_confidence_threshold", cfg.AudioBridge.VADConfidenceThreshold); err != nil {
			return err
		}
		if cfg.AudioBridge.ConsecutiveEvents < 1 {
			return fmt.Errorf("audio_bridge.consecutive_events must be at least 1, got %d", cfg.AudioBridge.ConsecutiveEvents)
		}
		if cfg.AudioBridge.CooldownEvents < 0 {
			return fmt.Errorf("audio_bridge.cooldown_events must be >= 0, got %d", cfg.AudioBridge.CooldownEvents)
		}
		if err := validateUnitThreshold("audio_bridge.derived_event_confidence", cfg.AudioBridge.DerivedEventConfidence); err != nil {
			return err
		}
	}

	// --- Auth ---
	if cfg.Auth.Enabled {
		alg := cfg.Auth.Algorithm
		if alg == "" {
			alg = "RS256" // Default to RS256.
		}
		switch alg {
		case "RS256":
			// RS256 requires at least a public key path (or private key path
			// from which the public key is derived).
			if cfg.Auth.PublicKeyPath == "" && cfg.Auth.PrivateKeyPath == "" {
				return fmt.Errorf("auth: RS256 requires public_key_path or private_key_path when auth is enabled")
			}
		case "HS256":
			if cfg.Auth.SigningKey == "" {
				return fmt.Errorf("auth.signing_key must not be empty when auth is enabled with HS256")
			}
			if len(cfg.Auth.SigningKey) < 32 {
				return fmt.Errorf("auth.signing_key must be at least 32 characters for HMAC-SHA256, got %d", len(cfg.Auth.SigningKey))
			}
		default:
			return fmt.Errorf("auth.algorithm must be RS256 or HS256, got %q", alg)
		}
	}

	// --- Telegram (advisory) ---
	// Alerting is optional, but log a clear warning so operators know it's disabled.
	if cfg.Telegram.ChatID == "" {
		fmt.Fprintln(os.Stderr, "[WARN] TELEGRAM_CHAT_ID is not set — Telegram alerting is DISABLED. Set it to enable critical infrastructure alerts.")
	}

	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// splitAndTrim splits a comma-separated string and trims whitespace from each
// element. Empty elements after trimming are discarded.
func splitAndTrim(s string) []string {
	raw := strings.Split(s, ",")
	result := make([]string, 0, len(raw))
	for _, item := range raw {
		trimmed := strings.TrimSpace(item)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func validateUnitThreshold(name string, value float32) error {
	if value <= 0 || value > 1 {
		return fmt.Errorf("%s must be > 0 and <= 1, got %f", name, value)
	}
	return nil
}
