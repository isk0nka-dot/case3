// Package main is the entry point for the Argus AI background Worker service.
//
// The Worker service processes CPU-bound tasks (video export, forensic PDF
// generation) that are dispatched via Redis-backed asynq queues. It scales
// independently from the API server, ensuring that heavy archive creation
// and PDF rendering never impact API latency.
//
// Supported job types:
//
//   - job:video_export    — TAR.GZ archive of session evidence
//   - job:forensic_report — PDF forensic integrity report
//
// The Worker shares the same config file as the API server and connects
// to the same PostgreSQL, ClickHouse, MinIO, and Telegram instances.
//
// Usage:
//
//	go run cmd/worker/main.go -config deployments/config.yaml
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hibiken/asynq"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/argus-ai/event-collector/internal/infrastructure/alerting"
	"github.com/argus-ai/event-collector/internal/infrastructure/clickhouse"
	"github.com/argus-ai/event-collector/internal/infrastructure/config"
	integrityInfra "github.com/argus-ai/event-collector/internal/infrastructure/integrity"
	minioStore "github.com/argus-ai/event-collector/internal/infrastructure/minio"
	"github.com/argus-ai/event-collector/internal/infrastructure/postgres"
	"github.com/argus-ai/event-collector/internal/infrastructure/worker"
)

var (
	version   = "dev"
	buildTime = "unknown"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "argus-worker: fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// =================================================================
	// STEP 1: Parse CLI flags.
	// =================================================================
	configPath := flag.String("config", "deployments/config.yaml", "path to the YAML configuration file")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("argus-worker version=%s build_time=%s\n", version, buildTime)
		return nil
	}

	// =================================================================
	// STEP 2: Load configuration (shared with API server).
	// =================================================================
	cfg, err := config.LoadWithEnv(*configPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// =================================================================
	// STEP 3: Initialise logger.
	// =================================================================
	logger, err := buildLogger(cfg.Logger)
	if err != nil {
		return fmt.Errorf("failed to initialise logger: %w", err)
	}
	defer func() { _ = logger.Sync() }()

	logger.Info("starting argus-worker",
		zap.String("version", version),
		zap.String("build_time", buildTime),
		zap.String("redis_addr", cfg.Redis.Addr),
		zap.Int("concurrency", cfg.Redis.WorkerConcurrency),
	)

	// =================================================================
	// STEP 4: Initialise ClickHouse (read-only, for queries).
	// =================================================================
	chWriter, err := clickhouse.NewWriter(clickhouse.WriterConfig{
		Addrs:         cfg.ClickHouse.Addrs,
		Database:      cfg.ClickHouse.Database,
		Username:      cfg.ClickHouse.Username,
		Password:      cfg.ClickHouse.Password,
		BatchSize:     cfg.ClickHouse.BatchSize,
		FlushInterval: cfg.ClickHouse.FlushInterval,
		MaxRetries:    cfg.ClickHouse.MaxRetries,
	}, logger)
	if err != nil {
		return fmt.Errorf("failed to initialise clickhouse: %w", err)
	}
	defer func() {
		if err := chWriter.Close(); err != nil {
			logger.Error("clickhouse close error", zap.Error(err))
		} else {
			logger.Info("clickhouse writer closed (buffers flushed)")
		}
	}()
	logger.Info("clickhouse connected",
		zap.Strings("addrs", cfg.ClickHouse.Addrs),
		zap.String("database", cfg.ClickHouse.Database),
	)

	// =================================================================
	// STEP 5: Initialise PostgreSQL.
	// =================================================================
	pgRepo, err := postgres.NewRepository(postgres.Config{
		Host:            cfg.Postgres.Host,
		Port:            cfg.Postgres.Port,
		Database:        cfg.Postgres.Database,
		Username:        cfg.Postgres.Username,
		Password:        cfg.Postgres.Password,
		SSLMode:         cfg.Postgres.SSLMode,
		MaxOpenConns:    cfg.Postgres.MaxOpenConns,
		MaxIdleConns:    cfg.Postgres.MaxIdleConns,
		ConnMaxLifetime: cfg.Postgres.ConnMaxLifetime,
	}, logger)
	if err != nil {
		return fmt.Errorf("failed to initialise postgres: %w", err)
	}
	defer func() {
		if err := pgRepo.Close(); err != nil {
			logger.Error("postgresql close error", zap.Error(err))
		} else {
			logger.Info("postgresql connection pool closed")
		}
	}()
	logger.Info("postgresql connected",
		zap.String("host", cfg.Postgres.Host),
		zap.Int("port", cfg.Postgres.Port),
		zap.String("database", cfg.Postgres.Database),
	)

	// =================================================================
	// STEP 6: Initialise MinIO (S3 evidence + export storage).
	// =================================================================
	evidenceStore, err := minioStore.NewStore(minioStore.StoreConfig{
		Endpoint:      cfg.MinIO.Endpoint,
		AccessKey:     cfg.MinIO.AccessKey,
		SecretKey:     cfg.MinIO.SecretKey,
		Bucket:        cfg.MinIO.Bucket,
		Region:        cfg.MinIO.Region,
		UseSSL:        cfg.MinIO.UseSSL,
		PresignTTL:    cfg.MinIO.PresignTTL,
		UploadTimeout: cfg.MinIO.UploadTimeout,
	}, logger)
	}
	if err != nil {
		logger.Error("failed to initialise minio, continuing anyway", zap.Error(err))
		evidenceStore = nil
	}
	defer func() {
		if evidenceStore != nil {
			if err := evidenceStore.Close(); err != nil {
				logger.Error("minio store close error", zap.Error(err))
			} else {
				logger.Info("minio store closed")
			}
		}
	}()

	minioClient := minioStore.Client(evidenceStore)
	if minioClient == nil {
		logger.Error("failed to extract minio client from evidence store, continuing anyway")
	}
	logger.Info("minio connected",
		zap.String("endpoint", cfg.MinIO.Endpoint),
		zap.String("bucket", cfg.MinIO.Bucket),
	)

	// =================================================================
	// STEP 7: Initialise Telegram alerter (optional).
	// =================================================================
	telegramAlerter := alerting.NewTelegramProvider(alerting.TelegramConfig{
		BotToken: cfg.Telegram.BotToken,
		ChatID:   cfg.Telegram.ChatID,
	}, logger)

	// =================================================================
	// STEP 8: Initialise integrity verifier (for forensic reports).
	// =================================================================
	integrityVerifier := integrityInfra.NewVerifier(chWriter.Conn(), evidenceStore, logger)

	// =================================================================
	// STEP 9: Create asynq task handlers.
	// =================================================================
	videoExportHandler := worker.NewVideoExportHandler(
		pgRepo.DB(), chWriter.Conn(), minioClient,
		cfg.Export, cfg.MinIO.Bucket, logger, telegramAlerter,
	)

	forensicReportHandler := worker.NewForensicReportHandler(
		chWriter.Conn(), integrityVerifier, pgRepo,
		minioClient, logger, telegramAlerter,
	)

	enrollStudentHandler := worker.NewEnrollStudentHandler(
		cfg.Inference.GRPCAddr(),
		pgRepo,
		logger,
	)

	aiAnalysisHandler := worker.NewAIAnalysisHandler(
		cfg.Inference.GRPCAddr(),
		chWriter.Conn(), chWriter, pgRepo,
		minioClient, cfg.MinIO.Bucket,
		logger, telegramAlerter,
		worker.AIAnalysisThresholds{
			FaceMismatch:     cfg.Inference.FaceMismatchThreshold,
			Liveness:         cfg.Inference.LivenessThreshold,
			ObjectConfidence: cfg.Inference.ObjectConfidenceThreshold,
			SpoofConfidence:  cfg.Inference.SpoofConfidenceThreshold,
		},
	)
	aiAnalysisHandler.ConfigureFrameExtraction(worker.FrameExtractionConfig{
		IntervalSec:    cfg.Inference.FrameSampleIntervalSec,
		MaxVideoDurSec: cfg.Inference.MaxVideoDurSec,
	})
	aiAnalysisHandler.ConfigureMaxFrameBytes(cfg.Inference.MaxFrameBytes)

	// =================================================================
	// STEP 10: Create and configure asynq servers.
	//
	// Two isolated servers prevent GPU-bound AI inference from starving
	// lightweight export/forensic/alert tasks:
	//
	//   Server 1 (general): critical + default + low queues
	//     → concurrency: cfg.Redis.WorkerConcurrency (default 10)
	//
	//   Server 2 (inference): inference queue only
	//     → concurrency: cfg.Redis.InferenceConcurrency (default 2)
	//
	// This guarantees that even if all inference slots are occupied with
	// 15-minute GPU scans, exports and forensic PDFs continue processing.
	// =================================================================

	redisOpt := asynq.RedisClientOpt{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	}

	retryFunc := func(n int, err error, t *asynq.Task) time.Duration {
		// Exponential backoff: 10s, 40s, 90s, ...
		return time.Duration(n*n) * 10 * time.Second
	}

	errorHandler := asynq.ErrorHandlerFunc(func(ctx context.Context, task *asynq.Task, err error) {
		retried, _ := asynq.GetRetryCount(ctx)
		maxRetry, _ := asynq.GetMaxRetry(ctx)
		logger.Error("asynq task failed",
			zap.String("type", task.Type()),
			zap.Int("retry", retried),
			zap.Int("max_retry", maxRetry),
			zap.Error(err),
		)

		// Only send Telegram alert on final failure (all retries exhausted).
		if retried >= maxRetry {
			worker.NotifyJobFailed(telegramAlerter, task.Type(), task.ResultWriter().TaskID(), err)
		}
	})

	// ── Server 1: General (critical + default + low) ──────────────────
	srv := asynq.NewServer(redisOpt, asynq.Config{
		Concurrency: cfg.Redis.WorkerConcurrency,
		Queues: map[string]int{
			worker.QueueCritical: 6,
			worker.QueueDefault:  3,
			worker.QueueLow:      1,
		},
		StrictPriority: true, // critical always drained before default/low
		RetryDelayFunc: retryFunc,
		ErrorHandler:   errorHandler,
		Logger:         &asynqZapLogger{logger: logger.Named("asynq.general")},
	})

	mux := asynq.NewServeMux()
	mux.Handle(worker.TypeVideoExport, videoExportHandler)
	mux.Handle(worker.TypeForensicReport, forensicReportHandler)
	mux.Handle(worker.TypeEnrollStudent, enrollStudentHandler)

	// ── Server 2: Inference (isolated GPU queue) ──────────────────────
	inferenceSrv := asynq.NewServer(redisOpt, asynq.Config{
		Concurrency: cfg.Redis.InferenceConcurrency,
		Queues: map[string]int{
			worker.QueueInference: 1,
		},
		RetryDelayFunc: retryFunc,
		ErrorHandler:   errorHandler,
		Logger:         &asynqZapLogger{logger: logger.Named("asynq.inference")},
	})

	inferenceMux := asynq.NewServeMux()
	inferenceMux.Handle(worker.TypeAIAnalysis, aiAnalysisHandler)
	inferenceMux.Handle(worker.TypeAIFrameAnalysis, aiAnalysisHandler)

	logger.Info("asynq handlers registered (dual-server isolation)",
		zap.String(worker.TypeVideoExport, "VideoExportHandler → general"),
		zap.String(worker.TypeForensicReport, "ForensicReportHandler → general"),
		zap.String(worker.TypeEnrollStudent, "EnrollStudentHandler → general"),
		zap.String(worker.TypeAIAnalysis, "AIAnalysisHandler → inference"),
		zap.String(worker.TypeAIFrameAnalysis, "AIAnalysisHandler(frame) → inference"),
		zap.Int("general_concurrency", cfg.Redis.WorkerConcurrency),
		zap.Int("inference_concurrency", cfg.Redis.InferenceConcurrency),
	)

	// =================================================================
	// STEP 11: Send startup notification.
	// =================================================================
	if telegramAlerter != nil {
		if err := telegramAlerter.SendDirect(alerting.MsgWorkerStartup(
			version, cfg.Redis.Addr, cfg.Redis.WorkerConcurrency,
			time.Now().Format("2006-01-02 15:04:05 MST"),
			[]string{"job:video_export", "job:forensic_report", "job:ai_analysis"},
		)); err != nil {
			logger.Warn("failed to send startup alert", zap.Error(err))
		}
	}

	// =================================================================
	// STEP 12: Start both servers (blocks until signal).
	// =================================================================

	// Capture SIGINT/SIGTERM for graceful shutdown of both servers.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		logger.Info("received shutdown signal", zap.String("signal", sig.String()))
		srv.Shutdown()
		inferenceSrv.Shutdown()
	}()

	// Start inference server in a background goroutine.
	inferenceErrCh := make(chan error, 1)
	go func() {
		logger.Info("inference worker starting", zap.Int("concurrency", cfg.Redis.InferenceConcurrency))
		if err := inferenceSrv.Run(inferenceMux); err != nil {
			inferenceErrCh <- fmt.Errorf("inference asynq server error: %w", err)
		}
		close(inferenceErrCh)
	}()

	logger.Info("argus-worker running — waiting for tasks...")

	// General server runs in the main goroutine.
	if err := srv.Run(mux); err != nil {
		return fmt.Errorf("general asynq server error: %w", err)
	}

	// Wait for inference server to finish.
	if err := <-inferenceErrCh; err != nil {
		return err
	}

	// =================================================================
	// STEP 13: Graceful shutdown.
	// =================================================================
	logger.Info("argus-worker stopped gracefully")

	if telegramAlerter != nil {
		if err := telegramAlerter.SendDirect(alerting.MsgWorkerShutdown(
			time.Now().Format("2006-01-02 15:04:05 MST"),
		)); err != nil {
			logger.Warn("failed to send shutdown alert", zap.Error(err))
		}
		telegramAlerter.Close()
	}

	return nil
}

// ---------------------------------------------------------------------------
// Logger
// ---------------------------------------------------------------------------

func buildLogger(cfg config.LoggerConfig) (*zap.Logger, error) {
	var level zapcore.Level
	switch cfg.Level {
	case "debug":
		level = zap.DebugLevel
	case "info":
		level = zap.InfoLevel
	case "warn":
		level = zap.WarnLevel
	case "error":
		level = zap.ErrorLevel
	default:
		level = zap.InfoLevel
	}

	zapCfg := zap.Config{
		Level:            zap.NewAtomicLevelAt(level),
		Development:      cfg.Development,
		Encoding:         cfg.Encoding,
		EncoderConfig:    zap.NewProductionEncoderConfig(),
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}

	if cfg.Encoding == "console" {
		zapCfg.EncoderConfig = zap.NewDevelopmentEncoderConfig()
	}

	return zapCfg.Build()
}

// ---------------------------------------------------------------------------
// asynq ↔ zap logger bridge
// ---------------------------------------------------------------------------

// asynqZapLogger adapts zap.Logger to the asynq.Logger interface.
type asynqZapLogger struct {
	logger *zap.Logger
}

func (l *asynqZapLogger) Debug(args ...interface{}) {
	l.logger.Debug(fmt.Sprint(args...))
}

func (l *asynqZapLogger) Info(args ...interface{}) {
	l.logger.Info(fmt.Sprint(args...))
}

func (l *asynqZapLogger) Warn(args ...interface{}) {
	l.logger.Warn(fmt.Sprint(args...))
}

func (l *asynqZapLogger) Error(args ...interface{}) {
	l.logger.Error(fmt.Sprint(args...))
}

func (l *asynqZapLogger) Fatal(args ...interface{}) {
	l.logger.Fatal(fmt.Sprint(args...))
}
