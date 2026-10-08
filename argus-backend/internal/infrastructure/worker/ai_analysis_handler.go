package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/hibiken/asynq"
	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	inferencepb "github.com/argus-ai/event-collector/api/proto/v1/inferencepb"
	"github.com/argus-ai/event-collector/internal/domain/entity"
	"github.com/argus-ai/event-collector/internal/domain/valueobject"
	"github.com/argus-ai/event-collector/internal/infrastructure/alerting"
	"github.com/argus-ai/event-collector/internal/infrastructure/clickhouse"
	"github.com/argus-ai/event-collector/internal/infrastructure/forensic"
	"github.com/argus-ai/event-collector/internal/infrastructure/postgres"
	"github.com/argus-ai/event-collector/pkg/randutil"
)

// AIAnalysisHandler processes TypeAIAnalysis asynq tasks.
// It streams session evidence to the inference gateway for GPU-accelerated
// analysis, writes detected anomalies back to ClickHouse as source=BACKEND_AI
// events, and fires CRITICAL Telegram alerts for fraud.
type AIAnalysisHandler struct {
	inferenceAddr  string
	chConn         driver.Conn
	chWriter       *clickhouse.Writer
	scorer         *forensic.Scorer
	pgRepo         *postgres.Repository
	minioClient    *minio.Client
	minioBucket    string
	alerter        alerting.Provider
	logger         *zap.Logger
	thresholds     AIAnalysisThresholds
	frameExtractor FrameExtractor
	maxFrameBytes  int
}

type AIAnalysisThresholds struct {
	FaceMismatch     float32
	Liveness         float32
	ObjectConfidence float32
	SpoofConfidence  float32
}

const defaultAIAnalysisMaxFrameBytes = 10 * 1024 * 1024

// NewAIAnalysisHandler creates a new asynq handler for AI deep scan jobs.
func NewAIAnalysisHandler(
	inferenceAddr string,
	chConn driver.Conn,
	chWriter *clickhouse.Writer,
	pgRepo *postgres.Repository,
	minioClient *minio.Client,
	minioBucket string,
	logger *zap.Logger,
	alerter alerting.Provider,
	thresholds ...AIAnalysisThresholds,
) *AIAnalysisHandler {
	h := &AIAnalysisHandler{
		inferenceAddr: inferenceAddr,
		chConn:        chConn,
		chWriter:      chWriter,
		scorer:        forensic.NewScorer(chConn, logger),
		pgRepo:        pgRepo,
		minioClient:   minioClient,
		minioBucket:   minioBucket,
		alerter:       alerter,
		logger:        logger.Named("asynq_ai_analysis"),
		thresholds:    normalizeAIAnalysisThresholds(firstAIAnalysisThresholds(thresholds)),
		maxFrameBytes: defaultAIAnalysisMaxFrameBytes,
	}
	h.ConfigureFrameExtraction(FrameExtractionConfig{})
	return h
}

func (h *AIAnalysisHandler) ConfigureFrameExtraction(cfg FrameExtractionConfig) {
	h.frameExtractor = NewFFmpegFrameExtractor(cfg)
}

func (h *AIAnalysisHandler) ConfigureMaxFrameBytes(maxBytes int) {
	if maxBytes < 1 {
		maxBytes = defaultAIAnalysisMaxFrameBytes
	}
	h.maxFrameBytes = maxBytes
}

// ProcessTask implements the asynq.Handler interface.
func (h *AIAnalysisHandler) ProcessTask(ctx context.Context, t *asynq.Task) error {
	if t.Type() == TypeAIFrameAnalysis {
		return h.processFrameTask(ctx, t)
	}

	start := time.Now()

	var payload AIAnalysisPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return fmt.Errorf("unmarshal AIAnalysisPayload: %w", err)
	}

	// Apply per-exam threshold overrides if present in payload.
	thresholds := h.thresholds
	if payload.FaceMismatchThreshold > 0 {
		thresholds.FaceMismatch = payload.FaceMismatchThreshold
	}
	if payload.LivenessThreshold > 0 {
		thresholds.Liveness = payload.LivenessThreshold
	}
	if payload.ObjectConfidenceThreshold > 0 {
		thresholds.ObjectConfidence = payload.ObjectConfidenceThreshold
	}
	if payload.SpoofConfidenceThreshold > 0 {
		thresholds.SpoofConfidence = payload.SpoofConfidenceThreshold
	}

	h.logger.Info("processing AI analysis job",
		zap.String("session_id", payload.SessionID),
		zap.String("org_id", payload.OrgID),
		zap.String("exam_id", payload.ExamID),
		zap.String("analysis_type", payload.AnalysisType),
		zap.Float32("face_mismatch_threshold", thresholds.FaceMismatch),
		zap.Float32("liveness_threshold", thresholds.Liveness),
	)

	// ── Step 0: Load reference embedding from enrollment (if not in payload) ─
	// The HTTP trigger may not supply ReferenceEmbedding; look it up from
	// student_enrollments so the inference gateway can run identity checks.
	if len(payload.ReferenceEmbedding) == 0 && h.pgRepo != nil {
		enrollment, err := h.pgRepo.GetEnrollment(ctx, payload.StudentID, payload.OrgID)
		if err != nil {
			h.logger.Warn("failed to look up enrollment, proceeding without identity check",
				zap.String("student_id", payload.StudentID),
				zap.String("org_id", payload.OrgID),
				zap.Error(err),
			)
		} else if enrollment != nil {
			payload.ReferenceEmbedding = enrollment.Embedding
			h.logger.Info("loaded reference embedding from enrollment",
				zap.String("student_id", payload.StudentID),
				zap.Int("embedding_dim", len(enrollment.Embedding)),
			)
		}
	}

	// ── Step 1: Connect to inference gateway ──────────────────────────────
	conn, err := grpc.NewClient(
		h.inferenceAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("connect to inference gateway at %s: %w", h.inferenceAddr, err)
	}
	defer conn.Close()

	client := inferencepb.NewInferenceServiceClient(conn)

	h.logger.Info("connected to inference gateway",
		zap.String("addr", h.inferenceAddr),
	)

	// ── Step 2: Query evidence fragments from ClickHouse ──────────────────
	fragments, err := h.queryEvidenceFragments(ctx, payload.SessionID)
	if err != nil {
		return fmt.Errorf("query evidence fragments for %s: %w", payload.SessionID, err)
	}

	if len(fragments) == 0 {
		h.logger.Warn("no evidence fragments found for session, skipping analysis",
			zap.String("session_id", payload.SessionID),
		)
		NotifyJobCompleted(h.alerter, TypeAIAnalysis, payload.SessionID, time.Since(start))
		return nil
	}

	h.logger.Info("found evidence fragments",
		zap.String("session_id", payload.SessionID),
		zap.Int("count", len(fragments)),
	)

	// ── Step 3: Stream each fragment to inference gateway ──────────────────
	var allFrames []*inferencepb.FrameAnalysis
	var totalSummary aggregateSummary

	for _, frag := range fragments {
		resp, err := h.analyzeFragment(ctx, client, payload, frag)
		if err != nil {
			if errors.Is(err, errUnsupportedEvidenceContentType) {
				h.logger.Info("skipping evidence fragment until bounded frame extractor is available",
					zap.String("session_id", payload.SessionID),
					zap.String("object_key", frag.ObjectKey),
					zap.String("content_type", frag.ContentType),
				)
				continue
			}

			h.logger.Warn("failed to analyze fragment, skipping",
				zap.String("session_id", payload.SessionID),
				zap.String("object_key", frag.ObjectKey),
				zap.Error(err),
			)
			continue
		}

		allFrames = append(allFrames, resp.Frames...)
		totalSummary.merge(resp.Summary)
	}

	h.logger.Info("inference analysis complete",
		zap.String("session_id", payload.SessionID),
		zap.Int("total_frames", len(allFrames)),
		zap.String("verdict", totalSummary.verdict()),
	)

	// ── Step 4: Write detected anomalies back to ClickHouse ───────────────
	eventsWritten, err := h.writeBackAnomalies(ctx, payload, allFrames, thresholds)
	if err != nil {
		h.logger.Error("failed to write anomalies to clickhouse",
			zap.String("session_id", payload.SessionID),
			zap.Error(err),
		)
	}

	h.logger.Info("anomalies written to clickhouse",
		zap.String("session_id", payload.SessionID),
		zap.Int("events_written", eventsWritten),
	)

	// ── Step 5: Critical fraud → Telegram alert + recompute score ─────────
	verdict := totalSummary.verdict()
	if verdict == "fraud" || totalSummary.maxFraudConf > 0.9 {
		h.fireFraudAlert(payload, totalSummary)
	}

	// Recompute integrity score — the write-back events are now in ClickHouse.
	if eventsWritten > 0 && h.pgRepo != nil {
		settings, settingsErr := h.pgRepo.GetExamProctoringSettings(ctx, payload.OrgID, payload.ExamID)
		if settingsErr == nil && settings != nil {
			_, scoreErr := h.scorer.ComputeScoreWithConfig(ctx, payload.SessionID, settings)
			if scoreErr != nil {
				h.logger.Warn("failed to recompute integrity score",
					zap.String("session_id", payload.SessionID),
					zap.Error(scoreErr),
				)
			}
		}
	}

	duration := time.Since(start)

	h.logger.Info("AI analysis job completed",
		zap.String("session_id", payload.SessionID),
		zap.String("verdict", verdict),
		zap.Int("events_written", eventsWritten),
		zap.Duration("duration", duration),
	)

	NotifyJobCompleted(h.alerter, TypeAIAnalysis, payload.SessionID, duration)
	return nil
}

func (h *AIAnalysisHandler) processFrameTask(ctx context.Context, t *asynq.Task) error {
	start := time.Now()

	var payload AIFrameAnalysisPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		return fmt.Errorf("unmarshal AIFrameAnalysisPayload: %w", err)
	}
	if strings.TrimSpace(payload.SessionID) == "" {
		return fmt.Errorf("AIFrameAnalysisPayload session_id is required")
	}
	if len(payload.FrameData) == 0 {
		return fmt.Errorf("AIFrameAnalysisPayload frame_data is required")
	}
	if len(payload.FrameData) > h.effectiveMaxFrameBytes() {
		return fmt.Errorf("AIFrameAnalysisPayload frame exceeds maximum size: %d bytes > %d bytes", len(payload.FrameData), h.effectiveMaxFrameBytes())
	}
	if !isSupportedEvidenceFrameContentType(payload.ContentType) {
		return fmt.Errorf("%w: %s", errUnsupportedEvidenceContentType, payload.ContentType)
	}

	thresholds := h.thresholds
	if payload.FaceMismatchThreshold > 0 {
		thresholds.FaceMismatch = payload.FaceMismatchThreshold
	}
	if payload.LivenessThreshold > 0 {
		thresholds.Liveness = payload.LivenessThreshold
	}
	if payload.ObjectConfidenceThreshold > 0 {
		thresholds.ObjectConfidence = payload.ObjectConfidenceThreshold
	}
	if payload.SpoofConfidenceThreshold > 0 {
		thresholds.SpoofConfidence = payload.SpoofConfidenceThreshold
	}

	if len(payload.ReferenceEmbedding) == 0 && h.pgRepo != nil {
		enrollment, err := h.pgRepo.GetEnrollment(ctx, payload.StudentID, payload.OrgID)
		if err != nil {
			h.logger.Warn("failed to look up enrollment for frame analysis",
				zap.String("session_id", payload.SessionID),
				zap.String("student_id", payload.StudentID),
				zap.Error(err),
			)
		} else if enrollment != nil {
			payload.ReferenceEmbedding = enrollment.Embedding
		}
	}

	conn, err := grpc.NewClient(
		h.inferenceAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("connect to inference gateway at %s: %w", h.inferenceAddr, err)
	}
	defer conn.Close()

	client := inferencepb.NewInferenceServiceClient(conn)
	resp, err := client.AnalyzeFrame(ctx, &inferencepb.AnalyzeFrameRequest{
		SessionId:          payload.SessionID,
		StudentId:          payload.StudentID,
		ExamId:             payload.ExamID,
		OrgId:              payload.OrgID,
		FrameData:          payload.FrameData,
		ContentType:        payload.ContentType,
		VideoTimestampSec:  payload.VideoTimestampSec,
		ReferenceEmbedding: append([]float32(nil), payload.ReferenceEmbedding...),
	})
	if err != nil {
		return fmt.Errorf("analyze realtime frame: %w", err)
	}

	frame := &inferencepb.FrameAnalysis{
		TimestampSec: payload.VideoTimestampSec,
		Faces:        resp.Faces,
		Objects:      resp.Objects,
		Liveness:     resp.Liveness,
	}
	eventsWritten, err := h.writeBackAnomalies(ctx, AIAnalysisPayload{
		SessionID:          payload.SessionID,
		OrgID:              payload.OrgID,
		ExamID:             payload.ExamID,
		StudentID:          payload.StudentID,
		ReferenceEmbedding: payload.ReferenceEmbedding,
	}, []*inferencepb.FrameAnalysis{frame}, thresholds)
	if err != nil {
		return fmt.Errorf("write realtime frame anomalies: %w", err)
	}

	if eventsWritten > 0 && h.pgRepo != nil {
		settings, settingsErr := h.pgRepo.GetExamProctoringSettings(ctx, payload.OrgID, payload.ExamID)
		if settingsErr == nil && settings != nil {
			_, scoreErr := h.scorer.ComputeScoreWithConfig(ctx, payload.SessionID, settings)
			if scoreErr != nil {
				h.logger.Warn("failed to recompute integrity score for realtime frame",
					zap.String("session_id", payload.SessionID),
					zap.Error(scoreErr),
				)
			}
		}
	}

	h.logger.Debug("AI frame analysis completed",
		zap.String("session_id", payload.SessionID),
		zap.Int("events_written", eventsWritten),
		zap.Duration("duration", time.Since(start)),
	)
	return nil
}

// ---------------------------------------------------------------------------
// Evidence fragment query
// ---------------------------------------------------------------------------

type evidenceFragment struct {
	ObjectKey   string
	ContentType string
	SizeBytes   int64
}

func (h *AIAnalysisHandler) queryEvidenceFragments(ctx context.Context, sessionID string) ([]evidenceFragment, error) {
	rows, err := h.chConn.Query(ctx,
		`SELECT object_key, content_type, size_bytes
		 FROM evidence_fragments
		 WHERE session_id = ?
		 ORDER BY fragment_index ASC`,
		sessionID,
	)
	if err != nil {
		return nil, fmt.Errorf("query evidence_fragments: %w", err)
	}
	defer rows.Close()

	var fragments []evidenceFragment
	for rows.Next() {
		var f evidenceFragment
		if err := rows.Scan(&f.ObjectKey, &f.ContentType, &f.SizeBytes); err != nil {
			return nil, fmt.Errorf("scan evidence fragment: %w", err)
		}
		fragments = append(fragments, f)
	}
	return fragments, rows.Err()
}

// ---------------------------------------------------------------------------
// Fragment analysis via inference gateway
// ---------------------------------------------------------------------------

var errUnsupportedEvidenceContentType = errors.New("unsupported evidence content type for frame inference")

func (h *AIAnalysisHandler) analyzeFragment(
	ctx context.Context,
	client inferencepb.InferenceServiceClient,
	payload AIAnalysisPayload,
	frag evidenceFragment,
) (*inferencepb.AnalyzeVideoResponse, error) {
	if !isSupportedEvidenceContentType(frag.ContentType) {
		return nil, fmt.Errorf("%w: %s", errUnsupportedEvidenceContentType, frag.ContentType)
	}

	// Download fragment from MinIO.
	obj, err := h.minioClient.GetObject(ctx, h.minioBucket, frag.ObjectKey, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get object %s: %w", frag.ObjectKey, err)
	}
	defer obj.Close()

	if isSupportedEvidenceVideoContentType(frag.ContentType) {
		return h.analyzeVideoEvidence(ctx, client, payload, frag, obj)
	}

	return h.analyzeImageEvidence(ctx, client, payload, frag, obj)
}

func (h *AIAnalysisHandler) analyzeImageEvidence(
	ctx context.Context,
	client inferencepb.InferenceServiceClient,
	payload AIAnalysisPayload,
	frag evidenceFragment,
	imageReader io.Reader,
) (*inferencepb.AnalyzeVideoResponse, error) {
	maxBytes := h.effectiveMaxFrameBytes()
	if frag.SizeBytes > int64(maxBytes) {
		return nil, fmt.Errorf("image evidence %s exceeds maximum frame size: %d bytes > %d bytes", frag.ObjectKey, frag.SizeBytes, maxBytes)
	}

	frameData, err := io.ReadAll(io.LimitReader(imageReader, int64(maxBytes)+1))
	if err != nil {
		return nil, fmt.Errorf("read image evidence %s: %w", frag.ObjectKey, err)
	}
	if len(frameData) > maxBytes {
		return nil, fmt.Errorf("image evidence %s exceeds maximum frame size: %d bytes > %d bytes", frag.ObjectKey, len(frameData), maxBytes)
	}

	return h.analyzeEvidenceFrames(ctx, client, payload, []evidenceFrame{
		{
			Data:         frameData,
			ContentType:  frag.ContentType,
			TimestampSec: 0,
		},
	})
}

func (h *AIAnalysisHandler) effectiveMaxFrameBytes() int {
	if h.maxFrameBytes < 1 {
		return defaultAIAnalysisMaxFrameBytes
	}
	return h.maxFrameBytes
}

func (h *AIAnalysisHandler) analyzeVideoEvidence(
	ctx context.Context,
	client inferencepb.InferenceServiceClient,
	payload AIAnalysisPayload,
	frag evidenceFragment,
	videoReader io.Reader,
) (*inferencepb.AnalyzeVideoResponse, error) {
	extractor := h.frameExtractor
	if extractor == nil {
		extractor = NewFFmpegFrameExtractor(FrameExtractionConfig{})
	}

	extractedFrames, err := extractor.ExtractFrames(ctx, videoReader, frag.ContentType)
	if err != nil {
		return nil, fmt.Errorf("extract frames from %s: %w", frag.ObjectKey, err)
	}

	return h.analyzeEvidenceFrames(ctx, client, payload, extractedFrames)
}

func (h *AIAnalysisHandler) analyzeEvidenceFrames(
	ctx context.Context,
	client inferencepb.InferenceServiceClient,
	payload AIAnalysisPayload,
	extractedFrames []evidenceFrame,
) (*inferencepb.AnalyzeVideoResponse, error) {
	frames := make([]*inferencepb.FrameAnalysis, 0, len(extractedFrames))
	totalProcessingMs := 0.0
	for _, frame := range extractedFrames {
		resp, err := client.AnalyzeFrame(ctx, &inferencepb.AnalyzeFrameRequest{
			SessionId:          payload.SessionID,
			StudentId:          payload.StudentID,
			ExamId:             payload.ExamID,
			OrgId:              payload.OrgID,
			FrameData:          frame.Data,
			ContentType:        frame.ContentType,
			VideoTimestampSec:  frame.TimestampSec,
			ReferenceEmbedding: append([]float32(nil), payload.ReferenceEmbedding...),
		})
		if err != nil {
			return nil, fmt.Errorf("analyze extracted frame at %.2fs: %w", frame.TimestampSec, err)
		}
		totalProcessingMs += resp.ProcessingTimeMs
		frames = append(frames, &inferencepb.FrameAnalysis{
			TimestampSec: frame.TimestampSec,
			Faces:        resp.Faces,
			Objects:      resp.Objects,
			Liveness:     resp.Liveness,
		})
	}

	return &inferencepb.AnalyzeVideoResponse{
		Frames:                frames,
		Summary:               summarizeAIFrames(frames, h.thresholds),
		TotalProcessingTimeMs: totalProcessingMs,
	}, nil
}

func isSupportedEvidenceContentType(contentType string) bool {
	return isSupportedEvidenceFrameContentType(contentType) || isSupportedEvidenceVideoContentType(contentType)
}

func isSupportedEvidenceFrameContentType(contentType string) bool {
	mediaType := strings.ToLower(strings.TrimSpace(strings.Split(contentType, ";")[0]))
	switch mediaType {
	case "image/jpeg", "image/jpg", "image/png", "image/webp":
		return true
	default:
		return false
	}
}

func isSupportedEvidenceVideoContentType(contentType string) bool {
	mediaType := strings.ToLower(strings.TrimSpace(strings.Split(contentType, ";")[0]))
	switch mediaType {
	case "video/mp4", "video/webm":
		return true
	default:
		return false
	}
}

// ---------------------------------------------------------------------------
// Write-back anomalies to ClickHouse
// ---------------------------------------------------------------------------

func (h *AIAnalysisHandler) writeBackAnomalies(
	ctx context.Context,
	payload AIAnalysisPayload,
	frames []*inferencepb.FrameAnalysis,
	thresholds AIAnalysisThresholds,
) (int, error) {
	count := 0
	now := time.Now().UTC()

	for _, frame := range frames {
		if frame == nil {
			continue
		}
		anomalies := classifyFrameAnomalies(frame, thresholds)
		if err := h.chWriter.Write(ctx, h.buildEvent(payload, now, frame.TimestampSec, detectedAIAnomaly{
			eventType:   valueobject.BackendAIFrameAnalyzed,
			severity:    valueobject.SeverityInfo,
			label:       "Backend AI frame analyzed",
			confidence:  1,
			payload:     marshalAIAnomalyPayload(newAIFrameAnalyzedPayload(frame, len(anomalies))),
			payloadType: "ai_frame",
		})); err != nil {
			h.logger.Warn("failed to write AI frame telemetry event", zap.Error(err))
		} else {
			count++
		}
		for _, anomaly := range anomalies {
			evt := h.buildEvent(payload, now, frame.TimestampSec, anomaly)
			if err := h.chWriter.Write(ctx, evt); err != nil {
				h.logger.Warn("failed to write AI anomaly event", zap.Error(err))
				continue
			}
			count++
		}
	}

	return count, nil
}

type aiFrameAnalyzedPayload struct {
	FaceCount        int  `json:"face_count"`
	ObjectCount      int  `json:"object_count"`
	AnomalyCount     int  `json:"anomaly_count"`
	LivenessReported bool `json:"liveness_reported"`
}

func newAIFrameAnalyzedPayload(frame *inferencepb.FrameAnalysis, anomalyCount int) aiFrameAnalyzedPayload {
	if frame == nil {
		return aiFrameAnalyzedPayload{AnomalyCount: anomalyCount}
	}
	return aiFrameAnalyzedPayload{
		FaceCount:        len(frame.Faces),
		ObjectCount:      len(frame.Objects),
		AnomalyCount:     anomalyCount,
		LivenessReported: frame.Liveness != nil,
	}
}

type detectedAIAnomaly struct {
	eventType         valueobject.EventType
	severity          valueobject.Severity
	label             string
	confidence        float32
	payload           []byte
	payloadType       string
	faceBBox          string
	faceEmbedding     []float32
	faceSimilarity    float32
	faceSimilaritySet bool
	livenessScore     float32
	livenessScoreSet  bool
	headYaw           float32
	headPitch         float32
	headRoll          float32
}

type aiFaceDetectionPayload struct {
	Match      bool    `json:"match"`
	Similarity float32 `json:"similarity,omitempty"`
	FaceCount  int32   `json:"face_count,omitempty"`
	IsSpoof    bool    `json:"is_spoof,omitempty"`
	SpoofType  string  `json:"spoof_type,omitempty"`
}

type aiObjectDetectionPayload struct {
	ObjectType          string  `json:"object_type"`
	BboxX               float32 `json:"bbox_x,omitempty"`
	BboxY               float32 `json:"bbox_y,omitempty"`
	BboxW               float32 `json:"bbox_w,omitempty"`
	BboxH               float32 `json:"bbox_h,omitempty"`
	DetectionConfidence float32 `json:"detection_confidence"`
}

type aiLivenessPayload struct {
	LivenessScore float32 `json:"liveness_score"`
	SpoofVector   string  `json:"spoof_vector,omitempty"`
}

type aiPersonBox struct {
	BboxX      float32 `json:"bbox_x"`
	BboxY      float32 `json:"bbox_y"`
	BboxW      float32 `json:"bbox_w"`
	BboxH      float32 `json:"bbox_h"`
	Confidence float32 `json:"confidence"`
}

type aiMultiplePersonsPayload struct {
	ObjectType          string        `json:"object_type"`
	PersonCount         int           `json:"person_count"`
	FaceCount           int32         `json:"face_count"`
	DetectionConfidence float32       `json:"detection_confidence"`
	Persons             []aiPersonBox `json:"persons"`
}

func classifyFrameAnomalies(frame *inferencepb.FrameAnalysis, thresholds AIAnalysisThresholds) []detectedAIAnomaly {
	if frame == nil {
		return nil
	}

	thresholds = normalizeAIAnalysisThresholds(thresholds)
	anomalies := make([]detectedAIAnomaly, 0, len(frame.Faces)+len(frame.Objects)+1)
	faceCount := int32(len(frame.Faces))

	for _, face := range frame.Faces {
		if hasComputedFaceSimilarity(face) && face.Similarity < thresholds.FaceMismatch {
			payload := aiFaceDetectionPayload{
				Match:      false,
				Similarity: face.Similarity,
				FaceCount:  faceCount,
			}
			anomalies = append(anomalies, detectedAIAnomaly{
				eventType:         valueobject.BackendAIFaceMismatch,
				severity:          valueobject.SeverityCritical,
				label:             fmt.Sprintf("Backend AI: face mismatch (similarity=%.2f)", face.Similarity),
				confidence:        face.Confidence,
				payload:           marshalAIAnomalyPayload(payload),
				payloadType:       "face_detection",
				faceBBox:          faceBBoxJSON(face),
				faceEmbedding:     append([]float32(nil), face.Embedding...),
				faceSimilarity:    face.Similarity,
				faceSimilaritySet: true,
				headYaw:           face.HeadYaw,
				headPitch:         face.HeadPitch,
				headRoll:          face.HeadRoll,
			})
		}

		if face.IsSpoof && face.Confidence >= thresholds.SpoofConfidence {
			label := fmt.Sprintf("Backend AI: spoof detected (type=%s)", face.SpoofType)
			evtType := valueobject.BackendAIFaceMismatch
			if face.SpoofType == "deepfake" {
				evtType = valueobject.BackendAIDeepfakeDetected
			}
			hasSimilarity := hasComputedFaceSimilarity(face)
			payload := aiFaceDetectionPayload{
				Match:      !hasSimilarity || face.Similarity >= thresholds.FaceMismatch,
				Similarity: face.Similarity,
				FaceCount:  faceCount,
				IsSpoof:    true,
				SpoofType:  face.SpoofType,
			}
			anomalies = append(anomalies, detectedAIAnomaly{
				eventType:         evtType,
				severity:          valueobject.SeverityCritical,
				label:             label,
				confidence:        face.Confidence,
				payload:           marshalAIAnomalyPayload(payload),
				payloadType:       "face_detection",
				faceBBox:          faceBBoxJSON(face),
				faceEmbedding:     append([]float32(nil), face.Embedding...),
				faceSimilarity:    face.Similarity,
				faceSimilaritySet: hasSimilarity,
				headYaw:           face.HeadYaw,
				headPitch:         face.HeadPitch,
				headRoll:          face.HeadRoll,
			})
		}
	}

		// Люди, найденные детектором объектов (выше порога уверенности)
	var persons []aiPersonBox
	var maxPersonConf float32

	for _, obj := range frame.Objects {
		if obj.Confidence < thresholds.ObjectConfidence {
			continue
		}

		var evtType valueobject.EventType
		severity := valueobject.SeverityWarning
		switch strings.ToLower(strings.TrimSpace(obj.ObjectType)) {
		case "phone", "cell phone":
			evtType = valueobject.PhoneDetected
			severity = valueobject.SeverityCritical
		case "book":
			evtType = valueobject.BookDetected
		case "earbuds":
			evtType = valueobject.EarbudsDetected
		case "screen_reflection":
			evtType = valueobject.BackendAIScreenReflection
		case "person":
			persons = append(persons, aiPersonBox{
				BboxX:      obj.BboxX,
				BboxY:      obj.BboxY,
				BboxW:      obj.BboxW,
				BboxH:      obj.BboxH,
				Confidence: obj.Confidence,
			})
			if obj.Confidence > maxPersonConf {
				maxPersonConf = obj.Confidence
			}
			continue
		default:
			evtType = valueobject.BackendAIHiddenObject
		}

		payload := aiObjectDetectionPayload{
			ObjectType:          obj.ObjectType,
			BboxX:               obj.BboxX,
			BboxY:               obj.BboxY,
			BboxW:               obj.BboxW,
			BboxH:               obj.BboxH,
			DetectionConfidence: obj.Confidence,
		}
		anomalies = append(anomalies, detectedAIAnomaly{
			eventType:   evtType,
			severity:    severity,
			label:       fmt.Sprintf("Backend AI: %s detected", obj.ObjectType),
			confidence:  obj.Confidence,
			payload:     marshalAIAnomalyPayload(payload),
			payloadType: "object_detection",
		})
	}

	if len(persons) > 1 {
		payload := aiMultiplePersonsPayload{
			ObjectType:          "person",
			PersonCount:         len(persons),
			FaceCount:           faceCount,
			DetectionConfidence: maxPersonConf,
			Persons:             persons,
		}
		anomalies = append(anomalies, detectedAIAnomaly{
			eventType:   valueobject.MultiplePersons,
			severity:    valueobject.SeverityCritical,
			label:       fmt.Sprintf("Backend AI: multiple persons detected (count=%d, conf=%.2f)", len(persons), maxPersonConf),
			confidence:  maxPersonConf,
			payload:     marshalAIAnomalyPayload(payload),
			payloadType: "object_detection",
		})
	}

	if isConfiguredLivenessFailure(frame.Liveness, thresholds.Liveness) {
		payload := aiLivenessPayload{
			LivenessScore: frame.Liveness.Score,
			SpoofVector:   frame.Liveness.Method,
		}
		anomalies = append(anomalies, detectedAIAnomaly{
			eventType:        valueobject.BackendAIFaceMismatch,
			severity:         valueobject.SeverityCritical,
			label:            fmt.Sprintf("Backend AI: liveness failed (score=%.2f, method=%s)", frame.Liveness.Score, frame.Liveness.Method),
			confidence:       1.0 - frame.Liveness.Score,
			payload:          marshalAIAnomalyPayload(payload),
			payloadType:      "liveness",
			livenessScore:    frame.Liveness.Score,
			livenessScoreSet: true,
		})
	}

	return anomalies
}

func hasComputedFaceSimilarity(face *inferencepb.FaceDetection) bool {
	return face != nil && len(face.Embedding) > 0
}

func isConfiguredLivenessFailure(liveness *inferencepb.LivenessResult, threshold float32) bool {
	if liveness == nil || liveness.IsLive || liveness.Score >= threshold {
		return false
	}

	switch strings.ToLower(strings.TrimSpace(liveness.Method)) {
	case "", "not_configured", "unconfigured", "unavailable", "unknown":
		return false
	default:
		return true
	}
}

func marshalAIAnomalyPayload(payload any) []byte {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil
	}
	return data
}

func faceBBoxJSON(face *inferencepb.FaceDetection) string {
	if face == nil {
		return ""
	}
	return fmt.Sprintf(`{"x":%f,"y":%f,"w":%f,"h":%f}`, face.BboxX, face.BboxY, face.BboxW, face.BboxH)
}

func summarizeAIFrames(frames []*inferencepb.FrameAnalysis, thresholds AIAnalysisThresholds) *inferencepb.AnalysisSummary {
	thresholds = normalizeAIAnalysisThresholds(thresholds)

	summary := &inferencepb.AnalysisSummary{
		TotalFramesAnalyzed: int32(len(frames)),
		Verdict:             "clean",
	}

	for _, frame := range frames {
		if frame == nil {
			continue
		}
		if len(frame.Objects) > 0 {
			summary.ObjectDetectionFrames++
		}
		for _, face := range frame.Faces {
			if hasComputedFaceSimilarity(face) && face.Similarity < thresholds.FaceMismatch {
				summary.FaceMismatchFrames++
			}
			if face.IsSpoof && face.Confidence >= thresholds.SpoofConfidence {
				summary.SpoofFrames++
			}
		}

		anomalies := classifyFrameAnomalies(frame, thresholds)
		if len(anomalies) == 0 {
			continue
		}

		summary.FraudFrames++
		for _, anomaly := range anomalies {
			if anomaly.confidence > summary.MaxFraudConfidence {
				summary.MaxFraudConfidence = anomaly.confidence
			}
		}
	}

	if summary.TotalFramesAnalyzed == 0 {
		return summary
	}

	fraudRate := float64(summary.FraudFrames) / float64(summary.TotalFramesAnalyzed)
	switch {
	case fraudRate > 0.15 || summary.MaxFraudConfidence > 0.9:
		summary.Verdict = "fraud"
	case fraudRate > 0.05 || summary.MaxFraudConfidence > 0.7:
		summary.Verdict = "suspicious"
	}
	return summary
}

func firstAIAnalysisThresholds(thresholds []AIAnalysisThresholds) AIAnalysisThresholds {
	if len(thresholds) == 0 {
		return AIAnalysisThresholds{}
	}
	return thresholds[0]
}

func normalizeAIAnalysisThresholds(thresholds AIAnalysisThresholds) AIAnalysisThresholds {
	if thresholds.FaceMismatch <= 0 || thresholds.FaceMismatch > 1 {
		thresholds.FaceMismatch = 0.6
	}
	if thresholds.Liveness <= 0 || thresholds.Liveness > 1 {
		thresholds.Liveness = 0.3
	}
	if thresholds.ObjectConfidence <= 0 || thresholds.ObjectConfidence > 1 {
		thresholds.ObjectConfidence = 0.7
	}
	if thresholds.SpoofConfidence <= 0 || thresholds.SpoofConfidence > 1 {
		thresholds.SpoofConfidence = 0.7
	}
	return thresholds
}

func (h *AIAnalysisHandler) buildEvent(
	payload AIAnalysisPayload,
	serverTime time.Time,
	videoTimestamp float64,
	anomaly detectedAIAnomaly,
) *entity.ProctoringEvent {
	event := &entity.ProctoringEvent{
		EventID:         randutil.HexToken(16),
		SessionID:       payload.SessionID,
		StudentID:       payload.StudentID,
		ExamID:          payload.ExamID,
		OrgID:           payload.OrgID,
		EventType:       anomaly.eventType,
		Severity:        anomaly.severity,
		Source:          valueobject.SourceBackendAI,
		ServerTimestamp: serverTime,
		ClientTimestamp: serverTime,
		VideoTimestamp:  videoTimestamp,
		Label:           anomaly.label,
		Confidence:      anomaly.confidence,
		Payload:         anomaly.payload,
		PayloadType:     anomaly.payloadType,
		HeadYaw:         anomaly.headYaw,
		HeadPitch:       anomaly.headPitch,
		HeadRoll:        anomaly.headRoll,
		FaceBBox:        anomaly.faceBBox,
		FaceEmbedding:   append([]float32(nil), anomaly.faceEmbedding...),
		LivenessScore:   -1,
		FaceSimilarity:  -1,
		AudioRmsDb:      -100,
	}
	if anomaly.faceSimilaritySet {
		event.FaceSimilarity = anomaly.faceSimilarity
	}
	if anomaly.livenessScoreSet {
		event.LivenessScore = anomaly.livenessScore
	}
	return event
}

// ---------------------------------------------------------------------------
// Telegram fraud alert
// ---------------------------------------------------------------------------

func (h *AIAnalysisHandler) fireFraudAlert(payload AIAnalysisPayload, summary aggregateSummary) {
	if h.alerter == nil {
		return
	}

	msg := alerting.MsgAIFraudDetected(
		payload.SessionID, payload.StudentID, payload.ExamID,
		summary.verdict(), summary.maxFraudConf,
		summary.fraudFrames, summary.totalFrames,
		time.Now().Format("2006-01-02 15:04:05 MST"),
	)

	_ = h.alerter.SendDirect(msg)
}

// ---------------------------------------------------------------------------
// Aggregate summary across multiple fragments
// ---------------------------------------------------------------------------

type aggregateSummary struct {
	totalFrames  int
	fraudFrames  int
	maxFraudConf float32
}

func (s *aggregateSummary) merge(summary *inferencepb.AnalysisSummary) {
	if summary == nil {
		return
	}
	s.totalFrames += int(summary.TotalFramesAnalyzed)
	s.fraudFrames += int(summary.FraudFrames)
	if summary.MaxFraudConfidence > s.maxFraudConf {
		s.maxFraudConf = summary.MaxFraudConfidence
	}
}

func (s *aggregateSummary) verdict() string {
	if s.totalFrames == 0 {
		return "clean"
	}
	fraudRate := float64(s.fraudFrames) / float64(s.totalFrames)
	switch {
	case fraudRate > 0.15 || s.maxFraudConf > 0.9:
		return "fraud"
	case fraudRate > 0.05 || s.maxFraudConf > 0.7:
		return "suspicious"
	default:
		return "clean"
	}
}
