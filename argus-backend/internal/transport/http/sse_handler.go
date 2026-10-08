// =============================================================================
// Argus AI — Server-Sent Events (SSE) Handler for Live Session Monitoring
// =============================================================================
//
// Provides a real-time event stream for individual proctoring sessions.
// Replaces the 10-second polling interval with a push-based architecture.
//
// Features:
//   - Live violation events pushed as they arrive
//   - Continuous integrity score recomputation (every 3s)
//   - Auto-terminate when score crosses configurable threshold
//   - Telegram alerting on fraud verdict and auto-termination
//   - SSE keepalive heartbeats (every 15s)
//
// Endpoint:
//   GET /api/v1/monitoring/sessions/{sessionId}/stream?token=<jwt>
// =============================================================================
package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/argus-ai/event-collector/internal/domain/entity"
	"github.com/argus-ai/event-collector/internal/infrastructure/alerting"
	"github.com/argus-ai/event-collector/internal/infrastructure/forensic"
	"github.com/argus-ai/event-collector/internal/infrastructure/postgres"
	"go.uber.org/zap"
)

// SSEHandler serves real-time session event streams via Server-Sent Events.
type SSEHandler struct {
	chConn  driver.Conn
	scorer  *forensic.Scorer
	pgRepo  *postgres.Repository
	alerter alerting.Provider
	logger  *zap.Logger

	jwtSigningKey []byte
	repo          adminRepo
}

// NewSSEHandler creates a new SSE handler for live session monitoring.
func NewSSEHandler(
	chConn driver.Conn,
	scorer *forensic.Scorer,
	pgRepo *postgres.Repository,
	alerter alerting.Provider,
	logger *zap.Logger,
	jwtSigningKey []byte,
) *SSEHandler {
	return &SSEHandler{
		chConn:        chConn,
		scorer:        scorer,
		pgRepo:        pgRepo,
		alerter:       alerter,
		logger:        logger.Named("sse_handler"),
		jwtSigningKey: jwtSigningKey,
		repo:          pgRepo,
	}
}

// RegisterRoutes registers SSE endpoints on the given mux.
func (h *SSEHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/v1/monitoring/sessions/{sessionId}/stream", h.handleSSE)
}

// ==========================================================================
// SSE Event Types
// ==========================================================================

type sseViolation struct {
	EventType  string  `json:"eventType"`
	Severity   string  `json:"severity"`
	Label      string  `json:"label"`
	Confidence float64 `json:"confidence"`
	Source     string  `json:"source"`
	Timestamp  string  `json:"timestamp"`
}

type sseScoreUpdate struct {
	Score        float64 `json:"score"`
	Verdict      string  `json:"verdict"`
	VerdictLabel string  `json:"verdictLabel"`
	TotalEvents  int     `json:"totalEvents"`
}

type sseTerminate struct {
	Reason string  `json:"reason"`
	Score  float64 `json:"score"`
}

// ==========================================================================
// SSE Handler
// ==========================================================================

func (h *SSEHandler) handleSSE(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("sessionId")
	if sessionID == "" {
		http.Error(w, "sessionId is required", http.StatusBadRequest)
		return
	}

	// Auth via query param (same as PDF endpoint pattern).
	token := r.URL.Query().Get("token")
	if token == "" {
		token = r.Header.Get("Authorization")
		if len(token) > 7 && token[:7] == "Bearer " {
			token = token[7:]
		}
	}
	if token == "" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if _, err := h.verifyToken(r.Context(), token); err != nil {
		http.Error(w, "Invalid or expired token", http.StatusUnauthorized)
		return
	}

	// Verify http.Flusher support.
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	// Set SSE headers.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("X-Accel-Buffering", "no") // Disable nginx buffering
	flusher.Flush()

	h.logger.Info("sse: client connected",
		zap.String("session_id", sessionID),
		zap.String("remote_addr", r.RemoteAddr),
	)

	// Load exam proctoring settings for config-aware scoring.
	var settings *entity.ExamProctoringSettings
	orgID, examID, _, metaErr := h.scorer.QuerySessionMeta(r.Context(), sessionID)
	if metaErr == nil && orgID != "" && examID != "" {
		s, err := h.pgRepo.GetExamProctoringSettings(r.Context(), orgID, examID)
		if err == nil {
			settings = s
		}
	}

	// Event loop: poll for new violations every 3 seconds.
	pollTicker := time.NewTicker(3 * time.Second)
	heartbeatTicker := time.NewTicker(15 * time.Second)
	defer pollTicker.Stop()
	defer heartbeatTicker.Stop()

	lastPollTime := time.Now().UTC().Add(-30 * time.Second) // Start 30s back to catch recent events
	fraudDetected := false

	ctx := r.Context()

	for {
		select {
		case <-ctx.Done():
			h.logger.Info("sse: client disconnected",
				zap.String("session_id", sessionID),
			)
			return

		case <-heartbeatTicker.C:
			// SSE keepalive comment.
			fmt.Fprintf(w, ": heartbeat %s\n\n", time.Now().UTC().Format(time.RFC3339))
			flusher.Flush()

		case <-pollTicker.C:
			// 1. Query new violations since last poll.
			violations, err := h.queryNewViolations(ctx, sessionID, lastPollTime)
			if err != nil {
				h.logger.Warn("sse: violation query failed",
					zap.String("session_id", sessionID),
					zap.Error(err),
				)
				continue
			}

			lastPollTime = time.Now().UTC()

			// Push violation events.
			for _, v := range violations {
				data, _ := json.Marshal(v)
				fmt.Fprintf(w, "event: violation\ndata: %s\n\n", data)
			}

			// 2. Recompute integrity score.
			var score *forensic.IntegrityScore
			if settings != nil {
				score, err = h.scorer.ComputeScoreWithConfig(ctx, sessionID, settings)
			} else {
				score, err = h.scorer.ComputeScore(ctx, sessionID)
			}
			if err != nil {
				h.logger.Warn("sse: score computation failed",
					zap.String("session_id", sessionID),
					zap.Error(err),
				)
				continue
			}

			// Push score update.
			scoreData, _ := json.Marshal(sseScoreUpdate{
				Score:        score.Score,
				Verdict:      score.Verdict,
				VerdictLabel: score.VerdictLabel,
				TotalEvents:  score.TotalEvents,
			})
			fmt.Fprintf(w, "event: score_update\ndata: %s\n\n", scoreData)

			// 3. Check fraud verdict — fire alert on first detection.
			if score.Verdict == "fraud" && !fraudDetected {
				fraudDetected = true
				if h.alerter != nil {
					h.alerter.Send(alerting.Alert{
						Component: "proctoring-fraud-" + sessionID,
						SessionID: sessionID,
						Severity:  "CRITICAL",
						Error: fmt.Sprintf(
							"Fraud verdict detected. Score: %.1f%%. Student: %s, Exam: %s, Org: %s",
							score.Score, score.StudentID, score.ExamID, score.OrgID,
						),
						Timestamp: time.Now(),
					})
				}
			}

			// 4. Auto-terminate if enabled and score crossed threshold.
			if settings != nil && settings.AutoTerminate && score.Score <= settings.AutoTerminateAt {
				// Check if already terminated.
				terminated, _ := h.pgRepo.IsSessionTerminated(ctx, orgID, sessionID)
				if !terminated {
					reason := fmt.Sprintf(
						"Auto-terminated: integrity score %.1f%% below threshold %.1f%%",
						score.Score, settings.AutoTerminateAt,
					)

					// Insert termination record (scoped to org).
					_ = h.pgRepo.TerminateSession(ctx, orgID, sessionID, "system", reason)

					// Push terminate event.
					termData, _ := json.Marshal(sseTerminate{
						Reason: reason,
						Score:  score.Score,
					})
					fmt.Fprintf(w, "event: terminate\ndata: %s\n\n", termData)
					flusher.Flush()

					// Fire Telegram alert.
					if h.alerter != nil {
						h.alerter.Send(alerting.Alert{
							Component: "proctoring-terminate-" + sessionID,
							SessionID: sessionID,
							Severity:  "CRITICAL",
							Error:     reason,
							Timestamp: time.Now(),
						})
					}

					h.logger.Error("sse: session auto-terminated",
						zap.String("session_id", sessionID),
						zap.Float64("score", score.Score),
						zap.Float64("threshold", settings.AutoTerminateAt),
					)

					return // Close SSE connection after termination.
				}
			}

			flusher.Flush()
		}
	}
}

// queryNewViolations fetches violation events newer than the given timestamp.
func (h *SSEHandler) queryNewViolations(ctx context.Context, sessionID string, since time.Time) ([]sseViolation, error) {
	qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	rows, err := h.chConn.Query(qctx, `
		SELECT
			event_type, severity, label, confidence, source,
			server_timestamp
		FROM proctoring_events
		WHERE session_id = ?
		  AND server_timestamp > ?
		  AND severity IN ('critical', 'warning')
		ORDER BY server_timestamp ASC
		LIMIT 100`,
		sessionID, since,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var violations []sseViolation
	for rows.Next() {
		var v sseViolation
		var ts time.Time
		if err := rows.Scan(&v.EventType, &v.Severity, &v.Label, &v.Confidence, &v.Source, &ts); err != nil {
			continue
		}
		v.Timestamp = ts.Format(time.RFC3339)
		v.EventType = strings.ToUpper(v.EventType)
		violations = append(violations, v)
	}

	return violations, nil
}

// ==========================================================================
// Auth (same pattern as ForensicHandler)
// ==========================================================================

func (h *SSEHandler) verifyToken(ctx context.Context, tokenStr string) (*entity.User, error) {
	parts := splitToken(tokenStr)
	if parts == nil {
		return nil, errInvalidToken
	}

	signingInput := parts[0] + "." + parts[1]
	expectedSig := hmacSHA256([]byte(signingInput), h.jwtSigningKey)
	actualSig, err := base64URLDecode(parts[2])
	if err != nil {
		return nil, errInvalidToken
	}
	if !hmacEqual(expectedSig, actualSig) {
		return nil, errInvalidToken
	}

	payloadBytes, err := base64URLDecode(parts[1])
	if err != nil {
		return nil, errInvalidToken
	}

	var claims struct {
		Sub string `json:"sub"`
		Exp int64  `json:"exp"`
	}
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return nil, errInvalidToken
	}
	if time.Now().Unix() > claims.Exp {
		return nil, errInvalidToken
	}

	user, err := h.repo.GetUserByID(ctx, claims.Sub)
	if err != nil {
		return nil, err
	}

	return user, nil
}
