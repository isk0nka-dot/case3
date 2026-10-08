package worker

import (
	"encoding/json"
	"testing"
	"time"

	inferencepb "github.com/argus-ai/event-collector/api/proto/v1/inferencepb"
	"github.com/argus-ai/event-collector/internal/domain/valueobject"
)

func TestClassifyFrameAnomaliesUsesConfigurableThresholds(t *testing.T) {
	frame := &inferencepb.FrameAnalysis{
		Faces: []*inferencepb.FaceDetection{
			{Similarity: 0.59, Confidence: 0.8, Embedding: []float32{0.1, 0.2, 0.3}},
			{IsSpoof: true, SpoofType: "deepfake", Confidence: 0.65},
		},
		Objects: []*inferencepb.ObjectDetection{
			{ObjectType: "phone", Confidence: 0.69},
			{ObjectType: "book", Confidence: 0.91},
		},
		Liveness: &inferencepb.LivenessResult{IsLive: false, Score: 0.31, Method: "blink"},
	}

	anomalies := classifyFrameAnomalies(frame, AIAnalysisThresholds{
		FaceMismatch:     0.6,
		Liveness:         0.3,
		ObjectConfidence: 0.7,
		SpoofConfidence:  0.7,
	})

	if len(anomalies) != 2 {
		t.Fatalf("expected face mismatch and high-confidence object only, got %d anomalies: %#v", len(anomalies), anomalies)
	}
	if anomalies[0].eventType != valueobject.BackendAIFaceMismatch {
		t.Fatalf("expected face mismatch event, got %s", anomalies[0].eventType)
	}
	if anomalies[1].eventType != valueobject.BackendAIHiddenObject {
		t.Fatalf("expected hidden object event, got %s", anomalies[1].eventType)
	}
}

func TestClassifyFrameAnomaliesAllowsThresholdTuning(t *testing.T) {
	frame := &inferencepb.FrameAnalysis{
		Faces: []*inferencepb.FaceDetection{
			{Similarity: 0.59, Confidence: 0.8},
			{IsSpoof: true, SpoofType: "deepfake", Confidence: 0.65},
		},
		Objects: []*inferencepb.ObjectDetection{
			{ObjectType: "phone", Confidence: 0.69},
		},
		Liveness: &inferencepb.LivenessResult{IsLive: false, Score: 0.31, Method: "blink"},
	}

	anomalies := classifyFrameAnomalies(frame, AIAnalysisThresholds{
		FaceMismatch:     0.55,
		Liveness:         0.4,
		ObjectConfidence: 0.6,
		SpoofConfidence:  0.6,
	})

	if len(anomalies) != 3 {
		t.Fatalf("expected tuned thresholds to emit spoof, object, and liveness, got %d anomalies: %#v", len(anomalies), anomalies)
	}
}

func TestClassifyFrameAnomaliesIgnoresUnconfiguredLiveness(t *testing.T) {
	frame := &inferencepb.FrameAnalysis{
		Liveness: &inferencepb.LivenessResult{
			IsLive: false,
			Score:  0,
			Method: "not_configured",
		},
	}

	anomalies := classifyFrameAnomalies(frame, AIAnalysisThresholds{Liveness: 0.4})

	if len(anomalies) != 0 {
		t.Fatalf("expected unconfigured liveness to be ignored, got %d anomalies: %#v", len(anomalies), anomalies)
	}
}

func TestClassifyFrameAnomaliesIgnoresPersonObjectWithoutCorroboration(t *testing.T) {
	frame := &inferencepb.FrameAnalysis{
		Objects: []*inferencepb.ObjectDetection{
			{ObjectType: "person", Confidence: 0.92},
		},
	}

	anomalies := classifyFrameAnomalies(frame, AIAnalysisThresholds{ObjectConfidence: 0.7})

	if len(anomalies) != 0 {
		t.Fatalf("expected standalone person detection to be ignored, got %d anomalies: %#v", len(anomalies), anomalies)
	}
}

func TestClassifyFrameAnomaliesFlagsNegativeFaceSimilarity(t *testing.T) {
	frame := &inferencepb.FrameAnalysis{
		Faces: []*inferencepb.FaceDetection{
			{
				Similarity: -0.12,
				Confidence: 0.97,
				Embedding:  []float32{0.1, 0.2, 0.3},
			},
		},
	}

	anomalies := classifyFrameAnomalies(frame, AIAnalysisThresholds{FaceMismatch: 0.62})

	if len(anomalies) != 1 {
		t.Fatalf("expected negative similarity to emit mismatch, got %d anomalies: %#v", len(anomalies), anomalies)
	}
	if anomalies[0].eventType != valueobject.BackendAIFaceMismatch {
		t.Fatalf("expected face mismatch event, got %s", anomalies[0].eventType)
	}
	if !anomalies[0].faceSimilaritySet || anomalies[0].faceSimilarity != -0.12 {
		t.Fatalf("expected negative face similarity to be preserved, got set=%v value=%f", anomalies[0].faceSimilaritySet, anomalies[0].faceSimilarity)
	}
}

func TestClassifyFrameAnomaliesCarriesObjectDetectionPayloadForClickHouse(t *testing.T) {
	frame := &inferencepb.FrameAnalysis{
		TimestampSec: 12,
		Objects: []*inferencepb.ObjectDetection{
			{
				ObjectType: "phone",
				Confidence: 0.91,
				BboxX:      0.11,
				BboxY:      0.22,
				BboxW:      0.33,
				BboxH:      0.44,
			},
		},
	}

	anomalies := classifyFrameAnomalies(frame, AIAnalysisThresholds{ObjectConfidence: 0.7})

	if len(anomalies) != 1 {
		t.Fatalf("expected one object anomaly, got %d", len(anomalies))
	}
	if anomalies[0].payloadType != "object_detection" {
		t.Fatalf("payload type = %q, want object_detection", anomalies[0].payloadType)
	}

	var objectPayload struct {
		ObjectType          string  `json:"object_type"`
		BboxX               float32 `json:"bbox_x"`
		BboxY               float32 `json:"bbox_y"`
		BboxW               float32 `json:"bbox_w"`
		BboxH               float32 `json:"bbox_h"`
		DetectionConfidence float32 `json:"detection_confidence"`
	}
	if err := json.Unmarshal(anomalies[0].payload, &objectPayload); err != nil {
		t.Fatalf("object payload is not valid JSON: %v", err)
	}
	if objectPayload.ObjectType != "phone" || objectPayload.DetectionConfidence != 0.91 {
		t.Fatalf("unexpected object payload: %#v", objectPayload)
	}
	if objectPayload.BboxX != 0.11 || objectPayload.BboxY != 0.22 || objectPayload.BboxW != 0.33 || objectPayload.BboxH != 0.44 {
		t.Fatalf("object bbox was not preserved: %#v", objectPayload)
	}

	event := (&AIAnalysisHandler{}).buildEvent(
		AIAnalysisPayload{SessionID: "session-1", StudentID: "student-1", ExamID: "exam-1", OrgID: "org-1"},
		time.Unix(100, 0).UTC(),
		frame.TimestampSec,
		anomalies[0],
	)
	if event.PayloadType != "object_detection" || len(event.Payload) == 0 {
		t.Fatalf("event payload was not attached: type=%q payload=%s", event.PayloadType, string(event.Payload))
	}
	if event.FaceSimilarity != -1 || event.LivenessScore != -1 || event.AudioRmsDb != -100 {
		t.Fatalf("event sentinels not initialized: face_similarity=%f liveness=%f audio=%f", event.FaceSimilarity, event.LivenessScore, event.AudioRmsDb)
	}
}

func TestClassifyFrameAnomaliesCarriesFacePayloadAndDenormalizedFields(t *testing.T) {
	frame := &inferencepb.FrameAnalysis{
		TimestampSec: 18,
		Faces: []*inferencepb.FaceDetection{
			{
				Confidence: 0.88,
				Similarity: 0.42,
				BboxX:      0.12,
				BboxY:      0.23,
				BboxW:      0.34,
				BboxH:      0.45,
				Embedding:  []float32{0.1, 0.2, 0.3},
			},
		},
	}

	anomalies := classifyFrameAnomalies(frame, AIAnalysisThresholds{FaceMismatch: 0.6})

	if len(anomalies) != 1 {
		t.Fatalf("expected one face mismatch anomaly, got %d", len(anomalies))
	}
	if anomalies[0].payloadType != "face_detection" {
		t.Fatalf("payload type = %q, want face_detection", anomalies[0].payloadType)
	}

	var facePayload struct {
		Match      bool    `json:"match"`
		Similarity float32 `json:"similarity"`
		FaceCount  int32   `json:"face_count"`
		IsSpoof    bool    `json:"is_spoof"`
		SpoofType  string  `json:"spoof_type"`
	}
	if err := json.Unmarshal(anomalies[0].payload, &facePayload); err != nil {
		t.Fatalf("face payload is not valid JSON: %v", err)
	}
	if facePayload.Match || facePayload.Similarity != 0.42 || facePayload.FaceCount != 1 {
		t.Fatalf("unexpected face payload: %#v", facePayload)
	}

	event := (&AIAnalysisHandler{}).buildEvent(
		AIAnalysisPayload{SessionID: "session-1", StudentID: "student-1", ExamID: "exam-1", OrgID: "org-1"},
		time.Unix(100, 0).UTC(),
		frame.TimestampSec,
		anomalies[0],
	)
	if event.PayloadType != "face_detection" || len(event.Payload) == 0 {
		t.Fatalf("event payload was not attached: type=%q payload=%s", event.PayloadType, string(event.Payload))
	}
	if event.FaceSimilarity != 0.42 {
		t.Fatalf("face similarity = %f, want 0.42", event.FaceSimilarity)
	}
	if event.FaceBBox != `{"x":0.120000,"y":0.230000,"w":0.340000,"h":0.450000}` {
		t.Fatalf("face bbox was not denormalized: %s", event.FaceBBox)
	}
	if len(event.FaceEmbedding) != 3 {
		t.Fatalf("face embedding was not preserved, got len=%d", len(event.FaceEmbedding))
	}
}

func TestClassifyFrameAnomaliesMultiplePersons(t *testing.T) {
	frame := &inferencepb.FrameAnalysis{
		Objects: []*inferencepb.ObjectDetection{
			{ObjectType: "person", Confidence: 0.9},
			{ObjectType: "person", Confidence: 0.8},
		},
	}
	got := classifyFrameAnomalies(frame, AIAnalysisThresholds{ObjectConfidence: 0.7})
	if len(got) != 1 || got[0].eventType != valueobject.MultiplePersons {
		t.Fatalf("expected one MultiplePersons anomaly, got %#v", got)
	}
}

func TestPhoneBuildsHiddenObjectEvent(t *testing.T) {
	frame := &inferencepb.FrameAnalysis{
		Objects: []*inferencepb.ObjectDetection{{ObjectType: "phone", Confidence: 0.9}},
	}
	got := classifyFrameAnomalies(frame, AIAnalysisThresholds{ObjectConfidence: 0.7})
	if len(got) != 1 {
		t.Fatalf("expected 1 anomaly, got %d", len(got))
	}
	a := got[0]
	if a.eventType != valueobject.BackendAIHiddenObject || a.payloadType != "object_detection" {
		t.Fatalf("unexpected phone anomaly: %#v", a)
	}
	var p aiObjectDetectionPayload
	if err := json.Unmarshal(a.payload, &p); err != nil || p.ObjectType != "phone" {
		t.Fatalf("payload must carry object_type=phone, got %s (err=%v)", a.payload, err)
	}
}
