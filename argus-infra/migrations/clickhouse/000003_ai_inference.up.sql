USE argus_analytics;
-- =============================================================================
-- Argus AI — ClickHouse Migration 003: AI Inference Support
-- =============================================================================
--
-- Adds columns for AI Vision and Audio inference results to enable
-- direct columnar queries on head pose, liveness scores, face embeddings,
-- and audio analysis without parsing the JSON payload column.
--
-- Design decisions:
--   1. Head pose (yaw/pitch/roll) as Float32 columns — enables efficient
--      range scans for "students looking away" analytics.
--   2. Face embedding as Array(Float32) — 512-dim vector stored columnar.
--      ClickHouse supports cosineDistance() for similarity search.
--   3. Audio features as dedicated columns — enables real-time SAD dashboards.
--   4. All columns have defaults — backward compatible with existing events.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- AI Vision columns on proctoring_events
-- ---------------------------------------------------------------------------

ALTER TABLE argus_analytics.proctoring_events
  ADD COLUMN IF NOT EXISTS head_yaw Float32 DEFAULT 0
  COMMENT 'Head pose yaw in degrees (-90 to +90)';

ALTER TABLE argus_analytics.proctoring_events
  ADD COLUMN IF NOT EXISTS head_pitch Float32 DEFAULT 0
  COMMENT 'Head pose pitch in degrees (-90 to +90)';

ALTER TABLE argus_analytics.proctoring_events
  ADD COLUMN IF NOT EXISTS head_roll Float32 DEFAULT 0
  COMMENT 'Head pose roll in degrees (-180 to +180)';

ALTER TABLE argus_analytics.proctoring_events
  ADD COLUMN IF NOT EXISTS face_bbox String DEFAULT ''
  COMMENT 'Face bounding box as JSON: {"x":0.1,"y":0.2,"w":0.3,"h":0.4}';

ALTER TABLE argus_analytics.proctoring_events
  ADD COLUMN IF NOT EXISTS liveness_score Float32 DEFAULT -1
  COMMENT 'Liveness probability (0=spoof, 1=real, -1=not computed)';

ALTER TABLE argus_analytics.proctoring_events
  ADD COLUMN IF NOT EXISTS face_embedding Array(Float32) DEFAULT []
  COMMENT '512-dim face embedding vector (ArcFace)';

ALTER TABLE argus_analytics.proctoring_events
  ADD COLUMN IF NOT EXISTS face_similarity Float32 DEFAULT -1
  COMMENT 'Cosine similarity to enrolled reference (-1=not computed)';

-- ---------------------------------------------------------------------------
-- AI Audio columns on proctoring_events
-- ---------------------------------------------------------------------------

ALTER TABLE argus_analytics.proctoring_events
  ADD COLUMN IF NOT EXISTS audio_rms_db Float32 DEFAULT -100
  COMMENT 'RMS dB level A-weighted (-100=silence)';

ALTER TABLE argus_analytics.proctoring_events
  ADD COLUMN IF NOT EXISTS vad_active UInt8 DEFAULT 0
  COMMENT 'Voice Activity Detection flag (0=no, 1=yes)';

ALTER TABLE argus_analytics.proctoring_events
  ADD COLUMN IF NOT EXISTS audio_classification LowCardinality(String) DEFAULT ''
  COMMENT 'Audio class: silence, speech, whisper, music, keyboard, ambient';

ALTER TABLE argus_analytics.proctoring_events
  ADD COLUMN IF NOT EXISTS speaker_count UInt8 DEFAULT 0
  COMMENT 'Number of distinct speakers detected';

ALTER TABLE argus_analytics.proctoring_events
  ADD COLUMN IF NOT EXISTS speaker_match UInt8 DEFAULT 0
  COMMENT 'Whether voice matches enrolled voiceprint (0=no, 1=yes)';

-- ---------------------------------------------------------------------------
-- Performance indices for AI queries
-- ---------------------------------------------------------------------------

ALTER TABLE argus_analytics.proctoring_events
  ADD INDEX IF NOT EXISTS idx_liveness (liveness_score) TYPE minmax GRANULARITY 4;

ALTER TABLE argus_analytics.proctoring_events
  ADD INDEX IF NOT EXISTS idx_face_similarity (face_similarity) TYPE minmax GRANULARITY 4;

ALTER TABLE argus_analytics.proctoring_events
  ADD INDEX IF NOT EXISTS idx_audio_class (audio_classification) TYPE set(20) GRANULARITY 4;

ALTER TABLE argus_analytics.proctoring_events
  ADD INDEX IF NOT EXISTS idx_vad (vad_active) TYPE set(2) GRANULARITY 4;

ALTER TABLE argus_analytics.proctoring_events
  ADD INDEX IF NOT EXISTS idx_head_yaw (head_yaw) TYPE minmax GRANULARITY 4;

-- ---------------------------------------------------------------------------
-- Materialized view: AI inference summary per session
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS argus_analytics.session_ai_summary
(
    org_id              String,
    exam_id             String,
    session_id          String,
    student_id          String,
    -- Vision aggregates
    avg_liveness_score  Float64,
    liveness_count      UInt64,
    min_face_similarity Float32,
    avg_face_similarity Float64,
    face_check_count    UInt64,
    head_pose_anomalies UInt64,
    face_occluded_count UInt64,
    -- Audio aggregates
    speech_segments     UInt64,
    whisper_detections  UInt64,
    second_speaker_count UInt64,
    avg_audio_rms_db    Float64,
    audio_sample_count  UInt64,
    -- Time range
    first_event_time    SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    last_event_time     SimpleAggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree()
ORDER BY (org_id, exam_id, session_id)
TTL toDateTime(last_event_time) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192, allow_dimensions_outside_sorting_key = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS argus_analytics.session_ai_summary_mv
TO argus_analytics.session_ai_summary
AS SELECT
    org_id,
    exam_id,
    session_id,
    student_id,
    -- Vision
    avgIf(liveness_score, liveness_score >= 0) AS avg_liveness_score,
    countIf(liveness_score >= 0) AS liveness_count,
    minIf(face_similarity, face_similarity >= 0) AS min_face_similarity,
    avgIf(face_similarity, face_similarity >= 0) AS avg_face_similarity,
    countIf(face_similarity >= 0) AS face_check_count,
    countIf(event_type = 'head_pose_anomaly') AS head_pose_anomalies,
    countIf(event_type = 'face_occluded') AS face_occluded_count,
    -- Audio
    countIf(vad_active = 1) AS speech_segments,
    countIf(event_type = 'whisper_detected') AS whisper_detections,
    countIf(event_type = 'second_speaker_detected') AS second_speaker_count,
    avgIf(audio_rms_db, audio_rms_db > -100) AS avg_audio_rms_db,
    countIf(audio_rms_db > -100) AS audio_sample_count,
    -- Time
    min(server_timestamp) AS first_event_time,
    max(server_timestamp) AS last_event_time
FROM argus_analytics.proctoring_events
GROUP BY org_id, exam_id, session_id, student_id;

-- ---------------------------------------------------------------------------
-- Schema version tracking
-- ---------------------------------------------------------------------------

INSERT INTO argus_analytics.schema_version (version, description, applied_at)
VALUES ('3.0.0', 'AI inference columns: head pose, liveness, face embedding, audio analysis, session AI summary MV', now());
