USE argus_analytics;
-- =============================================================================
-- Argus AI — ClickHouse Migration 004: Reliability Hardening
-- =============================================================================
--
-- Two changes:
--   1. Explicit TTL DELETE on proctoring_events (no-op — clarifies intent).
--   2. Engine migration to ReplacingMergeTree for DLQ replay deduplication.
--
-- ReplacingMergeTree guarantees:
--   - Duplicate events (same ORDER BY key) are collapsed to one row on merge.
--   - The row with the highest `server_timestamp` is kept (version column).
--   - Queries must use FINAL or argMax() for strict dedup before background
--     merges complete. After OPTIMIZE TABLE ... FINAL, dedup is guaranteed.
--
-- Migration strategy:
--   1. Rename existing MergeTree table to _legacy.
--   2. Create new ReplacingMergeTree table with event_id in ORDER BY.
--   3. Migrate data from legacy table.
--   4. Drop legacy table.
--   5. Recreate all materialized views on the new table.
-- =============================================================================


-- ---------------------------------------------------------------------------
-- Step 1: Rename existing table
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS argus_analytics.proctoring_events_legacy SYNC;
RENAME TABLE argus_analytics.proctoring_events TO argus_analytics.proctoring_events_legacy;


-- ---------------------------------------------------------------------------
-- Step 2: Create new proctoring_events with ReplacingMergeTree
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS argus_analytics.proctoring_events
(
    -- Identity
    event_id            String,
    session_id          String,
    student_id          String,
    exam_id             String,
    org_id              String,

    -- Classification
    event_type          LowCardinality(String),
    severity            LowCardinality(String),
    source              LowCardinality(String),

    -- Timestamps
    server_timestamp    DateTime64(3, 'UTC'),
    client_timestamp    DateTime64(3, 'UTC'),
    video_timestamp_sec Float64         DEFAULT 0,

    -- Detection
    label               String          DEFAULT '',
    confidence          Float32         DEFAULT 0,
    payload             String          DEFAULT '',
    payload_type        LowCardinality(String) DEFAULT '',

    -- Client Metadata
    user_agent          String          DEFAULT '',
    sdk_version         LowCardinality(String) DEFAULT '',
    resolution          LowCardinality(String) DEFAULT '',
    timezone_offset_min Int32           DEFAULT 0,
    ip_address          String          DEFAULT '',
    region              LowCardinality(String) DEFAULT '',

    -- Causal ordering (migration 005)
    causal_sequence     UInt64          DEFAULT 0,

    -- AI Vision (migration 003)
    head_yaw            Float32         DEFAULT 0,
    head_pitch          Float32         DEFAULT 0,
    head_roll           Float32         DEFAULT 0,
    face_bbox           String          DEFAULT '',
    liveness_score      Float32         DEFAULT -1,
    face_embedding      Array(Float32)  DEFAULT [],
    face_similarity     Float32         DEFAULT -1,

    -- AI Audio (migration 003)
    audio_rms_db        Float32         DEFAULT -100,
    vad_active          UInt8           DEFAULT 0,
    audio_classification LowCardinality(String) DEFAULT '',
    speaker_count       UInt8           DEFAULT 0,
    speaker_match       UInt8           DEFAULT 0

    -- Data Skipping Indices
    , INDEX idx_event_id       event_id       TYPE bloom_filter(0.01) GRANULARITY 1
    , INDEX idx_session_id     session_id     TYPE bloom_filter(0.01) GRANULARITY 1
    , INDEX idx_student_id     student_id     TYPE bloom_filter(0.01) GRANULARITY 1
    , INDEX idx_ip_address     ip_address     TYPE bloom_filter(0.01) GRANULARITY 4

    , INDEX idx_event_type     event_type     TYPE set(100)           GRANULARITY 4
    , INDEX idx_severity       severity       TYPE set(10)            GRANULARITY 4
    , INDEX idx_source         source         TYPE set(20)            GRANULARITY 4
    , INDEX idx_payload_type   payload_type   TYPE set(20)            GRANULARITY 4

    , INDEX idx_client_ts      client_timestamp TYPE minmax           GRANULARITY 4
    , INDEX idx_confidence     confidence     TYPE minmax             GRANULARITY 4

    -- AI indices (migration 003)
    , INDEX idx_liveness       liveness_score TYPE minmax             GRANULARITY 4
    , INDEX idx_face_similarity face_similarity TYPE minmax           GRANULARITY 4
    , INDEX idx_audio_class    audio_classification TYPE set(20)      GRANULARITY 4
    , INDEX idx_vad            vad_active     TYPE set(2)             GRANULARITY 4
    , INDEX idx_head_yaw       head_yaw       TYPE minmax             GRANULARITY 4
)
ENGINE = ReplacingMergeTree(server_timestamp)
PARTITION BY toYYYYMM(server_timestamp)
ORDER BY (org_id, exam_id, session_id, event_id)
TTL toDateTime(server_timestamp) + INTERVAL 90 DAY DELETE
SETTINGS
    index_granularity = 8192,
    min_bytes_for_wide_part = 10485760,
    parts_to_delay_insert = 300,
    parts_to_throw_insert = 600;


-- ---------------------------------------------------------------------------
-- Step 3: Migrate data from legacy table
-- ---------------------------------------------------------------------------

INSERT INTO argus_analytics.proctoring_events
    (event_id, session_id, student_id, exam_id, org_id,
     event_type, severity, source,
     server_timestamp, client_timestamp, video_timestamp_sec,
     label, confidence, payload, payload_type,
     user_agent, sdk_version, resolution, timezone_offset_min, ip_address, region,
     head_yaw, head_pitch, head_roll, face_bbox, liveness_score, face_embedding, face_similarity,
     audio_rms_db, vad_active, audio_classification, speaker_count, speaker_match)
SELECT
    event_id, session_id, student_id, exam_id, org_id,
    event_type, severity, source,
    server_timestamp, client_timestamp, video_timestamp_sec,
    label, confidence, payload, payload_type,
    user_agent, sdk_version, resolution, timezone_offset_min, ip_address, region,
    head_yaw, head_pitch, head_roll, face_bbox, liveness_score, face_embedding, face_similarity,
    audio_rms_db, vad_active, audio_classification, speaker_count, speaker_match
FROM argus_analytics.proctoring_events_legacy;


-- ---------------------------------------------------------------------------
-- Step 4: Drop legacy table
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS argus_analytics.proctoring_events_legacy;


-- ---------------------------------------------------------------------------
-- Step 5: Recreate materialized views
--
-- Materialized views in ClickHouse are INSERT triggers on the source table.
-- After renaming/recreating the source, MVs must be dropped and recreated
-- to bind to the new table.
-- ---------------------------------------------------------------------------

-- 5a: session_event_counts
DROP VIEW IF EXISTS argus_analytics.session_event_counts_mv;
CREATE MATERIALIZED VIEW IF NOT EXISTS argus_analytics.session_event_counts_mv
TO argus_analytics.session_event_counts
AS
SELECT
    session_id,
    exam_id,
    org_id,
    event_type,
    severity,
    toUInt64(1)          AS count,
    server_timestamp     AS last_seen,
    confidence           AS min_confidence,
    confidence           AS max_confidence
FROM argus_analytics.proctoring_events;

-- 5b: hourly_event_stats
DROP VIEW IF EXISTS argus_analytics.hourly_event_stats_mv;
CREATE MATERIALIZED VIEW IF NOT EXISTS argus_analytics.hourly_event_stats_mv
TO argus_analytics.hourly_event_stats
AS
SELECT
    toStartOfHour(server_timestamp)  AS hour,
    org_id,
    exam_id,
    event_type,
    severity,
    toUInt64(1)                      AS event_count,
    toUInt64(1)                      AS unique_sessions,
    toFloat64(confidence)            AS avg_confidence,
    toUInt64(1)                      AS confidence_count
FROM argus_analytics.proctoring_events;

-- 5c: critical_events_recent
DROP VIEW IF EXISTS argus_analytics.critical_events_recent_mv;
CREATE MATERIALIZED VIEW IF NOT EXISTS argus_analytics.critical_events_recent_mv
TO argus_analytics.critical_events_recent
AS
SELECT
    event_id,
    session_id,
    student_id,
    exam_id,
    org_id,
    event_type,
    source,
    server_timestamp,
    label,
    confidence,
    payload_type
FROM argus_analytics.proctoring_events
WHERE severity = 'critical';

-- 5d: student_session_summary
DROP VIEW IF EXISTS argus_analytics.student_session_summary_mv;
CREATE MATERIALIZED VIEW IF NOT EXISTS argus_analytics.student_session_summary_mv
TO argus_analytics.student_session_summary
AS
SELECT
    student_id,
    session_id,
    exam_id,
    org_id,
    toUInt64(1)                                              AS total_events,
    toUInt64(if(severity = 'critical', 1, 0))                AS critical_count,
    toUInt64(if(severity = 'warning', 1, 0))                 AS warning_count,
    toUInt64(if(severity = 'info', 1, 0))                    AS info_count,
    server_timestamp                                         AS first_event_time,
    server_timestamp                                         AS last_event_time
FROM argus_analytics.proctoring_events;

-- 5e: global_org_stats
DROP VIEW IF EXISTS argus_analytics.global_org_stats_mv;
CREATE MATERIALIZED VIEW IF NOT EXISTS argus_analytics.global_org_stats_mv
TO argus_analytics.global_org_stats
AS
SELECT
    toDate(server_timestamp)                          AS day,
    org_id,
    toUInt64(1)                                       AS event_count,
    toUInt64(if(severity = 'critical', 1, 0))         AS critical_count,
    toUInt64(if(severity = 'warning', 1, 0))          AS warning_count,
    toUInt64(1)                                       AS session_count,
    toUInt64(1)                                       AS exam_count
FROM argus_analytics.proctoring_events;

-- 5f: session_ai_summary (migration 003)
DROP VIEW IF EXISTS argus_analytics.session_ai_summary_mv;
CREATE MATERIALIZED VIEW IF NOT EXISTS argus_analytics.session_ai_summary_mv
TO argus_analytics.session_ai_summary
AS SELECT
    org_id,
    exam_id,
    session_id,
    student_id,
    avgIf(liveness_score, liveness_score >= 0) AS avg_liveness_score,
    countIf(liveness_score >= 0) AS liveness_count,
    minIf(face_similarity, face_similarity >= 0) AS min_face_similarity,
    avgIf(face_similarity, face_similarity >= 0) AS avg_face_similarity,
    countIf(face_similarity >= 0) AS face_check_count,
    countIf(event_type = 'head_pose_anomaly') AS head_pose_anomalies,
    countIf(event_type = 'face_occluded') AS face_occluded_count,
    countIf(vad_active = 1) AS speech_segments,
    countIf(event_type = 'whisper_detected') AS whisper_detections,
    countIf(event_type = 'second_speaker_detected') AS second_speaker_count,
    avgIf(audio_rms_db, audio_rms_db > -100) AS avg_audio_rms_db,
    countIf(audio_rms_db > -100) AS audio_sample_count,
    min(server_timestamp) AS first_event_time,
    max(server_timestamp) AS last_event_time
FROM argus_analytics.proctoring_events
GROUP BY org_id, exam_id, session_id, student_id;


-- ---------------------------------------------------------------------------
-- Schema version tracking
-- ---------------------------------------------------------------------------

INSERT INTO argus_analytics.schema_version (version, description, applied_at)
VALUES ('4.0.0', 'Reliability hardening: ReplacingMergeTree for DLQ dedup, explicit TTL DELETE, event_id in ORDER BY', now());
