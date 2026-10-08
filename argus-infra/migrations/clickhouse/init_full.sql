-- =============================================================================
--  Argus AI — Event Collector ClickHouse Schema
--  Database: argus_analytics
--
--  This file defines the complete analytical storage layer for real-time
--  proctoring events. It is executed on first container startup via the
--  docker-entrypoint-initdb.d mechanism and is idempotent (IF NOT EXISTS).
--
-- =============================================================================
--  Architecture-level design decisions:
--
--  1. MergeTree engine family.
--     MergeTree is the only ClickHouse engine that provides all four properties
--     needed for production analytics: column compression, sorted storage,
--     concurrent inserts, and background part merges. We use SummingMergeTree
--     for pre-aggregated materialized views (automatic summation on merge).
--
--  2. LowCardinality(String) for enum-like columns.
--     ClickHouse stores LowCardinality as a dictionary-encoded integer
--     internally. For columns with <10K distinct values (event_type, severity,
--     source, payload_type, sdk_version, resolution, region), this achieves
--     5-20x compression improvement AND 2-3x faster GROUP BY / WHERE filtering
--     because comparisons operate on integer keys, not byte strings.
--
--  3. Monthly partitioning (PARTITION BY toYYYYMM).
--     Partition granularity is chosen for data lifecycle management, NOT query
--     performance (ClickHouse's sparse index handles range scans regardless).
--     Monthly partitions enable:
--       - Single ALTER TABLE DROP PARTITION for instant deletion of expired data
--       - Partition-level BACKUP/RESTORE for cold-storage archival
--       - Partition-level MOVE TO DISK for tiered storage (hot SSD → cold HDD)
--
--  4. PRIMARY KEY / ORDER BY = (org_id, exam_id, session_id, server_timestamp).
--     Chosen for the dominant query pattern: "all events for session X in exam Y
--     for org Z, ordered by time." ClickHouse stores data physically sorted by
--     this key, turning these queries into sequential disk reads. The sparse
--     index marks every 8192 rows, enabling sub-millisecond index lookups even
--     at billions of rows.
--
--  5. TTL 90 days.
--     Automatic data expiry during background merges. No external cron job or
--     cleanup script needed. Kafka retains events for 7 days as source of truth;
--     ClickHouse keeps 90 days for historical dashboard queries.
--
--  6. Data Skipping Indices.
--     Secondary indices that ClickHouse evaluates BEFORE reading data granules.
--     Unlike traditional B-tree indices, these are probabilistic (bloom filters)
--     or set-based — they tell ClickHouse which granules to SKIP, not which rows
--     to read. This is critical for point lookups (event_id, student_id) on a
--     table sorted by a different key.
--
--  7. Materialized Views as pre-computed aggregations.
--     ClickHouse materialized views are INSERT triggers — they transform and
--     insert data at write time, not read time. This moves computation from
--     dashboard queries (many reads) to ingestion (one write), achieving
--     sub-millisecond dashboard response times regardless of data volume.
--
--  Column count: 21 (matches Go writer's insertBatch exactly)
--  Event types:  40 (mapped 1:1 from proto EventType enum)
--  Payloads:     9 type-specific JSON schemas
-- =============================================================================


-- =============================================================================
--  1. PRIMARY TABLE: proctoring_events
--
--  Stores every raw proctoring event ingested from browser proctoring sessions.
--  This is the source-of-truth analytical table — all materialized views and
--  dashboards derive from it. The Kafka topic is the ultimate source of truth;
--  this table can be fully rebuilt from Kafka if necessary.
--
--  Expected write throughput: 10,000+ events/sec (peak during exam periods)
--  Expected table size: ~500M-2B rows/month at full global scale
--  Query latency target: <100ms for session-scoped queries, <1s for org-wide
-- =============================================================================

CREATE TABLE IF NOT EXISTS proctoring_events
(
    -- ─── Identity Fields ─────────────────────────────────────────────────
    -- These form the core entity relationships: org → exam → session → event.
    -- All are String type (not UUID/FixedString) for flexibility with
    -- client-generated IDs and cross-system compatibility.

    event_id            String                  COMMENT 'UUIDv7 — unique event identifier, time-ordered for chronological sorting',
    session_id          String                  COMMENT 'Active proctoring session identifier — primary join key for session replay',
    student_id          String                  COMMENT 'Student IIN (Individual Identification Number) — links to user service',
    exam_id             String                  COMMENT 'Exam identifier from ExamProctoringConfig — links to exam service',
    org_id              String                  COMMENT 'Organization/institution ID — top-level tenant isolation boundary',

    -- ─── Classification Fields ───────────────────────────────────────────
    -- Enum-like columns with <100 distinct values. LowCardinality encodes
    -- these as dictionary-compressed integers, yielding 10-20x compression
    -- and 2-3x faster filtering compared to plain String.

    event_type          LowCardinality(String)  COMMENT 'Event type: one of 40 types across 8 categories (video, object, audio, browser, network, psychometry, kernel, telemetry)',
    severity            LowCardinality(String)  COMMENT 'Severity level: unspecified | info | warning | critical',
    source              LowCardinality(String)  COMMENT 'Event origin: webcam | side_camera | system | browser | kernel_agent | network_probe',

    -- ─── Timestamps ──────────────────────────────────────────────────────
    -- DateTime64(3) = millisecond precision in UTC. ClickHouse stores these
    -- as Int64 epoch-millis internally, enabling fast arithmetic and range
    -- comparisons while displaying human-readable format.

    server_timestamp    DateTime64(3, 'UTC')    COMMENT 'Server-side receipt timestamp — authoritative time used for ORDER BY and TTL',
    client_timestamp    DateTime64(3, 'UTC')    COMMENT 'Client-side event timestamp — may have clock drift, used for latency analysis',
    video_timestamp_sec Float64         DEFAULT 0 COMMENT 'Seconds elapsed from session start — enables frame-accurate video archive replay',

    -- ─── Detection Data ──────────────────────────────────────────────────
    -- The core analytical payload for each event.

    label               String          DEFAULT '' COMMENT 'Human-readable event description (e.g., "Phone detected in upper-left quadrant")',
    confidence          Float32         DEFAULT 0  COMMENT 'AI model confidence score [0.0, 1.0] — used for threshold filtering and quality metrics',
    payload             String          DEFAULT '' COMMENT 'JSON-encoded type-specific payload (GazeDeviation, FaceDetection, ObjectDetection, etc.)',
    payload_type        LowCardinality(String) DEFAULT '' COMMENT 'Payload type discriminator: gaze | face | object | audio | browser | system | psychometry | network | kernel',

    -- ─── Client Metadata ─────────────────────────────────────────────────
    -- Captured at the transport layer for debugging, compatibility tracking,
    -- and geographic analytics. These fields are NOT part of the event domain
    -- model — they are infrastructure metadata.

    user_agent          String          DEFAULT '' COMMENT 'Browser User-Agent string — parsed for browser/OS analytics',
    sdk_version         LowCardinality(String) DEFAULT '' COMMENT 'Client SDK version (e.g., "1.2.3") — tracks deployment rollout and compatibility',
    resolution          LowCardinality(String) DEFAULT '' COMMENT 'Screen resolution (e.g., "1920x1080") — used for video processing calibration',
    timezone_offset_min Int32           DEFAULT 0  COMMENT 'Client timezone offset in minutes from UTC (e.g., +360 for UTC+6)',
    ip_address          String          DEFAULT '' COMMENT 'Client IP address — set server-side, used for geo-mapping and VPN detection',
    region              LowCardinality(String) DEFAULT '' COMMENT 'Geographic region derived from IP (e.g., "kz-astana") — set server-side'

    -- ─── Data Skipping Indices ───────────────────────────────────────────
    -- These are NOT traditional B-tree indices. They are probabilistic
    -- structures evaluated BEFORE reading column data. ClickHouse checks
    -- each index against query predicates and SKIPS granules (groups of
    -- 8192 rows) that provably cannot contain matching rows.
    --
    -- Index types used:
    --   bloom_filter: Probabilistic membership test. Uses ~10 bytes/granule.
    --     False positive rate ~1% (configurable). Ideal for high-cardinality
    --     point lookups (event_id, student_id, session_id, ip_address) where
    --     the column is NOT in the primary key or is not the leftmost prefix.
    --
    --   set(N): Stores the set of distinct values per granule (up to N).
    --     Exact match (no false positives). Ideal for low-cardinality columns
    --     (event_type, severity, source, payload_type) where N distinct values
    --     per granule is realistic.
    --
    --   minmax: Stores min and max values per granule. Zero overhead,
    --     extremely fast. Ideal for monotonic or semi-monotonic columns
    --     (client_timestamp) where range queries can skip large regions.
    --
    -- Granularity parameter = number of granules per index block.
    -- granularity=1: index entry per 8192 rows (finest, most memory)
    -- granularity=4: index entry per 32768 rows (coarser, less memory)
    -- We use granularity=1 for point lookups and 4 for range/set filters.

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
)
ENGINE = MergeTree

-- ─── Partitioning ────────────────────────────────────────────────────────
-- Monthly partitions for lifecycle management. Each month is an independent
-- "partition" that can be:
--   - DROP PARTITION'd for instant deletion (no row-by-row delete)
--   - DETACH'd and ATTACH'd for cold-storage archival
--   - MOVE'd to different storage policies (SSD → HDD tiering)
--   - BACKUP'd independently for granular disaster recovery
PARTITION BY toYYYYMM(server_timestamp)

-- ─── Sort Order (Primary Key) ───────────────────────────────────────────
-- The physical sort order determines query performance for range scans.
-- This 4-column key is optimized for the dominant dashboard query:
--
--   SELECT * FROM proctoring_events
--   WHERE org_id = ? AND exam_id = ? AND session_id = ?
--   ORDER BY server_timestamp
--
-- ClickHouse creates a sparse index with marks every 8192 rows.
-- For this query pattern, it skips 99.99%+ of data on a billion-row table.
--
-- Prefix queries (org_id only, or org_id + exam_id) are also efficient
-- because they match the leftmost prefix of the sort key.
ORDER BY (org_id, exam_id, session_id, server_timestamp)

-- ─── TTL (Time To Live) ─────────────────────────────────────────────────
-- Events older than 90 days are automatically deleted during background
-- merges. No external cleanup job required.
--
-- TTL evaluation happens:
--   1. During regular part merges (lazy — eventual consistency)
--   2. Explicitly via OPTIMIZE TABLE ... FINAL (forced)
--
-- Kafka retains events for 7 days as the source of truth.
-- ClickHouse provides 90 days of historical query capability.
-- For compliance requirements >90 days, configure S3/GCS cold storage
-- with ALTER TABLE MODIFY TTL ... TO DISK 'cold_storage'.
TTL toDateTime(server_timestamp) + INTERVAL 90 DAY

SETTINGS
    -- Sparse index granularity. 8192 rows between consecutive index marks.
    -- This is the universal default for analytical workloads. Smaller values
    -- (e.g., 256) improve point-query selectivity but increase index memory.
    -- 8192 balances memory usage (~10MB index per billion rows) with scan
    -- performance for our mixed workload (point + range queries).
    index_granularity = 8192,

    -- Minimum size threshold for wide-format storage. Parts with more than
    -- this many rows use a column-per-file layout (more efficient for
    -- selective column reads). Smaller parts use compact format (fewer files,
    -- better for small inserts from the batch writer).
    min_bytes_for_wide_part = 10485760,

    -- Merge time settings for optimal background merge behavior.
    -- These prevent the merge scheduler from creating too many small parts
    -- (which degrade read performance) or too few large merges (which
    -- increase write amplification).
    parts_to_delay_insert = 300,
    parts_to_throw_insert = 600;


-- =============================================================================
--  2. MATERIALIZED VIEW: session_event_counts
--
--  Pre-aggregated event counts per (session, event_type, severity).
--
--  Dashboard query this powers:
--    "How many violations of each type occurred in session X?"
--    "Show me the violation breakdown for the current exam."
--
--  Engine: SummingMergeTree.
--    ClickHouse automatically sums the `count` column when merging data
--    parts. Final results require sum(count) in SELECT to account for
--    not-yet-merged parts. The AggregateFunction(max, ...) pattern is used
--    for last_seen to get the true maximum across merges.
--
--  Expected size: ~100 rows per session (40 event types × few severities)
--  Query latency: <5ms for session-scoped lookups
-- =============================================================================

CREATE TABLE IF NOT EXISTS session_event_counts
(
    session_id      String,
    exam_id         String,
    org_id          String,
    event_type      LowCardinality(String),
    severity        LowCardinality(String),
    count           UInt64,
    last_seen       DateTime64(3, 'UTC')        COMMENT 'Timestamp of the most recent event in this group',
    min_confidence  Float32         DEFAULT 1.0  COMMENT 'Minimum confidence across events in this group',
    max_confidence  Float32         DEFAULT 0.0  COMMENT 'Maximum confidence across events in this group',

    -- Skip index for fast session lookups on this aggregated table.
    INDEX idx_sc_session session_id TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = SummingMergeTree(count)
PARTITION BY toYYYYMM(last_seen)
ORDER BY (org_id, exam_id, session_id, event_type, severity)
TTL toDateTime(last_seen) + INTERVAL 90 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS session_event_counts_mv
TO session_event_counts
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
FROM proctoring_events;


-- =============================================================================
--  3. MATERIALIZED VIEW: hourly_event_stats
--
--  Hourly aggregated statistics per (org, exam, event_type, severity).
--
--  Dashboard queries this powers:
--    "How many events per hour across the entire platform?"
--    "Which exams have the highest violation rates?"
--    "What is the average AI confidence trend over time?"
--    "Show me a heatmap of events by hour for the past week."
--
--  Engine: SummingMergeTree with multiple summing columns.
--    event_count, unique_sessions, avg_confidence, confidence_count are
--    all summed during merges. Final queries must use sum() for correctness.
--    Average confidence = sum(avg_confidence) / sum(confidence_count).
--
--  TTL: 180 days (longer than raw events for trend analysis).
--
--  Expected size: ~50K rows/day (24 hours × orgs × exams × types × severities)
--  Query latency: <50ms for week-range queries, <200ms for month-range
-- =============================================================================

CREATE TABLE IF NOT EXISTS hourly_event_stats
(
    hour             DateTime        COMMENT 'Truncated to hour boundary (toStartOfHour)',
    org_id           String,
    exam_id          String,
    event_type       LowCardinality(String),
    severity         LowCardinality(String),
    event_count      UInt64          COMMENT 'Number of events in this (hour, org, exam, type, severity) bucket',
    unique_sessions  UInt64          COMMENT 'Number of unique sessions (approximate after merge — use uniqMerge for exact)',
    avg_confidence   Float64         COMMENT 'Running sum of confidence — divide by confidence_count for true average',
    confidence_count UInt64          COMMENT 'Number of events contributing to avg_confidence sum',

    -- Skip index for org-level time-range queries on aggregated data.
    INDEX idx_hs_hour hour TYPE minmax GRANULARITY 1
)
ENGINE = SummingMergeTree((event_count, unique_sessions, avg_confidence, confidence_count))
PARTITION BY toYYYYMM(hour)
ORDER BY (org_id, exam_id, event_type, severity, hour)
TTL hour + INTERVAL 180 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_event_stats_mv
TO hourly_event_stats
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
FROM proctoring_events;


-- =============================================================================
--  4. MATERIALIZED VIEW: critical_events_recent
--
--  Real-time feed of critical-severity events, sorted for instant retrieval.
--
--  Dashboard queries this powers:
--    "Show me the last 50 critical violations across all active exams."
--    "Alert: which students triggered critical events in the past 10 minutes?"
--
--  This view exists because querying critical events from the main table
--  requires scanning all severity values (even with the set index). A
--  dedicated table with pre-filtered critical events provides sub-millisecond
--  lookups for the real-time alert dashboard — the highest-priority feature
--  for exam proctors.
--
--  Engine: MergeTree (not SummingMergeTree — we need individual event rows).
--  TTL: 7 days (critical alerts are only actionable in real-time).
--  Sort: (org_id, server_timestamp DESC) for "latest critical events" queries.
-- =============================================================================

CREATE TABLE IF NOT EXISTS critical_events_recent
(
    event_id         String,
    session_id       String,
    student_id       String,
    exam_id          String,
    org_id           String,
    event_type       LowCardinality(String),
    source           LowCardinality(String),
    server_timestamp DateTime64(3, 'UTC'),
    label            String          DEFAULT '',
    confidence       Float32         DEFAULT 0,
    payload_type     LowCardinality(String) DEFAULT '',

    INDEX idx_cr_session  session_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_cr_student  student_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_cr_type     event_type TYPE set(100)           GRANULARITY 2
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(server_timestamp)
ORDER BY (org_id, exam_id, server_timestamp)
TTL toDateTime(server_timestamp) + INTERVAL 7 DAY
SETTINGS index_granularity = 4096;

CREATE MATERIALIZED VIEW IF NOT EXISTS critical_events_recent_mv
TO critical_events_recent
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
FROM proctoring_events
WHERE severity = 'critical';


-- =============================================================================
--  5. MATERIALIZED VIEW: student_session_summary
--
--  Per-student, per-session summary with violation counts and risk scoring.
--
--  Dashboard queries this powers:
--    "Show me a ranked list of students by total violations in exam X."
--    "Which students had the most critical events?"
--    "Generate the post-exam integrity report for student Y."
--
--  This is the foundation for the "Student Risk Score" feature — the
--  weighted sum of violation counts that determines whether a session
--  is flagged for manual review.
--
--  Engine: SummingMergeTree (sums all counter columns on merge).
--  TTL: 90 days (matches raw event retention for report generation).
-- =============================================================================

CREATE TABLE IF NOT EXISTS student_session_summary
(
    student_id          String,
    session_id          String,
    exam_id             String,
    org_id              String,
    total_events        UInt64          COMMENT 'Total events in this session',
    critical_count      UInt64          COMMENT 'Number of critical-severity events',
    warning_count       UInt64          COMMENT 'Number of warning-severity events',
    info_count          UInt64          COMMENT 'Number of info-severity events',
    first_event_time    DateTime64(3, 'UTC') COMMENT 'Earliest event timestamp (use min() in final query)',
    last_event_time     DateTime64(3, 'UTC') COMMENT 'Latest event timestamp (use max() in final query)',

    INDEX idx_ss_student student_id TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = SummingMergeTree((total_events, critical_count, warning_count, info_count))
PARTITION BY toYYYYMM(last_event_time)
ORDER BY (org_id, exam_id, student_id, session_id)
TTL toDateTime(last_event_time) + INTERVAL 90 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS student_session_summary_mv
TO student_session_summary
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
FROM proctoring_events;


-- =============================================================================
--  6. SYSTEM TABLE: schema_version
--
--  Tracks DDL migration history for this database. Each migration script
--  records its version here. The event-collector service can query this
--  table on startup to verify schema compatibility.
-- =============================================================================

CREATE TABLE IF NOT EXISTS schema_version
(
    version         String          COMMENT 'Semantic version of the schema migration (e.g., 1.0.0)',
    description     String          COMMENT 'Human-readable description of the migration',
    applied_at      DateTime64(3, 'UTC') DEFAULT now64(3) COMMENT 'When this migration was applied',
    checksum        String          DEFAULT '' COMMENT 'SHA-256 of the migration SQL for drift detection'
)
ENGINE = MergeTree
ORDER BY (version, applied_at);

-- =============================================================================
--  7. MATERIALIZED VIEW: global_org_stats
--
--  Cross-organization aggregation for the Super Admin dashboard.
--
--  This view pre-computes per-org daily statistics so the Super Admin can
--  see "which organizations are most active?" without scanning billions of
--  raw events. The daily granularity is sufficient for the admin dashboard
--  while keeping the table small (~365 rows × num_orgs × num_event_types/year).
--
--  Dashboard queries this powers:
--    "Show me total events per organization today."
--    "Which org has the most critical violations this week?"
--    "Platform-wide event volume trend for the last 30 days."
--
--  Performance optimization for Super Admin (org_id = '*'):
--    Instead of scanning the full proctoring_events table (billions of rows),
--    the Super Admin's aggregate queries hit this ~10K row table.
--    Expected query time: <10ms for 30-day range across all organizations.
--
--  Engine: SummingMergeTree (auto-sums counters on part merge).
--  TTL: 365 days (yearly trend data).
-- =============================================================================

CREATE TABLE IF NOT EXISTS global_org_stats
(
    day              Date            COMMENT 'Truncated to day boundary',
    org_id           String          COMMENT 'Organization identifier',
    event_count      UInt64          COMMENT 'Total events for this org on this day',
    critical_count   UInt64          COMMENT 'Critical severity events',
    warning_count    UInt64          COMMENT 'Warning severity events',
    session_count    UInt64          COMMENT 'Approximate unique sessions (pre-merge)',
    exam_count       UInt64          COMMENT 'Approximate unique exams (pre-merge)',

    INDEX idx_gos_org org_id TYPE set(1000) GRANULARITY 1
)
ENGINE = SummingMergeTree((event_count, critical_count, warning_count, session_count, exam_count))
PARTITION BY toYYYYMM(day)
ORDER BY (day, org_id)
TTL day + INTERVAL 365 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS global_org_stats_mv
TO global_org_stats
AS
SELECT
    toDate(server_timestamp)                          AS day,
    org_id,
    toUInt64(1)                                       AS event_count,
    toUInt64(if(severity = 'critical', 1, 0))         AS critical_count,
    toUInt64(if(severity = 'warning', 1, 0))          AS warning_count,
    toUInt64(1)                                       AS session_count,
    toUInt64(1)                                       AS exam_count
FROM proctoring_events;


-- =============================================================================
--  8. EVIDENCE TABLE: evidence_fragments
--
--  Stores metadata for immutable video evidence fragments captured from
--  proctoring sessions during violation events. Each fragment is linked to:
--    - A specific violation event (event_id)
--    - A proctoring session (session_id)
--    - An S3 object in MinIO (uri)
--    - A SHA-256 hash for integrity verification (sha256_hash)
--
--  The actual video data is stored in MinIO (S3) with GOVERNANCE retention.
--  This table stores only metadata for queryability and chain of custody.
--
--  TTL: 730 days (2 years) — legal compliance requirement exceeding the
--  90-day TTL of raw events, since evidence may be needed for appeals,
--  investigations, and legal proceedings.
--
--  Engine: MergeTree (individual rows needed, not aggregated).
--  Partition: Monthly by upload timestamp.
--  Sort: (org_id, session_id, uploaded_at) for session-scoped evidence queries.
-- =============================================================================

CREATE TABLE IF NOT EXISTS evidence_fragments
(
    -- ─── Identity ───────────────────────────────────────────────────────
    fragment_id     String              COMMENT 'Unique evidence fragment identifier',
    session_id      String              COMMENT 'Proctoring session that produced this evidence',
    event_id        String              COMMENT 'Violation event that triggered evidence capture',
    org_id          String              COMMENT 'Organization (tenant) that owns this evidence',
    exam_id         String              COMMENT 'Exam identifier',
    student_id      String              COMMENT 'Student identifier',

    -- ─── Integrity ──────────────────────────────────────────────────────
    sha256_hash     String              COMMENT 'SHA-256 hash of evidence file — tamper-evidence verification',
    uri             String              COMMENT 'S3 storage location (s3://argus-evidence/org/session/fragment.webm)',
    size_bytes      Int64               COMMENT 'File size in bytes',
    content_type    LowCardinality(String) COMMENT 'MIME type (video/webm, video/mp4)',

    -- ─── Temporal ───────────────────────────────────────────────────────
    duration_sec    Float64             COMMENT 'Evidence clip duration in seconds',
    start_time      DateTime64(3, 'UTC') COMMENT 'Timestamp of earliest frame in clip',
    end_time        DateTime64(3, 'UTC') COMMENT 'Timestamp of latest frame in clip',
    uploaded_at     DateTime64(3, 'UTC') DEFAULT now64(3) COMMENT 'When this evidence was uploaded to MinIO',

    -- ─── Forensic Ledger (Hash Chain) ───────────────────────────────────
    -- Cryptographic hash chaining for tamper detection (v2.1).
    -- Each record links to the previous via SHA-256, forming an immutable
    -- per-session chain. Detects insertion, deletion, and reordering.
    sequence_num    UInt64  DEFAULT 0   COMMENT 'Monotonic sequence number per session (starts at 1, 0 = legacy)',
    previous_hash   String  DEFAULT ''  COMMENT 'SHA-256 of previous chain record (GENESIS for first record)',
    record_hash     String  DEFAULT ''  COMMENT 'SHA-256 chain hash: H(seq|prev_hash|fragment_id|sha256|uploaded_at)'

    -- ─── Data Skipping Indices ──────────────────────────────────────────
    , INDEX idx_ef_fragment_id  fragment_id TYPE bloom_filter(0.01) GRANULARITY 1
    , INDEX idx_ef_session_id   session_id  TYPE bloom_filter(0.01) GRANULARITY 1
    , INDEX idx_ef_event_id     event_id    TYPE bloom_filter(0.01) GRANULARITY 1
    , INDEX idx_ef_sha256       sha256_hash TYPE bloom_filter(0.01) GRANULARITY 1
    , INDEX idx_ef_student_id   student_id  TYPE bloom_filter(0.01) GRANULARITY 4
    , INDEX idx_ef_record_hash  record_hash TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(uploaded_at)
ORDER BY (org_id, session_id, uploaded_at)
TTL toDateTime(uploaded_at) + INTERVAL 730 DAY
SETTINGS index_granularity = 8192;


-- Record migrations.
INSERT INTO schema_version (version, description)
VALUES ('1.0.0', 'Initial schema: proctoring_events + 5 materialized views + data skipping indices + cross-org aggregation');

INSERT INTO schema_version (version, description)
VALUES ('1.1.0', 'Evidence storage: evidence_fragments table with 730-day TTL for chain of custody');

INSERT INTO schema_version (version, description)
VALUES ('2.1.0', 'Forensic ledger: hash chaining columns (sequence_num, previous_hash, record_hash) on evidence_fragments');
-- =============================================================================
--  Argus AI — Migration 002: Forensic Ledger Hash Chaining
--  Database: argus_analytics
--
--  Adds cryptographic hash chaining columns to evidence_fragments table.
--  These columns form an immutable forensic ledger that detects tampering:
--    - Insertion of forged records (breaks sequence_num continuity)
--    - Deletion of legitimate records (breaks previous_hash linkage)
--    - Reordering of records (breaks record_hash verification)
--
--  Hash chain per session:
--    record_hash = SHA-256(sequence_num || previous_hash || fragment_id || sha256_hash || uploaded_at)
--    First record in session: previous_hash = "GENESIS"
--
--  This migration is idempotent — safe to run multiple times.
-- =============================================================================


-- ── Add hash chain columns to evidence_fragments ────────────────────────────

-- Monotonic sequence number per session. Starts at 1 for the first fragment.
-- Used for ordering and gap detection during chain verification.
ALTER TABLE evidence_fragments
    ADD COLUMN IF NOT EXISTS sequence_num UInt64 DEFAULT 0
    COMMENT 'Monotonic sequence number per session (starts at 1, 0 = legacy pre-chain record)';

-- SHA-256 hash of the previous record in this session chain.
-- "GENESIS" for the first record. Enables forward-only chain traversal.
ALTER TABLE evidence_fragments
    ADD COLUMN IF NOT EXISTS previous_hash String DEFAULT ''
    COMMENT 'SHA-256 of previous chain record (GENESIS for first record in session)';

-- SHA-256 hash of this record computed from chain inputs.
-- Formula: SHA-256(sequence_num|previous_hash|fragment_id|sha256_hash|uploaded_at)
-- Enables independent verification of chain integrity.
ALTER TABLE evidence_fragments
    ADD COLUMN IF NOT EXISTS record_hash String DEFAULT ''
    COMMENT 'SHA-256 chain hash: H(seq|prev_hash|fragment_id|sha256|uploaded_at)';


-- ── Data skipping index for chain queries ───────────────────────────────────
-- Enable fast lookups by record_hash for integrity verification endpoints.
ALTER TABLE evidence_fragments
    ADD INDEX IF NOT EXISTS idx_ef_record_hash record_hash TYPE bloom_filter(0.01) GRANULARITY 1;


-- ── Record migration ────────────────────────────────────────────────────────
INSERT INTO schema_version (version, description)
VALUES ('2.1.0', 'Forensic ledger: hash chaining columns (sequence_num, previous_hash, record_hash) on evidence_fragments');

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
SETTINGS index_granularity = 8192;

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
-- =============================================================================
-- Migration 005: Hybrid Logical Clock — Causal Sequence Column
--
-- Adds a server-authoritative causal ordering column to the events table.
-- CausalSequence is a uint64 HLC value: [48-bit physical ms][16-bit logical].
-- It guarantees monotonic ordering immune to client clock manipulation.
--
-- Usage: ORDER BY causal_sequence for globally consistent event ordering.
-- The existing server_timestamp remains for human-readable queries.
-- =============================================================================

ALTER TABLE argus_analytics.proctoring_events
    ADD COLUMN IF NOT EXISTS causal_sequence UInt64 DEFAULT 0
    COMMENT 'Hybrid Logical Clock value for causal ordering (physical_ms << 16 | logical)';

