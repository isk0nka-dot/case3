USE argus_analytics;
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
