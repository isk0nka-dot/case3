USE argus_analytics;
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
