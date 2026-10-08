// Package minio implements the port.EvidenceStore interface using MinIO
// (S3-compatible object storage). It provides immutable evidence storage
// with SHA-256 integrity hashing, S3 Object Lock (WORM), and presigned URLs.
//
// Architecture:
//   - Upload: Single-pass SHA-256 via io.TeeReader during upload.
//   - WORM: PutObjectRetention GOVERNANCE mode after successful upload.
//   - Presign: Time-limited GET URLs for secure evidence retrieval.
//   - Key format: {org_id}/{session_id}/{fragment_id}.webm
//
// This adapter follows the same patterns as clickhouse/writer.go:
//   - Fail-fast: bucket existence is verified at construction time.
//   - Implements port.HealthChecker for readiness probes.
//   - Structured logging with zap.
package minio

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"

	"github.com/argus-ai/event-collector/internal/domain/entity"
)

// StoreConfig holds configuration for the MinIO evidence store.
type StoreConfig struct {
	Endpoint      string        // MinIO server address (host:port, no protocol).
	AccessKey     string        // MinIO access key (username).
	SecretKey     string        // MinIO secret key (password).
	Bucket        string        // S3 bucket for evidence storage.
	Region        string        // S3 region (default: "us-east-1").
	UseSSL        bool          // Enable TLS for the MinIO connection.
	PresignTTL    time.Duration // Default TTL for presigned URLs.
	UploadTimeout time.Duration // Maximum time for a single upload.
}

// Store implements port.EvidenceStore using MinIO S3-compatible storage.
// It is safe for concurrent use by multiple goroutines.
type Store struct {
	client *minio.Client
	bucket string
	region string
	config StoreConfig
	logger *zap.Logger
}

// NewStore creates a new MinIO evidence store. It verifies bucket existence
// at construction time (fail-fast pattern) and returns an error if the
// bucket does not exist or MinIO is unreachable.
func NewStore(cfg StoreConfig, logger *zap.Logger) (*Store, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("minio: failed to create client: %w", err)
	}

	// Fail-fast: verify the bucket exists at startup.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	exists, err := client.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("minio: failed to check bucket %q: %w", cfg.Bucket, err)
	}
	if !exists {
		return nil, fmt.Errorf("minio: bucket %q does not exist — run minio-init container first", cfg.Bucket)
	}

	logger.Info("minio evidence store initialised",
		zap.String("endpoint", cfg.Endpoint),
		zap.String("bucket", cfg.Bucket),
		zap.Bool("ssl", cfg.UseSSL),
		zap.Duration("presign_ttl", cfg.PresignTTL),
	)

	return &Store{
		client: client,
		bucket: cfg.Bucket,
		region: cfg.Region,
		config: cfg,
		logger: logger,
	}, nil
}

// Upload stores an evidence fragment in MinIO with single-pass SHA-256 hashing.
// The reader provides the raw video data. Returns the computed hash, S3 URI,
// and size in bytes.
//
// S3 key format: {org_id}/{session_id}/{fragment_id}.webm
//
// After upload, GOVERNANCE retention is applied to prevent deletion.
func (s *Store) Upload(ctx context.Context, fragment *entity.EvidenceFragment, reader io.Reader) (sha256Hash string, uri string, sizeBytes int64, err error) {
	// Build the S3 object key.
	objectKey := s.objectKey(fragment)

	// Create SHA-256 hasher with TeeReader for single-pass hashing.
	hasher := sha256.New()
	teeReader := io.TeeReader(reader, hasher)

	// Apply upload timeout.
	uploadCtx, cancel := context.WithTimeout(ctx, s.config.UploadTimeout)
	defer cancel()

	// Upload to MinIO. PutObject reads from teeReader, which simultaneously
	// feeds the hasher. This avoids a second pass over the data.
	info, err := s.client.PutObject(uploadCtx, s.bucket, objectKey, teeReader, -1, minio.PutObjectOptions{
		ContentType: fragment.ContentType,
		UserMetadata: map[string]string{
			"session-id":  fragment.SessionID,
			"event-id":    fragment.EventID,
			"org-id":      fragment.OrgID,
			"exam-id":     fragment.ExamID,
			"student-id":  fragment.StudentID,
			"fragment-id": fragment.FragmentID,
		},
	})
	if err != nil {
		return "", "", 0, fmt.Errorf("minio: upload failed for %s: %w", objectKey, err)
	}

	// Compute the final SHA-256 hash.
	hash := hex.EncodeToString(hasher.Sum(nil))

	// Build the S3 URI.
	s3URI := fmt.Sprintf("s3://%s/%s", s.bucket, objectKey)

	// Apply GOVERNANCE retention (best-effort — log warning if it fails).
	retentionCtx, retCancel := context.WithTimeout(ctx, 5*time.Second)
	defer retCancel()

	retentionMode := minio.Governance
	retainUntil := time.Now().Add(365 * 24 * time.Hour)
	err = s.client.PutObjectRetention(retentionCtx, s.bucket, objectKey, minio.PutObjectRetentionOptions{
		Mode:            &retentionMode,
		RetainUntilDate: &retainUntil,
		VersionID:       info.VersionID,
	})
	if err != nil {
		// Log but don't fail — retention may not be available in dev mode.
		s.logger.Warn("minio: failed to set GOVERNANCE retention (may not be available in dev mode)",
			zap.String("key", objectKey),
			zap.Error(err),
		)
	}

	s.logger.Debug("evidence fragment uploaded",
		zap.String("key", objectKey),
		zap.String("sha256", hash),
		zap.Int64("size_bytes", info.Size),
		zap.String("version_id", info.VersionID),
	)

	return hash, s3URI, info.Size, nil
}

// PresignURL generates a time-limited presigned GET URL for downloading
// an evidence fragment. The URI must be in the format "s3://bucket/key".
func (s *Store) PresignURL(ctx context.Context, uri string, ttl time.Duration) (string, error) {
	objectKey, err := s.parseURI(uri)
	if err != nil {
		return "", err
	}

	reqParams := make(url.Values)
	presignedURL, err := s.client.PresignedGetObject(ctx, s.bucket, objectKey, ttl, reqParams)
	if err != nil {
		return "", fmt.Errorf("minio: failed to generate presigned URL for %s: %w", objectKey, err)
	}

	return presignedURL.String(), nil
}

// VerifyIntegrity re-downloads the object and computes SHA-256 to verify
// against the expected hash. Returns true if the hashes match.
func (s *Store) VerifyIntegrity(ctx context.Context, uri string, expectedHash string) (bool, error) {
	objectKey, err := s.parseURI(uri)
	if err != nil {
		return false, err
	}

	obj, err := s.client.GetObject(ctx, s.bucket, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return false, fmt.Errorf("minio: failed to get object %s for verification: %w", objectKey, err)
	}
	defer obj.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, obj); err != nil {
		return false, fmt.Errorf("minio: failed to read object %s for hashing: %w", objectKey, err)
	}

	computedHash := hex.EncodeToString(hasher.Sum(nil))
	return computedHash == expectedHash, nil
}

// Close releases resources. MinIO client doesn't hold persistent connections
// that need explicit cleanup, but this satisfies the interface contract.
func (s *Store) Close() error {
	s.logger.Info("minio evidence store closed")
	return nil
}

// Check implements port.HealthChecker — verifies MinIO connectivity.
func (s *Store) Check(ctx context.Context) error {
	exists, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("minio health check failed: %w", err)
	}
	if !exists {
		return fmt.Errorf("minio bucket %q not found", s.bucket)
	}
	return nil
}

// Name implements port.HealthChecker — returns the dependency name.
func (s *Store) Name() string {
	return "minio"
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// objectKey builds the S3 object key for an evidence fragment.
// Format: {org_id}/{session_id}/{fragment_id}.webm
func (s *Store) objectKey(fragment *entity.EvidenceFragment) string {
	ext := "webm"
	if fragment.ContentType == "video/mp4" {
		ext = "mp4"
	}
	return fmt.Sprintf("%s/%s/%s.%s",
		fragment.OrgID,
		fragment.SessionID,
		fragment.FragmentID,
		ext,
	)
}

// StreamKey fetches an object by raw S3 key and streams it to the response writer.
// Handles HTTP Range requests properly via MinIO's SetRange — this avoids using
// http.ServeContent which requires io.ReadSeeker (unsupported by MinIO objects).
func (s *Store) StreamKey(ctx context.Context, key string, w http.ResponseWriter, r *http.Request) error {
	// First: get object size via a stat-only request.
	opts := minio.GetObjectOptions{}
	statObj, err := s.client.GetObject(ctx, s.bucket, key, opts)
	if err != nil {
		return fmt.Errorf("minio: get object %s: %w", key, err)
	}
	info, err := statObj.Stat()
	statObj.Close()
	if err != nil {
		return fmt.Errorf("minio: stat object %s: %w", key, err)
	}

	totalSize := info.Size
	start, end := int64(0), totalSize-1
	isRange := false

	// Parse Range header (e.g. "bytes=0-1023").
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		var rs, re int64
		n, _ := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &rs, &re)
		if n >= 1 {
			start = rs
			if n == 2 && re < totalSize {
				end = re
			}
			if start < 0 || start >= totalSize || end < start {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", totalSize))
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return nil
			}
			isRange = true
		}
	}

	// Fetch only the required byte range from MinIO.
	rangeOpts := minio.GetObjectOptions{}
	_ = rangeOpts.SetRange(start, end)
	obj, err := s.client.GetObject(ctx, s.bucket, key, rangeOpts)
	if err != nil {
		return fmt.Errorf("minio: get object %s range [%d-%d]: %w", key, start, end, err)
	}
	defer obj.Close()

	contentLength := end - start + 1

	w.Header().Set("Content-Type", "video/mp4")
	w.Header().Set("Content-Disposition", "inline")
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", contentLength))

	if isRange {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, totalSize))
		w.WriteHeader(http.StatusPartialContent)
	}

	io.Copy(w, obj) //nolint:errcheck
	return nil
}

// Bucket returns the configured bucket name.
func (s *Store) Bucket() string { return s.bucket }

// MinIOClient returns the underlying MinIO client for direct access.
// Used by the export worker to download evidence fragments from the
// evidence bucket and upload archives to the exports bucket.
func (s *Store) MinIOClient() *minio.Client {
	return s.client
}

// Client is a package-level helper that extracts the MinIO client from
// an EvidenceStore interface. Returns nil if the store is not a MinIO Store.
func Client(store interface{}) *minio.Client {
	if s, ok := store.(*Store); ok && s != nil {
		return s.MinIOClient()
	}
	return nil
}

// parseURI extracts the object key from an S3 URI ("s3://bucket/key").
func (s *Store) parseURI(uri string) (string, error) {
	prefix := fmt.Sprintf("s3://%s/", s.bucket)
	if !strings.HasPrefix(uri, prefix) {
		return "", fmt.Errorf("minio: invalid URI %q — expected prefix %q", uri, prefix)
	}
	return strings.TrimPrefix(uri, prefix), nil
}
