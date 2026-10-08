package rules_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/argus-ai/event-collector/internal/domain/rules"
)

func setupTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}

	client := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	return s, client
}

func TestEvaluateWindow_Basic(t *testing.T) {
	s, rdb := setupTestRedis(t)
	defer s.Close()
	defer rdb.Close()

	ctx := context.Background()
	sessionID := "sess-1"
	ruleName := "test_rule"
	window := 10 * time.Second
	threshold := 3
	cooldown := 60 * time.Second

	baseTime := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)

	// Event 1 - shouldn't trigger
	triggered, err := rules.EvaluateWindow(ctx, rdb, sessionID, ruleName, baseTime, window, threshold, cooldown)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if triggered {
		t.Fatalf("expected not triggered on 1st event")
	}

	// Event 2 - shouldn't trigger
	triggered, err = rules.EvaluateWindow(ctx, rdb, sessionID, ruleName, baseTime.Add(1*time.Second), window, threshold, cooldown)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if triggered {
		t.Fatalf("expected not triggered on 2nd event")
	}

	// Event 3 - should trigger (reaches threshold of 3 within 10s)
	triggered, err = rules.EvaluateWindow(ctx, rdb, sessionID, ruleName, baseTime.Add(2*time.Second), window, threshold, cooldown)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !triggered {
		t.Fatalf("expected triggered on 3rd event")
	}

	// Event 4 - shouldn't trigger (on cooldown)
	triggered, err = rules.EvaluateWindow(ctx, rdb, sessionID, ruleName, baseTime.Add(3*time.Second), window, threshold, cooldown)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if triggered {
		t.Fatalf("expected not triggered on 4th event due to cooldown")
	}
}

func TestEvaluateWindow_OutdatedEvents(t *testing.T) {
	s, rdb := setupTestRedis(t)
	defer s.Close()
	defer rdb.Close()

	ctx := context.Background()
	sessionID := "sess-2"
	ruleName := "test_rule"
	window := 10 * time.Second
	threshold := 3
	cooldown := 60 * time.Second

	baseTime := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)

	// Event 1
	rules.EvaluateWindow(ctx, rdb, sessionID, ruleName, baseTime, window, threshold, cooldown)
	// Event 2 at baseTime + 5s
	rules.EvaluateWindow(ctx, rdb, sessionID, ruleName, baseTime.Add(5*time.Second), window, threshold, cooldown)
	
	// Event 3 at baseTime + 15s (Event 1 is now out of the 10s window)
	// So we only have Event 2 and Event 3 in the window, total = 2.
	triggered, err := rules.EvaluateWindow(ctx, rdb, sessionID, ruleName, baseTime.Add(15*time.Second), window, threshold, cooldown)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if triggered {
		t.Fatalf("expected not triggered because old event should have been removed")
	}
}

func TestEvaluateWindow_IndependentSessions(t *testing.T) {
	s, rdb := setupTestRedis(t)
	defer s.Close()
	defer rdb.Close()

	ctx := context.Background()
	ruleName := "test_rule"
	window := 10 * time.Second
	threshold := 2
	cooldown := 60 * time.Second

	baseTime := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)

	// Session A - Event 1
	rules.EvaluateWindow(ctx, rdb, "sess-A", ruleName, baseTime, window, threshold, cooldown)
	
	// Session B - Event 1
	triggered, _ := rules.EvaluateWindow(ctx, rdb, "sess-B", ruleName, baseTime, window, threshold, cooldown)
	if triggered {
		t.Fatalf("expected not triggered for Session B")
	}
	
	// Session B - Event 2 (Triggers session B)
	triggered, _ = rules.EvaluateWindow(ctx, rdb, "sess-B", ruleName, baseTime.Add(1*time.Second), window, threshold, cooldown)
	if !triggered {
		t.Fatalf("expected triggered for Session B")
	}
	
	// Session A - Event 2 (Triggers session A independently)
	triggered, _ = rules.EvaluateWindow(ctx, rdb, "sess-A", ruleName, baseTime.Add(2*time.Second), window, threshold, cooldown)
	if !triggered {
		t.Fatalf("expected triggered for Session A")
	}
}

