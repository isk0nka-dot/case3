package rules

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var slidingWindowScript = redis.NewScript(`
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local threshold = tonumber(ARGV[3])
local cooldown = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])

-- Check cooldown
local on_cd = redis.call("EXISTS", KEYS[2])
if on_cd == 1 then
    return 0
end

-- Add current event
redis.call("LPUSH", KEYS[1], now)

-- Remove old events
local cutoff = now - window
while true do
    local last = redis.call("LINDEX", KEYS[1], -1)
    if not last then break end
    if tonumber(last) < cutoff then
        redis.call("RPOP", KEYS[1])
    else
        break
    end
end

-- Update TTL
redis.call("PEXPIRE", KEYS[1], ttl)

-- Check threshold
local count = redis.call("LLEN", KEYS[1])
if count >= threshold then
    -- Trigger! Set cooldown
    redis.call("PSETEX", KEYS[2], cooldown, "1")
    redis.call("DEL", KEYS[1])
    return 1
end

return 0
`)

// EvaluateWindow checks if an event should be triggered based on a sliding window.
// It returns true if the threshold is met and cooldown allows it.
func EvaluateWindow(
	ctx context.Context,
	rdb redis.Cmdable,
	sessionID string,
	ruleName string,
	eventTime time.Time,
	window time.Duration,
	threshold int,
	cooldown time.Duration,
) (bool, error) {
	if rdb == nil {
		// Fallback for tests or missing redis
		return false, fmt.Errorf("redis client is nil")
	}

	keyList := fmt.Sprintf("argus:sw:%s:%s", ruleName, sessionID)
	keyCD := fmt.Sprintf("argus:sw:%s:%s:cd", ruleName, sessionID)

	nowMs := eventTime.UnixMilli()
	windowMs := window.Milliseconds()
	cooldownMs := cooldown.Milliseconds()
	ttlMs := (window + cooldown).Milliseconds() * 2

	res, err := slidingWindowScript.Run(ctx, rdb, []string{keyList, keyCD},
		nowMs, windowMs, threshold, cooldownMs, ttlMs).Result()

	if err != nil {
		return false, err
	}

	triggered, ok := res.(int64)
	if !ok {
		return false, fmt.Errorf("unexpected script result type")
	}

	return triggered == 1, nil
}
