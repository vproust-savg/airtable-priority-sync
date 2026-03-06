# Future Improvements

Items identified during AI code reviews (Grok, GPT, DeepSeek, Minimax) on 2026-03-06.
None of these block current development — revisit when appropriate.

## 1. Daily Validation Job
**Source:** Grok
**Description:** A scheduled status-only sync that runs daily and flags any records where Priority ≠ Airtable. Surfaces mismatches in the Sync Errors table or Slack.
**Effort:** Medium — new scheduled workflow using existing status sync logic.

## 2. Unit Tests (pytest)
**Source:** DeepSeek, Minimax
**Description:** Add a pytest suite with mocked API calls covering: field mapping transforms, build_patch_body / build_airtable_patch, loop prevention logic, sub-form pattern A/B/C, rate limiting, error handling (individual record failures don't stop batch).
**Effort:** Large — significant test coverage to build.

## 3. Conflict Resolution (Phase 6 — beyond detection)
**Source:** All reviewers
**Description:** Current system detects conflicts (both sides changed since last sync) and supports 3 strategies: SOURCE_WINS, LOG_ONLY, SKIP_RECORD. Future work: implement field-level ownership (Priority owns price/status, Airtable owns marketing fields) and/or 3-way merge for non-conflicting fields. Surface conflicts via Sync Errors table + notifications.
**Effort:** Large — requires business rules definition from project owner.

## 4. Workflow Lock (prevent overlapping syncs)
**Source:** Grok, Minimax
**Status:** Already implemented ✅ — per-workflow locks exist in server.py. CLI runs are not locked (single-user use case).

## 5. Webhook Test-Base Support
**Source:** Code audit (2026-03-06)
**Description:** Add `?test-base=true` query param to webhook endpoints so P→A webhooks can write to the test Airtable base instead of production. Currently test-base is CLI-only.
**Effort:** Low — ~10 lines in server.py, pass through to engine constructor.

## Completed Quick Wins (2026-03-06)
- ✅ `hmac.compare_digest` for timing-safe API key comparison (server.py)
- ✅ Verified dry-run correctly skips all sub-form writes
- ✅ Health check now pings Priority and Airtable APIs, returns ok/degraded
