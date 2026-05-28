# Compute Classification Score From Checklist

## Problem

`classify_email` reads `score` straight from Claude's JSON response. The prompt tells Claude to compute the score using a deterministic formula over the 11-key checklist (base 3 + positive weights - negative weights, clamped 1-10), but Claude sometimes returns a `score` that doesn't match its own checklist.

Concrete divergences (May 2026):

- Cloudflare invoice 2026-05-23 (`Stordlr6d2o-`): checklist gives `3 + 3 + 2 + 1 - 2 = 7`. Claude returned `score=5`. Below the threshold of 6, skipped.
- Cloudflare invoice 2026-04-30 (`StpIt9MH4Myw`): checklist gives 7. Claude returned `score=4`. Same skip.

Even Claude's own `reasoning` text on the first one says "= 7, clamped to 7". The arithmetic is right in the prompt and the reasoning, but the `score` field is wrong.

## Solution

Compute the score deterministically from the checklist in our own code, and stop reading the `score` field Claude returns. The checklist is already parsed and stored separately (`ClassificationResult.checklist`, `classification_cache.checklist_json`), so the change is local: one new pure function, one call site swap, one cache-row migration.

## Behavior

- Score is always computed from the checklist using the formula already documented in CLAUDE.md and the prompt.
- The 11-key checklist is the sole input. Anything else (missing checklist, missing keys, extra keys) treats the email as a parse failure: log it, set `score=0`, do not cache the result. Next run re-classifies fresh.
- Claude's `score` field is still emitted by the prompt (we don't change the JSON schema sent to Claude) but the code never reads it. Keeping it preserves the coherence of Claude's `reasoning` text, which references the number.
- A one-time backfill deletes cached classification rows that pre-date the checklist (`checklist_json IS NULL`) so they get re-classified once under the new path.

## The scoring function

A new pure function near the existing checklist-parsing block, with weights centralized in a module-level constant:

```python
CHECKLIST_WEIGHTS: dict[str, int] = {
    # Positive signals
    "specific_amount": +3,
    "confirmation_language": +3,
    "transaction_date": +2,
    "payment_method": +2,
    "merchant_identified": +1,
    "account_match": +1,
    # Negative signals
    "marketing": -5,
    "approximate_amount": -5,
    "balance_credit": -4,
    "shipping_only": -2,
    "reminder_only": -2,
}


def compute_score(checklist: dict[str, bool] | None) -> int | None:
    """Apply the documented formula to a checklist dict.

    Returns the clamped score (1-10) on a usable checklist, or None if the
    checklist is missing, malformed, or has the wrong key set.
    """
    if not checklist or set(checklist.keys()) != set(CHECKLIST_WEIGHTS.keys()):
        return None
    total = 3 + sum(w for k, w in CHECKLIST_WEIGHTS.items() if checklist[k])
    return max(1, min(10, total))
```

Notes:

- Strict key-set match. Missing keys, extras, or renames all return `None`. No silent partial scoring.
- Weights live in one place. The prompt's weight tables stay (Claude still needs to see them when filling in the checklist) but the code no longer duplicates the numbers in inline arithmetic.

## Wiring into `classify_email`

Replace the existing `score=int(data.get("score", 0))` at the `ClassificationResult` construction (currently line 1620):

```python
computed = compute_score(checklist)
if computed is None:
    return ClassificationResult(
        score=0,
        reasoning="Failed to compute score: missing or malformed checklist",
    )

return ClassificationResult(
    score=computed,
    ...,
    checklist=checklist,
)
```

The main-loop cache guard (currently line 2596) skips caching for reasoning strings starting with `"Failed to parse"` or `"Parse error"`. Add `"Failed to compute"` to that prefix list so a missing-checklist result is treated the same way: skipped this run, retried next run, never cached as a permanent zero.

## Cache hit path

No code change needed in `get_cached_classification`. The cached `score` column was written by `classify_email`, so once the new code is live every newly-cached row already holds the deterministic value. Legacy pre-checklist rows are removed by the migration below before any cache reads happen.

## Migration: invalidate pre-checklist cache rows

A one-time delete of cache rows that pre-date the `checklist_json` column. Runs on startup, guarded by a marker in `ynab_sync_state` so it only fires once.

Marker key: `cache_backfill_checklist_v1`. Approximate shape:

```python
def _backfill_invalidate_pre_checklist_cache(conn: sqlite3.Connection) -> None:
    marker = "cache_backfill_checklist_v1"
    cur = conn.execute(
        "SELECT value FROM ynab_sync_state WHERE key = ?", (marker,)
    )
    if cur.fetchone() is not None:
        return

    deleted = conn.execute(
        "DELETE FROM classification_cache WHERE checklist_json IS NULL"
    ).rowcount
    conn.execute(
        "INSERT INTO ynab_sync_state(key, value) VALUES (?, ?)",
        (marker, "done"),
    )
    conn.commit()
    log.info("Cache backfill: invalidated %d pre-checklist rows", deleted)
```

Called once from the startup path right after the existing `ensure_column("classification_cache", "checklist_json")` call. Using `ynab_sync_state` (the project's existing key-value metadata table) rather than SQLite's `PRAGMA user_version` so future migrations can coexist as additional named markers without coordinating a global integer.

## Logging

Add one DEBUG-level line in `classify_email` immediately after computing the score:

```python
log.debug(
    "  Computed score: %d (Claude said: %s)",
    computed,
    data.get("score"),
)
```

Observability for future curiosity. No behavior change.

## Testing

The project has no test framework configured but does have one standalone test script (`test_batch_mapping.py`). Add `test_compute_score.py` in the same style, runnable via `uv run test_compute_score.py`. Coverage:

1. The four worked examples from CLAUDE.md:
   - Amazon shipping (`specific_amount + merchant_identified + shipping_only`): `3 + 3 + 1 - 2 = 5`.
   - School notification (`merchant_identified + reminder_only`): `3 + 1 - 2 = 2`.
   - Apple purchase receipt (all positives, no negatives): `3 + 3 + 3 + 2 + 2 + 1 + 1 = 15` clamped to `10`.
   - Cell plan renewal (`specific_amount + transaction_date + merchant_identified + payment_method - reminder_only - approximate_amount`, no `confirmation_language` since it's a reminder, not a receipt): `3 + 3 + 2 + 1 + 2 - 2 - 5 = 4`.
2. The two real divergence cases from May 2026: a checklist matching the Cloudflare invoices' signals returns `7`, not `5`/`4`.
3. Edge cases that return `None`: empty dict, `None`, missing one key, one extra key, wrong key name.
4. Floor and ceiling: an all-negatives checklist clamps to `1`; an all-positives checklist clamps to `10`.

## Documentation

Update CLAUDE.md and README.md to reflect that the score is now computed from the checklist in code, not read from Claude's JSON output. Module docstring in `fastmail2ynab.py` may not need a change since the CLI surface is unchanged.

## Out of scope (intentional)

- Removing `score` from the prompt's JSON schema. Keeping it lets Claude's `reasoning` stay coherent and adds negligible token cost.
- A "log and compare" disagreement-tracking phase before flipping authority. The May 2026 cases plus the formula already being deterministic make this unnecessary.
- Recomputing scores in place for pre-checklist cache rows. The backfill wipes them instead, and the next classification call regenerates them with the current prompt.
- Refactoring the prompt's weight tables to be generated from `CHECKLIST_WEIGHTS`. The duplication is mild and the prompt is one large f-string; leave it.
- The `fastmail2ynab-three-range-scoring` idea. Lands separately on top of this once the score is reliable.
