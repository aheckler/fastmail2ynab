# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Documentation Requirements

**When adding new features or making significant changes, always update:**

1. **This file (CLAUDE.md)** - Update CLI flags table, architecture section, data structures, etc.
2. **README.md** - Update user-facing documentation including CLI options, examples, and troubleshooting
3. **Module docstring** in `fastmail2ynab.py` - Update the Usage section with new flags

Keep documentation consistent across all three locations.

## Project Overview

A Python script that automatically imports receipt emails from Fastmail into YNAB (You Need A Budget). It uses Claude AI to classify emails and extract transaction details.

## Running the Script

```bash
uv run fastmail2ynab.py
```

Dependencies are declared inline using PEP 723 script metadata, so uv handles them automatically.

## CLI Flags

| Flag | Description |
|------|-------------|
| `--force` | Reprocess all emails and bypass YNAB's duplicate detection. Use to reimport transactions deleted from YNAB. |

## Verification

Verification runs automatically via a PostToolUse hook after editing Python files. The hook:
- Auto-fixes linting issues (`ruff check --fix`)
- Auto-formats code (`ruff format`)
- Type-checks (`pyright`)

To test the CLI:

```bash
uv run fastmail2ynab.py --help
```

## Architecture

The entire application is in a single file (`fastmail2ynab.py`) with these main components:

1. **Fastmail JMAP integration**: Fetches recent emails using the JMAP protocol (up to 200KB per body value). HTML bodies are converted to plain text with `html2text` before being passed to Claude. The `text/plain` alternative is preferred unless it's a stub ("please enable HTML") or a broken CSS-source dump (some senders, notably Shopify/Klaviyo, emit their stylesheet as plaintext); in those cases the HTML alternative is used. Archives successfully imported emails after YNAB upload.
2. **Claude classification**: Uses Claude API to score emails 1-10 and extract transaction data (merchant, amount, currency, date, date_confidence, inflow/outflow, account). When an email shows multiple currencies, Claude picks the USD amount; when only non-USD currencies appear, the email is skipped (no conversion performed).
3. **YNAB API integration**: Creates unapproved transactions in YNAB (batched in groups of 5), fetches payees for name matching. Uses scheduled transactions API for future-dated bills with high confidence. After all creates, runs a settle-then-enforce category phase (see "Category enforcement" below). YNAB's batch-create response returns transactions sorted by date, not in submission order, so created IDs are matched back to their `PendingTransaction` by `import_id` (`_map_batch_create_results`) — never by list position.
4. **Payee name matching**: Claude matches merchant names to existing YNAB payees, handling abbreviations and variations
5. **Multi-account routing**: Claude determines which YNAB account each transaction belongs to based on account descriptions in `.env.notes`. Accounts marked `"skip": true` are untracked cards (e.g. company cards); receipts routed to them are recorded as processed but never imported.
6. **Scheduled transactions**: Future dates (like autopay due dates) with "certain" confidence use YNAB's scheduled transactions API; others are capped to today
7. **SQLite database**: Five tables - `processed_emails` (tracking), `classification_cache` (Claude results), `ynab_payees` (cached payee list), `ynab_sync_state` (delta sync metadata + one-shot migration markers), `runs` (script execution history). On startup, `_process_emails_impl` runs a one-time backfill that deletes `classification_cache` rows pre-dating the `checklist_json` column, gated by a `cache_backfill_checklist_v1` marker in `ynab_sync_state`.
8. **File-based logging**: Each run writes a detailed log to `logs/YYYY-MM-DD_HH-MM-SS.log` (DEBUG level). Console output is quieter (INFO level, milestones and accepted transactions only). Logs auto-prune after 90 days.

## Category enforcement

YNAB's server-side auto-categorization can fire asynchronously after a POST and race with an immediate PATCH, overwriting our intended category. To guarantee our values win, after all transactions are created the script runs:

1. Sleep 10s to let YNAB's initial async auto-categorization land.
2. Bulk PATCH every regular transaction's `category_id` (outflows → `null`, inflows → "Inflow: Ready to Assign"). Scheduled transactions are PUT one-by-one (no batch API).
3. Sleep 10s to catch any late async writes.
4. GET the transactions back (one call for regulars via `since_date`, one call for scheduled) and compare to expected.
5. Re-PATCH any "stragglers" whose `category_id` doesn't match. Once only — no second verify loop.

The Ready-to-Assign `category_id` is looked up once per budget and cached in `ynab_sync_state`. `SETTLE_DELAY_SECONDS = 10` controls both delays. Total added wall-clock time per run: ~20s.

## Key Data Structures

- `Account`: name, ynab_id, notes, default, skip (for multi-account routing; `skip` marks an untracked card, and `ynab_id` is `None` for skip accounts)
- `Email`: id, subject, from_email, received_at, body
- `ClassificationResult`: score (1-10), is_inflow, merchant, amount, currency (3-letter ISO code; non-USD values cause the email to be skipped), date, date_confidence ("certain"/"likely"/None), description, reasoning, account_name, checklist
- `PendingTransaction`: email_id, account_id, amount, date, payee_name, memo, import_id, is_inflow, is_scheduled (used for batch creation and scheduled transactions)

### Classification Checklist

Claude uses an explicit checklist to score emails, making classification stable and predictable:

**Positive signals (weighted):**
| Signal | Weight | Rationale |
|--------|--------|-----------|
| `specific_amount` | +3 | Core requirement for any transaction |
| `confirmation_language` | +3 | Distinguishes receipts from notices |
| `transaction_date` | +2 | Strong transaction indicator |
| `payment_method` | +2 | Confirms payment occurred |
| `merchant_identified` | +1 | Helpful but common in all emails |
| `account_match` | +1 | Bonus for account routing |

**Negative signals (weighted):**
| Signal | Weight | Rationale |
|--------|--------|-----------|
| `marketing` | -5 | Never import marketing emails |
| `balance_credit` | -4 | Not real money movement |
| `shipping_only` | -2 | Financial data present, just not a charge |
| `reminder_only` | -2 | May have amount, but no transaction yet |
| `approximate_amount` | -5 | Amount would be wrong; real receipt comes later |

**Score calculation (deterministic, computed in code):**
1. Start with base score of 3
2. Add positive weights for TRUE signals
3. Subtract negative weights for TRUE signals
4. Clamp to range 1-10

The script computes this in `compute_score()` from the checklist Claude returns. Claude is still asked to emit a `score` field in its JSON response (so its `reasoning` text stays coherent), but the code ignores it. The single source of truth for weights is the `CHECKLIST_WEIGHTS` dict in `fastmail2ynab.py`.

A missing or malformed checklist (wrong key set, missing keys, extras) is treated as a parse failure: the email is skipped this run and not cached, so the next run re-classifies fresh.

**Example scores:**
- Amazon shipping (amount + merchant + shipping_only): 3 + 3 + 1 - 2 = **5**
- School notification (merchant + reminder_only): 3 + 1 - 2 = **2**
- Apple purchase receipt (all positives, no negatives): 3 + 3 + 3 + 2 + 2 + 1 + 1 = **10** (clamped)
- Cell plan renewal ("$5 + taxes", reminder + approximate): 3 + 3 + 2 + 1 + 2 - 2 - 5 = **4**

## Configuration

### Environment variables in `.env`:
- `FASTMAIL_TOKEN`, `ANTHROPIC_API_KEY`, `YNAB_TOKEN` - API credentials
- `YNAB_BUDGET_ID` - Target YNAB budget
- `YNAB_ACCOUNTS` - JSON array of account configurations (see below)
- `MIN_SCORE` (default 6) - Minimum AI confidence score to import

### `YNAB_ACCOUNTS` format:
```json
[
  {"name": "Chase Freedom", "ynab_id": "abc-123", "default": true},
  {"name": "Apple Card", "ynab_id": "def-456"},
  {"name": "SoFi Checking", "ynab_id": "ghi-789"},
  {"name": "Company Card", "skip": true}
]
```

Requirements:
- Each account must have a `name`
- Each account must have a `ynab_id`, except accounts marked `"skip": true`
- Exactly one account must have `"default": true`; the default cannot be a skip account
- Account names must be unique

### Untracked cards (skip accounts)

An account marked `"skip": true` represents a card not tracked in YNAB (e.g. a company card). It needs no `ynab_id`, but it still needs a `.env.notes` description stating the card's last 4 digits — that is what Claude routes by, the same as for a normal account. When a receipt is routed to a skip account, the processing loop skips it: the email is recorded as processed so it does not resurface, but left in the Fastmail inbox (not archived) so it can still be acted on.

### Account descriptions in `.env.notes`:
```
Chase Freedom:
Primary credit card. Default for unknown transactions.

Apple Card:
Goldman Sachs Apple Card. Emails from @apple.com with "Apple Card Transaction".
```

The `.env.notes` file provides detailed descriptions to help Claude route transactions to the correct account. Account names must match exactly with names in `YNAB_ACCOUNTS`.

## Dependencies

Uses `requests` for HTTP, `anthropic` for Claude API, `python-dotenv` for env loading, `html2text` for converting HTML email bodies to plain text, `questionary` for interactive prompts. No test framework configured.
