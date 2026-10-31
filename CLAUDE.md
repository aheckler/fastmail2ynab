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

**The script needs a real terminal.** Transaction selection goes through `questionary`/`prompt_toolkit`, which raise an opaque `OSError` from inside the asyncio event loop when stdin isn't a TTY. `select_transactions_interactive()` guards on `sys.stdin.isatty()` and returns `None` instead, which the caller already handles as "import nothing, mark nothing." So a piped, cron, launchd, or agent-shell invocation classifies emails (caching the results) and then stops cleanly before the import phase. Don't test end-to-end behavior through a non-interactive shell; it will always stop at that point.

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

Six test files run standalone, no pytest needed. Each has its own PEP 723 header, which must list every dependency `fastmail2ynab.py` imports — including `claude-preflight` and its `[tool.uv.sources]` path entry. Adding an import to the script without updating all six headers breaks them with `ModuleNotFoundError`.

```bash
uv run test_compute_score.py     # score is a pure function of the checklist
uv run test_batch_mapping.py     # batch results matched by import_id, not position
uv run test_secrets_loader.py    # Keychain loader + config.json reader contract
uv run test_response_parsing.py  # text read by block type; refusal/max_tokens/parse-failure handling
uv run test_api_outage.py        # an outage mid-run stops Claude calls and leaves the rest for next run
uv run test_prompt_caching.py    # static prompt is one cached system block, identical for every email
```

Each test file imports `fastmail2ynab`, which runs `load_secrets()` and `load_local_config()` at import. So every test process silently reads the real Keychain entries and the real `config.json` (a missing `config.json` is fine: it loads as `{}`). `test_secrets_loader.py` itself stubs `subprocess.run` and uses scratch config files, so it never depends on the real ones.

## Architecture

The entire application is in a single file (`fastmail2ynab.py`) with these main components:

1. **Fastmail JMAP integration**: Fetches recent emails using the JMAP protocol (up to 200KB per body value). HTML bodies are converted to plain text with `html2text` before being passed to Claude. The `text/plain` alternative is preferred unless it's a stub ("please enable HTML") or a broken CSS-source dump (some senders, notably Shopify/Klaviyo, emit their stylesheet as plaintext); in those cases the HTML alternative is used. Archives successfully imported emails after YNAB upload.
2. **Claude classification**: Uses Claude API to score emails 1-10 and extract transaction data (merchant, amount, currency, date, date_confidence, inflow/outflow, account). When an email shows multiple currencies, Claude picks the USD amount; when only non-USD currencies appear, the email is skipped (no conversion performed). The model is `CLAUDE_MODEL` (Claude Sonnet 5.5). The request uses structured outputs (`output_config.format` with `CLASSIFICATION_SCHEMA`, whose checklist keys are built from `CHECKLIST_WEIGHTS`), so a successful response is schema-valid JSON with nothing to extract; there is no regex fallback. The prompt is split for prompt caching: `_classification_system_prompt()` builds the static part (payee list, account list, rules; about 22k tokens) and `classify_email()` sends it as one `system` block with `cache_control: {"type": "ephemeral"}`, with only the email (from, subject, body) in the user turn. The first call in a run writes the cache and later calls read it at a fraction of the input price; each call logs a `Token usage` DEBUG line with `cache_write` and `cache_read`. Keep `_classification_system_prompt()` a pure function of the payees and accounts: anything per-email or time-dependent in it changes the prefix and silently turns caching off. `test_prompt_caching.py` pins this. It sets `thinking: {"type": "adaptive"}` explicitly (also the model's default) and `effort: high`, so a response can contain thinking blocks ahead of the JSON. Always pull response text with `_response_text()`, which selects blocks by `.type == "text"` — never index `message.content[0]`, which is a `ThinkingBlock` whenever Claude decides to reason about an email. `max_tokens` (16000) caps thinking and response text combined. `_classification_from_message()` checks `stop_reason` before reading content (see "Classification Checklist" below for how each outcome is handled).
3. **YNAB API integration**: Creates unapproved transactions in YNAB (batched in groups of 5), fetches payees for name matching. Uses scheduled transactions API for future-dated bills with high confidence. After all creates, runs a settle-then-enforce category phase (see "Category enforcement" below). YNAB's batch-create response returns transactions sorted by date, not in submission order, so created IDs are matched back to their `PendingTransaction` by `import_id` (`_map_batch_create_results`) — never by list position. YNAB rejects a whole batch with a 400 when any one transaction is invalid, so `_create_regular_batch()` retries a 400'd batch one transaction at a time; only the bad one counts as an error and stays unprocessed.
4. **Payee name matching**: Claude matches merchant names to existing YNAB payees, handling abbreviations and variations. Only the first `MAX_PROMPT_PAYEES` (3000) payees in sorted order go into the prompt. Past that, the payees at the end of the sort can't be matched, so their receipts import under Claude's raw merchant name and create duplicate payees; the run logs a WARNING when this happens. The cap was 2000 until Oct 2026 (at ~1,940 payees). The better long-term fix is to send only recently used payees rather than cutting alphabetically. YNAB's internal payees (`YNAB_INTERNAL_PAYEE_PREFIXES`: `Transfer : `, `Starting Balance`, the balance-adjustment payees) are never offered: `get_cached_payees()` drops them, because YNAB rejects them as a `payee_name` (a transfer can only be created by `payee_id`). As a backstop, a reserved payee that still reaches the transaction-building step (e.g. from an older cached classification) is replaced with Claude's raw merchant name. The script never creates real YNAB transfers: each transfer arrives as one email per side, and each email would create its own full transfer
5. **Multi-account routing**: Claude determines which YNAB account each transaction belongs to based on account descriptions in `.env.notes` (accounts themselves are listed in `config.json`). Accounts marked `"skip": true` are untracked cards (e.g. company cards); receipts routed to them are recorded as processed but never imported.
6. **Scheduled transactions**: Future dates (like autopay due dates) with "certain" confidence use YNAB's scheduled transactions API; others are capped to today
7. **SQLite database**: Five tables - `processed_emails` (tracking), `classification_cache` (Claude results), `ynab_payees` (cached payee list), `ynab_sync_state` (delta sync metadata + one-shot migration markers), `runs` (script execution history). At the start of each `_process_emails_impl` call (every real run, not on `--help` or import), two one-time backfills run in sequence, each gated by its own marker in `ynab_sync_state` and wrapped in `try/except sqlite3.Error` so a transient SQLite failure doesn't abort the whole run: (1) `cache_backfill_checklist_v1` deletes `classification_cache` rows pre-dating the `checklist_json` column; (2) `cache_backfill_checklist_v2` recomputes the `score` column in place for rows with a valid checklist, deleting rows whose checklist is unusable. Future migrations follow the same pattern — new function, new marker key, appended to the wrapped call sequence.
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

A missing or malformed checklist (wrong key set, missing keys, extras) is treated as a parse failure: the main loop `continue`s before `cache_classification` or `mark_processed` runs, so the email stays in the inbox and is genuinely re-classified on the next run. The same applies to JSON-decode failures and other parse errors — any `ClassificationResult` whose `reasoning` starts with `"Failed to parse"`, `"Parse error"`, or `"Failed to compute"` is treated as transient.

Two `stop_reason` values are handled before any parsing:

- `"max_tokens"`: the response is treated as failed even if its text parses (reasoning `"Failed to parse: response hit max_tokens"`), so it is transient and retried next run.
- `"refusal"`: a Sonnet 5.5 safety classifier declined the email. The result is score 0 with reasoning `"Refused by Claude (<category>)"` and a console WARNING. This is deliberately *not* transient: it is cached and marked processed (left in the inbox, not archived), so a refused email isn't re-sent on every run. There is no server-side fallback model.

An API exception from `classify_email()` is never cached or marked processed. If `claude_preflight.api_error_kind()` calls it an outage (connection error, timeout, 429, 5xx, 529) or an auth error (401, 403), the loop logs one WARNING with the number of emails left and stops calling Claude for the rest of the run. Emails classified before the failure are kept, and the run continues to the review step with them. Any other exception is a per-email error, as before.

**Example scores:**
- Amazon shipping (amount + merchant + shipping_only): 3 + 3 + 1 - 2 = **5**
- School notification (merchant + reminder_only): 3 + 1 - 2 = **2**
- Apple purchase receipt (all positives, no negatives): 3 + 3 + 3 + 2 + 2 + 1 + 1 = **10** (clamped)
- Cell plan renewal ("$5 + taxes", reminder + approximate): 3 + 3 + 2 + 1 + 2 - 2 - 5 = **4**

## Configuration

Secrets and settings live in two different places, on purpose.

### Secrets: the macOS Keychain

`FASTMAIL_TOKEN`, `ANTHROPIC_API_KEY` and `YNAB_TOKEN` are generic passwords in the login Keychain under the service `com.aheckler.fastmail2ynab`, with the variable name as the account. Each entry carries a `-j` comment saying what it is and where to get a new one. This follows Adam's naming convention for hand-made Keychain entries.

`load_secrets()` runs at import. For each name in `SECRET_NAMES` it runs `/usr/bin/security find-generic-password -s ... -a NAME -w` and exports the value into `os.environ`. That read is silent: no Touch ID, no 1Password, nothing that can block. The rules:

- A variable already in the environment, even an empty one, wins and is never looked up. That keeps `YNAB_TOKEN=... uv run fastmail2ynab.py` working as an override, and is the only source off macOS.
- A missing entry leaves the variable unset. The "Missing configuration" check at the start of `process_emails()` reports it and prints the `security add-generic-password` command.
- Only the trailing newline is stripped (`removesuffix`, not `strip`), so values come back exactly.
- Values must be printable ASCII. `security -w` prints anything else as hex, which would silently corrupt the secret.

Until 2026-10-31 these came from a 1Password Environments FIFO mounted at `.env`. That needed 1Password unlocked, a timeout thread, and Touch ID approval, and an unattended read hung or came back empty. Don't bring back `.env` or `python-dotenv`.

### Settings: `config.json`

`config.json` sits next to the script and is gitignored, because the repo is public and the budget and account IDs are Adam's. `config.example.json` shows the shape. `load_local_config()` reads it once at import into `LOCAL_CONFIG`:

- `ynab_budget_id` (string): target YNAB budget
- `min_score` (integer 1-10, default 6): minimum AI confidence score to import
- `accounts` (list): account configurations, format below

A missing file loads as `{}` so tests can import the module; the startup checks then report what's missing. A file that exists but isn't a JSON object, has an unknown key, or has a value of the wrong type exits with a message. A `bool` doesn't count as an int, and `null` is a wrong type, not a request for the default: leave a key out to get its default. `min_score` must also fall in `MIN_SCORE_RANGE` (1-10). `compute_score()` clamps every score to that range, so a value below 1 would import everything and one above 10 nothing. Because of these checks, `min_score` can never reach the startup "Missing configuration" check. That's stricter than the old `MIN_SCORE` env var, which silently fell back to 6 on a bad value.

### `accounts` format:
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

The `.env.notes` file provides detailed descriptions to help Claude route transactions to the correct account. Account names must match exactly with the names in `config.json`'s `accounts`.

## Dependencies

Uses `requests` for HTTP, `anthropic` for Claude API, the macOS `security` CLI (no Python package) for secrets, `html2text` for converting HTML email bodies to plain text, `questionary` for interactive prompts, `claude-preflight` (editable, from `~/Code/claude-preflight`) for the pre-run Claude status gate. Tests are plain scripts with `assert`, run via `uv run` — no test framework.
