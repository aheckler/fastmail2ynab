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

1. **Fastmail JMAP integration**: Fetches recent emails using the JMAP protocol
2. **Claude classification**: Uses Claude API to score emails 1-10 and extract transaction data (merchant, amount, date, date_confidence, inflow/outflow, account)
3. **YNAB API integration**: Creates unapproved transactions in YNAB (batched in groups of 5), fetches payees for name matching. Uses scheduled transactions API for future-dated bills with high confidence.
4. **Payee name matching**: Claude matches merchant names to existing YNAB payees, handling abbreviations and variations
5. **Multi-account routing**: Claude determines which YNAB account each transaction belongs to based on account descriptions in `.env.notes`
6. **Scheduled transactions**: Future dates (like autopay due dates) with "certain" confidence use YNAB's scheduled transactions API; others are capped to today
7. **SQLite database**: Five tables - `processed_emails` (tracking), `classification_cache` (Claude results), `ynab_payees` (cached payee list), `ynab_sync_state` (delta sync metadata), `runs` (script execution history)

## Key Data Structures

- `Account`: name, ynab_id, notes, default (for multi-account routing)
- `Email`: id, subject, from_email, received_at, body
- `ClassificationResult`: score (1-10), is_inflow, merchant, amount, currency, date, date_confidence ("certain"/"likely"/None), description, reasoning, account_name, checklist
- `PendingTransaction`: email_id, account_id, amount, date, payee_name, memo, import_id, is_inflow, is_scheduled (used for batch creation and scheduled transactions)

### Classification Checklist

Claude uses an explicit checklist to score emails, making classification stable and predictable:

**Positive signals (reasons TO import):**
1. `specific_amount` - Contains a specific dollar amount
2. `transaction_date` - Contains a transaction/purchase/payment date
3. `merchant_identified` - Names a merchant or payee
4. `payment_method` - References a real payment method (card, bank, PayPal)
5. `confirmation_language` - Uses confirmation language ("charged", "paid", "refunded")
6. `account_match` - Mentions one of the user's configured financial accounts

**Negative signals (reasons NOT to import):**
7. `balance_credit` - Credits to store balance, gift card, rewards points (not real money)
8. `shipping_only` - Shipping/delivery notification without a charge
9. `reminder_only` - Reminder, alert, or notice (not a confirmation)
10. `marketing` - Marketing or promotional content

**Score mapping:**
- 8-10: Multiple positive signals, no negative signals
- 6-7: Some positive signals but missing key details
- 4-5: Financially related but has negative signals
- 1-3: No positive signals OR strong negative signals

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
  {"name": "SoFi Checking", "ynab_id": "ghi-789"}
]
```

Requirements:
- Each account must have `name` and `ynab_id`
- Exactly one account must have `default: true`
- Account names must be unique

### Account descriptions in `.env.notes`:
```
Chase Freedom:
Primary credit card. Default for unknown transactions.

Apple Card:
Goldman Sachs Apple Card. Emails from @apple.com with "Apple Card Transaction".
```

The `.env.notes` file provides detailed descriptions to help Claude route transactions to the correct account. Account names must match exactly with names in `YNAB_ACCOUNTS`.

## Dependencies

Uses `requests` for HTTP, `anthropic` for Claude API, `python-dotenv` for env loading. No test framework configured.
