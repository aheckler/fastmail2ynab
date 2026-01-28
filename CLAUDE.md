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
| `--clear-cache` | Clear Claude's classification cache before running. Forces re-analysis of all emails. Useful after prompt changes. |
| `--undo` | Undo the most recent run by deleting its transactions from YNAB and removing processed email records. |
| `--confirm` | Interactively select which transactions to create. Cancel (Ctrl+C) to preview without marking emails as processed. |

Flags can be combined: `uv run fastmail2ynab.py --force --clear-cache`

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
2. **Claude classification**: Uses Claude API to score emails 1-10 and extract transaction data (merchant, amount, date, inflow/outflow)
3. **YNAB API integration**: Creates unapproved transactions in YNAB (batched in groups of 5), fetches payees for name matching
4. **Payee name matching**: Claude matches merchant names to existing YNAB payees, handling abbreviations and variations
5. **Amazon routing**: If configured, transactions from Amazon are routed to a separate YNAB account (detected via merchant name or sender email)
6. **SQLite database**: Five tables - `processed_emails` (tracking), `classification_cache` (Claude results), `ynab_payees` (cached payee list), `ynab_sync_state` (delta sync metadata), `runs` (script execution history for undo)

## Key Data Structures

- `Email`: id, subject, from_email, received_at, body
- `ClassificationResult`: score (1-10), is_inflow, merchant, amount, currency, date, description, reasoning
- `PendingTransaction`: email_id, account_id, amount, date, payee_name, memo, import_id, is_inflow (used for batch creation)

## Configuration

Environment variables in `.env`:
- `FASTMAIL_TOKEN`, `ANTHROPIC_API_KEY`, `YNAB_TOKEN` - API credentials
- `YNAB_BUDGET_ID`, `YNAB_ACCOUNT_ID` - Target YNAB account
- `YNAB_AMAZON_ACCOUNT_ID` (optional) - Separate account for Amazon transactions
- `MIN_SCORE` (default 6) - Minimum AI confidence score to import

## Dependencies

Uses `requests` for HTTP, `anthropic` for Claude API, `python-dotenv` for env loading. No test framework configured.
