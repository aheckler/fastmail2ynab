# Fastmail2YNAB

A local Python script that automatically detects receipt emails in Fastmail and creates transactions in YNAB.

## How it works

1. Fetches recent emails from your Fastmail inbox via JMAP
2. Uses Claude to classify each email and extract transaction details (merchant, amount, date, inflow/outflow)
3. Matches merchant names to existing YNAB payees using fuzzy matching for consistent naming
4. Routes transactions to the appropriate YNAB account based on AI classification
5. Creates unapproved transactions in YNAB in batches of 5
6. Tracks processed emails and run history in a local SQLite database

## Setup

### 1. Install uv

```bash
# macOS/Linux
brew install uv
# or: curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 2. Configure credentials

```bash
cp .env.example .env
cp .env.notes.example .env.notes
```

Edit `.env` with your credentials:

**Fastmail:**
1. Go to Settings -> Privacy & Security -> Integrations -> API tokens
2. Create a new token with "Mail" read access

**YNAB:**
1. Go to Account Settings -> Developer Settings
2. Create a Personal Access Token
3. Get account IDs from the URL when viewing each account

**Anthropic:**
1. Go to [console.anthropic.com](https://console.anthropic.com/)
2. Create an API key

### 3. Configure accounts

Edit the `YNAB_ACCOUNTS` setting in `.env`:

```json
[
  {"name": "Example Card", "ynab_id": "abc-123-your-account-id", "default": true},
  {"name": "Another Card", "ynab_id": "def-456-your-account-id"},
  {"name": "Bank Checking", "ynab_id": "ghi-789-your-account-id"}
]
```

- Each account must have a `name` and `ynab_id`
- Exactly one account must have `"default": true`
- Get `ynab_id` from the YNAB URL: `app.ynab.com/.../accounts/ACCOUNT_ID_HERE`

### 4. Add account descriptions (optional but recommended)

Edit `.env.notes` to describe each account:

```
Example Card:
Primary credit card. Default for unknown transactions. Most merchant receipts go here. Used for everyday purchases, dining, etc.

Another Card:
Used for most travel-related expenses, e.g. hotels, airfare, tourism, and so on.

Bank Checking:
Main checking account. Used for mobile check deposits, direct deposits, Zelle transfers. Venmo and PayPal transfers often land here.
```

These descriptions help Claude route transactions to the correct account.

### 5. Run the script

```bash
uv run fastmail2ynab.py
```

Dependencies are declared inline in the script using PEP 723, so uv handles them automatically.

## Command Line Options

| Flag | Description |
|------|-------------|
| `--force` | Reprocess all emails and bypass YNAB's duplicate detection. Use to reimport transactions deleted from YNAB. |
| `--clear-cache` | Clear Claude's classification cache and re-analyze all emails. Useful if you've updated scoring criteria. |

Examples:

```bash
# Normal run - interactively select transactions to create
# Use Ctrl+C during selection to preview without marking emails as processed
uv run fastmail2ynab.py

# Reimport transactions deleted from YNAB
uv run fastmail2ynab.py --force

# Re-analyze all emails with fresh Claude classifications
uv run fastmail2ynab.py --clear-cache
```

## Scheduling

Create `~/Library/LaunchAgents/com.user.fastmail2ynab.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.user.fastmail2ynab</string>
    <key>ProgramArguments</key>
    <array>
        <string>/opt/homebrew/bin/uv</string>
        <string>run</string>
        <string>/path/to/fastmail2ynab/fastmail2ynab.py</string>
    </array>
    <key>StartInterval</key>
    <integer>7200</integer>
    <key>WorkingDirectory</key>
    <string>/path/to/fastmail2ynab</string>
    <key>StandardOutPath</key>
    <string>/tmp/fastmail2ynab.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/fastmail2ynab.log</string>
</dict>
</plist>
```

Replace `/path/to/fastmail2ynab` with the actual path (e.g., `/Users/adam/Projects/fastmail2ynab`).

Then load it:

```bash
launchctl load ~/Library/LaunchAgents/com.user.fastmail2ynab.plist
```

To unload:

```bash
launchctl unload ~/Library/LaunchAgents/com.user.fastmail2ynab.plist
```

## Configuration

Edit `.env` to adjust optional settings:

| Variable | Default | Description |
|----------|---------|-------------|
| `MIN_SCORE` | 6 | Minimum AI score (1-10) to import a transaction |

## How scoring works

Claude scores each email from 1-10 on how confident it is that the email represents a financial transaction:

- **1-3**: Clearly not a transaction (newsletters, marketing, shipping updates without prices)
- **4-5**: Unlikely but has some financial language
- **6-7**: Probably a transaction but missing some details
- **8-10**: Clearly a financial transaction with amount and merchant

Emails scoring 6 or higher (by default) are imported into YNAB.

## Inflow vs Outflow detection

Claude also determines whether each transaction is:

- **OUTFLOW**: Money you spent (purchases, subscriptions, bills, fees)
- **INFLOW**: Money you received (refunds, credits, cashback, deposits)

This is reflected correctly in YNAB—outflows show as negative amounts, inflows as positive.

Each imported transaction includes a memo in the format: `fm2ynab | Run: abc12345`

## Multi-Account Routing

Claude determines which YNAB account each transaction belongs to based on:

1. **Account descriptions** in `.env.notes` - Detailed descriptions help Claude understand which emails belong to which account
2. **Email sender** - e.g., emails from @apple.com might go to Apple Card
3. **Email content** - e.g., "SoFi Checking" mentioned in the email

If Claude can't determine the account, or the suggested account doesn't exist, the transaction goes to the default account.

## Scheduled Transactions for Future Dates

For bills with autopay due dates in the future (like "Due Date: Feb 19, 2026"), the script uses YNAB's scheduled transactions API when Claude is confident about the date:

- **"certain" confidence**: The email explicitly states the exact date (e.g., "Due Date: Feb 19, 2026"). Creates a one-time scheduled transaction for that future date.
- **"likely" or unknown confidence**: The date is implied or uncertain. The transaction is created with today's date instead.

This prevents incorrect future-dated transactions while properly handling autopay bills where the due date is clearly stated.

## Data storage

All data is stored in `processed_emails.db` (SQLite) with five tables:

| Table | Purpose |
|-------|---------|
| `processed_emails` | Tracks which emails have been handled to avoid reprocessing |
| `classification_cache` | Caches Claude's analysis to avoid redundant API calls |
| `ynab_payees` | Caches YNAB payee list for merchant name matching |
| `ynab_sync_state` | Stores sync metadata for efficient delta updates |
| `runs` | Tracks script executions (run_id appears in transaction memos) |

Delete this file to start fresh and reprocess all emails.

## Payee Matching

When classifying emails, Claude matches the extracted merchant name to your existing YNAB payees. This handles:

- **Abbreviations**: "Example Homeowners Association" → "Example HOA"
- **Suffixes**: "Anthropic PBC" → "Anthropic" (ignores Inc, LLC, PBC, etc.)
- **Common variations**: Different capitalizations, punctuation, etc.

This ensures transactions use your existing payee names for consistent categorization and reporting.

## Costs

**Claude API:**
- ~$0.003-0.015 per email (depending on length)
- At 10 receipts/day: ~$1-5/month

## Troubleshooting

**"Missing configuration"**
- Ensure `.env` exists and all values are filled in

**"No accounts configured"**
- Add `YNAB_ACCOUNTS` to your `.env` file (see Setup section)

**"No account marked as default"**
- One account in `YNAB_ACCOUNTS` must have `"default": true`

**"Could not find Inbox"**
- Verify your Fastmail token has mail read permissions

**YNAB 400 errors**
- Check that your budget ID and account IDs are correct
- Verify your YNAB token hasn't expired

**Duplicate transactions**
- The `import_id` prevents duplicates in YNAB
- If you need to reimport a deleted transaction, use `--force`

**Payee names not matching**
- Payee cache refreshes every 24 hours automatically using delta updates
- Use `--clear-cache` to re-classify emails if you've added new payees to YNAB

**Transactions going to wrong account**
- Improve account descriptions in `.env.notes`
- Use `--clear-cache` to re-classify emails with updated descriptions

**Want to re-analyze emails with Claude**
- Use `--clear-cache` to clear classifications and re-analyze all emails

**Want to preview before importing**
- Press Ctrl+C during transaction selection to preview without importing
- Classifications are cached, so re-running won't call Claude again
- Emails won't be marked as processed, so they'll reappear on the next run
