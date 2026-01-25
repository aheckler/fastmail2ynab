# Fastmail2YNAB

A local Python script that automatically detects receipt emails in Fastmail and creates transactions in YNAB.

## How it works

1. Fetches recent emails from your Fastmail inbox via JMAP
2. Uses Claude to classify each email and extract transaction details (merchant, amount, date, inflow/outflow)
3. Matches merchant names to existing YNAB payees using fuzzy matching for consistent naming
4. Routes Amazon transactions to a separate account (configurable)
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
```

Edit `.env` with your credentials:

**Fastmail:**
1. Go to Settings -> Privacy & Security -> Integrations -> API tokens
2. Create a new token with "Mail" read access

**YNAB:**
1. Go to Account Settings -> Developer Settings
2. Create a Personal Access Token

**Anthropic:**
1. Go to [console.anthropic.com](https://console.anthropic.com/)
2. Create an API key

### 3. Run the script

```bash
uv run fastmail2ynab.py
```

Dependencies are declared inline in the script using PEP 723, so uv handles them automatically.

## Command Line Options

| Flag | Description |
|------|-------------|
| `--dry-run` | Preview what transactions would be created without actually creating them or marking emails as processed. |
| `--force` | Bypass YNAB's duplicate detection. Use to reimport transactions that were deleted from YNAB. |
| `--clear-cache` | Clear Claude's classification cache and re-analyze all emails. Useful if you've updated scoring criteria. |
| `--refresh-payees` | Force refresh of YNAB payee cache. By default, payees are cached for 24 hours with delta updates. |
| `--undo` | Undo the most recent run by deleting its transactions from YNAB and removing processed email records. |

Examples:

```bash
# Normal run
uv run fastmail2ynab.py

# Preview what would be imported (classifications are cached for later)
uv run fastmail2ynab.py --dry-run

# Reimport transactions deleted from YNAB
uv run fastmail2ynab.py --force

# Re-analyze all emails with fresh Claude classifications
uv run fastmail2ynab.py --clear-cache

# Force refresh payee list from YNAB
uv run fastmail2ynab.py --refresh-payees

# Undo the last run (delete transactions from YNAB)
uv run fastmail2ynab.py --undo
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

Edit `.env` to adjust:

| Variable | Default | Description |
|----------|---------|-------------|
| `HOURS_BACK` | 24 | How far back to scan for emails |
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

Each imported transaction includes in the memo field:
- `IMPORTED FROM AI SCRIPT` marker
- The AI confidence score (e.g., `Score: 8/10`)
- Direction (`INFLOW` or `OUTFLOW`)
- A brief description of the transaction

## Data storage

All data is stored in `processed_emails.db` (SQLite) with five tables:

| Table | Purpose |
|-------|---------|
| `processed_emails` | Tracks which emails have been handled to avoid reprocessing |
| `classification_cache` | Caches Claude's analysis to avoid redundant API calls |
| `ynab_payees` | Caches YNAB payee list for merchant name matching |
| `ynab_sync_state` | Stores sync metadata for efficient delta updates |
| `runs` | Tracks script executions for undo functionality |

Delete this file to start fresh and reprocess all emails.

## Payee Matching

When creating transactions, the script attempts to match Claude's extracted merchant name to existing YNAB payees:

1. **Exact match** (case-insensitive): "AMAZON.COM" matches "Amazon.com"
2. **Fuzzy match** (80% similarity threshold): "Amazon.com" matches "Amazon"

This ensures transactions use your existing payee names for consistent categorization and reporting.

## Amazon Routing

Transactions from Amazon are automatically routed to a separate YNAB account. This is useful if you pay for Amazon purchases with a store card or gift card balance rather than your primary credit card.

Detection checks both:
- Merchant name contains "amazon" (case-insensitive)
- Sender email contains "amazon" (e.g., @amazon.com, @amazon.co.uk)

The Amazon account ID is configured in the script as `AMAZON_ACCOUNT_ID`.

## Costs

**Claude API:**
- ~$0.003-0.015 per email (depending on length)
- At 10 receipts/day: ~$1-5/month

## Troubleshooting

**"Missing configuration"**
- Ensure `.env` exists and all values are filled in

**"Could not find Inbox"**
- Verify your Fastmail token has mail read permissions

**YNAB 400 errors**
- Check that your budget ID and account ID are correct
- Verify your YNAB token hasn't expired

**Duplicate transactions**
- The `import_id` prevents duplicates in YNAB
- If you need to reimport a deleted transaction, use `--force`

**Payee names not matching**
- Payee cache refreshes every 24 hours automatically
- Use `--refresh-payees` to force an immediate refresh
- Fuzzy matching uses 80% similarity threshold

**Want to re-analyze emails with Claude**
- Use `--clear-cache` to clear classifications and re-analyze all emails

**Made a mistake and want to undo**
- Use `--undo` to delete all transactions from the most recent run
- This also removes the processed email records so they can be reprocessed

**Want to preview before importing**
- Use `--dry-run` to see what would be imported
- Classifications are cached, so the real run won't re-call Claude
