# Fastmail2YNAB

A local Python script that automatically detects receipt emails in Fastmail and creates transactions in YNAB.

## How it works

1. Fetches recent emails from your Fastmail inbox via JMAP
2. Uses Claude to classify each email and extract transaction details (merchant, amount, date, inflow/outflow)
3. Matches merchant names to existing YNAB payees using Claude for consistent naming
4. Routes transactions to the appropriate YNAB account based on AI classification
5. Creates unapproved transactions in YNAB in batches of 5
6. Overrides YNAB's auto-assigned categories: outflows are left uncategorized for review, inflows are routed to "Inflow: Ready to Assign"
7. Archives the source emails in Fastmail (moves from Inbox to Archive); receipts paid with an untracked card are skipped and left in the inbox instead
8. Tracks processed emails and run history in a local SQLite database

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
2. Create a new token with "Mail" read and write access

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
  {"name": "Bank Checking", "ynab_id": "ghi-789-your-account-id"},
  {"name": "Company Card", "skip": true}
]
```

- Each account must have a `name`
- Each account must have a `ynab_id`, except untracked-card accounts marked `"skip": true` (see [Untracked cards](#untracked-cards-skip-accounts))
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

Examples:

```bash
# Normal run - interactively select transactions to create
# Use Ctrl+C during selection to preview without marking emails as processed
# Needs a real terminal: with no TTY the script stops before importing anything
uv run fastmail2ynab.py

# Reimport transactions deleted from YNAB
uv run fastmail2ynab.py --force
```

## Configuration

Edit `.env` to adjust optional settings:

| Variable | Default | Description |
|----------|---------|-------------|
| `MIN_SCORE` | 6 | Minimum AI score (1-10) to import a transaction |

## How scoring works

Each email is scored 1-10 by a deterministic formula in the script. Claude fills in an 11-item checklist of positive and negative signals (e.g. "specific_amount", "confirmation_language", "marketing", "approximate_amount"), and the script applies fixed weights to compute the score:

- **1-3**: Clearly not a transaction (newsletters, marketing, shipping updates without prices)
- **4-5**: Unlikely but has some financial language
- **6-7**: Probably a transaction but missing some details
- **8-10**: Clearly a financial transaction with amount and merchant

Emails scoring 6 or higher (by default) are imported into YNAB. Computing the score in code rather than reading it from Claude's response makes scoring stable across model versions and across runs of the same email.

## Inflow vs Outflow detection

Claude also determines whether each transaction is:

- **OUTFLOW**: Money you spent (purchases, subscriptions, bills, fees)
- **INFLOW**: Money you received (refunds, credits, cashback, deposits)

This is reflected correctly in YNAB—outflows show as negative amounts, inflows as positive.

Each imported transaction includes the memo: `Imported by fastmail2ynab`

## Non-USD transactions

The script is USD-only and does not perform currency conversion. Claude identifies every currency in each email and:

- If an email shows **only** non-USD currency (e.g., a receipt entirely in GBP or EUR), the email is skipped with a `Non-USD currency (XXX), skipping` log line.
- If an email shows **USD alongside other currencies** (e.g., a travel booking with both a USD total and a GBP breakdown), the USD amount is used.
- If no currency can be identified, the email is treated as USD.

## Multi-Account Routing

Claude determines which YNAB account each transaction belongs to based on:

1. **Account descriptions** in `.env.notes` - Detailed descriptions help Claude understand which emails belong to which account
2. **Email sender** - e.g., emails from @apple.com might go to Apple Card
3. **Email content** - e.g., "SoFi Checking" mentioned in the email

If Claude can't determine the account, or the suggested account doesn't exist, the transaction goes to the default account.

## Untracked cards (skip accounts)

Some receipts that land in your inbox are paid with a card you do not track in YNAB — a company card, a spouse's employer benefits card, and so on. Importing those would create transactions for money that isn't in your budget.

Mark such a card with `"skip": true` in `YNAB_ACCOUNTS`. A skip account needs no `ynab_id`:

```json
{"name": "Company Card", "skip": true}
```

Give it a `.env.notes` description that states its last 4 digits, exactly like a normal account:

```
Company Card:
Employer-issued company card, Visa ending in 1234. Not tracked in YNAB.
```

When Claude routes a receipt to a skip account (using the `.env.notes` description you wrote), the script:

- does **not** create a transaction in YNAB,
- records the email as processed so it does not resurface on the next run,
- leaves the email in your Fastmail inbox (it is **not** archived), so you can still act on it — for example, forward it for reimbursement.

The default account cannot be a skip account.

## Scheduled Transactions for Future Dates

For bills with autopay due dates in the future (like "Due Date: Feb 19, 2026"), the script uses YNAB's scheduled transactions API when Claude is confident about the date:

- **"certain" confidence**: The email explicitly states the exact date (e.g., "Due Date: Feb 19, 2026"). Creates a one-time scheduled transaction for that future date.
- **"likely" or unknown confidence**: The date is implied or uncertain. The transaction is created with today's date instead.

This prevents incorrect future-dated transactions while properly handling autopay bills where the due date is clearly stated.

## Logging

Each run writes a detailed log file to `logs/` with timestamps and debug-level detail (JMAP requests, classification scores, routing decisions). Console output is quieter, showing only milestones, accepted transactions, and the summary.

Log files are named `YYYY-MM-DD_HH-MM-SS.log` and auto-pruned after 90 days.

## Data storage

All data is stored in `processed_emails.db` (SQLite) with five tables:

| Table | Purpose |
|-------|---------|
| `processed_emails` | Tracks which emails have been handled to avoid reprocessing |
| `classification_cache` | Caches Claude's analysis to avoid redundant API calls |
| `ynab_payees` | Caches YNAB payee list for merchant name matching |
| `ynab_sync_state` | Stores sync metadata for efficient delta updates |
| `runs` | Tracks script executions |

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

**"1Password isn't running" / "Timed out waiting for 1Password to write .env"**
- Only applies when `.env` is a 1Password Environments pipe instead of a regular file
- Launch 1Password and unlock the vault, then run again
- If 1Password is running and unlocked, check that the Environments destination for this project is still enabled

**The script hangs at startup with no output and no log file**
- Same cause as above, on a version of the script without the guard
- Opening a named pipe for reading blocks until a writer attaches, and 1Password is the writer
- No log appears because the `.env` load happens at import, before logging starts
- Confirm the file type with `ls -l .env` — a leading `p` means it's a pipe

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
- New payees will be matched on future emails automatically

**Transactions going to wrong account**
- Improve account descriptions in `.env.notes`
- Future emails will use the updated descriptions

**Want to preview before importing**
- Press Ctrl+C during transaction selection to preview without importing
- Classifications are cached, so re-running won't call Claude again
- Emails won't be marked as processed, so they'll reappear on the next run

**"No terminal attached, so the selection prompt cannot be shown"**
- Transaction selection is interactive and needs a real terminal, so the script stops here when stdin isn't a TTY — piped input, cron, launchd, or a non-interactive shell
- Nothing is imported and no emails are marked as processed, so re-running from an interactive shell picks up exactly where it left off
- Classifications from the stopped run are cached, so the re-run won't call Claude again for those emails

**Emails not archiving**
- Your Fastmail API token must have mail write access (not just read)
- Check logs for "Cannot archive: Archive mailbox not found" which means your Fastmail account has no Archive mailbox
- Archiving failures are non-fatal — transactions are still created in YNAB
