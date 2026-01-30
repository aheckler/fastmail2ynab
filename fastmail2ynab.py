#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "requests>=2.31.0",
#     "python-dotenv>=1.0.0",
#     "anthropic>=0.18.0",
#     "questionary>=2.0.0",
# ]
# ///
"""
Fastmail2YNAB - Automatically import receipt emails to YNAB.

This script integrates three systems to automate personal finance tracking:

    1. Fastmail (JMAP API) - Fetches recent emails from your inbox
    2. Claude AI (Anthropic API) - Analyzes emails to identify receipts and
       extract transaction details (merchant, amount, date, direction, account)
    3. YNAB (API) - Creates unapproved transactions for review

The workflow:
    - Fetch the 100 most recent emails from inbox
    - For each unprocessed email, ask Claude to score it (1-10) as a receipt
    - If score meets threshold, extract merchant/amount/date
    - Create an unapproved transaction in YNAB (or scheduled transaction for
      future-dated bills with high confidence)
    - Track processed emails in SQLite to avoid duplicates

Usage:
    # Normal run - process recent emails
    uv run fastmail2ynab.py

    # Force reimport (reprocess all emails, bypass YNAB duplicate detection)
    uv run fastmail2ynab.py --force

    # Clear Claude's classification cache and re-analyze everything
    uv run fastmail2ynab.py --clear-cache

    # Undo the most recent run (delete transactions from YNAB)
    uv run fastmail2ynab.py --undo

Environment Variables (in .env file):
    FASTMAIL_TOKEN         - Fastmail API token with mail read access
    ANTHROPIC_API_KEY      - Claude API key
    YNAB_TOKEN             - YNAB personal access token
    YNAB_BUDGET_ID         - Target budget UUID
    YNAB_ACCOUNTS          - JSON array of account configurations (see .env.example)
    MIN_SCORE              - Minimum AI confidence to import (default: 6)

Account Descriptions (in .env.notes file):
    Detailed descriptions for each account to help AI route transactions.
    See .env.notes.example for format.
"""

# =============================================================================
# Imports
# =============================================================================

# Standard library
import argparse
import fcntl
import hashlib
import json
import os
import re
import sqlite3
import traceback
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from html.parser import HTMLParser
from pathlib import Path

# Third-party
import anthropic
import questionary
import requests
from dotenv import load_dotenv

# =============================================================================
# Configuration & Constants
# =============================================================================


def safe_int(value: str | None, default: int) -> int:
    """Safely convert string to int with fallback.

    Args:
        value: String to convert, or None.
        default: Value to return if conversion fails.

    Returns:
        The integer value, or default if value is None or not a valid integer.
    """
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


# Load environment variables from .env file
load_dotenv()


def parse_env_notes(script_dir: Path) -> dict[str, str]:
    """Parse .env.notes file to get account descriptions.

    The format is:
        Account Name:
        Notes about this account on following lines
        until the next account name (line ending with colon).

    Args:
        script_dir: Directory containing .env.notes file.

    Returns:
        Dict mapping account name to notes string.
    """
    notes_path = script_dir / ".env.notes"
    if not notes_path.exists():
        return {}

    notes: dict[str, str] = {}
    current_account: str | None = None
    current_lines: list[str] = []

    try:
        content = notes_path.read_text()
    except Exception:
        return {}

    for line in content.splitlines():
        # Check if this line is an account header (ends with colon, no leading whitespace)
        if line.rstrip().endswith(":") and not line.startswith((" ", "\t")):
            # Save previous account if any
            if current_account is not None:
                notes[current_account] = "\n".join(current_lines).strip()
            # Start new account
            current_account = line.rstrip(":").strip()
            current_lines = []
        elif current_account is not None:
            current_lines.append(line)

    # Save last account
    if current_account is not None:
        notes[current_account] = "\n".join(current_lines).strip()

    return notes


def load_accounts(script_dir: Path) -> list["Account"]:
    """Load YNAB accounts from YNAB_ACCOUNTS env var and merge with .env.notes.

    Args:
        script_dir: Directory containing .env.notes file.

    Returns:
        List of Account objects.

    Raises:
        SystemExit: If configuration is invalid.
    """
    # Import here to avoid circular reference at module level
    from dataclasses import fields as dataclass_fields

    del dataclass_fields  # Only imported to ensure Account is defined

    accounts_json = os.getenv("YNAB_ACCOUNTS")
    if not accounts_json:
        return []

    try:
        accounts_data = json.loads(accounts_json)
    except json.JSONDecodeError as e:
        raise SystemExit(f"Error: YNAB_ACCOUNTS is not valid JSON: {e}") from None

    if not isinstance(accounts_data, list):
        raise SystemExit("Error: YNAB_ACCOUNTS must be a JSON array") from None

    # Parse .env.notes for account descriptions
    env_notes = parse_env_notes(script_dir)

    accounts: list[Account] = []
    seen_names: set[str] = set()

    for i, acct in enumerate(accounts_data):
        if not isinstance(acct, dict):
            raise SystemExit(f"Error: YNAB_ACCOUNTS[{i}] must be an object") from None

        name = acct.get("name")
        ynab_id = acct.get("ynab_id")

        if not name:
            raise SystemExit(f"Error: YNAB_ACCOUNTS[{i}] missing 'name'") from None
        if not ynab_id:
            raise SystemExit(f"Error: YNAB_ACCOUNTS[{i}] missing 'ynab_id'") from None

        if name in seen_names:
            raise SystemExit(f"Error: Duplicate account name '{name}'") from None
        seen_names.add(name)

        # Get notes from .env.notes file
        notes = env_notes.get(name)

        accounts.append(
            Account(
                name=name,
                ynab_id=ynab_id,
                notes=notes,
                default=bool(acct.get("default", False)),
            )
        )

    # Validate exactly one default account
    default_accounts = [a for a in accounts if a.default]
    if len(default_accounts) == 0:
        raise SystemExit("Error: No account marked as default in YNAB_ACCOUNTS") from None
    if len(default_accounts) > 1:
        names = ", ".join(a.name for a in default_accounts)
        raise SystemExit(f"Error: Multiple default accounts: {names}") from None

    # Warn about accounts without notes
    for acct in accounts:
        if not acct.notes:
            print(f"Warning: Account '{acct.name}' has no notes in .env.notes")

    return accounts


# Script directory for config files
SCRIPT_DIR = Path(__file__).parent

# Configuration dictionary - all settings loaded from environment
CONFIG: dict = {
    # API credentials
    "fastmail_token": os.getenv("FASTMAIL_TOKEN"),  # Fastmail JMAP bearer token
    "anthropic_api_key": os.getenv("ANTHROPIC_API_KEY"),  # Claude API key
    "ynab_token": os.getenv("YNAB_TOKEN"),  # YNAB personal access token
    # YNAB target location
    "ynab_budget_id": os.getenv("YNAB_BUDGET_ID"),  # Budget to add transactions to
    # Processing settings
    "min_score": safe_int(os.getenv("MIN_SCORE"), 6),  # Min AI score (1-10) to import
}

# Load multi-account configuration
# Deferred loading happens later, after the Account dataclass is defined
ACCOUNTS: list["Account"] = []


def _init_accounts():
    """Initialize ACCOUNTS list after module load."""
    global ACCOUNTS
    ACCOUNTS = load_accounts(SCRIPT_DIR)


# Database path - stored alongside the script for easy backup/inspection
# Contains: processed_emails (tracking) and classification_cache (AI results)
DB_PATH = Path(__file__).parent / "processed_emails.db"

# Lock file path - used to prevent concurrent execution
LOCK_PATH = Path(__file__).parent / ".fastmail2ynab.lock"


@contextmanager
def acquire_lock():
    """Acquire exclusive lock to prevent concurrent execution.

    Uses OS-level file locking (fcntl.flock) to ensure only one instance
    of the script runs at a time. The lock is automatically released when
    the context manager exits.

    Raises:
        SystemExit: If another instance is already running.
    """
    lock_file = LOCK_PATH.open("w")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock_file.close()
        raise SystemExit("Another instance is already running. Exiting.") from None
    try:
        yield
    finally:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        lock_file.close()


# API endpoint URLs
FASTMAIL_JMAP_URL = "https://api.fastmail.com/jmap/session"  # JMAP session endpoint
YNAB_BASE_URL = "https://api.ynab.com/v1"  # YNAB REST API base


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class Account:
    """Represents a YNAB account for transaction routing.

    Claude AI determines which account to route transactions to based on
    account names and descriptions provided in the classification prompt.

    Attributes:
        name: Human-readable account name (e.g., "Chase Freedom", "Apple Card").
        ynab_id: YNAB account UUID.
        notes: Optional description to help AI route transactions correctly.
        default: If True, this account receives transactions when no specific
            account is detected. Exactly one account must be marked as default.
    """

    name: str
    ynab_id: str
    notes: str | None = None
    default: bool = False


@dataclass
class Email:
    """Represents an email fetched from Fastmail via JMAP.

    This is a simplified view of an email containing only the fields needed
    for receipt classification. The body is extracted in preference order:
    plain text > stripped HTML > preview snippet.

    Attributes:
        id: Fastmail's unique email identifier (used for tracking).
        subject: Email subject line.
        from_email: Sender's email address.
        received_at: ISO 8601 timestamp when the email was received.
        body: Plain text content of the email (HTML stripped if necessary).
    """

    id: str
    subject: str
    from_email: str
    received_at: str
    body: str


@dataclass
class ClassificationResult:
    """Result of Claude's analysis of an email for receipt/transaction content.

    Claude scores each email 1-10 on likelihood of being a financial transaction
    and extracts structured data if it appears to be one.

    Attributes:
        score: Confidence score from 1-10. Higher = more likely a transaction.
            1-3: Not a transaction (newsletters, marketing, etc.)
            4-5: Unlikely but has financial language
            6-7: Probably a transaction, may be missing details
            8-10: Clearly a transaction with amount and merchant
        is_inflow: Direction of money flow.
            True = money coming in (refunds, credits, payments received)
            False = money going out (purchases, bills, subscriptions)
        merchant: Business or source name (e.g., "Amazon", "Uber").
        matched_payee: Existing YNAB payee name that best matches the merchant,
            or None if no good match was found.
        amount: Transaction amount as a positive float (e.g., 29.99).
        currency: Three-letter currency code (e.g., "USD", "GBP").
        date: Transaction date in YYYY-MM-DD format.
        description: Brief description of what the transaction was for.
        reasoning: Claude's explanation of why it gave this score/classification.
        account_name: Name of the YNAB account to route this transaction to,
            or None to use the default account.
    """

    score: int
    is_inflow: bool = False
    merchant: str | None = None
    matched_payee: str | None = None
    amount: float | None = None
    currency: str | None = None
    date: str | None = None
    date_confidence: str | None = None  # "certain", "likely", or None
    description: str | None = None
    reasoning: str | None = None
    account_name: str | None = None


@dataclass
class PendingTransaction:
    """A transaction ready to be created in YNAB.

    Used for batch transaction creation - we collect all pending transactions
    first, then create them in batches of 5. Also used for scheduled transactions.

    Attributes:
        email_id: Fastmail's unique email identifier.
        account_id: YNAB account ID to create the transaction in.
        amount: Transaction amount as a positive float.
        date: Transaction date in YYYY-MM-DD format.
        payee_name: Merchant or source name.
        memo: Optional memo/notes for the transaction.
        import_id: Unique ID for YNAB deduplication (not used for scheduled).
        is_inflow: True for money received, False for money spent.
        is_scheduled: True for scheduled transactions (future dates).
    """

    email_id: str
    account_id: str
    amount: float
    date: str
    payee_name: str
    memo: str
    import_id: str | None
    is_inflow: bool
    is_scheduled: bool = False


def validate_transaction_date(
    date_str: str | None, email_received_at: str
) -> tuple[str | None, bool]:
    """Validate transaction date and determine if it's in the future.

    YNAB's regular transactions API requires dates not in the future.
    For future dates (like autopay due dates), callers should use the
    scheduled transactions API when confidence is high.

    Args:
        date_str: Extracted date string in YYYY-MM-DD format, or None.
        email_received_at: ISO timestamp when email was received (fallback).

    Returns:
        Tuple of (validated_date, is_future):
        - validated_date: A valid YYYY-MM-DD string, or None if unrecoverable
        - is_future: True if the date is in the future (after today)
    """
    today = datetime.now(UTC).date()
    five_years_ago = today - timedelta(days=5 * 365)
    ninety_days_future = today + timedelta(days=90)

    # Try the extracted date first
    if date_str:
        try:
            parsed = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC).date()
            if five_years_ago <= parsed <= ninety_days_future:
                is_future = parsed > today
                return (date_str, is_future)
        except ValueError:
            pass  # Invalid format, fall through

    # Fall back to email received date
    try:
        received_dt = datetime.fromisoformat(email_received_at.replace("Z", "+00:00"))
        fallback = received_dt.strftime("%Y-%m-%d")
        parsed = datetime.strptime(fallback, "%Y-%m-%d").replace(tzinfo=UTC).date()
        if five_years_ago <= parsed <= today:
            return (fallback, False)  # Email date is never future
    except (ValueError, AttributeError):
        pass

    return (None, False)  # Unrecoverable


# =============================================================================
# HTML Processing
# =============================================================================


class HTMLStripper(HTMLParser):
    """Extracts plain text from HTML by parsing and collecting text nodes.

    Uses Python's built-in HTMLParser to walk through HTML structure,
    skipping script and style tags (which contain non-visible code/CSS)
    and collecting all other text content.

    Usage:
        stripper = HTMLStripper()
        stripper.feed("<p>Hello <b>world</b></p>")
        text = stripper.get_text()  # "Hello world"
    """

    def __init__(self):
        super().__init__()
        self.text = []  # Accumulates text fragments
        self.skip = False  # True when inside script/style tags

    def handle_starttag(self, tag, attrs):
        """Called for each opening tag. Enables skip mode for script/style."""
        if tag in ("script", "style"):
            self.skip = True

    def handle_endtag(self, tag):
        """Called for each closing tag. Disables skip mode for script/style."""
        if tag in ("script", "style"):
            self.skip = False

    def handle_data(self, data):
        """Called for text content between tags. Collects if not skipping."""
        if not self.skip:
            self.text.append(data)

    def get_text(self) -> str:
        """Returns all collected text joined with spaces."""
        return " ".join(self.text)


def strip_html(html: str) -> str:
    """Remove HTML tags and return plain text.

    Attempts proper HTML parsing first, falling back to regex-based
    tag removal if parsing fails (e.g., malformed HTML).

    Args:
        html: Raw HTML string to convert.

    Returns:
        Plain text with HTML tags removed and whitespace normalized.
    """
    stripper = HTMLStripper()
    try:
        stripper.feed(html)
        return stripper.get_text()
    except Exception:
        # Fallback for malformed HTML: strip tags with regex
        text = re.sub(r"<[^>]+>", " ", html)
        return " ".join(text.split())


# =============================================================================
# Database Functions
# =============================================================================
#
# SQLite database with two tables:
#
# 1. processed_emails - Tracks which emails have been handled
#    - Prevents re-processing the same email on subsequent runs
#    - Records whether it was a receipt and any YNAB transaction ID
#
# 2. classification_cache - Stores Claude's analysis results
#    - Avoids redundant API calls for previously analyzed emails
#    - Useful when re-running after clearing processed_emails
#    - Can be cleared with --clear-cache flag
# =============================================================================


def init_db():
    """Initialize SQLite database tables if they don't exist.

    Creates two tables:
    - processed_emails: Tracks which emails have been handled
    - classification_cache: Stores Claude's analysis to avoid repeat API calls
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()

        def ensure_column(table: str, column: str, col_type: str = "TEXT"):
            cursor.execute(f"PRAGMA table_info({table})")
            if column not in [row[1] for row in cursor.fetchall()]:
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")

        # Table 1: Track which emails have been processed
        # email_id: Fastmail's unique email ID (primary key)
        # processed_at: ISO timestamp when we processed this email
        # is_receipt: 1 if it was a receipt, 0 if not (for stats/debugging)
        # ynab_transaction_id: The YNAB transaction ID if one was created
        # run_id: Links to the runs table for undo functionality
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processed_emails (
                email_id TEXT PRIMARY KEY,
                processed_at TEXT,
                is_receipt INTEGER,
                ynab_transaction_id TEXT,
                run_id TEXT
            )
        """)

        # Add run_id column if it doesn't exist (migration for existing databases)
        ensure_column("processed_emails", "run_id")

        # Table 2: Cache Claude's classification results
        # This avoids paying for repeat API calls when:
        # - Re-running the script after clearing processed_emails
        # - Adjusting min_score threshold and re-evaluating
        # Can be cleared with --clear-cache to force re-analysis
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS classification_cache (
                email_id TEXT PRIMARY KEY,
                classified_at TEXT,
                score INTEGER,
                is_inflow INTEGER,
                merchant TEXT,
                matched_payee TEXT,
                amount REAL,
                currency TEXT,
                date TEXT,
                description TEXT,
                reasoning TEXT
            )
        """)

        # Add columns if they don't exist (migrations for existing databases)
        ensure_column("classification_cache", "matched_payee")
        ensure_column("classification_cache", "account_name")
        ensure_column("classification_cache", "date_confidence")

        # Table 3: Cache YNAB payees for name matching
        # Reduces API calls by caching payee list with delta updates
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ynab_payees (
                payee_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                transfer_account_id TEXT,
                deleted INTEGER DEFAULT 0
            )
        """)

        # Table 4: Track sync state for YNAB data
        # Stores server_knowledge for delta updates and last_sync timestamp
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ynab_sync_state (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        # Table 5: Track script runs for undo functionality
        # run_id: Unique identifier for each script execution
        # started_at: ISO timestamp when the run started
        # completed_at: ISO timestamp when the run completed (null if incomplete)
        # transactions_created: Number of transactions created in this run
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                started_at TEXT,
                completed_at TEXT,
                transactions_created INTEGER DEFAULT 0
            )
        """)
        conn.commit()


def get_cached_classification(email_id: str) -> ClassificationResult | None:
    """Retrieve a cached classification result for an email.

    Args:
        email_id: Fastmail's unique email identifier.

    Returns:
        ClassificationResult if cached, None if not found.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT score, is_inflow, merchant, matched_payee, amount, currency, date, description, reasoning, account_name, date_confidence
               FROM classification_cache WHERE email_id = ?""",
            (email_id,),
        )
        row = cursor.fetchone()

    if row is None:
        return None

    return ClassificationResult(
        score=row[0],
        is_inflow=bool(row[1]),
        merchant=row[2],
        matched_payee=row[3],
        amount=row[4],
        currency=row[5],
        date=row[6],
        description=row[7],
        reasoning=row[8],
        account_name=row[9],
        date_confidence=row[10],
    )


def cache_classification(email_id: str, result: ClassificationResult):
    """Store a classification result in the cache.

    Uses INSERT OR REPLACE to update if the email was previously cached.

    Args:
        email_id: Fastmail's unique email identifier.
        result: The classification result from Claude.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """INSERT OR REPLACE INTO classification_cache
               (email_id, classified_at, score, is_inflow, merchant, matched_payee, amount, currency, date, description, reasoning, account_name, date_confidence)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                email_id,
                datetime.now(UTC).isoformat(),
                result.score,
                int(result.is_inflow),
                result.merchant,
                result.matched_payee,
                result.amount,
                result.currency,
                result.date,
                result.description,
                result.reasoning,
                result.account_name,
                result.date_confidence,
            ),
        )
        conn.commit()


def to_milliunits(amount: float, is_inflow: bool) -> int:
    """Convert amount to YNAB milliunits (positive=inflow, negative=outflow)."""
    milliunits = round(amount * 1000)
    return milliunits if is_inflow else -milliunits


def extract_ynab_error(response: requests.Response) -> str:
    """Extract error detail from YNAB API response."""
    try:
        return response.json().get("error", {}).get("detail", response.text)
    except Exception:
        return response.text


def is_processed(email_id: str) -> bool:
    """Check if an email has already been processed.

    Args:
        email_id: Fastmail's unique email identifier.

    Returns:
        True if the email has been processed before, False otherwise.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM processed_emails WHERE email_id = ?", (email_id,))
        return cursor.fetchone() is not None


def mark_processed(
    email_id: str,
    is_receipt: bool,
    ynab_id: str | None = None,
    run_id: str | None = None,
):
    """Mark an email as processed.

    Args:
        email_id: Fastmail's unique email identifier.
        is_receipt: Whether this email was determined to be a receipt.
        ynab_id: The YNAB transaction ID if a transaction was created.
        run_id: The run ID for undo functionality.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR REPLACE INTO processed_emails
            (email_id, processed_at, is_receipt, ynab_transaction_id, run_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            (email_id, datetime.now(UTC).isoformat(), int(is_receipt), ynab_id, run_id),
        )
        conn.commit()


# =============================================================================
# Run Tracking
# =============================================================================
#
# Functions for tracking script executions to enable undo functionality.
# Each run gets a unique ID stored in the `runs` table, and transactions
# created during that run are linked via run_id in processed_emails.
#
# This allows users to roll back the most recent run with --undo, which:
#   1. Finds the last completed run
#   2. Deletes all YNAB transactions created in that run
#   3. Removes the processed_emails records so emails can be reprocessed
# =============================================================================


def start_run() -> str:
    """Create a new run record and return its ID.

    Returns:
        The unique run_id for this script execution.
    """
    run_id = str(uuid.uuid4())
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO runs (run_id, started_at) VALUES (?, ?)",
            (run_id, datetime.now(UTC).isoformat()),
        )
        conn.commit()
    return run_id


def complete_run(run_id: str, transactions_created: int):
    """Mark a run as complete with the count of transactions created.

    Args:
        run_id: The run ID to update.
        transactions_created: Number of transactions created in this run.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """UPDATE runs
               SET completed_at = ?, transactions_created = ?
               WHERE run_id = ?""",
            (datetime.now(UTC).isoformat(), transactions_created, run_id),
        )
        conn.commit()


def get_last_run() -> tuple[str, int] | None:
    """Get the most recent completed run.

    Returns:
        Tuple of (run_id, transactions_created) or None if no runs exist.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT run_id, transactions_created FROM runs
               WHERE completed_at IS NOT NULL
               ORDER BY completed_at DESC LIMIT 1"""
        )
        row = cursor.fetchone()
    return (row[0], row[1]) if row else None


def get_transactions_for_run(run_id: str) -> list[tuple[str, str]]:
    """Get all transactions created in a specific run.

    Args:
        run_id: The run ID to query.

    Returns:
        List of (email_id, ynab_transaction_id) tuples.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """SELECT email_id, ynab_transaction_id FROM processed_emails
               WHERE run_id = ? AND ynab_transaction_id IS NOT NULL""",
            (run_id,),
        )
        return cursor.fetchall()


def delete_run_records(run_id: str):
    """Delete all records associated with a run.

    Args:
        run_id: The run ID to delete.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM processed_emails WHERE run_id = ?", (run_id,))
        cursor.execute("DELETE FROM runs WHERE run_id = ?", (run_id,))
        conn.commit()


# =============================================================================
# Account Routing
# =============================================================================
#
# Transactions are routed to YNAB accounts based on AI classification.
# Claude analyzes each email and determines which account it belongs to
# based on the account descriptions provided in .env.notes.
#
# If no account is specified or the account name is not found in the
# configuration, the transaction is routed to the default account.
# =============================================================================


def get_default_account(accounts: list[Account]) -> Account:
    """Get the default account from the configuration.

    Args:
        accounts: List of configured Account objects.

    Returns:
        The account marked as default.

    Raises:
        ValueError: If no default account is configured.
    """
    for acct in accounts:
        if acct.default:
            return acct
    raise ValueError("No default account configured")


def get_account_for_transaction(account_name: str | None, accounts: list[Account]) -> Account:
    """Get the YNAB account for a transaction based on AI classification.

    Args:
        account_name: The account name from AI classification, or None.
        accounts: List of configured Account objects.

    Returns:
        The matching Account, or the default account if not found.
    """
    if account_name:
        for acct in accounts:
            if acct.name == account_name:
                return acct
    return get_default_account(accounts)


# =============================================================================
# Fastmail JMAP Functions
# =============================================================================
#
# JMAP (JSON Meta Application Protocol) is a modern, efficient API for email.
# Fastmail uses JMAP instead of IMAP for programmatic access.
#
# The email fetch process has three steps:
#   1. Get session - Authenticate and get API URL + account ID
#   2. Find mailbox - Query for the Inbox mailbox ID
#   3. Fetch emails - Query for email IDs, then get full email details
#
# Note: Fastmail's JMAP doesn't support date filtering in queries, so we
# fetch recent emails and filter by date in Python.
# =============================================================================


def get_jmap_session(token: str) -> dict:
    """Authenticate with Fastmail and get JMAP session information.

    The session response contains the API URL and account ID needed
    for subsequent requests.

    Args:
        token: Fastmail API bearer token.

    Returns:
        Session dict containing 'apiUrl' and 'primaryAccounts' keys.

    Raises:
        requests.HTTPError: If authentication fails.
    """
    print("  [DEBUG] Connecting to Fastmail JMAP...")
    response = requests.get(
        FASTMAIL_JMAP_URL,
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    response.raise_for_status()
    session = response.json()
    print(f"  [DEBUG] API URL: {session.get('apiUrl')}")
    print(
        f"  [DEBUG] Account ID: {session.get('primaryAccounts', {}).get('urn:ietf:params:jmap:mail')}"
    )
    return session


def jmap_request(api_url: str, token: str, method_calls: list, debug_label: str = "") -> dict:
    """Execute a JMAP request with one or more method calls.

    JMAP requests are JSON-RPC-like: you send method calls and get responses.
    Each method call is a tuple of [methodName, arguments, callId].

    Args:
        api_url: The JMAP API endpoint URL from the session.
        token: Fastmail API bearer token.
        method_calls: List of JMAP method calls, each as [name, args, id].
        debug_label: Optional label for debug output on errors.

    Returns:
        JMAP response containing 'methodResponses' list.

    Raises:
        requests.HTTPError: If the HTTP request fails.
    """
    response = requests.post(
        api_url,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        json={
            # Declare which JMAP capabilities we're using
            "using": ["urn:ietf:params:jmap:core", "urn:ietf:params:jmap:mail"],
            "methodCalls": method_calls,
        },
        timeout=30,
    )

    if not response.ok:
        print(f"  [DEBUG] JMAP request failed: {response.status_code}")
        print(f"  [DEBUG] Response: {response.text[:500]}")
    response.raise_for_status()

    result = response.json()

    # Check for JMAP-level errors (separate from HTTP errors)
    for method_response in result.get("methodResponses", []):
        if method_response[0] == "error":
            print(f"  [DEBUG] JMAP error in {debug_label}: {method_response[1]}")

    return result


def fetch_recent_emails(token: str) -> list[Email]:
    """Fetch the 100 most recent emails from the Fastmail inbox.

    Performs a three-step JMAP workflow:
    1. Get session (authentication + API URL)
    2. Find Inbox mailbox ID
    3. Query and fetch emails

    Fetches the 100 most recent emails. Already-processed emails are
    filtered out by the caller, so multiple runs will catch up on
    any backlog.

    Args:
        token: Fastmail API bearer token.

    Returns:
        List of Email objects for the 100 most recent inbox emails.

    Raises:
        ValueError: If the Inbox mailbox cannot be found.
        requests.HTTPError: If any API request fails.
    """
    # Step 1: Get session info (API URL and account ID)
    session = get_jmap_session(token)
    api_url = session["apiUrl"]
    account_id = session["primaryAccounts"]["urn:ietf:params:jmap:mail"]

    # Step 2: Find the Inbox mailbox ID
    # Mailbox/query finds mailboxes by name
    mailbox_response = jmap_request(
        api_url,
        token,
        [["Mailbox/query", {"accountId": account_id, "filter": {"name": "Inbox"}}, "0"]],
        debug_label="Mailbox/query",
    )

    mailbox_result = mailbox_response["methodResponses"][0][1]
    inbox_ids = mailbox_result.get("ids", [])
    print(f"  [DEBUG] Mailbox query returned {len(inbox_ids)} results: {inbox_ids}")

    if not inbox_ids:
        # Debugging: list all mailboxes if Inbox not found
        print("  [DEBUG] No Inbox found, listing all mailboxes...")
        all_mailboxes = jmap_request(
            api_url,
            token,
            [["Mailbox/get", {"accountId": account_id, "properties": ["name", "role"]}, "0"]],
            debug_label="Mailbox/get (all)",
        )
        mailboxes = all_mailboxes["methodResponses"][0][1].get("list", [])
        for mb in mailboxes:
            print(f"    - {mb.get('name')} (role: {mb.get('role')}, id: {mb.get('id')})")
        raise ValueError("Could not find Inbox - see mailbox list above")

    inbox_id = inbox_ids[0]
    print(f"  [DEBUG] Using Inbox ID: {inbox_id}")

    # Step 3a: Query for email IDs in the Inbox
    # Note: No date filter here - Fastmail doesn't support it in queries
    # We fetch extra emails and filter by date in Python below
    query_response = jmap_request(
        api_url,
        token,
        [
            [
                "Email/query",
                {
                    "accountId": account_id,
                    "filter": {
                        "inMailbox": inbox_id,
                    },
                    "sort": [{"property": "receivedAt", "isAscending": False}],
                    "limit": 100,  # Fetch more since we'll filter by date
                },
                "1",
            ]
        ],
        debug_label="Email/query",
    )

    query_result = query_response["methodResponses"][0][1]
    email_ids = query_result.get("ids", [])
    print(f"  [DEBUG] Email query returned {len(email_ids)} IDs")

    if not email_ids:
        return []

    # Step 3b: Fetch full email details for the IDs we got
    # Request body content with fetchTextBodyValues/fetchHTMLBodyValues
    email_response = jmap_request(
        api_url,
        token,
        [
            [
                "Email/get",
                {
                    "accountId": account_id,
                    "ids": email_ids,
                    "properties": [
                        "id",
                        "receivedAt",
                        "from",
                        "subject",
                        "preview",  # Short text snippet (fallback)
                        "textBody",  # Plain text parts
                        "htmlBody",  # HTML parts
                        "bodyValues",  # Actual content of body parts
                    ],
                    "fetchTextBodyValues": True,  # Include text body content
                    "fetchHTMLBodyValues": True,  # Include HTML body content
                    "maxBodyValueBytes": 50000,  # Limit body size
                },
                "2",
            ]
        ],
        debug_label="Email/get",
    )

    emails = []

    for email_data in email_response["methodResponses"][0][1].get("list", []):
        # Extract body content in preference order:
        # 1. Plain text body (cleanest for AI analysis)
        # 2. HTML body (stripped to plain text)
        # 3. Preview snippet (last resort)
        body = ""

        # Extract text body content
        # Note: For HTML-only emails, textBody may contain HTML parts (type="text/html")
        # In that case, we still need to strip HTML tags
        text_body = ""
        text_parts = email_data.get("textBody") or []
        for part in text_parts:
            body_value = email_data.get("bodyValues", {}).get(part.get("partId"), {})
            if body_value.get("value"):
                content = body_value["value"]
                # Check if this "text" part is actually HTML
                if part.get("type", "").startswith("text/html"):
                    content = strip_html(content)
                text_body += content

        # Extract HTML body content (stripped of tags)
        html_body = ""
        html_parts = email_data.get("htmlBody") or []
        for part in html_parts:
            body_value = email_data.get("bodyValues", {}).get(part.get("partId"), {})
            if body_value.get("value"):
                html_body += strip_html(body_value["value"])

        # Decide which body to use
        # Some emails have a stub text body like "Please enable HTML to view this email"
        # In those cases, prefer the HTML body which has the actual content
        stub_phrases = ["enable html", "view this email", "html version", "html-enabled"]
        text_is_stub = (
            text_body
            and len(text_body) < 2000
            and any(phrase in text_body.lower() for phrase in stub_phrases)
        )

        if text_body and not text_is_stub:
            body = text_body
        elif html_body:
            body = html_body
        else:
            body = text_body  # Use stub if nothing else available

        # Last resort: use the preview snippet
        if not body:
            body = email_data.get("preview", "")

        # Extract sender email address
        from_list = email_data.get("from") or []
        from_email = from_list[0].get("email", "unknown") if from_list else "unknown"

        emails.append(
            Email(
                id=email_data["id"],
                subject=email_data.get("subject") or "",
                from_email=from_email,
                received_at=email_data.get("receivedAt", ""),
                body=body,
            )
        )

    print(f"  [DEBUG] Fetched {len(emails)} emails")
    return emails


# =============================================================================
# Claude Classification
# =============================================================================
#
# Uses Claude to analyze emails and determine:
# 1. Is this email about a financial transaction? (score 1-10)
# 2. If so, extract: merchant, amount, date, direction (inflow/outflow)
#
# The prompt asks for JSON output with a specific schema. We parse with
# fallback handling in case Claude wraps JSON in markdown or other text.
# =============================================================================


def classify_email(
    email: Email,
    client: anthropic.Anthropic,
    payee_names: list[str],
    accounts: list[Account],
) -> ClassificationResult:
    """Use Claude to analyze an email and extract transaction details.

    Sends the email content to Claude with a structured prompt asking for:
    - A confidence score (1-10) that this is a financial transaction
    - Transaction direction (inflow = money received, outflow = money spent)
    - Structured data: merchant, amount, currency, date, description
    - Payee matching: match merchant to existing YNAB payee if possible
    - Account routing: which YNAB account this transaction belongs to

    The prompt includes a scoring rubric:
        1-3: Not a transaction (marketing, shipping updates, etc.)
        4-5: Unlikely, but has financial language
        6-7: Probably a transaction, may be missing details
        8-10: Clearly a transaction with amount and merchant

    Args:
        email: The email to classify.
        client: Initialized Anthropic client.
        payee_names: List of existing YNAB payee names for matching.
        accounts: List of Account objects for routing.

    Returns:
        ClassificationResult with score and extracted data.
        Returns score=0 if parsing fails.
    """
    # Truncate body to avoid token limits (8000 chars ≈ 2000 tokens)
    truncated_body = email.body[:8000]

    # Format payee list for the prompt
    # Limit to 2000 payees to stay within reasonable token limits
    sorted_payees = sorted(payee_names)
    payee_list = "\n".join(sorted_payees[:2000])

    # Format account list for the prompt
    account_lines = []
    for acct in accounts:
        default_marker = " (DEFAULT)" if acct.default else ""
        account_lines.append(f"- {acct.name}{default_marker}")
        if acct.notes:
            # Indent notes under account name
            account_lines.extend(f"  {line}" for line in acct.notes.splitlines())
    accounts_text = "\n".join(account_lines) if account_lines else "(no accounts configured)"

    # Structured prompt asking for JSON output
    # Includes scoring rubric and field definitions
    prompt = f"""Analyze this email and determine if it's related to a financial transaction.

FROM: {email.from_email}
SUBJECT: {email.subject}

BODY:
{truncated_body}

---

EXISTING PAYEES (for matching):
{payee_list}

---

ACCOUNTS (for routing):
{accounts_text}

---

Score this email from 1-10 on how confident you are that money HAS MOVED or IS SCHEDULED TO MOVE:
- 1-3: Not financial (newsletters, marketing, shipping updates without prices)
- 4-5: Financially related but no transaction occurred (expiration notices, renewal reminders, price change notices, payment method alerts)
- 6-7: Probably a transaction but missing key details
- 8-10: Confirmed transaction - receipt, charge confirmation, payment confirmation, or autopay bill with specific due date

Also determine if this is:
- OUTFLOW: Money I spent (purchases, subscriptions, bills, fees, charges)
- INFLOW: Money I received (refunds, credits, rebates, cashback, deposits, payments to me)

Respond with JSON in this exact format:
{{
  "score": 8,
  "direction": "outflow",
  "merchant": "Store Name or Source",
  "matched_payee": "Existing Payee Name",
  "account_name": "Account Name",
  "amount": 29.99,
  "currency": "USD",
  "date": "2024-01-15",
  "date_confidence": "certain",
  "description": "Brief description of transaction",
  "reasoning": "Why you gave this score and direction"
}}

Rules:
- "score" must be an integer from 1-10
- "direction" must be either "inflow" or "outflow"
- "amount" must be a positive number (no currency symbols), or null if not found
- "date" must be YYYY-MM-DD format. For purchase receipts, use the purchase date. For bills with autopay, use the due date (when payment will be charged). For payment confirmations, use the payment date. Use null if not found.
- "date_confidence" indicates how certain you are about the date:
  - "certain": The email explicitly states this exact date (e.g., "Due Date: Feb 19, 2026" or "Payment scheduled for 2/19/26")
  - "likely": The date is implied but not explicitly stated
  - null: Date was inferred or uncertain
  Future dates require "certain" confidence to be used; otherwise they'll be adjusted to today.
- "merchant" should be the business/source name as it appears in the email, or null if not found
- "matched_payee" should be the EXACT name from the EXISTING PAYEES list that best matches this merchant. Use null if no good match exists. Consider abbreviations (e.g., "HOA" = "Homeowners Association"), common variations, and ignore suffixes like "Inc", "LLC", "Co.", etc. Only use a value from the provided list.
- "account_name" should be the EXACT name from the ACCOUNTS list that this transaction belongs to based on the account descriptions. Use null to route to the default account. Only use a value from the provided list.
- "description" should briefly describe the transaction

Examples of OUTFLOW: purchase receipts, subscription charges, bill payments, fees
Examples of INFLOW: refund confirmations, credit applied, cashback earned, payment received, reimbursement

Respond ONLY with valid JSON, no other text."""

    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )

    response_text = message.content[0].text.strip()

    # Parse JSON response with fallback strategies
    # Strategy 1: Direct parse (prompt asks for JSON only)
    try:
        data = json.loads(response_text)
    except json.JSONDecodeError:
        # Strategy 2: Extract JSON block if wrapped in markdown or other text
        json_match = re.search(r"\{[\s\S]*\}", response_text)
        if not json_match:
            return ClassificationResult(score=0, reasoning="Failed to parse response")
        try:
            data = json.loads(json_match.group())
        except json.JSONDecodeError:
            return ClassificationResult(score=0, reasoning="Failed to parse JSON from response")

    # Convert parsed JSON to ClassificationResult
    try:
        direction = (data.get("direction") or "outflow").lower()
        return ClassificationResult(
            score=int(data.get("score", 0)),
            is_inflow=(direction == "inflow"),
            merchant=data.get("merchant"),
            matched_payee=data.get("matched_payee"),
            amount=float(data["amount"]) if data.get("amount") else None,
            currency=data.get("currency", "USD"),
            date=data.get("date"),
            date_confidence=data.get("date_confidence"),
            description=data.get("description"),
            reasoning=data.get("reasoning"),
            account_name=data.get("account_name"),
        )

    except (KeyError, ValueError) as e:
        return ClassificationResult(score=0, reasoning=f"Parse error: {e}")


# =============================================================================
# YNAB Functions
# =============================================================================
#
# YNAB (You Need A Budget) uses a REST API with some conventions:
#
# - Amounts are in "milliunits": $29.99 = 29990 milliunits
# - Positive amounts = inflow (money in), negative = outflow (money out)
# - import_id enables deduplication: same ID = same transaction
# - Transactions created with approved=False appear for review
#
# The import_id is our key to preventing duplicates. We generate it from
# the email ID, so re-running the script won't create duplicate transactions.
# =============================================================================


def generate_import_id(email_id: str, amount: float, date: str, force: bool = False) -> str:
    """Generate a unique import ID for YNAB deduplication.

    YNAB uses import_id to prevent duplicate transactions. If you try to
    create a transaction with an import_id that already exists, YNAB
    returns 409 Conflict.

    The import_id format is: YNAB:{date}:{hash16}
    The 16-char hash provides 64 bits of entropy, reducing collision risk
    at scale (collision probability ~0.00001% at 65K transactions).

    Args:
        email_id: Fastmail's unique email identifier.
        amount: Transaction amount (unused but kept for API compatibility).
        date: Transaction date in YYYY-MM-DD format.
        force: If True, adds a timestamp to bypass deduplication.
            Use this to reimport transactions that were deleted from YNAB.

    Returns:
        Import ID string, max 36 characters (YNAB limit).
    """
    del amount  # Unused, kept for backwards compatibility
    hash_input = email_id.encode()
    if force:
        # Add timestamp to make the ID unique even for the same email
        hash_input += datetime.now(UTC).isoformat().encode()
    hash_hex = hashlib.md5(hash_input, usedforsecurity=False).hexdigest()[:16]
    import_id = f"YNAB:{date}:{hash_hex}"
    return import_id[:36]  # YNAB limit


def create_ynab_transaction(
    token: str,
    budget_id: str,
    account_id: str,
    amount: float,
    date: str,
    payee_name: str,
    memo: str | None,
    import_id: str,
    is_inflow: bool = False,
) -> tuple[str | None, bool]:
    """Create a transaction in YNAB.

    Creates an unapproved, uncleared transaction that will appear in
    YNAB for manual review and categorization.

    Args:
        token: YNAB personal access token.
        budget_id: Target budget UUID.
        account_id: Target account UUID (e.g., credit card account).
        amount: Transaction amount as a positive float.
        date: Transaction date in YYYY-MM-DD format.
        payee_name: Merchant or source name.
        memo: Optional memo/notes for the transaction.
        import_id: Unique ID for deduplication (see generate_import_id).
        is_inflow: True for money received, False for money spent.

    Returns:
        Tuple of (transaction_id, already_existed).
        If already_existed is True, transaction_id will be None.

    Raises:
        requests.HTTPError: If the API request fails (except 409 Conflict).
    """
    # Convert to YNAB milliunits: $29.99 = 29990
    # Positive = inflow (deposits, refunds), Negative = outflow (purchases)
    milliunits = to_milliunits(amount, is_inflow)

    # Build transaction payload
    transaction = {
        "account_id": account_id,  # Which account (credit card, checking, etc.)
        "date": date,  # YYYY-MM-DD format
        "amount": milliunits,  # Amount in milliunits (signed)
        "payee_name": payee_name,  # Merchant name (YNAB may match to existing)
        "memo": memo,  # Our metadata (score, description, etc.)
        "cleared": "uncleared",  # Not yet matched to bank transaction
        "approved": False,  # Requires manual review in YNAB
        "import_id": import_id,  # For deduplication
    }

    response = requests.post(
        f"{YNAB_BASE_URL}/budgets/{budget_id}/transactions",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json={"transaction": transaction},
        timeout=30,
    )

    # 409 Conflict = import_id already exists (duplicate transaction)
    if response.status_code == 409:
        return (None, True)

    response.raise_for_status()
    return (response.json()["data"]["transaction"]["id"], False)


def create_ynab_transactions_batch(
    token: str,
    budget_id: str,
    pending_transactions: list[PendingTransaction],
) -> list[tuple[str, str, bool]]:
    """Create multiple transactions in YNAB in a single API call.

    YNAB supports batch creation of up to 1000 transactions per call.
    We typically batch in groups of 5 for better error handling.

    Args:
        token: YNAB personal access token.
        budget_id: Target budget UUID.
        pending_transactions: List of PendingTransaction objects to create.

    Returns:
        List of tuples (email_id, transaction_id, already_existed).
        For duplicates, transaction_id will be None and already_existed True.

    Raises:
        requests.HTTPError: If the API request fails.
    """
    if not pending_transactions:
        return []

    # Build the transactions payload
    transactions = []
    for pt in pending_transactions:
        milliunits = to_milliunits(pt.amount, pt.is_inflow)

        transactions.append(
            {
                "account_id": pt.account_id,
                "date": pt.date,
                "amount": milliunits,
                "payee_name": pt.payee_name,
                "memo": pt.memo,
                "cleared": "uncleared",
                "approved": False,
                "import_id": pt.import_id,
            }
        )

    response = requests.post(
        f"{YNAB_BASE_URL}/budgets/{budget_id}/transactions",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json={"transactions": transactions},
        timeout=30,
    )
    if not response.ok:
        print(f"    -> YNAB API error: {extract_ynab_error(response)}")
    response.raise_for_status()

    data = response.json()["data"]
    created_ids = [t["id"] for t in data.get("transactions", [])]
    duplicate_import_ids = set(data.get("duplicate_import_ids", []))

    # Map results back to email IDs
    results = []
    created_idx = 0
    for pt in pending_transactions:
        if pt.import_id in duplicate_import_ids:
            results.append((pt.email_id, None, True))
        else:
            transaction_id = created_ids[created_idx] if created_idx < len(created_ids) else None
            results.append((pt.email_id, transaction_id, False))
            created_idx += 1

    return results


def create_ynab_scheduled_transaction(
    token: str,
    budget_id: str,
    account_id: str,
    date: str,
    amount: float,
    payee_name: str,
    memo: str | None,
    is_inflow: bool = False,
) -> str:
    """Create a one-time scheduled transaction in YNAB for a future date.

    YNAB's regular transactions API doesn't accept future dates. For bills
    with known autopay dates, we use scheduled transactions instead.

    Args:
        token: YNAB personal access token.
        budget_id: Target budget UUID.
        account_id: Target account UUID.
        date: Transaction date in YYYY-MM-DD format (must be in the future).
        amount: Transaction amount as a positive float.
        payee_name: Merchant or source name.
        memo: Optional memo/notes for the transaction.
        is_inflow: True for money received, False for money spent.

    Returns:
        The scheduled transaction ID.

    Raises:
        requests.HTTPError: If the API request fails.
    """
    # Convert to YNAB milliunits: $29.99 = 29990
    milliunits = to_milliunits(amount, is_inflow)

    # Build scheduled transaction payload
    # frequency: "never" creates a one-time scheduled transaction
    scheduled_transaction = {
        "account_id": account_id,
        "date": date,
        "frequency": "never",
        "amount": milliunits,
        "payee_name": payee_name,
        "memo": memo,
    }

    response = requests.post(
        f"{YNAB_BASE_URL}/budgets/{budget_id}/scheduled_transactions",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json={"scheduled_transaction": scheduled_transaction},
        timeout=30,
    )

    if not response.ok:
        print(f"    -> YNAB API error: {extract_ynab_error(response)}")
    response.raise_for_status()

    return response.json()["data"]["scheduled_transaction"]["id"]


def delete_ynab_transaction(token: str, budget_id: str, transaction_id: str) -> bool:
    """Delete a transaction from YNAB.

    Args:
        token: YNAB personal access token.
        budget_id: Target budget UUID.
        transaction_id: The transaction ID to delete.

    Returns:
        True if deleted successfully, False if not found.

    Raises:
        requests.HTTPError: If the API request fails (except 404).
    """
    response = requests.delete(
        f"{YNAB_BASE_URL}/budgets/{budget_id}/transactions/{transaction_id}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )

    if response.status_code == 404:
        return False

    response.raise_for_status()
    return True


def fetch_ynab_payees(token: str, budget_id: str) -> tuple[list[dict], int]:
    """Fetch all payees from YNAB, using delta updates if possible.

    Uses the server_knowledge parameter for efficient delta syncing.
    On first call, fetches all payees. On subsequent calls, only fetches
    payees that have changed since the last sync.

    Args:
        token: YNAB personal access token.
        budget_id: Target budget UUID.

    Returns:
        Tuple of (list of payee dicts, new server_knowledge value).

    Raises:
        requests.HTTPError: If the API request fails.
    """
    # Check for existing server_knowledge for delta updates
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM ynab_sync_state WHERE key = 'payees_server_knowledge'")
        row = cursor.fetchone()
        server_knowledge = int(row[0]) if row else None

    # Build URL with optional server_knowledge parameter
    url = f"{YNAB_BASE_URL}/budgets/{budget_id}/payees"
    if server_knowledge:
        url += f"?last_knowledge_of_server={server_knowledge}"

    response = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    response.raise_for_status()

    data = response.json()["data"]
    return (data["payees"], data["server_knowledge"])


def cache_ynab_payees(payees: list[dict], server_knowledge: int):
    """Store payees in the local SQLite cache and update sync state.

    Uses INSERT OR REPLACE to handle both new payees and updates.
    Also updates the server_knowledge and last_sync timestamp.

    Args:
        payees: List of payee dicts from the YNAB API.
        server_knowledge: The server_knowledge value from the API response.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()

        # Upsert each payee
        for payee in payees:
            cursor.execute(
                """INSERT OR REPLACE INTO ynab_payees
                   (payee_id, name, transfer_account_id, deleted)
                   VALUES (?, ?, ?, ?)""",
                (
                    payee["id"],
                    payee["name"],
                    payee.get("transfer_account_id"),
                    int(payee.get("deleted", False)),
                ),
            )

        # Update sync state
        cursor.execute(
            "INSERT OR REPLACE INTO ynab_sync_state (key, value) VALUES (?, ?)",
            ("payees_server_knowledge", str(server_knowledge)),
        )
        cursor.execute(
            "INSERT OR REPLACE INTO ynab_sync_state (key, value) VALUES (?, ?)",
            ("payees_last_sync", datetime.now(UTC).isoformat()),
        )
        conn.commit()


def get_cached_payees() -> list[str]:
    """Get list of cached payee names (non-deleted only).

    Returns:
        List of payee name strings from the local cache.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM ynab_payees WHERE deleted = 0")
        return [row[0] for row in cursor.fetchall()]


def is_payee_cache_stale() -> bool:
    """Check if the payee cache is older than 24 hours.

    Returns:
        True if cache is stale (>24 hours old) or doesn't exist.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM ynab_sync_state WHERE key = 'payees_last_sync'")
        row = cursor.fetchone()

    if not row:
        return True  # No sync state means cache is empty/stale

    try:
        last_sync = datetime.fromisoformat(row[0])
        age = datetime.now(UTC) - last_sync
        return age > timedelta(hours=24)
    except ValueError:
        return True  # Invalid timestamp, treat as stale


def refresh_payee_cache_if_needed(token: str, budget_id: str) -> list[str]:
    """Refresh the payee cache if stale, return list of payee names.

    The cache is refreshed if:
    - Cache is empty (first run)
    - Cache is older than 24 hours

    Uses delta updates when possible to minimize API data transfer.

    Args:
        token: YNAB personal access token.
        budget_id: Target budget UUID.

    Returns:
        List of payee name strings.
    """
    if is_payee_cache_stale():
        print("  Refreshing stale YNAB payee cache...")
        payees, server_knowledge = fetch_ynab_payees(token, budget_id)
        cache_ynab_payees(payees, server_knowledge)
        print(f"  Cached {len(payees)} payees from YNAB")

    return get_cached_payees()


# =============================================================================
# Main Processing
# =============================================================================


def select_transactions_interactive(
    pending: list[PendingTransaction],
    display_data: dict[str, tuple[str, str, float, bool, int]],
) -> list[PendingTransaction] | None:
    """Show interactive checkbox selection for pending transactions.

    Args:
        pending: List of pending transactions to select from.
        display_data: Dict mapping email_id to (date, payee, amount, is_inflow, score).

    Returns:
        List of selected transactions if user confirmed (may be empty).
        None if user cancelled with Ctrl+C.
    """
    if not pending:
        return pending

    # Override questionary's indicators to use clearer checkbox markers
    # Must patch in common module where they're actually used, not just constants
    from questionary.prompts import common

    common.INDICATOR_SELECTED = "[X]"
    common.INDICATOR_UNSELECTED = "[ ]"

    # Build choices with transaction details
    choices = []
    for txn in pending:
        date, payee, amount, is_inflow, score = display_data[txn.email_id]
        sign = "+" if is_inflow else "-"
        label = f"{date} | {payee[:30]:<30} | {sign}${amount:.2f} (score: {score})"
        choices.append(questionary.Choice(title=label, value=txn.email_id, checked=True))

    # Show checkbox selection
    print("[X] = import, [ ] = skip")
    print()
    selected_ids = questionary.checkbox(
        "Select transactions to import:",
        choices=choices,
        instruction="(space: toggle, a: all, enter: confirm)",
    ).ask()

    if selected_ids is None:  # User cancelled (Ctrl+C)
        return None

    return [txn for txn in pending if txn.email_id in selected_ids]


def process_emails(force: bool = False):
    """Main entry point: fetch emails, classify, and create YNAB transactions.

    Processing flow for each email:
    1. Skip if already processed (in our database)
    2. Check classification cache for previous Claude analysis
    3. If not cached, call Claude API to classify and cache result
    4. Skip if score below threshold (not confident it's a transaction)
    5. Skip if no amount extracted (can't create transaction without it)
    6. Use transaction date from email, or fall back to email received date
    7. Match merchant to existing YNAB payee for consistent naming
    8. Collect pending transactions for batch creation
    9. Show interactive selection UI for user to confirm transactions
    10. Create transactions in YNAB (in batches of 5)
    11. Mark emails as processed in our database

    Cancel (Ctrl+C) during selection to preview without marking emails as processed.

    Args:
        force: If True, reprocess all emails even if already in processed_emails,
            and bypass YNAB's import_id deduplication. Use this to reimport
            transactions that were previously deleted from YNAB.
    """
    # Validate all required config is present
    missing = [k for k, v in CONFIG.items() if not v]
    if missing:
        print(f"Error: Missing configuration: {', '.join(missing)}")
        print("Copy .env.example to .env and fill in your credentials.")
        return

    # Load and validate multi-account configuration
    _init_accounts()
    if not ACCOUNTS:
        print("Error: No accounts configured. Set YNAB_ACCOUNTS in .env")
        print(
            'Example: YNAB_ACCOUNTS=\'[{"name": "My Card", "ynab_id": "abc-123", "default": true}]\''
        )
        return

    # Acquire exclusive lock to prevent concurrent execution
    with acquire_lock():
        _process_emails_impl(force=force)


def _process_emails_impl(force: bool):
    """Internal implementation of process_emails (called with lock held)."""
    if force:
        print("*** FORCE MODE: Will bypass YNAB duplicate detection ***")
        print()

    # Initialize database tables
    init_db()

    # Start a new run
    run_id = start_run()

    # Refresh YNAB payee cache for matching merchant names
    payee_names = refresh_payee_cache_if_needed(
        CONFIG["ynab_token"],
        CONFIG["ynab_budget_id"],
    )
    print(f"Using {len(payee_names)} cached YNAB payees for matching")

    # Create Anthropic client once (reused for all emails)
    client = anthropic.Anthropic(api_key=CONFIG["anthropic_api_key"])

    # Fetch recent emails from Fastmail
    print("Fetching emails from inbox...")
    emails = fetch_recent_emails(CONFIG["fastmail_token"])
    print(f"Found {len(emails)} emails in Inbox")

    # Statistics counters
    receipts_added = 0  # New transactions created in YNAB
    scheduled_added = 0  # Scheduled transactions created for future dates
    duplicates = 0  # Skipped due to YNAB import_id conflict
    skipped = 0  # Skipped due to already in our processed_emails table
    cached = 0  # Used cached Claude classification (saved API calls)
    errors = 0  # Processing errors

    # Collect pending transactions for batch creation
    pending_transactions: list[PendingTransaction] = []
    # Track non-receipt emails to mark as processed after batch creation
    non_receipt_emails: list[str] = []
    # Track transaction display data by email_id: (date, payee, amount, is_inflow, score)
    transaction_display_data: dict[str, tuple[str, str, float, bool, int]] = {}
    # Track which transactions were actually created (not duplicates) by email_id
    created_email_ids: list[str] = []

    for email in emails:
        # Skip emails we've already processed (unless force mode)
        if not force and is_processed(email.id):
            print(f"  [SKIP] Already processed: {email.subject[:50]}")
            skipped += 1
            continue

        print(f"  [CHECK] {email.subject[:60]}")

        try:
            # Check classification cache before calling Claude API
            result = get_cached_classification(email.id)
            if result:
                print("    -> (cached)")
                cached += 1
            else:
                # No cache hit - call Claude API and cache the result
                result = classify_email(email, client, payee_names, ACCOUNTS)
                cache_classification(email.id, result)

            direction_str = "INFLOW" if result.is_inflow else "OUTFLOW"
            print(
                f"    -> Score: {result.score}/10, Direction: {direction_str} - {result.reasoning or 'N/A'}"
            )

            # Skip if below confidence threshold
            if result.score < CONFIG["min_score"]:
                print(f"    -> Below threshold ({CONFIG['min_score']}), skipping")
                non_receipt_emails.append(email.id)
                continue

            # Skip if Claude couldn't extract an amount
            if result.amount is None:
                print("    -> Missing amount, skipping")
                non_receipt_emails.append(email.id)
                continue

            # Determine transaction date
            # Returns (validated_date, is_future) tuple
            transaction_date, is_future = validate_transaction_date(result.date, email.received_at)
            if not transaction_date:
                print("    -> Invalid date and could not recover, skipping")
                non_receipt_emails.append(email.id)
                continue

            # Route future dates with high confidence to scheduled transactions API
            use_scheduled_api = is_future and result.date_confidence == "certain"

            # For future dates without high confidence, cap to today
            display_date = transaction_date
            if is_future and not use_scheduled_api:
                today_str = datetime.now(UTC).date().strftime("%Y-%m-%d")
                print(
                    f"    -> Date adjusted: {transaction_date} -> {today_str} "
                    f"(confidence: {result.date_confidence or 'unknown'})"
                )
                transaction_date = today_str

            if transaction_date != result.date and not is_future:
                if result.date:
                    print(f"    -> Date adjusted: {result.date} -> {transaction_date}")
                else:
                    print(f"    -> No transaction date found, using email date: {transaction_date}")

            sign = "+" if result.is_inflow else "-"
            print(
                f"    -> Importing: {result.merchant} {sign}${result.amount:.2f} on {transaction_date}"
            )

            # Use Claude's matched payee if available, otherwise fall back to merchant name
            final_payee = result.matched_payee or result.merchant or "Unknown"
            if result.matched_payee and result.matched_payee != result.merchant:
                print(f"    -> Matched payee: '{result.merchant}' -> '{result.matched_payee}'")

            # Determine which account to use based on AI classification
            account = get_account_for_transaction(result.account_name, ACCOUNTS)
            if result.account_name and result.account_name == account.name:
                print(f"    -> Routing to account: {account.name}")
            elif result.account_name:
                # AI suggested an account that doesn't exist, using default
                print(
                    f"    -> Unknown account '{result.account_name}', using default: {account.name}"
                )

            # Build memo with metadata for reference in YNAB
            memo = f"fm2ynab | Run: {run_id[:8]}"

            # Store display data for summary table (use original date for display)
            transaction_display_data[email.id] = (
                display_date if use_scheduled_api else transaction_date,
                final_payee,
                result.amount,
                result.is_inflow,
                result.score,
            )

            # Generate import_id for YNAB deduplication (not used for scheduled)
            import_id = (
                None
                if use_scheduled_api
                else generate_import_id(email.id, result.amount, transaction_date, force=force)
            )
            pending_transactions.append(
                PendingTransaction(
                    email_id=email.id,
                    account_id=account.ynab_id,
                    amount=result.amount,
                    date=transaction_date,
                    payee_name=final_payee[:50],
                    memo=memo,
                    import_id=import_id,
                    is_inflow=result.is_inflow,
                    is_scheduled=use_scheduled_api,
                )
            )

        except Exception as e:
            print(f"    -> Error: {e}")
            print(f"    -> {traceback.format_exc()}")
            errors += 1

    # Mark non-receipt emails as processed
    for email_id in non_receipt_emails:
        mark_processed(email_id, is_receipt=False, run_id=run_id)

    # Interactive selection of transactions
    if not pending_transactions:
        print()
        print("No transactions to review.")
        return

    print()
    original_pending = pending_transactions.copy()
    result = select_transactions_interactive(pending_transactions, transaction_display_data)

    if result is None:
        # User cancelled with Ctrl+C - don't mark anything as processed (preview mode)
        print("Cancelled. No emails marked as processed.")
        return

    pending_transactions = result

    # Mark skipped transactions as processed (user explicitly confirmed)
    selected_ids = {txn.email_id for txn in pending_transactions}
    skipped_count = 0
    for txn in original_pending:
        if txn.email_id not in selected_ids:
            mark_processed(txn.email_id, is_receipt=True, ynab_id=None, run_id=run_id)
            skipped_count += 1
    if skipped_count:
        print(f"Marked {skipped_count} skipped transaction(s) as processed.")

    if not pending_transactions:
        print("No transactions selected. Marked skipped emails as processed.")
        return

    # Split into scheduled and regular transactions
    scheduled_transactions = [t for t in pending_transactions if t.is_scheduled]
    regular_transactions = [t for t in pending_transactions if not t.is_scheduled]

    # Create scheduled transactions in YNAB (one at a time, no batch API)
    if scheduled_transactions:
        print()
        print(f"Creating {len(scheduled_transactions)} scheduled transaction(s) in YNAB...")

        for txn in scheduled_transactions:
            try:
                scheduled_id = create_ynab_scheduled_transaction(
                    token=CONFIG["ynab_token"],
                    budget_id=CONFIG["ynab_budget_id"],
                    account_id=txn.account_id,
                    date=txn.date,
                    amount=txn.amount,
                    payee_name=txn.payee_name,
                    memo=txn.memo,
                    is_inflow=txn.is_inflow,
                )
                print(f"    -> Created scheduled for {txn.date}: {scheduled_id}")
                mark_processed(txn.email_id, is_receipt=True, ynab_id=scheduled_id, run_id=run_id)
                scheduled_added += 1
                created_email_ids.append(txn.email_id)
            except Exception as e:
                print(f"    -> Error creating scheduled transaction: {e}")
                errors += 1

    # Batch create regular transactions in YNAB
    if regular_transactions:
        print()
        print(f"Creating {len(regular_transactions)} transaction(s) in YNAB...")

        # Process in batches of 5
        batch_size = 5
        for i in range(0, len(regular_transactions), batch_size):
            batch = regular_transactions[i : i + batch_size]
            print(f"  Batch {i // batch_size + 1}: {len(batch)} transactions")

            try:
                results = create_ynab_transactions_batch(
                    token=CONFIG["ynab_token"],
                    budget_id=CONFIG["ynab_budget_id"],
                    pending_transactions=batch,
                )

                for email_id, ynab_id, already_existed in results:
                    if already_existed:
                        print("    -> Already exists in YNAB (duplicate)")
                        duplicates += 1
                    else:
                        print(f"    -> Created: {ynab_id}")
                        receipts_added += 1
                        created_email_ids.append(email_id)

                    mark_processed(email_id, is_receipt=True, ynab_id=ynab_id, run_id=run_id)

            except Exception as e:
                print(f"    -> Batch error: {e}")
                print(f"    -> {traceback.format_exc()}")
                errors += len(batch)

    # Complete the run
    if scheduled_transactions or regular_transactions:
        complete_run(run_id, receipts_added + scheduled_added)
    else:
        complete_run(run_id, 0)

    # Print transaction summary table
    if created_email_ids:
        # Build list of transactions to display
        display_transactions = [
            transaction_display_data[email_id]
            for email_id in created_email_ids
            if email_id in transaction_display_data
        ]

        if display_transactions:
            print()
            print("Transactions created:")

            # Calculate column widths
            max_payee_len = max(len(payee) for _, payee, _, _, _ in display_transactions)
            payee_width = max(max_payee_len, 5)  # minimum "Payee" header width

            # Print header
            print(f"{'Date':<10}  {'Payee':<{payee_width}}  {'Amount':>9}  {'Score':>5}")
            print(f"{'-' * 10}  {'-' * payee_width}  {'-' * 9}  {'-' * 5}")

            # Print rows
            for date, payee, amount, is_inflow, score in display_transactions:
                sign = "+" if is_inflow else "-"
                amount_str = f"{sign}${amount:.2f}"
                print(f"{date:<10}  {payee:<{payee_width}}  {amount_str:>9}  {score:>5}")

    # Print summary statistics
    print()
    scheduled_msg = f", {scheduled_added} scheduled" if scheduled_added else ""
    print(
        f"Done! {receipts_added} added{scheduled_msg}, {duplicates} already in YNAB, {skipped} skipped, {cached} from cache, {errors} errors"
    )


def undo_last_run():
    """Undo the most recent script run by deleting its transactions from YNAB.

    This function:
    1. Finds the most recent completed run
    2. Gets all transactions created in that run
    3. Deletes each transaction from YNAB
    4. Removes the processed_emails and run records
    """
    # Validate required config
    if not CONFIG["ynab_token"] or not CONFIG["ynab_budget_id"]:
        print("Error: Missing YNAB configuration")
        return

    # Acquire exclusive lock to prevent concurrent execution
    with acquire_lock():
        _undo_last_run_impl()


def _undo_last_run_impl():
    """Internal implementation of undo_last_run (called with lock held)."""
    init_db()

    last_run = get_last_run()
    if not last_run:
        print("No runs found to undo.")
        return

    run_id, transaction_count = last_run
    print(f"Found last run: {run_id}")
    print(f"Transactions created: {transaction_count}")

    transactions = get_transactions_for_run(run_id)
    if not transactions:
        print("No transactions found for this run.")
        delete_run_records(run_id)
        print("Cleaned up run records.")
        return

    print(f"Deleting {len(transactions)} transactions from YNAB...")

    deleted = 0
    not_found = 0
    errors = 0

    for _, ynab_id in transactions:
        if not ynab_id:
            not_found += 1
            continue

        try:
            success = delete_ynab_transaction(
                CONFIG["ynab_token"],
                CONFIG["ynab_budget_id"],
                ynab_id,
            )
            if success:
                print(f"  Deleted: {ynab_id}")
                deleted += 1
            else:
                print(f"  Not found (already deleted?): {ynab_id}")
                not_found += 1
        except Exception as e:
            print(f"  Error deleting {ynab_id}: {e}")
            errors += 1

    # Clean up database records
    delete_run_records(run_id)

    print()
    print(f"Undo complete! {deleted} deleted, {not_found} not found, {errors} errors")


# =============================================================================
# CLI Entry Point
# =============================================================================


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Import receipt emails from Fastmail to YNAB",
        epilog="Environment variables should be set in a .env file. See CLAUDE.md for details.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help=(
            "Reprocess all emails and bypass YNAB's duplicate detection. "
            "Use this to reimport transactions that were deleted from YNAB."
        ),
    )
    parser.add_argument(
        "--clear-cache",
        action="store_true",
        help=(
            "Clear Claude's classification cache before running. Forces "
            "re-analysis of all emails (useful if you've updated the prompt "
            "or want fresh classifications)."
        ),
    )
    parser.add_argument(
        "--undo",
        action="store_true",
        help=(
            "Undo the most recent run by deleting its transactions from YNAB "
            "and removing the processed email records."
        ),
    )
    args = parser.parse_args()

    # Handle --undo: undo the last run and exit
    if args.undo:
        other_flags = []
        if args.force:
            other_flags.append("--force")
        if args.clear_cache:
            other_flags.append("--clear-cache")
        if other_flags:
            print(f"Warning: {', '.join(other_flags)} ignored when using --undo")
            print()
        undo_last_run()
        exit(0)

    # Handle --clear-cache: drop the classification_cache table
    if args.clear_cache:
        print("Clearing classification cache...")
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("DROP TABLE IF EXISTS classification_cache")
            conn.commit()
        print("Cache cleared.")
        print()

    process_emails(force=args.force)
