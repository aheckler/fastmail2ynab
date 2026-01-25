#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "requests>=2.31.0",
#     "python-dotenv>=1.0.0",
#     "anthropic>=0.18.0",
# ]
# ///
"""
Fastmail2YNAB - Automatically import receipt emails to YNAB.

This script integrates three systems to automate personal finance tracking:

    1. Fastmail (JMAP API) - Fetches recent emails from your inbox
    2. Claude AI (Anthropic API) - Analyzes emails to identify receipts and
       extract transaction details (merchant, amount, date, direction)
    3. YNAB (API) - Creates unapproved transactions for review

The workflow:
    - Fetch the 100 most recent emails from inbox
    - For each unprocessed email, ask Claude to score it (1-10) as a receipt
    - If score meets threshold, extract merchant/amount/date
    - Create an unapproved transaction in YNAB
    - Track processed emails in SQLite to avoid duplicates

Usage:
    # Normal run - process recent emails
    uv run fastmail2ynab.py

    # Preview what would be imported without creating transactions
    uv run fastmail2ynab.py --dry-run

    # Force reimport (bypass YNAB's duplicate detection)
    uv run fastmail2ynab.py --force

    # Clear Claude's classification cache and re-analyze everything
    uv run fastmail2ynab.py --clear-cache

    # Force refresh of YNAB payee cache
    uv run fastmail2ynab.py --refresh-payees

    # Undo the most recent run (delete transactions from YNAB)
    uv run fastmail2ynab.py --undo

Environment Variables (in .env file):
    FASTMAIL_TOKEN         - Fastmail API token with mail read access
    ANTHROPIC_API_KEY      - Claude API key
    YNAB_TOKEN             - YNAB personal access token
    YNAB_BUDGET_ID         - Target budget UUID
    YNAB_ACCOUNT_ID        - Target account UUID (e.g., credit card)
    YNAB_AMAZON_ACCOUNT_ID - Optional: separate account for Amazon transactions
    MIN_SCORE              - Minimum AI confidence to import (default: 6)
"""

# =============================================================================
# Imports
# =============================================================================

# Standard library
import argparse
import difflib
import hashlib
import json
import os
import re
import sqlite3
import traceback
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from html.parser import HTMLParser
from pathlib import Path

# Third-party
import anthropic
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

# Configuration dictionary - all settings loaded from environment
CONFIG = {
    # API credentials
    "fastmail_token": os.getenv("FASTMAIL_TOKEN"),  # Fastmail JMAP bearer token
    "anthropic_api_key": os.getenv("ANTHROPIC_API_KEY"),  # Claude API key
    "ynab_token": os.getenv("YNAB_TOKEN"),  # YNAB personal access token
    # YNAB target location
    "ynab_budget_id": os.getenv("YNAB_BUDGET_ID"),  # Budget to add transactions to
    "ynab_account_id": os.getenv("YNAB_ACCOUNT_ID"),  # Account (e.g., credit card)
    "ynab_amazon_account_id": os.getenv("YNAB_AMAZON_ACCOUNT_ID"),  # Optional: separate Amazon account
    # Processing settings
    "min_score": safe_int(os.getenv("MIN_SCORE"), 6),  # Min AI score (1-10) to import
}

# Database path - stored alongside the script for easy backup/inspection
# Contains: processed_emails (tracking) and classification_cache (AI results)
DB_PATH = Path(__file__).parent / "processed_emails.db"

# API endpoint URLs
FASTMAIL_JMAP_URL = "https://api.fastmail.com/jmap/session"  # JMAP session endpoint
YNAB_BASE_URL = "https://api.ynab.com/v1"  # YNAB REST API base


# =============================================================================
# Data Classes
# =============================================================================


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
        amount: Transaction amount as a positive float (e.g., 29.99).
        currency: Three-letter currency code (e.g., "USD", "GBP").
        date: Transaction date in YYYY-MM-DD format.
        description: Brief description of what the transaction was for.
        reasoning: Claude's explanation of why it gave this score/classification.
    """

    score: int
    is_inflow: bool = False
    merchant: str | None = None
    amount: float | None = None
    currency: str | None = None
    date: str | None = None
    description: str | None = None
    reasoning: str | None = None


@dataclass
class PendingTransaction:
    """A transaction ready to be created in YNAB.

    Used for batch transaction creation - we collect all pending transactions
    first, then create them in batches of 5.

    Attributes:
        email_id: Fastmail's unique email identifier.
        account_id: YNAB account ID to create the transaction in.
        amount: Transaction amount as a positive float.
        date: Transaction date in YYYY-MM-DD format.
        payee_name: Merchant or source name.
        memo: Optional memo/notes for the transaction.
        import_id: Unique ID for YNAB deduplication.
        is_inflow: True for money received, False for money spent.
    """

    email_id: str
    account_id: str
    amount: float
    date: str
    payee_name: str
    memo: str
    import_id: str
    is_inflow: bool


def validate_transaction_date(date_str: str | None, email_received_at: str) -> str | None:
    """Validate and fix transaction date for YNAB constraints.

    YNAB requires dates to be:
    - Not in the future
    - Not more than 5 years ago

    Returns a valid YYYY-MM-DD date string, or None if unrecoverable.
    """
    today = datetime.now().date()
    five_years_ago = today - timedelta(days=5 * 365)

    # Try the extracted date first
    if date_str:
        try:
            parsed = datetime.strptime(date_str, "%Y-%m-%d").date()
            if five_years_ago <= parsed <= today:
                return date_str  # Valid date
        except ValueError:
            pass  # Invalid format, fall through

    # Fall back to email received date
    try:
        received_dt = datetime.fromisoformat(email_received_at.replace("Z", "+00:00"))
        fallback = received_dt.strftime("%Y-%m-%d")
        parsed = datetime.strptime(fallback, "%Y-%m-%d").date()
        if five_years_ago <= parsed <= today:
            return fallback
    except (ValueError, AttributeError):
        pass

    return None  # Unrecoverable


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
        cursor.execute("PRAGMA table_info(processed_emails)")
        columns = [row[1] for row in cursor.fetchall()]
        if "run_id" not in columns:
            cursor.execute("ALTER TABLE processed_emails ADD COLUMN run_id TEXT")

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
                amount REAL,
                currency TEXT,
                date TEXT,
                description TEXT,
                reasoning TEXT
            )
        """)

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
            """SELECT score, is_inflow, merchant, amount, currency, date, description, reasoning
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
        amount=row[3],
        currency=row[4],
        date=row[5],
        description=row[6],
        reasoning=row[7],
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
               (email_id, classified_at, score, is_inflow, merchant, amount, currency, date, description, reasoning)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                email_id,
                datetime.now(UTC).isoformat(),
                result.score,
                int(result.is_inflow),
                result.merchant,
                result.amount,
                result.currency,
                result.date,
                result.description,
                result.reasoning,
            ),
        )
        conn.commit()


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
# Amazon Routing
# =============================================================================
#
# Amazon transactions are routed to a separate YNAB account for tracking.
# This is useful when Amazon purchases are paid via a store card or gift
# card balance rather than the primary credit card.
#
# Detection checks both:
#   - Merchant name contains "amazon" (case-insensitive)
#   - Sender email contains "amazon" (e.g., @amazon.com, @amazon.co.uk)
#
# If either condition matches and YNAB_AMAZON_ACCOUNT_ID is configured,
# the transaction goes to that account instead of the default.
# =============================================================================


def is_amazon_transaction(merchant: str | None, from_email: str) -> bool:
    """Check if a transaction is from Amazon.

    Checks both the merchant name and email sender for "amazon".

    Args:
        merchant: The merchant name extracted by Claude.
        from_email: The sender's email address.

    Returns:
        True if either merchant or sender contains "amazon".
    """
    merchant_lower = (merchant or "").lower()
    email_lower = from_email.lower()
    return "amazon" in merchant_lower or "amazon" in email_lower


def get_account_for_transaction(merchant: str | None, from_email: str) -> str:
    """Get the YNAB account ID for a transaction.

    Routes Amazon transactions to a separate account if configured.

    Args:
        merchant: The merchant name extracted by Claude.
        from_email: The sender's email address.

    Returns:
        The YNAB account ID to use for this transaction.
    """
    amazon_account = CONFIG["ynab_amazon_account_id"]
    if amazon_account and is_amazon_transaction(merchant, from_email):
        return amazon_account
    return CONFIG["ynab_account_id"]


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

        # Try text body first - cleanest format
        text_parts = email_data.get("textBody") or []
        for part in text_parts:
            body_value = email_data.get("bodyValues", {}).get(part.get("partId"), {})
            if body_value.get("value"):
                body += body_value["value"]

        # Fall back to HTML body, stripped of tags
        if not body:
            html_parts = email_data.get("htmlBody") or []
            for part in html_parts:
                body_value = email_data.get("bodyValues", {}).get(part.get("partId"), {})
                if body_value.get("value"):
                    body += strip_html(body_value["value"])

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


def classify_email(email: Email, client: anthropic.Anthropic) -> ClassificationResult:
    """Use Claude to analyze an email and extract transaction details.

    Sends the email content to Claude with a structured prompt asking for:
    - A confidence score (1-10) that this is a financial transaction
    - Transaction direction (inflow = money received, outflow = money spent)
    - Structured data: merchant, amount, currency, date, description

    The prompt includes a scoring rubric:
        1-3: Not a transaction (marketing, shipping updates, etc.)
        4-5: Unlikely, but has financial language
        6-7: Probably a transaction, may be missing details
        8-10: Clearly a transaction with amount and merchant

    Args:
        email: The email to classify.
        client: Initialized Anthropic client.

    Returns:
        ClassificationResult with score and extracted data.
        Returns score=0 if parsing fails.
    """
    # Truncate body to avoid token limits (8000 chars ≈ 2000 tokens)
    truncated_body = email.body[:8000]

    # Structured prompt asking for JSON output
    # Includes scoring rubric and field definitions
    prompt = f"""Analyze this email and determine if it's related to a financial transaction.

FROM: {email.from_email}
SUBJECT: {email.subject}

BODY:
{truncated_body}

---

Score this email from 1-10 on how confident you are that it represents a financial transaction:
- 1-3: Clearly not a transaction (newsletters, marketing, shipping updates without prices)
- 4-5: Unlikely but has some financial language
- 6-7: Probably a transaction but missing some details
- 8-10: Clearly a financial transaction with amount and merchant/source

Also determine if this is:
- OUTFLOW: Money I spent (purchases, subscriptions, bills, fees, charges)
- INFLOW: Money I received (refunds, credits, rebates, cashback, deposits, payments to me)

Respond with JSON in this exact format:
{{
  "score": 8,
  "direction": "outflow",
  "merchant": "Store Name or Source",
  "amount": 29.99,
  "currency": "USD",
  "date": "2024-01-15",
  "description": "Brief description of transaction",
  "reasoning": "Why you gave this score and direction"
}}

Rules:
- "score" must be an integer from 1-10
- "direction" must be either "inflow" or "outflow"
- "amount" must be a positive number (no currency symbols), or null if not found
- "date" must be YYYY-MM-DD format. Use the transaction date, not the email date. Use null if not found.
- "merchant" should be the business/source name, or null if not found
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
            amount=float(data["amount"]) if data.get("amount") else None,
            currency=data.get("currency", "USD"),
            date=data.get("date"),
            description=data.get("description"),
            reasoning=data.get("reasoning"),
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

    The import_id format is: YNAB:{amount_milliunits}:{date}:{hash}
    This incorporates the email ID to ensure uniqueness.

    Args:
        email_id: Fastmail's unique email identifier.
        amount: Transaction amount (used in the ID for clarity).
        date: Transaction date in YYYY-MM-DD format.
        force: If True, adds a timestamp to bypass deduplication.
            Use this to reimport transactions that were deleted from YNAB.

    Returns:
        Import ID string, max 36 characters (YNAB limit).
    """
    hash_input = email_id.encode()
    if force:
        # Add timestamp to make the ID unique even for the same email
        hash_input += datetime.now(UTC).isoformat().encode()
    hash_hex = hashlib.md5(hash_input).hexdigest()[:8]
    amount_milliunits = abs(int(amount * 1000))
    import_id = f"YNAB:{amount_milliunits}:{date}:{hash_hex}"
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
    milliunits = int(round(amount * 1000))
    if not is_inflow:
        milliunits = -milliunits

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
        milliunits = int(round(pt.amount * 1000))
        if not pt.is_inflow:
            milliunits = -milliunits

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
        # Extract YNAB error details before raising
        try:
            error_data = response.json().get("error", {})
            error_detail = error_data.get("detail", response.text)
        except Exception:
            error_detail = response.text
        print(f"    -> YNAB API error: {error_detail}")
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


def refresh_payee_cache_if_needed(token: str, budget_id: str, force: bool = False) -> list[str]:
    """Refresh the payee cache if stale or forced, return list of payee names.

    The cache is refreshed if:
    - Cache is empty (first run)
    - Cache is older than 24 hours
    - force=True (user passed --refresh-payees)

    Uses delta updates when possible to minimize API data transfer.

    Args:
        token: YNAB personal access token.
        budget_id: Target budget UUID.
        force: If True, force a refresh even if cache is fresh.

    Returns:
        List of payee name strings.
    """
    if force or is_payee_cache_stale():
        action = "Force refreshing" if force else "Refreshing stale"
        print(f"  {action} YNAB payee cache...")
        payees, server_knowledge = fetch_ynab_payees(token, budget_id)
        cache_ynab_payees(payees, server_knowledge)
        print(f"  Cached {len(payees)} payees from YNAB")

    return get_cached_payees()


def match_payee_name(
    merchant: str, existing_payees: list[str], threshold: float = 0.8
) -> str | None:
    """Match a merchant name to an existing YNAB payee.

    First tries an exact case-insensitive match, then falls back to
    fuzzy matching using difflib's sequence matcher.

    Args:
        merchant: The merchant name extracted by Claude.
        existing_payees: List of existing payee names from YNAB.
        threshold: Minimum similarity ratio for fuzzy matching (0-1).

    Returns:
        Matched payee name if found, None otherwise.
    """
    if not merchant or not existing_payees:
        return None

    merchant_lower = merchant.lower()

    # First try exact case-insensitive match
    for payee in existing_payees:
        if payee.lower() == merchant_lower:
            return payee

    # Fall back to fuzzy matching
    matches = difflib.get_close_matches(merchant, existing_payees, n=1, cutoff=threshold)
    return matches[0] if matches else None


# =============================================================================
# Main Processing
# =============================================================================


def process_emails(force: bool = False, refresh_payees: bool = False, dry_run: bool = False):
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
    9. Create transactions in YNAB (in batches of 5)
    10. Mark emails as processed in our database

    Args:
        force: If True, bypass YNAB's import_id deduplication by generating
            unique import IDs. Use this to reimport transactions that were
            previously deleted from YNAB but are still in our processed_emails
            table. Does NOT bypass our own processed_emails check.
        refresh_payees: If True, force refresh of the YNAB payee cache.
        dry_run: If True, show what would be created without actually creating
            transactions or marking emails as processed.
    """
    # Validate all required config is present
    missing = [k for k, v in CONFIG.items() if not v]
    if missing:
        print(f"Error: Missing configuration: {', '.join(missing)}")
        print("Copy .env.example to .env and fill in your credentials.")
        return

    if dry_run:
        print("*** DRY RUN MODE: No transactions will be created ***")
        print()
    elif force:
        print("*** FORCE MODE: Will bypass YNAB duplicate detection ***")
        print()

    # Initialize database tables
    init_db()

    # Start a new run (unless dry run)
    run_id = None if dry_run else start_run()

    # Refresh YNAB payee cache for matching merchant names
    payee_names = refresh_payee_cache_if_needed(
        CONFIG["ynab_token"],
        CONFIG["ynab_budget_id"],
        force=refresh_payees,
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
        # Skip emails we've already processed
        if is_processed(email.id):
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
                result = classify_email(email, client)
                cache_classification(email.id, result)

            direction_str = "INFLOW" if result.is_inflow else "OUTFLOW"
            print(
                f"    -> Score: {result.score}/10, Direction: {direction_str} - {result.reasoning or 'N/A'}"
            )

            # Skip if below confidence threshold
            if result.score < CONFIG["min_score"]:
                print(f"    -> Below threshold ({CONFIG['min_score']}), skipping")
                if not dry_run:
                    non_receipt_emails.append(email.id)
                continue

            # Skip if Claude couldn't extract an amount
            if result.amount is None:
                print("    -> Missing amount, skipping")
                if not dry_run:
                    non_receipt_emails.append(email.id)
                continue

            # Determine transaction date
            # Validates date format and YNAB constraints (not future, not >5 years old)
            # Falls back to email received date if extracted date is invalid
            transaction_date = validate_transaction_date(result.date, email.received_at)
            if not transaction_date:
                print("    -> Invalid date and could not recover, skipping")
                if not dry_run:
                    non_receipt_emails.append(email.id)
                continue

            if transaction_date != result.date:
                if result.date:
                    print(f"    -> Date adjusted: {result.date} -> {transaction_date}")
                else:
                    print(f"    -> No transaction date found, using email date: {transaction_date}")

            sign = "+" if result.is_inflow else "-"
            print(
                f"    -> Importing: {result.merchant} {sign}${result.amount:.2f} on {transaction_date}"
            )

            # Match merchant name to existing YNAB payee for consistent naming
            final_payee = (
                match_payee_name(result.merchant, payee_names)
                if result.merchant
                else None
            ) or result.merchant or "Unknown"
            if final_payee != result.merchant and result.merchant:
                print(f"    -> Matched payee: '{result.merchant}' -> '{final_payee}'")

            # Determine which account to use (Amazon routing)
            account_id = get_account_for_transaction(result.merchant, email.from_email)
            if account_id == CONFIG["ynab_amazon_account_id"]:
                print("    -> Routing to Amazon account")

            # Generate import_id for YNAB deduplication
            import_id = generate_import_id(email.id, result.amount, transaction_date, force=force)

            # Build memo with metadata for reference in YNAB
            memo = f"fm2ynab | Score: {result.score}/10"

            # Store display data for summary table
            transaction_display_data[email.id] = (
                transaction_date,
                final_payee,
                result.amount,
                result.is_inflow,
                result.score,
            )

            if dry_run:
                print(f"    -> [DRY RUN] Would create: {final_payee} {sign}${result.amount:.2f}")
                created_email_ids.append(email.id)
                receipts_added += 1
            else:
                # Add to pending transactions for batch creation
                pending_transactions.append(
                    PendingTransaction(
                        email_id=email.id,
                        account_id=account_id,
                        amount=result.amount,
                        date=transaction_date,
                        payee_name=final_payee[:50],
                        memo=memo,
                        import_id=import_id,
                        is_inflow=result.is_inflow,
                    )
                )

        except Exception as e:
            print(f"    -> Error: {e}")
            print(f"    -> {traceback.format_exc()}")
            errors += 1

    # Mark non-receipt emails as processed (not in dry run mode)
    if not dry_run:
        for email_id in non_receipt_emails:
            mark_processed(email_id, is_receipt=False, run_id=run_id)

    # Batch create transactions in YNAB (not in dry run mode)
    if not dry_run and pending_transactions:
        print()
        print(f"Creating {len(pending_transactions)} transactions in YNAB...")

        # Process in batches of 5
        batch_size = 5
        for i in range(0, len(pending_transactions), batch_size):
            batch = pending_transactions[i : i + batch_size]
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
        assert run_id is not None
        complete_run(run_id, receipts_added)

    # Print transaction summary table (for both dry run and live runs)
    if created_email_ids:
        # Build list of transactions to display
        display_transactions = [
            transaction_display_data[email_id]
            for email_id in created_email_ids
            if email_id in transaction_display_data
        ]

        if display_transactions:
            print()
            if dry_run:
                print("Transactions that would be created:")
            else:
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
    if dry_run:
        print(
            f"Dry run complete! {receipts_added} would be created, {skipped} skipped, {cached} from cache, {errors} errors"
        )
    else:
        print(
            f"Done! {receipts_added} added, {duplicates} already in YNAB, {skipped} skipped, {cached} from cache, {errors} errors"
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
            "Bypass YNAB's import_id deduplication. Use this to reimport "
            "transactions that were deleted from YNAB. Does not bypass our "
            "local processed_emails tracking."
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
        "--refresh-payees",
        action="store_true",
        help=(
            "Force refresh of YNAB payee cache (normally uses cached data "
            "with delta updates, refreshing automatically after 24 hours)."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Preview what transactions would be created without actually "
            "creating them in YNAB or marking emails as processed."
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

    process_emails(force=args.force, refresh_payees=args.refresh_payees, dry_run=args.dry_run)
