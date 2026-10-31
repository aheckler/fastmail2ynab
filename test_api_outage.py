#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "requests>=2.31.0",
#     "anthropic>=0.119.0",
#     "questionary>=2.0.0",
#     "html2text>=2024.2.26",
#     "claude-preflight",
# ]
#
# [tool.uv.sources]
# claude-preflight = { path = "/Users/Adam/Code/claude-preflight", editable = true }
# ///
"""Simulates a Claude outage partway through a run.

The fake client classifies email 1, raises APIConnectionError on email 2, and
must never be called for email 3. Email 1's result is kept (cached and marked
processed); emails 2 and 3 stay uncached and unprocessed for the next run; the
run logs one warning with no traceback and returns normally.

Fastmail, YNAB, the status preflight and the database path are stubbed, so
the test touches no network and no real data.

Run with:  uv run test_api_outage.py
"""

import contextlib
import io
import json
import logging
import tempfile
from pathlib import Path

import anthropic
from anthropic.types import Message, TextBlock, Usage

import fastmail2ynab as fm

MARKETING = json.dumps(
    {
        "checklist": {k: k == "marketing" for k in fm.CHECKLIST_WEIGHTS},
        "direction": "outflow",
        "score": 1,
        "reasoning": "Marketing email.",
    }
)


class FakeMessages:
    def __init__(self) -> None:
        self.calls = 0

    def create(self, **kwargs: object) -> Message:
        self.calls += 1
        if self.calls == 1:
            return Message.model_construct(
                content=[TextBlock(type="text", text=MARKETING)],
                stop_reason="end_turn",
                stop_details=None,
                usage=Usage(input_tokens=0, output_tokens=0),
            )
        if self.calls == 2:
            # The request is only stored on the exception, and the SDK's HTTP library has changed names.
            raise anthropic.APIConnectionError(request=None)  # type: ignore[arg-type]
        raise AssertionError("Claude was called again after an outage")


class FakeClient:
    def __init__(self, **kwargs: object) -> None:
        self.messages = FAKE


FAKE = FakeMessages()


class ListHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.DEBUG)
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(f"{record.levelname} {record.getMessage()}")


def test_outage_stops_the_run() -> None:
    emails = [
        fm.Email(
            id=f"e{i}",
            subject=f"Email {i}",
            from_email="a@b.c",
            received_at="2026-10-12T10:00:00Z",
            body="x",
        )
        for i in (1, 2, 3)
    ]
    handler = ListHandler()
    stderr = io.StringIO()
    with tempfile.TemporaryDirectory() as tmp:
        fm.DB_PATH = Path(tmp) / "test.db"
        fm.ACCOUNTS = [fm.Account(name="Card", ynab_id="acct-1", default=True)]
        fm.setup_logging = lambda run_id: Path(tmp) / "run.log"
        fm.claude_preflight_or_exit = lambda: None
        fm.check_api_health = lambda *a: None
        fm.refresh_payee_cache_if_needed = lambda *a: []
        fm.fetch_recent_emails = lambda token: emails
        fm.anthropic.Anthropic = FakeClient
        fm.log.setLevel(logging.DEBUG)
        fm.log.addHandler(handler)
        with contextlib.redirect_stderr(stderr):
            fm._process_emails_impl(force=False)

        assert FAKE.calls == 2, FAKE.calls
        assert fm.get_cached_classification("e1") is not None
        assert fm.is_processed("e1")
        for email_id in ("e2", "e3"):
            assert fm.get_cached_classification(email_id) is None, email_id
            assert not fm.is_processed(email_id), email_id

    warnings = [line for line in handler.lines if line.startswith("WARNING")]
    assert warnings == [
        "WARNING Claude API outage (APIConnectionError); stopped classifying, 2 email(s) left for the next run"
    ], warnings
    assert not any("Traceback" in line for line in handler.lines)
    assert "Traceback" not in stderr.getvalue()


if __name__ == "__main__":
    test_outage_stops_the_run()
    print("All tests passed.")
