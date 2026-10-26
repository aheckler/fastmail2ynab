#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "requests>=2.31.0",
#     "python-dotenv>=1.0.0",
#     "anthropic>=0.119.0",
#     "questionary>=2.0.0",
#     "html2text>=2024.2.26",
#     "claude-preflight",
# ]
#
# [tool.uv.sources]
# claude-preflight = { path = "/Users/Adam/Code/claude-preflight", editable = true }
# ///
"""Pins the request layout that lets prompt caching work in classify_email().

Prompt caching matches the start of a request byte for byte. The payees,
accounts, and rules are the same for every email in a run, so they go in one
system block marked `cache_control: ephemeral`, and the email goes in the
user turn. If anything per-email ever leaks into the system block, every
call writes a fresh cache entry and none is ever read, with no error. These
tests catch that.

The fake client records each request's kwargs and returns a schema-shaped
response, so nothing touches the network.

Run with:  uv run test_prompt_caching.py
"""

import json
from typing import Any

from anthropic.types import Message, TextBlock, Usage

import fastmail2ynab as fm

RECEIPT = json.dumps(
    {
        "checklist": {key: weight > 0 for key, weight in fm.CHECKLIST_WEIGHTS.items()},
        "score": 10,
        "direction": "outflow",
        "merchant": "Apple",
        "matched_payee": "Apple",
        "account_name": None,
        "amount": 2.99,
        "currency": "USD",
        "date": "2026-10-24",
        "date_confidence": "certain",
        "description": "iCloud+ subscription",
        "reasoning": "Receipt with amount, date, and card.",
    }
)

PAYEES = ["Zen Cleaning Services", "Apple", "Gables Bagels", "Duke Energy"]
ACCOUNTS = [
    fm.Account(name="Fidelity Card", ynab_id="acct-1", default=True, notes="Ends in 4331."),
    fm.Account(name="Company Card", ynab_id=None, skip=True, notes="Ends in 9999."),
]


class FakeMessages:
    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []

    def create(self, **kwargs: Any) -> Message:
        self.requests.append(kwargs)
        return Message.model_construct(
            content=[TextBlock(type="text", text=RECEIPT)],
            stop_reason="end_turn",
            stop_details=None,
            usage=Usage(input_tokens=0, output_tokens=0),
        )


class FakeClient:
    def __init__(self) -> None:
        self.messages = FakeMessages()


def _email(n: int) -> fm.Email:
    return fm.Email(
        id=f"e{n}",
        subject=f"Your receipt number {n}",
        from_email=f"sender{n}@example.com",
        received_at="2026-10-24T10:00:00Z",
        body=f"Body text unique to email {n}.",
    )


def _requests(*emails: fm.Email, payees: list[str] = PAYEES) -> list[dict[str, Any]]:
    client = FakeClient()
    for email in emails:
        fm.classify_email(email, client, payees, ACCOUNTS)  # type: ignore[arg-type]
    return client.messages.requests


def _system_text(request: dict[str, Any]) -> str:
    return request["system"][0]["text"]


def test_system_block_is_identical_across_emails() -> None:
    first, second = _requests(_email(1), _email(2))
    assert first["system"] == second["system"]
    assert len(first["system"]) == 1, first["system"]
    block = first["system"][0]
    assert block["type"] == "text", block
    assert block["cache_control"] == {"type": "ephemeral"}, block["cache_control"]


def test_email_is_only_in_the_user_turn() -> None:
    emails = [_email(1), _email(2)]
    for email, request in zip(emails, _requests(*emails), strict=True):
        assert len(request["messages"]) == 1, request["messages"]
        assert request["messages"][0]["role"] == "user", request["messages"]
        user_text = request["messages"][0]["content"]
        system_text = _system_text(request)
        for value in (email.from_email, email.subject, email.body):
            assert value in user_text, value
            assert value not in system_text, value


def test_payees_and_accounts_are_in_the_system_block() -> None:
    system_text = _system_text(_requests(_email(1))[0])
    for name in PAYEES:
        assert name in system_text, name
    for account in ACCOUNTS:
        assert account.name in system_text, account.name
        assert account.notes and account.notes in system_text, account.notes
    assert "## CLASSIFICATION CHECKLIST" in system_text


def test_payee_order_does_not_change_the_system_block() -> None:
    reordered = PAYEES[::-1]
    assert reordered != PAYEES
    (first,) = _requests(_email(1))
    (second,) = _requests(_email(1), payees=reordered)
    assert _system_text(first) == _system_text(second)


def test_response_still_parses() -> None:
    result = fm.classify_email(_email(1), FakeClient(), PAYEES, ACCOUNTS)  # type: ignore[arg-type]
    assert result.score == 10, result
    assert result.amount == 2.99 and result.matched_payee == "Apple", result


def main() -> None:
    tests = [
        test_system_block_is_identical_across_emails,
        test_email_is_only_in_the_user_turn,
        test_payees_and_accounts_are_in_the_system_block,
        test_payee_order_does_not_change_the_system_block,
        test_response_still_parses,
    ]
    for test in tests:
        test()
        print(f"PASS  {test.__name__}")
    print("All tests passed.")


if __name__ == "__main__":
    main()
