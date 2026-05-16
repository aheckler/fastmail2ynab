#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "requests>=2.31.0",
#     "python-dotenv>=1.0.0",
#     "anthropic>=0.18.0",
#     "questionary>=2.0.0",
#     "html2text>=2024.2.26",
# ]
# ///
"""Regression test for _map_batch_create_results.

YNAB's batch-create endpoint returns transactions sorted by date, not in
submission order. This test confirms results are paired back to their
PendingTransaction by import_id rather than by list position -- the bug that
filed a $5.99 Apple outflow under "Inflow: Ready to Assign".

Run with:  uv run test_batch_mapping.py
"""

from fastmail2ynab import PendingTransaction, _map_batch_create_results


def _pt(email_id: str, import_id: str, date: str) -> PendingTransaction:
    """Build a PendingTransaction with only the fields under test populated."""
    return PendingTransaction(
        email_id=email_id,
        account_id="acct-x",
        amount=1.0,
        date=date,
        payee_name="Payee",
        memo="memo",
        import_id=import_id,
        is_inflow=False,
        is_scheduled=False,
    )


def test_results_matched_by_import_id_not_position() -> None:
    """An out-of-order YNAB response still maps each email to its own id."""
    # Submission order: Barber, Apple, Ally1, Ally2, Cotton.
    submitted = [
        _pt("email-barber", "YNAB:2026-05-15:barber", "2026-05-15"),
        _pt("email-apple", "YNAB:2026-05-14:apple", "2026-05-14"),
        _pt("email-ally1", "YNAB:2026-05-13:ally1", "2026-05-13"),
        _pt("email-ally2", "YNAB:2026-05-13:ally2", "2026-05-13"),
        _pt("email-cotton", "YNAB:2026-05-12:cotton", "2026-05-12"),
    ]
    # YNAB returns them sorted by date ascending (the real-world behaviour).
    data = {
        "transactions": [
            {"id": "id-cotton", "import_id": "YNAB:2026-05-12:cotton"},
            {"id": "id-ally1", "import_id": "YNAB:2026-05-13:ally1"},
            {"id": "id-ally2", "import_id": "YNAB:2026-05-13:ally2"},
            {"id": "id-apple", "import_id": "YNAB:2026-05-14:apple"},
            {"id": "id-barber", "import_id": "YNAB:2026-05-15:barber"},
        ],
        "duplicate_import_ids": [],
    }

    results = _map_batch_create_results(submitted, data)

    assert results == [
        ("email-barber", "id-barber", False),
        ("email-apple", "id-apple", False),
        ("email-ally1", "id-ally1", False),
        ("email-ally2", "id-ally2", False),
        ("email-cotton", "id-cotton", False),
    ], results


def test_duplicate_import_id_flagged() -> None:
    """A duplicate import_id yields (email_id, None, True) and is not mismapped."""
    submitted = [
        _pt("email-a", "YNAB:2026-05-14:a", "2026-05-14"),
        _pt("email-b", "YNAB:2026-05-13:b", "2026-05-13"),
    ]
    # 'b' already existed: absent from transactions, listed as a duplicate.
    data = {
        "transactions": [
            {"id": "id-a", "import_id": "YNAB:2026-05-14:a"},
        ],
        "duplicate_import_ids": ["YNAB:2026-05-13:b"],
    }

    results = _map_batch_create_results(submitted, data)

    assert results == [
        ("email-a", "id-a", False),
        ("email-b", None, True),
    ], results


def main() -> None:
    tests = [
        test_results_matched_by_import_id_not_position,
        test_duplicate_import_id_flagged,
    ]
    for test in tests:
        test()
        print(f"PASS  {test.__name__}")
    print(f"\nAll {len(tests)} test(s) passed.")


if __name__ == "__main__":
    main()
