#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "requests>=2.31.0",
#     "python-dotenv>=1.0.0",
#     "anthropic>=0.119.0",
#     "questionary>=2.0.0",
#     "html2text>=2024.2.26",
# ]
# ///
"""Unit tests for compute_score().

The score for each classified email used to come straight from Claude's JSON
response, which sometimes contradicted the deterministic formula spelled out
in the prompt. This test pins the formula in code so the score is always a
pure function of the 11-key checklist.

Run with:  uv run test_compute_score.py
"""

from fastmail2ynab import CHECKLIST_WEIGHTS, compute_score


def _checklist(**overrides: bool) -> dict[str, bool]:
    """Build an all-False checklist with named keys flipped to True."""
    base = {key: False for key in CHECKLIST_WEIGHTS}
    base.update(overrides)
    return base


def test_amazon_shipping_example() -> None:
    """CLAUDE.md example: specific_amount + merchant + shipping_only -> 5."""
    checklist = _checklist(
        specific_amount=True,
        merchant_identified=True,
        shipping_only=True,
    )
    assert compute_score(checklist) == 5, compute_score(checklist)


def test_base_score_all_false() -> None:
    """All signals absent: base score of 3, no clamping."""
    checklist = _checklist()
    assert compute_score(checklist) == 3, compute_score(checklist)


def test_school_notification_example() -> None:
    """CLAUDE.md example: merchant + reminder_only -> 2."""
    checklist = _checklist(merchant_identified=True, reminder_only=True)
    assert compute_score(checklist) == 2, compute_score(checklist)


def test_apple_receipt_clamps_to_10() -> None:
    """CLAUDE.md example: all positives, no negatives -> 15 clamped to 10."""
    checklist = _checklist(
        specific_amount=True,
        confirmation_language=True,
        transaction_date=True,
        payment_method=True,
        merchant_identified=True,
        account_match=True,
    )
    assert compute_score(checklist) == 10, compute_score(checklist)


def test_cell_plan_renewal_example() -> None:
    """CLAUDE.md example: reminder with approximate amount -> 4."""
    checklist = _checklist(
        specific_amount=True,
        transaction_date=True,
        merchant_identified=True,
        payment_method=True,
        reminder_only=True,
        approximate_amount=True,
    )
    assert compute_score(checklist) == 4, compute_score(checklist)


def test_cloudflare_invoice_divergence_case() -> None:
    """Real May 2026 divergence: checklist gives 7, Claude returned 4 or 5.

    Formula per spec: 3 + specific_amount(3) + transaction_date(2) +
    merchant_identified(1) - reminder_only(2) = 7. No confirmation_language
    because it's an invoice/reminder, not a paid receipt.
    """
    checklist = _checklist(
        specific_amount=True,
        transaction_date=True,
        merchant_identified=True,
        reminder_only=True,
    )
    assert compute_score(checklist) == 7, compute_score(checklist)


def test_all_negatives_clamps_to_1() -> None:
    """Floor: heavy negatives clamp to 1, never lower."""
    checklist = _checklist(
        marketing=True,
        balance_credit=True,
        shipping_only=True,
        reminder_only=True,
        approximate_amount=True,
    )
    assert compute_score(checklist) == 1, compute_score(checklist)


def test_none_returns_none() -> None:
    assert compute_score(None) is None


def test_empty_dict_returns_none() -> None:
    assert compute_score({}) is None


def test_missing_one_key_returns_none() -> None:
    checklist = _checklist()
    del checklist["marketing"]
    assert compute_score(checklist) is None


def test_extra_key_returns_none() -> None:
    checklist = _checklist()
    checklist["unexpected_signal"] = True
    assert compute_score(checklist) is None


def test_renamed_key_returns_none() -> None:
    checklist = _checklist()
    checklist["spec_amount"] = checklist.pop("specific_amount")
    assert compute_score(checklist) is None


def main() -> None:
    tests = [
        test_amazon_shipping_example,
        test_base_score_all_false,
        test_school_notification_example,
        test_apple_receipt_clamps_to_10,
        test_cell_plan_renewal_example,
        test_cloudflare_invoice_divergence_case,
        test_all_negatives_clamps_to_1,
        test_none_returns_none,
        test_empty_dict_returns_none,
        test_missing_one_key_returns_none,
        test_extra_key_returns_none,
        test_renamed_key_returns_none,
    ]
    for test in tests:
        test()
        print(f"PASS  {test.__name__}")
    print(f"\nAll {len(tests)} test(s) passed.")


if __name__ == "__main__":
    main()
