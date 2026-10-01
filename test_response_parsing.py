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
"""Unit tests for _response_text() and _classification_from_message().

The classifier runs adaptive thinking, so a classification
response can begin with a ThinkingBlock. The old code read
`message.content[0].text`, which raised
`'ThinkingBlock' object has no attribute 'text'` on any email Claude
decided to reason about. These tests pin the block-type selection so the
positional assumption can't creep back in.

Uses the real anthropic block classes, not stand-ins, since the bug was
about those exact types.

The _classification_from_message() tests pin the stop_reason handling:
a refusal is a final (cacheable) score-0 result, a max_tokens stop is a
transient failure even when its text parses, and text that isn't one JSON
object is a transient failure, never a first-{-to-last-} regex merge.

Run with:  uv run test_response_parsing.py
"""

import json

from anthropic.types import Message, RefusalStopDetails, TextBlock, ThinkingBlock

from fastmail2ynab import CHECKLIST_WEIGHTS, _classification_from_message, _response_text

SAMPLE_JSON = '{"score": 10, "direction": "outflow", "merchant": "Obsidian"}'


def _message(
    *blocks: TextBlock | ThinkingBlock,
    stop_reason: str = "end_turn",
    stop_details: RefusalStopDetails | None = None,
) -> Message:
    """Wrap content blocks in a Message without full response validation."""
    return Message.model_construct(
        content=list(blocks), stop_reason=stop_reason, stop_details=stop_details
    )


def _thinking(text: str = "Deciding whether this is a real charge.") -> ThinkingBlock:
    return ThinkingBlock(type="thinking", thinking=text, signature="sig")


def _text(text: str) -> TextBlock:
    return TextBlock(type="text", text=text)


def test_thinking_block_first() -> None:
    """The regression: thinking leads, JSON follows. Must return the JSON."""
    message = _message(_thinking(), _text(SAMPLE_JSON))
    assert _response_text(message) == SAMPLE_JSON, _response_text(message)


def test_thinking_first_result_is_parseable() -> None:
    """End-to-end shape: what comes back still round-trips through json.loads."""
    message = _message(_thinking(), _text(SAMPLE_JSON))
    assert json.loads(_response_text(message))["merchant"] == "Obsidian"


def test_text_only() -> None:
    """No thinking: unchanged from the pre-Sonnet-5 behavior."""
    message = _message(_text(SAMPLE_JSON))
    assert _response_text(message) == SAMPLE_JSON, _response_text(message)


def test_multiple_text_blocks_are_joined() -> None:
    """A split response is concatenated, not truncated to the first block."""
    message = _message(_text('{"score": 10,'), _text(' "merchant": "Kagi"}'))
    assert json.loads(_response_text(message))["merchant"] == "Kagi"


def test_thinking_between_text_blocks() -> None:
    """Interleaved thinking is dropped wherever it appears."""
    message = _message(_text('{"score":'), _thinking(), _text(" 10}"))
    assert json.loads(_response_text(message))["score"] == 10


def test_surrounding_whitespace_stripped() -> None:
    message = _message(_thinking(), _text(f"\n\n  {SAMPLE_JSON}  \n"))
    assert _response_text(message) == SAMPLE_JSON, _response_text(message)


def test_thinking_only_returns_empty() -> None:
    """No text block at all: caller treats "" as a transient parse failure."""
    message = _message(_thinking())
    assert _response_text(message) == "", _response_text(message)


def test_empty_content_returns_empty() -> None:
    message = _message()
    assert _response_text(message) == "", _response_text(message)


def test_thinking_text_never_leaks_into_output() -> None:
    """Thinking prose must not be mistaken for response text."""
    message = _message(_thinking("The amount here is $999.99"), _text(SAMPLE_JSON))
    assert "999.99" not in _response_text(message), _response_text(message)


# Prefixes the main loop treats as transient (not cached, retried next run).
TRANSIENT_PREFIXES = ("Failed to parse", "Parse error", "Failed to compute")

# A full schema-shaped response: an Apple receipt that scores 10.
RECEIPT = {
    "checklist": {key: weight > 0 for key, weight in CHECKLIST_WEIGHTS.items()},
    "score": 10,
    "direction": "outflow",
    "merchant": "Apple",
    "matched_payee": "Apple",
    "account_name": None,
    "amount": 4.99,
    "currency": "USD",
    "date": "2026-09-30",
    "date_confidence": "Certain",
    "description": "iCloud+ subscription",
    "reasoning": "Receipt with amount, date, and card.",
}


def test_classify_end_turn_parses() -> None:
    message = _message(_thinking(), _text(json.dumps(RECEIPT)))
    result = _classification_from_message(message, "Your receipt from Apple")
    assert result.score == 10, result
    assert result.amount == 4.99 and result.merchant == "Apple", result
    assert result.date_confidence == "certain", result.date_confidence  # casing normalized


def test_classify_max_tokens_is_transient_even_with_valid_json() -> None:
    message = _message(_text(json.dumps(RECEIPT)), stop_reason="max_tokens")
    result = _classification_from_message(message, "Your receipt from Apple")
    assert result.score == 0, result
    assert (result.reasoning or "").startswith(TRANSIENT_PREFIXES), result.reasoning


def test_classify_refusal_is_final_with_category() -> None:
    details = RefusalStopDetails(type="refusal", category="general_harms")
    message = _message(stop_reason="refusal", stop_details=details)
    result = _classification_from_message(message, "Order confirmation")
    assert result.score == 0, result
    assert result.reasoning == "Refused by Claude (general_harms)", result.reasoning
    assert not result.reasoning.startswith(TRANSIENT_PREFIXES), result.reasoning


def test_classify_refusal_without_details() -> None:
    message = _message(stop_reason="refusal")
    result = _classification_from_message(message, "Order confirmation")
    assert result.reasoning == "Refused by Claude (unknown)", result.reasoning


def test_classify_draft_then_final_json_is_transient() -> None:
    """Two JSON objects must fail cleanly, not be merged by a greedy regex."""
    draft = dict(RECEIPT, amount=1.00)
    text = f"Draft: {json.dumps(draft)}\nFinal: {json.dumps(RECEIPT)}"
    result = _classification_from_message(_message(_text(text)), "Receipt")
    assert result.score == 0, result
    assert (result.reasoning or "").startswith(TRANSIENT_PREFIXES), result.reasoning


def test_classify_non_object_json_is_transient() -> None:
    result = _classification_from_message(_message(_text("[1, 2]")), "Receipt")
    assert (result.reasoning or "").startswith(TRANSIENT_PREFIXES), result.reasoning


def main() -> None:
    tests = [
        test_thinking_block_first,
        test_thinking_first_result_is_parseable,
        test_text_only,
        test_multiple_text_blocks_are_joined,
        test_thinking_between_text_blocks,
        test_surrounding_whitespace_stripped,
        test_thinking_only_returns_empty,
        test_empty_content_returns_empty,
        test_thinking_text_never_leaks_into_output,
        test_classify_end_turn_parses,
        test_classify_max_tokens_is_transient_even_with_valid_json,
        test_classify_refusal_is_final_with_category,
        test_classify_refusal_without_details,
        test_classify_draft_then_final_json_is_transient,
        test_classify_non_object_json_is_transient,
    ]
    for test in tests:
        test()
        print(f"PASS  {test.__name__}")
    print(f"\nAll {len(tests)} test(s) passed.")


if __name__ == "__main__":
    main()
