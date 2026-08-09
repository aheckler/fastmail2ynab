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
"""Unit tests for _response_text().

Claude Sonnet 5 runs adaptive thinking by default, so a classification
response can begin with a ThinkingBlock. The old code read
`message.content[0].text`, which raised
`'ThinkingBlock' object has no attribute 'text'` on any email Claude
decided to reason about. These tests pin the block-type selection so the
positional assumption can't creep back in.

Uses the real anthropic block classes, not stand-ins, since the bug was
about those exact types.

Run with:  uv run test_response_parsing.py
"""

import json

from anthropic.types import Message, TextBlock, ThinkingBlock

from fastmail2ynab import _response_text

SAMPLE_JSON = '{"score": 10, "direction": "outflow", "merchant": "Obsidian"}'


def _message(*blocks: TextBlock | ThinkingBlock) -> Message:
    """Wrap content blocks in a Message without full response validation."""
    return Message.model_construct(content=list(blocks))


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
    ]
    for test in tests:
        test()
        print(f"PASS  {test.__name__}")
    print(f"\nAll {len(tests)} test(s) passed.")


if __name__ == "__main__":
    main()
