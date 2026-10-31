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
"""Unit tests for the Keychain secrets loader and the config.json reader.

Secrets come from the macOS login Keychain via `security find-generic-password`,
and the non-secret settings come from config.json. These tests pin the contract
the rest of the script relies on: a variable already in the environment always
wins, a found entry is exported byte for byte, a missing entry or a missing
`security` binary leaves the variable unset instead of raising, and a malformed
config.json exits with a message instead of being half-used.

`subprocess.run` is stubbed throughout, so no test reads the real Keychain.

Run with:  uv run test_secrets_loader.py
"""

import os
import subprocess
import tempfile
from pathlib import Path

import fastmail2ynab
from fastmail2ynab import load_local_config, load_secrets

KEY = "FASTMAIL2YNAB_TEST_ONLY_SECRET"


class _StubRun:
    """Stand-in for subprocess.run that records calls and returns a canned result."""

    def __init__(self, returncode: int = 0, stdout: str = "", raises: BaseException | None = None):
        self.calls: list[list[str]] = []
        self.returncode = returncode
        self.stdout = stdout
        self.raises = raises

    def __call__(self, args, **kwargs):
        self.calls.append(list(args))
        if self.raises is not None:
            raise self.raises
        return subprocess.CompletedProcess(args, self.returncode, stdout=self.stdout, stderr="")


def _with_stub(stub: _StubRun, fn) -> None:
    """Run fn with fastmail2ynab's subprocess.run replaced by stub, then restore."""
    original = fastmail2ynab.subprocess.run
    fastmail2ynab.subprocess.run = stub
    os.environ.pop(KEY, None)
    try:
        fn()
    finally:
        fastmail2ynab.subprocess.run = original
        os.environ.pop(KEY, None)


def test_found_entry_is_exported_exactly() -> None:
    """Only the trailing newline security prints is removed; spaces survive."""
    stub = _StubRun(returncode=0, stdout="  two words  \n")

    def body() -> None:
        load_secrets((KEY,))
        assert os.environ.get(KEY) == "  two words  ", repr(os.environ.get(KEY))

    _with_stub(stub, body)


def test_reads_the_right_service_and_account() -> None:
    """The lookup names our service and the variable as the account, and asks for -w."""
    stub = _StubRun(returncode=0, stdout="x\n")
    _with_stub(stub, lambda: load_secrets((KEY,)))
    assert stub.calls == [
        [
            "/usr/bin/security",
            "find-generic-password",
            "-s",
            fastmail2ynab.KEYCHAIN_SERVICE,
            "-a",
            KEY,
            "-w",
        ]
    ], stub.calls


def test_existing_value_wins_even_when_empty() -> None:
    """A variable already set, even to "", is never looked up or overwritten."""
    stub = _StubRun(raises=AssertionError("security must not be called"))
    original = fastmail2ynab.subprocess.run
    fastmail2ynab.subprocess.run = stub
    os.environ[KEY] = ""
    try:
        load_secrets((KEY,))
        assert os.environ[KEY] == "", repr(os.environ[KEY])
        assert stub.calls == [], stub.calls
    finally:
        fastmail2ynab.subprocess.run = original
        os.environ.pop(KEY, None)


def test_missing_entry_leaves_variable_unset() -> None:
    """security exits 44 for a missing item; the variable stays unset, no exception."""
    stub = _StubRun(returncode=44, stdout="")

    def body() -> None:
        load_secrets((KEY,))
        assert KEY not in os.environ

    _with_stub(stub, body)


def test_no_security_binary_is_tolerated() -> None:
    """Off macOS there is no /usr/bin/security; environment variables are the only source."""
    stub = _StubRun(raises=FileNotFoundError("/usr/bin/security"))

    def body() -> None:
        load_secrets((KEY,))
        assert KEY not in os.environ

    _with_stub(stub, body)


def _expect_exit(fn, needle: str) -> None:
    """Assert fn() raises SystemExit whose message contains needle."""
    try:
        fn()
    except SystemExit as exc:
        assert needle in str(exc), str(exc)
    else:
        raise AssertionError(f"expected SystemExit mentioning {needle!r}")


def _config_file(tmpdir: str, text: str) -> Path:
    path = Path(tmpdir) / "config.json"
    path.write_text(text, encoding="utf-8")
    return path


def test_missing_config_returns_empty() -> None:
    """No config.json is not an error at import time; startup checks report it."""
    with tempfile.TemporaryDirectory() as tmpdir:
        assert load_local_config(Path(tmpdir) / "config.json") == {}


def test_valid_config_round_trips() -> None:
    text = (
        '{"ynab_budget_id": "b-1", "min_score": 7, "accounts": '
        '[{"name": "Café Card", "ynab_id": "a-1", "default": true}, {"name": "Work", "skip": true}]}'
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        data = load_local_config(_config_file(tmpdir, text))
    assert data["ynab_budget_id"] == "b-1", data
    assert data["min_score"] == 7, data
    assert [a["name"] for a in data["accounts"]] == ["Café Card", "Work"], data


def test_invalid_json_exits() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        path = _config_file(tmpdir, '{"min_score": 6,}')
        _expect_exit(lambda: load_local_config(path), "is not valid JSON")


def test_non_object_top_level_exits() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        path = _config_file(tmpdir, '[{"name": "Card"}]')
        _expect_exit(lambda: load_local_config(path), "must contain a JSON object")


def test_unknown_key_exits() -> None:
    """A typo like min_scroe must not silently fall back to the default."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = _config_file(tmpdir, '{"min_scroe": 8}')
        _expect_exit(lambda: load_local_config(path), "unknown key(s) in config.json: min_scroe")


def test_wrong_types_exit() -> None:
    cases = {
        '{"min_score": "6"}': "'min_score' in config.json has the wrong type",
        '{"min_score": true}': "'min_score' in config.json has the wrong type",
        '{"ynab_budget_id": 123}': "'ynab_budget_id' in config.json has the wrong type",
        '{"accounts": {"name": "Card"}}': "'accounts' in config.json has the wrong type",
    }
    for text, needle in cases.items():
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _config_file(tmpdir, text)
            _expect_exit(lambda path=path: load_local_config(path), needle)


def test_null_value_exits() -> None:
    """A present key set to null is a wrong type, not "use the default"."""
    for key, kind in (("min_score", "int"), ("ynab_budget_id", "str"), ("accounts", "list")):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _config_file(tmpdir, f'{{"{key}": null}}')
            _expect_exit(
                lambda path=path: load_local_config(path),
                f"'{key}' in config.json has the wrong type (expected {kind})",
            )


def test_min_score_out_of_range_exits() -> None:
    """Scores are clamped to 1-10, so 0 would import everything and 11 nothing."""
    for value in (0, -3, 11, 42):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _config_file(tmpdir, f'{{"min_score": {value}}}')
            _expect_exit(
                lambda path=path: load_local_config(path),
                f"'min_score' in config.json must be between 1 and 10 (got {value})",
            )


def test_min_score_range_edges_load() -> None:
    for value in (1, 10):
        with tempfile.TemporaryDirectory() as tmpdir:
            data = load_local_config(_config_file(tmpdir, f'{{"min_score": {value}}}'))
        assert data["min_score"] == value, data


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"PASS {name}")
    print("\nAll tests passed.")
