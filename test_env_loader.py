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
"""Unit tests for the .env loader.

.env here is a named pipe mounted by 1Password Environments. Opening a FIFO for
reading blocks until a writer attaches, so when 1Password wasn't running the
script hung at import with no output and no log file. These tests pin the
behaviors that fix depends on: a writerless pipe gives up on schedule, a written
pipe still comes back intact, and every stall exits with a message instead of
hanging.

Run with:  uv run test_env_loader.py
"""

import os
import tempfile
import threading
import time
from pathlib import Path

import fastmail2ynab
from fastmail2ynab import _read_fifo_with_timeout, load_env_or_exit


def _fifo(tmpdir: str) -> Path:
    """Create a fresh FIFO inside tmpdir and return its path."""
    path = Path(tmpdir) / "test.env"
    os.mkfifo(path)
    return path


def test_no_writer_times_out() -> None:
    """A FIFO nobody writes to returns None instead of blocking forever."""
    with tempfile.TemporaryDirectory() as tmpdir:
        path = _fifo(tmpdir)
        started = time.monotonic()
        result = _read_fifo_with_timeout(path, 1.0)
        elapsed = time.monotonic() - started

    assert result is None, result
    assert 1.0 <= elapsed < 3.0, elapsed


def test_writer_content_survives() -> None:
    """Content written to the FIFO comes back whole."""
    payload = "FASTMAIL_TOKEN=abc123\nMIN_SCORE=6\n"

    with tempfile.TemporaryDirectory() as tmpdir:
        path = _fifo(tmpdir)

        def _write() -> None:
            with path.open("w", encoding="utf-8") as stream:
                stream.write(payload)

        writer = threading.Thread(target=_write, daemon=True)
        writer.start()
        result = _read_fifo_with_timeout(path, 5.0)
        writer.join(5.0)

    assert result == payload, repr(result)


def test_slow_writer_still_read() -> None:
    """A writer that attaches late still gets read, as long as it beats the deadline."""
    payload = "YNAB_TOKEN=xyz789\n"

    with tempfile.TemporaryDirectory() as tmpdir:
        path = _fifo(tmpdir)

        def _write() -> None:
            time.sleep(0.5)
            with path.open("w", encoding="utf-8") as stream:
                stream.write(payload)

        writer = threading.Thread(target=_write, daemon=True)
        writer.start()
        result = _read_fifo_with_timeout(path, 5.0)
        writer.join(5.0)

    assert result == payload, repr(result)


def test_missing_path_raises() -> None:
    """A read error surfaces as an exception, not a silent timeout."""
    with tempfile.TemporaryDirectory() as tmpdir:
        missing = Path(tmpdir) / "nope.env"
        try:
            _read_fifo_with_timeout(missing, 1.0)
        except FileNotFoundError:
            pass
        else:
            raise AssertionError("expected FileNotFoundError")


def _expect_exit(fn, needle: str) -> None:
    """Assert fn() raises SystemExit whose message contains needle."""
    try:
        fn()
    except SystemExit as exc:
        assert needle in str(exc), str(exc)
    else:
        raise AssertionError(f"expected SystemExit mentioning {needle!r}")


def test_one_password_not_running_exits_immediately() -> None:
    """The exact failure Adam hit: pipe present, 1Password down, no writer."""
    original = fastmail2ynab._one_password_running
    fastmail2ynab._one_password_running = lambda: False
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _fifo(tmpdir)
            started = time.monotonic()
            _expect_exit(lambda: load_env_or_exit(path), "1Password isn't running")
            elapsed = time.monotonic() - started
    finally:
        fastmail2ynab._one_password_running = original

    # Must not wait out the FIFO timeout - the whole point is failing fast.
    assert elapsed < 2.0, elapsed


def test_stalled_pipe_exits_with_timeout_message() -> None:
    """1Password running but never writing (locked vault) exits, doesn't hang."""
    original_check = fastmail2ynab._one_password_running
    original_timeout = fastmail2ynab.ENV_FIFO_TIMEOUT_SECONDS
    fastmail2ynab._one_password_running = lambda: True
    fastmail2ynab.ENV_FIFO_TIMEOUT_SECONDS = 1
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _fifo(tmpdir)
            _expect_exit(lambda: load_env_or_exit(path), "Timed out after 1s")
    finally:
        fastmail2ynab._one_password_running = original_check
        fastmail2ynab.ENV_FIFO_TIMEOUT_SECONDS = original_timeout


def test_pipe_without_variables_exits() -> None:
    """A comment-only pipe (disabled destination) gets its own message."""
    original = fastmail2ynab._one_password_running
    fastmail2ynab._one_password_running = lambda: True
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _fifo(tmpdir)

            def _write() -> None:
                with path.open("w", encoding="utf-8") as stream:
                    stream.write("# 1Password Environments\n# nothing here\n")

            writer = threading.Thread(target=_write, daemon=True)
            writer.start()
            _expect_exit(lambda: load_env_or_exit(path), "contains no variables")
            writer.join(5.0)
    finally:
        fastmail2ynab._one_password_running = original


def test_populated_pipe_sets_environment() -> None:
    """The happy path: variables from the pipe land in os.environ."""
    key = "FASTMAIL2YNAB_TEST_ONLY_VAR"
    os.environ.pop(key, None)
    original = fastmail2ynab._one_password_running
    fastmail2ynab._one_password_running = lambda: True
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _fifo(tmpdir)

            def _write() -> None:
                with path.open("w", encoding="utf-8") as stream:
                    stream.write(f"{key}=loaded\n")

            writer = threading.Thread(target=_write, daemon=True)
            writer.start()
            load_env_or_exit(path)
            writer.join(5.0)

        assert os.environ.get(key) == "loaded", os.environ.get(key)
    finally:
        fastmail2ynab._one_password_running = original
        os.environ.pop(key, None)


def test_regular_file_still_works() -> None:
    """A plain .env file takes the ordinary python-dotenv path."""
    key = "FASTMAIL2YNAB_TEST_REGULAR_VAR"
    os.environ.pop(key, None)
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / ".env"
            path.write_text(f"{key}=from_file\n", encoding="utf-8")
            load_env_or_exit(path)

        assert os.environ.get(key) == "from_file", os.environ.get(key)
    finally:
        os.environ.pop(key, None)


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"PASS {name}")
    print("\nAll tests passed.")
