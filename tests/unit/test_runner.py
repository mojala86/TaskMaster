"""The CPU subprocess runner: results, progress, error mapping, crashes and hard cancellation.

The target functions must be module-level so the spawned child can import them.
"""

import asyncio
import multiprocessing
import os
import time
from typing import Any

import pytest

from taskmaster.errors import PermanentError
from taskmaster.processing.runner import SubprocessError, run_in_subprocess


def double_with_progress(x: int, progress: Any) -> int:
    progress(0.5, "half way")
    return x * 2


def raise_permanent(progress: Any) -> None:
    raise PermanentError("bad input")


def raise_value_error(progress: Any) -> None:
    raise ValueError("boom")


def exit_hard(progress: Any) -> None:
    os._exit(3)  # like being OOM-killed: no result, no exception


def sleep_forever(progress: Any) -> None:
    progress(0.0, "started")
    while True:
        time.sleep(0.05)


async def test_returns_result_and_forwards_progress() -> None:
    seen: list[tuple[float, str | None]] = []

    result = await run_in_subprocess(
        double_with_progress, 21, on_progress=lambda f, m: seen.append((f, m))
    )

    assert result == 42
    assert seen == [(0.5, "half way")]


async def test_permanent_error_keeps_its_type() -> None:
    with pytest.raises(PermanentError, match="bad input"):
        await run_in_subprocess(raise_permanent)


async def test_other_errors_become_subprocess_errors() -> None:
    with pytest.raises(SubprocessError, match="ValueError: boom"):
        await run_in_subprocess(raise_value_error)


async def test_crashed_subprocess_is_reported() -> None:
    with pytest.raises(SubprocessError, match="exit code 3"):
        await run_in_subprocess(exit_hard)


async def test_cancelling_terminates_the_subprocess() -> None:
    started = asyncio.Event()
    task = asyncio.create_task(
        run_in_subprocess(sleep_forever, on_progress=lambda f, m: started.set())
    )
    async with asyncio.timeout(15):
        await started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert multiprocessing.active_children() == []
