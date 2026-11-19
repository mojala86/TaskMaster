"""Run a CPU-bound function in its own subprocess, with progress reporting and hard cancellation.

Why a process per job and not a ProcessPoolExecutor: a pool can't stop a task that is already
running. Cancellation and lost leases (ADR 0004) must actually stop the work, and with a dedicated
process that is just terminate()/kill(). The cost is a process start per CPU job (spawn: roughly
0.1-0.3s), which is negligible next to work that justifies a process in the first place.
"""

import asyncio
import multiprocessing
import traceback
from collections.abc import Callable
from multiprocessing.connection import Connection
from typing import Any

import structlog

from taskmaster.errors import PermanentError

log = structlog.get_logger("taskmaster.processing")

Progress = Callable[[float, str | None], None]


class SubprocessError(RuntimeError):
    """The function raised in the subprocess, or the subprocess died (e.g. OOM-killed)."""


def _child_main(conn: Connection, fn: Callable[..., Any], args: tuple[Any, ...]) -> None:
    def progress(fraction: float, message: str | None = None) -> None:
        conn.send(("progress", float(fraction), message))

    try:
        result = fn(*args, progress)
    except BaseException as exc:
        conn.send(
            (
                "error",
                type(exc).__name__,
                str(exc),
                traceback.format_exc(),
                isinstance(exc, PermanentError),
            )
        )
    else:
        conn.send(("result", result))
    finally:
        conn.close()


async def run_in_subprocess(
    fn: Callable[..., Any],
    *args: Any,
    on_progress: Progress | None = None,
    poll_interval: float = 0.1,
    kill_timeout: float = 2.0,
) -> Any:
    """Run `fn(*args, progress)` in a fresh spawned process and return its result.

    `fn` must be a module-level function and its arguments and result must be picklable.
    Cancelling the awaiting task terminates the process (SIGTERM, then SIGKILL).
    """
    mp = multiprocessing.get_context("spawn")
    reader, writer = mp.Pipe(duplex=False)
    process = mp.Process(
        target=_child_main, args=(writer, fn, args), daemon=True, name=f"cpu-{fn.__name__}"
    )
    process.start()
    writer.close()  # the child holds the only write end now, so EOF means it exited
    try:
        while True:
            if not await asyncio.to_thread(reader.poll, poll_interval):
                continue
            try:
                message = reader.recv()
            except EOFError:
                await asyncio.to_thread(process.join, kill_timeout)
                raise SubprocessError(
                    f"Subprocess exited without a result (exit code {process.exitcode})"
                ) from None
            kind = message[0]
            if kind == "progress":
                if on_progress is not None:
                    on_progress(message[1], message[2])
            elif kind == "result":
                return message[1]
            else:
                _, name, text, remote_traceback, permanent = message
                log.debug("Subprocess failed", error=name, traceback=remote_traceback)
                raise (PermanentError if permanent else SubprocessError)(f"{name}: {text}")
    finally:
        reader.close()
        await _stop(process, kill_timeout)


async def _stop(process: Any, kill_timeout: float) -> None:
    if process.is_alive():
        process.terminate()
        await asyncio.to_thread(process.join, kill_timeout)
        if process.is_alive():
            log.warning("Subprocess ignored SIGTERM; killing it", pid=process.pid)
            process.kill()
            await asyncio.to_thread(process.join)
    else:
        process.join()
