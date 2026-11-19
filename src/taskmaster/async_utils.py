import asyncio
import contextlib


async def sleep_unless(stop: asyncio.Event, seconds: float) -> None:
    """Sleep for `seconds`, but return early once `stop` is set."""
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)
