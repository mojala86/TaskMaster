"""What a task handler gets besides its payload."""

import mimetypes
import uuid
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import structlog

from taskmaster.errors import PermanentError
from taskmaster.processing.runner import run_in_subprocess

if TYPE_CHECKING:
    from taskmaster.storage import ObjectInfo, Storage

# Presigned URLs handed to CPU subprocesses only need to outlive the request's start.
INTERNAL_URL_TTL = 3600


@dataclass(frozen=True)
class OutputFile:
    name: str
    key: str
    size: int
    content_type: str


class JobContext:
    """Per-run context: identity, logging, progress, and access to input/output files.

    Progress is kept in memory and written to the job row with the next heartbeat, so reporting
    it is free and handlers can call it as often as they like.
    """

    def __init__(
        self,
        *,
        job_id: uuid.UUID,
        attempt: int,
        max_attempts: int,
        log: structlog.stdlib.BoundLogger,
        storage: "Storage | None" = None,
        workdir: Path | None = None,
        output_prefix: str = "",
    ) -> None:
        self.job_id = job_id
        self.attempt = attempt
        self.max_attempts = max_attempts
        self.log = log
        self._storage = storage
        self._workdir = workdir
        self._output_prefix = output_prefix
        self.progress: float | None = None
        self.progress_message: str | None = None
        self.outputs: list[OutputFile] = []

    # --- Progress -------------------------------------------------------------------------

    def report_progress(self, fraction: float, message: str | None = None) -> None:
        self.progress = min(1.0, max(0.0, float(fraction)))
        self.progress_message = message

    # --- Files ----------------------------------------------------------------------------

    @property
    def workdir(self) -> Path:
        """Scratch directory for this run; deleted when the run ends."""
        if self._workdir is None:
            raise RuntimeError("No work directory for this run")
        return self._workdir

    @property
    def storage(self) -> "Storage":
        if self._storage is None:
            raise PermanentError("Object storage is not configured for this worker")
        return self._storage

    async def input_info(self, key: str) -> "ObjectInfo":
        info = await self.storage.head(key)
        if info is None:
            raise PermanentError(f"Input file {key!r} does not exist")
        return info

    async def stream_input(self, key: str, chunk_size: int = 1024 * 1024) -> AsyncIterator[bytes]:
        """Stream an input file in chunks; memory use is bounded by chunk_size."""
        async for chunk in self.storage.stream(key, chunk_size):
            yield chunk

    async def input_url(self, key: str) -> str:
        """Presigned GET URL on the internal endpoint, for code running in a subprocess."""
        await self.input_info(key)
        return await self.storage.presign_get(key, INTERNAL_URL_TTL, public=False)

    async def save_output(
        self, name: str, path: Path, content_type: str | None = None
    ) -> OutputFile:
        """Upload a result file. It becomes visible on the job only if this attempt succeeds."""
        content_type = content_type or mimetypes.guess_type(name)[0] or "application/octet-stream"
        key = f"{self._output_prefix}{name}"
        size = await self.storage.upload_file(key, path, content_type)
        output = OutputFile(name, key, size, content_type)
        self.outputs = [o for o in self.outputs if o.name != name] + [output]
        return output

    # --- CPU-bound work -------------------------------------------------------------------

    async def run_cpu(self, fn: Callable[..., Any], *args: Any) -> Any:
        """Run `fn(*args, progress)` in a dedicated subprocess (see processing.runner).

        Progress reported by `fn` is forwarded to this job; cancelling the job kills the process.
        """
        return await run_in_subprocess(fn, *args, on_progress=self.report_progress)
