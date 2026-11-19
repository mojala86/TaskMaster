"""Request body size limit, as plain ASGI middleware so it sees the body before anything parses it.

A declared Content-Length over the limit is refused up front; a chunked body (no length) is
counted as it streams and refused as soon as it crosses the limit, so a huge body is never
buffered in memory.
"""

from typing import Any

from fastapi import HTTPException
from starlette.responses import PlainTextResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send


class BodyTooLarge(HTTPException):
    """An HTTPException so FastAPI's body parsing passes it through as a 413 (not a 400)."""

    def __init__(self, max_bytes: int) -> None:
        super().__init__(413, f"Request body larger than {max_bytes} bytes")


class BodySizeLimit:
    def __init__(self, app: ASGIApp, max_bytes: int) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        declared = dict(scope["headers"]).get(b"content-length")
        if declared is not None and declared.isdigit() and int(declared) > self.max_bytes:
            await self._refuse(scope, receive, send)
            return

        received = 0
        response_started = False

        async def counting_receive() -> Message:
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_bytes:
                    raise BodyTooLarge(self.max_bytes)
            return message

        async def tracking_send(message: Message) -> None:
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
            await send(message)

        try:
            await self.app(scope, counting_receive, tracking_send)
        except BodyTooLarge:
            if response_started:
                raise
            await self._refuse(scope, receive, send)

    async def _refuse(self, scope: Scope, receive: Receive, send: Send) -> None:
        response: Any = PlainTextResponse(
            f"Request body larger than {self.max_bytes} bytes", status_code=413
        )
        await response(scope, receive, send)
