"""Entry point: taskmaster api | worker | relay | cleanup | apikey"""

import argparse
import asyncio
import json
import signal
import sys
from collections.abc import Awaitable, Callable, Coroutine
from pathlib import Path
from typing import Any

import structlog
import uvicorn
from prometheus_client import start_http_server

from taskmaster.auth import create_api_key, list_api_keys, revoke_api_key
from taskmaster.config import Settings, get_settings
from taskmaster.janitor import StorageJanitor
from taskmaster.log import configure_logging
from taskmaster.relay import Relay
from taskmaster.resources import Resources, open_resources
from taskmaster.worker import Worker

log = structlog.get_logger("taskmaster")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="taskmaster")
    sub = parser.add_subparsers(dest="command", required=True)

    api = sub.add_parser("api", help="Run the HTTP API")
    api.add_argument("--host", default="0.0.0.0")
    api.add_argument("--port", type=int, default=8000)
    api.add_argument("--reload", action="store_true", help="Reload on code changes (dev)")

    worker = sub.add_parser("worker", help="Run a worker")
    worker.add_argument("--concurrency", type=int, help="Jobs run concurrently by this process")

    sub.add_parser("relay", help="Run the outbox relay")
    sub.add_parser("cleanup", help="Run the storage janitor once and print what it deleted")

    keys = sub.add_parser("apikey", help="Manage API keys").add_subparsers(
        dest="action", required=True
    )
    create = keys.add_parser("create", help="Create a key (printed once)")
    create.add_argument("--name", required=True)
    create.add_argument("--rate-limit", type=int, help="Requests per minute (default: settings)")
    keys.add_parser("list", help="List keys")
    revoke = keys.add_parser("revoke", help="Revoke a key by its prefix")
    revoke.add_argument("prefix")

    args = parser.parse_args(argv)
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_format)

    if args.command == "api":
        uvicorn.run(
            "taskmaster.api.app:create_app",
            factory=True,
            host=args.host,
            port=args.port,
            reload=args.reload,
            reload_dirs=[str(Path(__file__).parent)] if args.reload else None,
            log_config=None,
        )
    elif args.command == "worker":
        if args.concurrency:
            settings = settings.model_copy(update={"worker_concurrency": args.concurrency})
        _start_metrics_server(settings)
        _run_service(settings, _worker)
    elif args.command == "relay":
        _start_metrics_server(settings)
        _run_service(settings, _relay, storage=False)
    elif args.command == "cleanup":
        _run_service(settings, _cleanup, streams=False)
    elif args.command == "apikey":
        _run_async(_apikey(settings, args))


def _start_metrics_server(settings: Settings) -> None:
    if settings.metrics_port:
        start_http_server(settings.metrics_port)


async def _apikey(settings: Settings, args: argparse.Namespace) -> None:
    async with open_resources(settings) as r:
        if args.action == "create":
            row, key = await create_api_key(r.db, args.name, args.rate_limit)
            limit = row.rate_limit_per_minute or f"{settings.rate_limit_per_minute} (default)"
            print(f"Created API key {row.name!r} (prefix {row.prefix}, {limit} requests/min).")
            print("Store it now; it can't be shown again:")
            print(key)
        elif args.action == "list":
            for k in await list_api_keys(r.db):
                state = f"revoked {k.revoked_at:%Y-%m-%d}" if k.revoked_at else "active"
                limit = k.rate_limit_per_minute or "default"
                print(f"{k.prefix}  {k.name:<24} {limit!s:>8}/min  {state}")
        elif args.action == "revoke":
            if not await revoke_api_key(r.db, args.prefix):
                raise SystemExit(f"No active key with prefix {args.prefix!r}")
            print(
                f"Revoked {args.prefix} (API processes may accept it for up to "
                f"{settings.api_key_cache_seconds:g}s more)"
            )


async def _worker(r: Resources, stop: asyncio.Event) -> None:
    await Worker(r.db, r.require_streams(), r.settings, storage=r.storage).run(stop)


async def _relay(r: Resources, stop: asyncio.Event) -> None:
    await Relay(r.db, r.require_streams(), r.settings).run(stop)


async def _cleanup(r: Resources, stop: asyncio.Event) -> None:
    report = await StorageJanitor(r.db, r.require_storage(), r.settings).run_once()
    print(json.dumps(report.as_dict() if report else {"skipped": "another cleanup is running"}))


Service = Callable[[Resources, asyncio.Event], Awaitable[None]]


def _run_service(
    settings: Settings, service: Service, *, streams: bool = True, storage: bool = True
) -> None:
    """Run a long-lived service until SIGTERM/SIGINT, with its connections opened around it."""

    async def runner() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()

        def request_stop() -> None:
            log.info("Shutdown requested")
            stop.set()

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, request_stop)
            except NotImplementedError:  # Windows
                signal.signal(sig, lambda *_: loop.call_soon_threadsafe(request_stop))

        async with open_resources(
            settings,
            streams=streams,
            storage=storage,
            pool_size=settings.worker_concurrency + 2,
        ) as resources:
            await service(resources, stop)

    _run_async(runner())


def _run_async(coro: Coroutine[Any, Any, None]) -> None:
    # psycopg's async mode needs a selector event loop; Windows defaults to Proactor.
    loop_factory = asyncio.SelectorEventLoop if sys.platform == "win32" else None
    asyncio.run(coro, loop_factory=loop_factory)


if __name__ == "__main__":
    main()
