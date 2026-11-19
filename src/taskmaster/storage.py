"""S3-compatible object storage for job input and output files.

Large files never pass through the API. Clients upload with a presigned PUT URL and download with
a presigned GET URL. Workers stream inputs straight from the store and upload outputs to it.

Two endpoints are configured because presigned URLs are signed for a specific host: services
reach the store at `s3_endpoint_url` (e.g. http://s3:9000 inside compose), while URLs handed to
clients must use `s3_public_endpoint_url` (e.g. http://localhost:9000).
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog
from aiobotocore.session import get_session
from botocore.config import Config
from botocore.exceptions import ClientError

from taskmaster.config import Settings

log = structlog.get_logger("taskmaster.storage")

_NOT_FOUND = {"404", "NoSuchKey", "NotFound", "NoSuchBucket"}


@dataclass(frozen=True)
class ObjectInfo:
    key: str
    size: int
    content_type: str | None


@dataclass(frozen=True)
class ListedObject:
    key: str
    size: int
    last_modified: datetime


DELETE_BATCH = 1000  # S3 DeleteObjects limit


class Storage:
    def __init__(self, settings: Settings) -> None:
        if not settings.s3_endpoint_url:
            raise ValueError("s3_endpoint_url is not configured")
        self.settings = settings
        self.bucket = settings.s3_bucket
        self._stack = AsyncExitStack()
        self._client: Any = None
        self._public: Any = None

    async def start(self) -> "Storage":
        session = get_session()
        self._client = await self._stack.enter_async_context(
            self._create_client(session, self.settings.s3_endpoint_url)
        )
        # Only used to sign URLs; it never connects anywhere.
        self._public = await self._stack.enter_async_context(
            self._create_client(
                session, self.settings.s3_public_endpoint_url or self.settings.s3_endpoint_url
            )
        )
        return self

    async def close(self) -> None:
        await self._stack.aclose()

    def _create_client(self, session: Any, endpoint_url: str | None) -> Any:
        return session.create_client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=self.settings.s3_region,
            aws_access_key_id=self.settings.s3_access_key,
            aws_secret_access_key=self.settings.s3_secret_key,
            verify=self.settings.s3_verify,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

    @property
    def client(self) -> Any:
        if self._client is None:
            raise RuntimeError("Storage.start() has not been called")
        return self._client

    async def ensure_bucket(self) -> None:
        try:
            await self.client.head_bucket(Bucket=self.bucket)
        except ClientError as exc:
            if _error_code(exc) not in _NOT_FOUND:
                raise
            try:
                await self.client.create_bucket(Bucket=self.bucket)
                log.info("Created bucket", bucket=self.bucket)
            except ClientError as create_exc:  # created concurrently by another process
                if _error_code(create_exc) not in {
                    "BucketAlreadyOwnedByYou",
                    "BucketAlreadyExists",
                }:
                    raise

    async def head(self, key: str) -> ObjectInfo | None:
        try:
            response = await self.client.head_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if _error_code(exc) in _NOT_FOUND:
                return None
            raise
        return ObjectInfo(key, int(response["ContentLength"]), response.get("ContentType"))

    async def list_objects(self, prefix: str) -> AsyncIterator[ListedObject]:
        paginator = self.client.get_paginator("list_objects_v2")
        async for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                yield ListedObject(obj["Key"], int(obj["Size"]), obj["LastModified"])

    async def delete_many(self, keys: list[str]) -> int:
        """Delete keys in batches. Returns how many were deleted; failures are logged."""
        deleted = 0
        for start in range(0, len(keys), DELETE_BATCH):
            batch = keys[start : start + DELETE_BATCH]
            response = await self.client.delete_objects(
                Bucket=self.bucket,
                Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
            )
            errors = response.get("Errors") or []
            for error in errors:
                log.warning(
                    "Could not delete object", key=error.get("Key"), reason=error.get("Message")
                )
            deleted += len(batch) - len(errors)
        return deleted

    async def stream(self, key: str, chunk_size: int = 1024 * 1024) -> AsyncIterator[bytes]:
        response = await self.client.get_object(Bucket=self.bucket, Key=key)
        async with response["Body"] as body:
            async for chunk in body.iter_chunks(chunk_size):
                yield chunk

    async def upload_file(self, key: str, path: Path, content_type: str) -> int:
        size = (await asyncio.to_thread(path.stat)).st_size
        f = await asyncio.to_thread(path.open, "rb")
        try:
            await self.client.put_object(
                Bucket=self.bucket, Key=key, Body=f, ContentType=content_type, ContentLength=size
            )
        finally:
            f.close()
        return size

    async def put_bytes(self, key: str, data: bytes, content_type: str) -> None:
        await self.client.put_object(
            Bucket=self.bucket, Key=key, Body=data, ContentType=content_type
        )

    async def presign_put(self, key: str, content_type: str, size: int, expires: int) -> str:
        """A PUT URL for exactly `size` bytes: Content-Length is part of the signature, so the
        store rejects an upload of any other size."""
        url: str = await self._public.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": self.bucket,
                "Key": key,
                "ContentType": content_type,
                "ContentLength": size,
            },
            ExpiresIn=expires,
        )
        return url

    async def presign_get(
        self, key: str, expires: int, *, public: bool = True, filename: str | None = None
    ) -> str:
        params: dict[str, str] = {"Bucket": self.bucket, "Key": key}
        if filename:
            params["ResponseContentDisposition"] = f'attachment; filename="{filename}"'
        client = self._public if public else self.client
        url: str = await client.generate_presigned_url(
            "get_object", Params=params, ExpiresIn=expires
        )
        return url


def _error_code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code", ""))
