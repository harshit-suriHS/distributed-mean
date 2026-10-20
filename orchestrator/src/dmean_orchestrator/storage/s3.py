"""S3 / MinIO access for cleanup. boto3 is blocking, so calls run in a thread off the event loop."""

import asyncio
from typing import Any

import boto3
from botocore.config import Config

from dmean_orchestrator.settings import CleanupSettings

DELETE_BATCH = 1000  # S3 DeleteObjects limit


class ObjectStore:
    def __init__(self, settings: CleanupSettings) -> None:
        self.bucket = settings.s3_bucket
        self._s3: Any = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint,
            region_name=settings.s3_region,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            config=Config(s3={"addressing_style": "path"}, retries={"mode": "standard"}),
        )

    async def delete(self, keys: list[str]) -> None:
        """Deletes keys in batches; deleting a key that doesn't exist is not an error."""
        await asyncio.to_thread(self._delete, keys)

    async def list_prefix(self, prefix: str) -> list[str]:
        return await asyncio.to_thread(self._list, prefix)

    def _delete(self, keys: list[str]) -> None:
        for i in range(0, len(keys), DELETE_BATCH):
            batch = [{"Key": k} for k in keys[i : i + DELETE_BATCH]]
            result = self._s3.delete_objects(
                Bucket=self.bucket, Delete={"Objects": batch, "Quiet": True}
            )
            if result.get("Errors"):
                raise RuntimeError(f"failed to delete {len(result['Errors'])} objects")

    def _list(self, prefix: str) -> list[str]:
        keys: list[str] = []
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            keys += [o["Key"] for o in page.get("Contents", [])]
        return keys
