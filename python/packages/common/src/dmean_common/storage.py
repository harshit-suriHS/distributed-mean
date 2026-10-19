"""Thin S3 wrapper (MinIO locally, AWS S3 in production: only the endpoint changes)."""

from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from .config import CommonSettings


class Storage:
    def __init__(self, settings: CommonSettings) -> None:
        self.bucket = settings.s3_bucket
        self._s3: Any = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint,
            region_name=settings.s3_region,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            config=Config(s3={"addressing_style": "path"}, retries={"mode": "standard"}),
        )

    def get_text(self, key: str) -> str:
        body = self._s3.get_object(Bucket=self.bucket, Key=key)["Body"]
        return body.read().decode("utf-8")

    def put_text(self, key: str, text: str) -> int:
        """A single PUT is atomic: readers see the old object or the new one, never half."""
        data = text.encode("utf-8")
        self._s3.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType="text/csv")
        return len(data)

    def size_if_exists(self, key: str) -> int | None:
        try:
            return int(self._s3.head_object(Bucket=self.bucket, Key=key)["ContentLength"])
        except ClientError as err:
            if err.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                return None
            raise
