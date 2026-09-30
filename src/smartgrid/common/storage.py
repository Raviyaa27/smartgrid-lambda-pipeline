"""
Thin wrapper over the S3 API (MinIO locally, S3 in a real deployment).

Every component talks to object storage through this module, so the client
configuration -- endpoint, credentials, retries, path-style addressing --
exists in exactly one place, and a missing object is `None` rather than an
exception each caller has to decode.
"""

from __future__ import annotations

from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from smartgrid.common.config import Settings, get_settings

_NOT_FOUND = {"404", "NoSuchKey", "NotFound"}


def s3_client(settings: Settings | None = None) -> Any:
    settings = settings or get_settings()
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.minio_root_user,
        aws_secret_access_key=settings.minio_root_password,
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},  # MinIO serves buckets as paths
            retries={"max_attempts": 5, "mode": "standard"},
        ),
    )


def put_bytes(client: Any, bucket: str, key: str, body: bytes, content_type: str) -> None:
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)


def get_bytes(client: Any, bucket: str, key: str) -> bytes | None:
    """The object's content, or None if it does not exist."""
    try:
        return client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in _NOT_FOUND:
            return None
        raise


def exists(client: Any, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in _NOT_FOUND:
            return False
        raise


def list_child_prefixes(client: Any, bucket: str, prefix: str) -> list[str]:
    """Immediate 'sub-folders' of `prefix` (which should end with '/')."""
    children: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        children.extend(p["Prefix"] for p in page.get("CommonPrefixes", []))
    return children


def list_keys(client: Any, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return keys


def delete_keys(client: Any, bucket: str, keys: list[str]) -> None:
    for start in range(0, len(keys), 1000):
        batch = keys[start : start + 1000]
        if batch:
            client.delete_objects(
                Bucket=bucket, Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True}
            )
