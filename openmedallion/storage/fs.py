"""storage/fs.py — filesystem abstraction for local paths and S3 URIs.

All pipeline layers (bronze, silver, gold, export) call this module instead of
using pathlib / shutil / glob directly. The module detects whether a path is an
S3 URI (``s3://...``) or a local path and routes each operation accordingly,
so the same pipeline code works against both the local filesystem and AWS S3
(or LocalStack).

Configuration
-------------
S3 credentials and endpoint are read from environment variables at call time:

  AWS_ACCESS_KEY_ID       — S3 access key (``test`` for LocalStack)
  AWS_SECRET_ACCESS_KEY   — S3 secret key (``test`` for LocalStack)
  AWS_DEFAULT_REGION      — AWS region    (``us-east-1`` for LocalStack)
  AWS_ENDPOINT_URL        — Custom S3 endpoint (set to ``http://localhost:4566``
                            for LocalStack; leave unset for real AWS)

When ``AWS_ENDPOINT_URL`` is not set the module uses boto3/s3fs defaults which
resolve to real AWS S3.
"""
from __future__ import annotations

import glob as _glob
import io
import os
import shutil
from pathlib import Path
from typing import Union

import polars as pl

PathLike = Union[str, Path]


# ---------------------------------------------------------------------------
# S3 detection helpers
# ---------------------------------------------------------------------------

def is_s3(path: PathLike) -> bool:
    """Return True if *path* is an S3 URI (starts with ``s3://``)."""
    return str(path).startswith("s3://")


def storage_opts() -> dict:
    """Build a Polars/PyArrow ``storage_options`` dict from environment variables.

    Returns an empty dict when ``AWS_ENDPOINT_URL`` is not set so that Polars
    uses its default credential chain (IAM roles, ``~/.aws/credentials``, etc.)
    for real AWS.  When the variable is set (e.g. for LocalStack) it returns a
    dict with all necessary keys including ``aws_allow_http`` so that plain
    HTTP endpoints are accepted.

    Returns:
        dict: Storage options suitable for passing to ``pl.read_parquet`` /
            ``pl.DataFrame.write_parquet`` as ``storage_options``.
    """
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if not endpoint:
        return {}
    return {
        "aws_access_key_id":     os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        "region_name":           os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        "endpoint_url":          endpoint,
        "aws_allow_http":        "true",
    }


def _s3fs():
    """Return a configured s3fs.S3FileSystem instance."""
    import s3fs  # imported lazily — only needed for S3 paths
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    kwargs: dict = {
        "key":    os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        "secret": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
    }
    if endpoint:
        kwargs["endpoint_url"] = endpoint
        kwargs["use_ssl"] = False
    return s3fs.S3FileSystem(**kwargs)


def _boto3_client():
    """Return a configured boto3 S3 client."""
    import boto3  # imported lazily
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    kwargs: dict = {
        "aws_access_key_id":     os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        "aws_secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        "region_name":           os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    }
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client("s3", **kwargs)


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def join(base: PathLike, *parts: str) -> str:
    """Join path segments, returning a string that works for both local and S3.

    Args:
        base: Base path or S3 URI.
        *parts: Additional path segments.

    Returns:
        str: Joined path string.  S3 URIs are joined with ``/``; local paths
            use ``os.path.join`` to respect the platform separator.
    """
    base = str(base)
    if is_s3(base):
        return base.rstrip("/") + "/" + "/".join(str(p).strip("/") for p in parts)
    return str(Path(base).joinpath(*parts))


# ---------------------------------------------------------------------------
# Filesystem operations
# ---------------------------------------------------------------------------

def mkdir(path: PathLike) -> None:
    """Create a directory (local) or no-op (S3, which has no real directories).

    Args:
        path: Local directory path or S3 URI.
    """
    if not is_s3(path):
        Path(path).mkdir(parents=True, exist_ok=True)


def exists(path: PathLike) -> bool:
    """Return True if *path* exists (local file or S3 object/prefix).

    Args:
        path: Local path or S3 URI.

    Returns:
        bool: True when the path exists.
    """
    path = str(path)
    if is_s3(path):
        return _s3fs().exists(path)
    return Path(path).exists()


def ls_parquets(directory: PathLike) -> list[str]:
    """List all ``.parquet`` files under *directory* (non-recursive).

    For S3 paths this performs a prefix listing one level deep.  For local
    paths it uses ``glob.glob``.

    Args:
        directory: Local directory path or S3 URI prefix.

    Returns:
        list[str]: Sorted list of absolute/URI paths to ``.parquet`` files.
    """
    directory = str(directory)
    if is_s3(directory):
        fs = _s3fs()
        prefix = directory.rstrip("/")
        try:
            entries = fs.ls(prefix, detail=False)
        except FileNotFoundError:
            return []
        return sorted(f"s3://{e}" if not e.startswith("s3://") else e
                      for e in entries if e.endswith(".parquet"))
    pattern = str(Path(directory) / "*.parquet")
    return sorted(_glob.glob(pattern))


def copy(src: PathLike, dst: PathLike) -> None:
    """Copy a file from *src* to *dst*, handling local↔local, S3↔S3, and mixed cases.

    Args:
        src: Source path or S3 URI.
        dst: Destination path or S3 URI.
    """
    src, dst = str(src), str(dst)
    src_s3, dst_s3 = is_s3(src), is_s3(dst)

    if src_s3 and dst_s3:
        src_no_scheme = src[len("s3://"):]
        dst_no_scheme = dst[len("s3://"):]
        src_bucket, src_key = src_no_scheme.split("/", 1)
        dst_bucket, dst_key = dst_no_scheme.split("/", 1)
        _boto3_client().copy_object(
            Bucket=dst_bucket,
            Key=dst_key,
            CopySource={"Bucket": src_bucket, "Key": src_key},
        )
    elif src_s3 and not dst_s3:
        fs = _s3fs()
        Path(dst).parent.mkdir(parents=True, exist_ok=True)
        fs.get(src, dst)
    elif not src_s3 and dst_s3:
        fs = _s3fs()
        fs.put(src, dst)
    else:
        shutil.copy2(src, dst)


# ---------------------------------------------------------------------------
# Parquet read / write
# ---------------------------------------------------------------------------

def read_parquet(path: PathLike) -> pl.DataFrame:
    """Read a Parquet file from a local path or S3 URI.

    Args:
        path: Local path or S3 URI to a ``.parquet`` file.

    Returns:
        pl.DataFrame: Loaded DataFrame.
    """
    path = str(path)
    if is_s3(path):
        return pl.read_parquet(path, storage_options=storage_opts())
    return pl.read_parquet(path)


def write_parquet(df: pl.DataFrame, path: PathLike) -> None:
    """Write a DataFrame as a Parquet file to a local path or S3 URI.

    Args:
        df: DataFrame to write.
        path: Destination local path or S3 URI.
    """
    path = str(path)
    if is_s3(path):
        df.write_parquet(path, storage_options=storage_opts())
    else:
        df.write_parquet(path)


def write_csv(df: pl.DataFrame, path: PathLike) -> None:
    """Write a DataFrame as CSV to a local path or S3 URI.

    For S3 destinations the CSV is serialised in-memory and uploaded via boto3.

    Args:
        df: DataFrame to write.
        path: Destination local path or S3 URI.
    """
    path = str(path)
    if is_s3(path):
        buf = io.BytesIO()
        df.write_csv(buf)
        buf.seek(0)
        no_scheme = path[len("s3://"):]
        bucket, key = no_scheme.split("/", 1)
        _boto3_client().put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    else:
        df.write_csv(path)
