# Storage API

Filesystem abstraction that works transparently with both local paths and S3 URIs. All pipeline code uses these functions instead of `pathlib`, `shutil`, or `pl.read_parquet` directly.

```python
from openmedallion.storage import (
    is_s3, storage_opts,
    join, mkdir, exists, ls_parquets, copy,
    read_parquet, write_parquet, write_csv,
)
```

!!! tip "Use in UDFs"
    Always use `openmedallion.storage` functions inside UDFs rather than `Path(silver_dir) / "file.parquet"` or `pl.read_parquet(path)` directly. This is what makes UDFs portable between local development and S3 production.

---

## S3 Configuration

S3 credentials are read from environment variables at call time:

| Variable | Description | LocalStack value |
| --- | --- | --- |
| `AWS_ACCESS_KEY_ID` | S3 access key | `test` |
| `AWS_SECRET_ACCESS_KEY` | S3 secret key | `test` |
| `AWS_DEFAULT_REGION` | AWS region | `us-east-1` |
| `AWS_ENDPOINT_URL` | Custom endpoint (set for LocalStack) | `http://localhost:4566` |

When `AWS_ENDPOINT_URL` is **not set**, the module uses boto3/s3fs defaults which resolve to real AWS S3 (IAM roles, `~/.aws/credentials`, etc.).

---

## Path Functions

### `is_s3`

```python
def is_s3(path: str | Path) -> bool
```

Return `True` if `path` is an S3 URI (starts with `s3://`).

```python
is_s3("s3://my-bucket/data/bronze")  # True
is_s3("./data/bronze")               # False
```

---

### `join`

```python
def join(base: str | Path, *parts: str) -> str
```

Join path segments, returning a string that works for both local paths and S3 URIs.

| Backend | Implementation |
| --- | --- |
| Local | `os.path.join(base, *parts)` |
| S3 | `base.rstrip("/") + "/" + "/".join(parts)` |

**Parameters:**

| Name | Type | Description |
| --- | --- | --- |
| `base` | `str \| Path` | Base directory or S3 URI. |
| `*parts` | `str` | Additional path segments. |

**Returns:** `str`

```python
from openmedallion.storage import join

join("./data/silver", "orders.parquet")
# "./data/silver/orders.parquet"

join("s3://my-bucket/data", "silver", "orders.parquet")
# "s3://my-bucket/data/silver/orders.parquet"
```

---

### `mkdir`

```python
def mkdir(path: str | Path) -> None
```

Create a directory. No-op for S3 paths (S3 has no real directories).

```python
mkdir("./data/silver")            # creates ./data/silver/ (and parents)
mkdir("s3://bucket/data/silver")  # no-op
```

---

### `exists`

```python
def exists(path: str | Path) -> bool
```

Return `True` if the path exists — local file or S3 object/prefix.

```python
exists("./data/silver/orders.parquet")   # True if file exists locally
exists("s3://bucket/silver/orders.parquet")  # True if S3 object exists
```

!!! note
    For S3, this uses `s3fs.S3FileSystem().exists()`. Requires the `openmedallion[s3]` extra.

---

### `ls_parquets`

```python
def ls_parquets(directory: str | Path) -> list[str]
```

List all `.parquet` files directly under `directory` (non-recursive).

**Returns:** `list[str]` — sorted list of absolute/URI paths.

```python
ls_parquets("./data/bronze/bronze/orders/")
# ["./data/bronze/bronze/orders/0001.parquet", "./data/bronze/bronze/orders/0002.parquet"]

ls_parquets("s3://bucket/bronze/orders/")
# ["s3://bucket/bronze/orders/0001.parquet", ...]
```

---

### `copy`

```python
def copy(src: str | Path, dst: str | Path) -> None
```

Copy a file from `src` to `dst`, handling all combinations of local and S3.

| src → dst | Implementation |
| --- | --- |
| local → local | `shutil.copy2` |
| local → S3 | `s3fs.put(src, dst)` |
| S3 → local | `s3fs.get(src, dst)` |
| S3 → S3 | `boto3.copy_object` (server-side, no download) |

```python
copy("./data/bronze/ORDERS.parquet", "s3://bucket/bronze/ORDERS.parquet")
```

---

## Parquet / CSV Functions

### `read_parquet`

```python
def read_parquet(path: str | Path) -> pl.DataFrame
```

Read a Parquet file from a local path or S3 URI.

**Parameters:**

| Name | Type | Description |
| --- | --- | --- |
| `path` | `str \| Path` | Local path or `s3://` URI to a `.parquet` file. |

**Returns:** `pl.DataFrame`

```python
from openmedallion.storage import read_parquet, join

df = read_parquet(join(silver_dir, "orders.parquet"))
```

---

### `write_parquet`

```python
def write_parquet(df: pl.DataFrame, path: str | Path) -> None
```

Write a DataFrame as Parquet to a local path or S3 URI.

**Parameters:**

| Name | Type | Description |
| --- | --- | --- |
| `df` | `pl.DataFrame` | DataFrame to serialise. |
| `path` | `str \| Path` | Destination local path or `s3://` URI. |

```python
from openmedallion.storage import write_parquet, join

write_parquet(df, join(gold_dir, "customer_summary.parquet"))
```

---

### `write_csv`

```python
def write_csv(df: pl.DataFrame, path: str | Path) -> None
```

Write a DataFrame as CSV to a local path or S3 URI.

For S3, the CSV is serialised in-memory and uploaded via `boto3.put_object`. No temporary local file is written.

**Parameters:**

| Name | Type | Description |
| --- | --- | --- |
| `df` | `pl.DataFrame` | DataFrame to serialise. |
| `path` | `str \| Path` | Destination local path or `s3://` URI. |

```python
from openmedallion.storage import write_csv, join

write_csv(df, join(export_dir, "customer_summary.csv"))
```

---

### `storage_opts`

```python
def storage_opts() -> dict
```

Build a Polars/PyArrow `storage_options` dict from environment variables. Used internally by `read_parquet` and `write_parquet` for S3 paths.

**Returns:** `dict` — empty dict for real AWS (uses default credential chain), or a full options dict when `AWS_ENDPOINT_URL` is set (e.g. LocalStack).

```python
storage_opts()
# {} — when AWS_ENDPOINT_URL is not set (real AWS, uses IAM/~/.aws/credentials)

# When AWS_ENDPOINT_URL=http://localhost:4566:
storage_opts()
# {
#   "aws_access_key_id": "test",
#   "aws_secret_access_key": "test",
#   "region_name": "us-east-1",
#   "endpoint_url": "http://localhost:4566",
#   "aws_allow_http": "true",
# }
```

---

## S3 Quickstart

To switch an existing local pipeline to S3:

**1. Update `main.yaml`:**
```yaml
paths:
  bronze: "s3://my-bucket/data/bronze"
  silver: "s3://my-bucket/data/silver"
  gold:   "s3://my-bucket/data/gold"
  export: "s3://my-bucket/data/export"
```

**2. Set environment variables:**
```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-east-1
# AWS_ENDPOINT_URL is only needed for LocalStack
```

**3. Install the S3 extra:**
```bash
pip install "openmedallion[s3]"
```

No pipeline code changes required.
