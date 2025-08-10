# FILE: src/textsummarizer/dbhandler/s3_handler.py
"""AWS S3 handler implementation for file, directory, and DataFrame operations.

This module provides a concrete implementation of the ``DBHandler`` interface
for Amazon S3. It supports:
- Uploading/downloading single files and whole directories.
- Streaming DataFrames, YAML, numpy arrays, and arbitrary Python objects to S3.
- Loading the same objects back from S3.
- Extracting a ZIP (downloaded from S3) directly in memory and re-uploading the
  extracted files to a destination S3 prefix.
- Streaming the bytes from an arbitrary URL directly to S3.

Design intent:
- Keep the rest of the pipeline backend-agnostic by exposing a consistent
  interface (context-managed, explicit close, uniform exceptions).
- Avoid large temporary files by preferring in-memory streams where possible.
- Provide production-grade logging around all I/O boundaries.
- Wrap all failures in the project-specific ``TextSummarizerError`` with
  contextual logging just before raising.

Notes:
- This class intentionally implements ``load_from_source`` from the base
  interface as "unsupported" because S3 typically requires a *key* or *URI*
  to load from. Use the more specific helpers (e.g., ``load_csv``,
  ``download_file``, ``download_dir``).
"""

from pathlib import Path
from io import BytesIO, StringIO
import os
import zipfile

import requests
import boto3
from botocore.exceptions import ClientError
import joblib
import numpy as np
import pandas as pd
import yaml

from src.textsummarizer.dbhandler.base_handler import DBHandler
from src.textsummarizer.entity.config_entity import S3HandlerConfig
from src.textsummarizer.exception.exception import TextSummarizerError
from src.textsummarizer.logging import logger


class S3Handler(DBHandler):
    """AWS S3 handler for file, directory, and DataFrame/object operations.

    This class stays intentionally *thin*: it provides small, composable S3
    primitives. Higher-level pipeline pieces decide *when* and *why* to call
    them. Every public method:
      - logs what it is about to do,
      - performs the smallest useful operation,
      - logs success/failure,
      - raises ``TextSummarizerError`` with the original exception chained.
    """

    def __init__(self, config: S3HandlerConfig) -> None:
        """Create an S3 client using the provided configuration.

        Args:
            config (S3HandlerConfig): S3 bucket and region configuration.

        Returns:
            None

        Raises:
            TextSummarizerError: If client initialization fails.
        """
        try:
            # Hold the config so all methods consistently use the same bucket/
            # region without passing them around.
            self.s3_config = config

            # Create a low-level S3 client. We prefer the client (not resource)
            # for explicit operations and clear error surfaces.
            self._client = boto3.client(
                "s3",
                region_name=self.s3_config.aws_region,
            )

            logger.info(
                "S3Handler initialized for bucket '%s' (region=%s).",
                self.s3_config.bucket_name,
                self.s3_config.aws_region,
            )
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to initialize S3 client.")
            raise TextSummarizerError(e, logger) from e
        return None

    # ------------------------------------------------------------------ #
    # Context manager protocol
    # ------------------------------------------------------------------ #
    def __enter__(self) -> "S3Handler":
        """Recreate client on entry if needed; return self for use-as.

        Returns:
            S3Handler: The handler instance, enabling ``with S3Handler(...)``.
        """
        # Some callers explicitly call close() between usages to release
        # resources. If so, we rebuild the client for the new context.
        if self._client is None:
            self._client = boto3.client(
                "s3", region_name=self.s3_config.aws_region
            )
            logger.info("S3Handler client re-initialized on context enter.")
        logger.info("S3Handler context entered.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Ensure resources are released on context exit.

        Returns:
            None
        """
        # Always release resources to avoid long-lived connections in
        # worker processes.
        self.close()
        logger.info("S3Handler context exited.")
        return None

    def close(self) -> None:
        """Release references that hold onto network resources.

        Returns:
            None
        """
        try:
            # boto3 clients do not expose an explicit .close(). Clearing our
            # reference is sufficient; the next __enter__ will recreate it.
            self._client = None
            logger.info("S3Handler resources released.")
        except Exception as e:  # noqa: BLE001
            # Close is best-effort. We do not re-raise here to avoid masking
            # any exception from the caller's context block.
            logger.error("Error while closing S3Handler: %s", str(e))
        return None

    # ------------------------------------------------------------------ #
    # Required interface
    # ------------------------------------------------------------------ #
    def load_from_source(self) -> pd.DataFrame:
        """Unsupported generic load for S3 handlers.

        Returns:
            pd.DataFrame: This method does not return; it raises.

        Raises:
            TextSummarizerError: Always raised to signal unsupported usage.
        """
        try:
            # We *require* callers to use a specific method for clarity, e.g.
            # load_csv(s3_uri), download_file(...), etc.
            raise RuntimeError(
                "S3Handler.load_from_source() is unsupported — use specific "
                "helpers like load_csv(s3_uri) or download_file(...)."
            )
        except Exception as e:  # noqa: BLE001
            logger.error("S3 generic load_from_source() is not supported.")
            raise TextSummarizerError(e, logger) from e

    # ------------------------------------------------------------------ #
    # Upload helpers
    # ------------------------------------------------------------------ #
    def upload_file(self, local_path: Path, s3_key: str) -> str:
        """Upload a single local file to the configured bucket.

        Args:
            local_path (Path): Path to the local file to upload.
            s3_key (str): Key under which to store the file in S3.

        Returns:
            str: s3:// URI of the uploaded object.

        Raises:
            TextSummarizerError: If upload fails.
        """
        try:
            # Validate the local file exists to fail fast with a clear message
            # instead of letting boto3 raise a less-informative error.
            if not local_path.is_file():
                raise FileNotFoundError(
                    f"Local file not found: {local_path.as_posix()}"
                )

            # Managed upload handles multipart automatically for large files.
            self._client.upload_file(
                Filename=str(local_path),
                Bucket=self.s3_config.bucket_name,
                Key=s3_key,
            )

            # Construct a canonical s3:// URI for calling layers and logs.
            s3_uri = f"s3://{self.s3_config.bucket_name}/{s3_key}"
            logger.info("Uploaded: %s -> %s", local_path.as_posix(), s3_uri)
            return s3_uri
        except ClientError as e:
            # ClientError indicates AWS-side issues (permissions/policies/etc.).
            logger.error("AWS ClientError during file upload: %s", str(e))
            raise TextSummarizerError(e, logger) from e
        except Exception as e:  # noqa: BLE001
            # Any other unexpected failure is still wrapped for consistency.
            logger.error("Unexpected error during file upload: %s", str(e))
            raise TextSummarizerError(e, logger) from e

    def sync_directory(self, local_dir: Path, s3_prefix: str) -> None:
        """Recursively upload an entire directory tree to S3.

        Args:
            local_dir (Path): Local directory to sync.
            s3_prefix (str): Prefix in S3 under which files are placed.

        Returns:
            None

        Raises:
            TextSummarizerError: If any step fails.
        """
        try:
            # Validate input directory. We fail early with a clear message.
            if not local_dir.is_dir():
                raise NotADirectoryError(
                    f"Local directory not found: {local_dir.as_posix()}"
                )

            logger.info(
                "Starting directory sync: %s -> s3://%s/%s",
                local_dir.as_posix(),
                self.s3_config.bucket_name,
                s3_prefix,
            )

            # Walk the tree and mirror its structure under the prefix. We avoid
            # clever concurrency; correctness and traceable logs are preferred.
            for root, _, files in os.walk(local_dir):
                for file in files:
                    local_file_path = Path(root) / file
                    # Compute path relative to local_dir to preserve structure.
                    relative_path = local_file_path.relative_to(local_dir)
                    remote_key = f"{s3_prefix}/{relative_path.as_posix()}"
                    self.upload_file(local_file_path, remote_key)

            logger.info(
                "Directory successfully synced: %s -> s3://%s/%s",
                local_dir.as_posix(),
                self.s3_config.bucket_name,
                s3_prefix,
            )
        except Exception as e:  # noqa: BLE001
            logger.error("Directory sync to S3 failed.")
            raise TextSummarizerError(e, logger) from e
        return None

    def upload_dir(self, local_dir: Path, s3_prefix: str) -> str:
        """Upload a directory and return the prefix URI.

        Args:
            local_dir (Path): Directory to upload.
            s3_prefix (str): Destination prefix in S3.

        Returns:
            str: s3:// URI of the uploaded prefix.

        Raises:
            TextSummarizerError: If upload fails.
        """
        # Delegate to sync_directory for the heavy lifting to keep behavior
        # centralized. This method is a convenience wrapper with a return URI.
        self.sync_directory(local_dir, s3_prefix)
        return f"s3://{self.s3_config.bucket_name}/{s3_prefix}"

    # ------------------------------------------------------------------ #
    # Download helpers
    # ------------------------------------------------------------------ #
    def download_file(self, s3_path: str, local_path: Path) -> Path:
        """Download a single S3 object to a local file path.

        ``s3_path`` can be a full s3://bucket/key or a key relative to the
        configured bucket.

        Args:
            s3_path (str): S3 URI or object key.
            local_path (Path): Local file path to write.

        Returns:
            Path: The local path that was written.

        Raises:
            TextSummarizerError: If download fails.
        """
        try:
            # Normalize into (bucket, key) regardless of input format, so
            # callers can use either "s3://bucket/key" or just "key".
            if s3_path.startswith("s3://"):
                bucket, key = self._parse_s3_uri(s3_path)
            else:
                bucket, key = self.s3_config.bucket_name, s3_path

            # Ensure the destination folder exists. This avoids opaque errors
            # from underlying libraries when parent dirs are missing.
            local_path = Path(local_path)
            local_path.parent.mkdir(parents=True, exist_ok=True)

            logger.info(
                "Downloading S3 object %s/%s -> %s",
                bucket,
                key,
                local_path.as_posix(),
            )
            self._client.download_file(bucket, key, str(local_path))
            return local_path
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to download file from S3.")
            raise TextSummarizerError(e, logger) from e

    def download_dir(self, s3_path: str, local_dir: Path) -> None:
        """Download all objects under an S3 prefix into a local directory.

        Directory structure is preserved relative to the provided prefix.

        Args:
            s3_path (str): s3://bucket/prefix or a prefix within the configured
                bucket.
            local_dir (Path): Destination directory.

        Returns:
            None

        Raises:
            TextSummarizerError: If listing or downloading fails.
        """
        try:
            # Normalize inputs for consistent behavior and logs.
            if s3_path.startswith("s3://"):
                bucket, prefix = self._parse_s3_uri(s3_path)
            else:
                bucket, prefix = self.s3_config.bucket_name, s3_path

            # Create the destination directory tree if needed.
            local_dir = Path(local_dir)
            local_dir.mkdir(parents=True, exist_ok=True)

            # Use a paginator to handle large keyspaces without loading all
            # keys into memory at once.
            paginator = self._client.get_paginator("list_objects_v2")
            logger.info("Listing objects for download: s3://%s/%s", bucket, prefix)

            found_any = False
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                contents = page.get("Contents", [])
                if not contents:
                    continue
                found_any = True

                # For each object, strip the prefix so we can recreate the same
                # relative structure locally under local_dir.
                for obj in contents:
                    key = obj["Key"]
                    relative = Path(key[len(prefix) :].lstrip("/"))
                    target = local_dir / relative
                    target.parent.mkdir(parents=True, exist_ok=True)

                    logger.info(
                        "Downloading S3 object %s/%s -> %s",
                        bucket,
                        key,
                        target.as_posix(),
                    )
                    self._client.download_file(bucket, key, str(target))

            # If the prefix is empty, we warn instead of erroring; an empty
            # prefix is a valid state and often intentional.
            if not found_any:
                logger.warning(
                    "No objects found at s3://%s/%s to download.", bucket, prefix
                )
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to download directory from S3.")
            raise TextSummarizerError(e, logger) from e
        return None

    # ------------------------------------------------------------------ #
    # DataFrame / object utilities
    # ------------------------------------------------------------------ #
    def load_csv(self, s3_uri: str) -> pd.DataFrame:
        """Load a CSV from S3 (s3://bucket/key) directly into a DataFrame.

        Args:
            s3_uri (str): Full S3 URI to a CSV object.

        Returns:
            pd.DataFrame: The loaded DataFrame.

        Raises:
            TextSummarizerError: If loading fails.
        """
        try:
            # Parse the URI and stream the object body directly into pandas.
            bucket, key = self._parse_s3_uri(s3_uri)
            obj = self._client.get_object(Bucket=bucket, Key=key)
            return pd.read_csv(obj["Body"])
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to load CSV from S3.")
            raise TextSummarizerError(e, logger) from e

    def stream_csv(self, df: pd.DataFrame, s3_key: str) -> str:
        """Write a DataFrame to S3 as CSV via a memory buffer.

        Args:
            df (pd.DataFrame): Data to write.
            s3_key (str): Destination key in the configured bucket.

        Returns:
            str: s3:// URI where the CSV was written.

        Raises:
            TextSummarizerError: If writing fails.
        """
        try:
            # Use a text buffer since DataFrame.to_csv emits text. We then
            # encode to UTF-8 bytes for put_object.
            buf = StringIO()
            df.to_csv(buf, index=False)
            buf.seek(0)

            self._client.put_object(
                Bucket=self.s3_config.bucket_name,
                Key=s3_key,
                Body=buf.getvalue().encode("utf-8"),
            )
            s3_uri = f"s3://{self.s3_config.bucket_name}/{s3_key}"
            logger.info("Streamed CSV to: %s", s3_uri)
            return s3_uri
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to stream CSV to S3.")
            raise TextSummarizerError(e, logger) from e

    def stream_yaml(self, data: dict, s3_key: str) -> str:
        """Serialize a mapping to YAML and upload to S3 via a memory buffer.

        Args:
            data (dict): Arbitrary (JSON-like) mapping to serialize.
            s3_key (str): Destination key in the configured bucket.

        Returns:
            str: s3:// URI where the YAML was written.

        Raises:
            TextSummarizerError: If serialization or upload fails.
        """

        def _convert(obj):
            """Convert numpy scalars/containers into pure Python recursively.

            Why:
                ``yaml.safe_dump`` cannot handle numpy scalar types. We walk
                the structure and replace numpy scalars (e.g., np.int64) with
                their native Python counterparts to ensure safe dumping.

            Args:
                obj (object): Arbitrary nested structure.

            Returns:
                object: Same structure, but with native Python types only.
            """
            if isinstance(obj, dict):
                # Rebuild dict to ensure recursive conversion on values.
                return {k: _convert(v) for k, v in obj.items()}
            if isinstance(obj, list):
                # Convert each list element.
                return [_convert(v) for v in obj]
            if isinstance(obj, tuple):
                # Preserve tuple type while converting elements.
                return tuple(_convert(v) for v in obj)
            if isinstance(obj, np.generic):
                # Extract native scalar from numpy scalar wrapper.
                return obj.item()
            # Base case: type already safe for YAML.
            return obj

        try:
            # Ensure YAML sees a clean, pure-Python structure.
            python_data = _convert(data)

            # Dump YAML into a text buffer, then encode to bytes for S3.
            buf = StringIO()
            yaml.safe_dump(python_data, buf)
            buf.seek(0)

            self._client.put_object(
                Bucket=self.s3_config.bucket_name,
                Key=s3_key,
                Body=buf.getvalue().encode("utf-8"),
            )
            s3_uri = f"s3://{self.s3_config.bucket_name}/{s3_key}"
            logger.info("Streamed YAML to: %s", s3_uri)
            return s3_uri
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to stream YAML to S3.")
            raise TextSummarizerError(e, logger) from e

    def stream_object(self, obj: object, s3_key: str) -> str:
        """Serialize an arbitrary Python object with joblib and upload to S3.

        Args:
            obj (object): Object to serialize.
            s3_key (str): Destination key.

        Returns:
            str: s3:// URI of the uploaded object.

        Raises:
            TextSummarizerError: If serialization or upload fails.
        """
        try:
            # Serialize entirely in memory to avoid disk I/O and to keep this
            # helper safe for serverless/ephemeral environments.
            buf = BytesIO()
            joblib.dump(obj, buf)
            buf.seek(0)

            self._client.put_object(
                Bucket=self.s3_config.bucket_name,
                Key=s3_key,
                Body=buf.read(),
            )
            uri = f"s3://{self.s3_config.bucket_name}/{s3_key}"
            logger.info("Streamed object to: %s", uri)
            return uri
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to stream object to S3.")
            raise TextSummarizerError(e, logger) from e

    def stream_npy(self, array: np.ndarray, s3_key: str) -> str:
        """Save a numpy array (.npy) into S3 via an in-memory buffer.

        Args:
            array (np.ndarray): Array to serialize.
            s3_key (str): Destination key.

        Returns:
            str: s3:// URI of the uploaded .npy file.

        Raises:
            TextSummarizerError: If serialization or upload fails.
        """
        try:
            # Use allow_pickle=False for safety (avoid arbitrary code loading).
            buf = BytesIO()
            np.save(buf, array, allow_pickle=False)
            buf.seek(0)

            self._client.put_object(
                Bucket=self.s3_config.bucket_name,
                Key=s3_key,
                Body=buf.read(),
            )
            uri = f"s3://{self.s3_config.bucket_name}/{s3_key}"
            logger.info("Streamed .npy to: %s", uri)
            return uri
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to stream .npy to S3.")
            raise TextSummarizerError(e, logger) from e

    def load_npy(self, s3_uri: str) -> np.ndarray:
        """Load a numpy array (.npy) from S3 into memory.

        Args:
            s3_uri (str): Full s3://bucket/key to a .npy object.

        Returns:
            np.ndarray: Loaded numpy array.

        Raises:
            TextSummarizerError: If retrieval or deserialization fails.
        """
        try:
            # Pull object bytes and construct an in-memory buffer for np.load.
            bucket, key = self._parse_s3_uri(s3_uri)
            resp = self._client.get_object(Bucket=bucket, Key=key)
            data = resp["Body"].read()

            buf = BytesIO(data)
            buf.seek(0)

            arr = np.load(buf, allow_pickle=False)
            logger.info("Loaded .npy from S3: %s", s3_uri)
            return arr
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to load .npy from S3.")
            raise TextSummarizerError(e, logger) from e

    def load_object(self, s3_uri: str) -> object:
        """Load a joblib-serialized object from S3.

        Args:
            s3_uri (str): Full s3://bucket/key to the joblib object.

        Returns:
            object: Deserialized Python object.

        Raises:
            TextSummarizerError: If retrieval or deserialization fails.
        """
        try:
            # Stream into memory and let joblib deserialize from the buffer.
            bucket, key = self._parse_s3_uri(s3_uri)
            resp = self._client.get_object(Bucket=bucket, Key=key)
            data = resp["Body"].read()

            buf = BytesIO(data)
            buf.seek(0)

            obj = joblib.load(buf)
            logger.info("Loaded object from S3: %s", s3_uri)
            return obj
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to load object from S3: %s", s3_uri)
            raise TextSummarizerError(e, logger) from e

    def stream_df_as_csv(self, df: pd.DataFrame, s3_key: str) -> str:
        """Stream a DataFrame to S3 as CSV using a bytes buffer.

        Args:
            df (pd.DataFrame): Frame to serialize as CSV.
            s3_key (str): Destination key in the bucket.

        Returns:
            str: s3:// URI to the written CSV.

        Raises:
            TextSummarizerError: If serialization or upload fails.
        """
        try:
            # Use a bytes buffer to avoid text encoding surprises; pandas will
            # emit UTF-8 by default when writing CSV.
            buf = BytesIO()
            df.to_csv(buf, index=False)
            buf.seek(0)

            self._client.put_object(
                Bucket=self.s3_config.bucket_name,
                Key=s3_key,
                Body=buf.read(),
                ContentType="text/csv",
            )
            uri = f"s3://{self.s3_config.bucket_name}/{s3_key}"
            logger.info("Streamed DataFrame as CSV to: %s", uri)
            return uri
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to stream DataFrame as CSV to S3.")
            raise TextSummarizerError(e, logger) from e

    # ------------------------------------------------------------------ #
    # Higher-level workflows
    # ------------------------------------------------------------------ #
    def extract_and_stream_zip_to_s3(
        self, source_zip_s3_key: str, destination_s3_key: str
    ) -> str:
        """Download a ZIP from S3, extract in memory, upload contents to S3.

        Args:
            source_zip_s3_key (str): Source ZIP key in the configured bucket.
            destination_s3_key (str): Destination prefix to receive extracted
                files.

        Returns:
            str: s3:// URI to the destination prefix.

        Raises:
            TextSummarizerError: If download, extraction, or upload fails.
        """
        try:
            logger.info("Downloading ZIP from S3: %s", source_zip_s3_key)
            s3_obj = self._client.get_object(
                Bucket=self.s3_config.bucket_name,
                Key=source_zip_s3_key,
            )
            zip_bytes = s3_obj["Body"].read()

            # Minimal sanity check: ZIP files begin with "PK". This catches
            # common cases where the wrong asset was uploaded.
            if not zip_bytes.startswith(b"PK"):
                logger.error(
                    "S3 object is not a valid ZIP file (missing ZIP signature)."
                )
                raise TextSummarizerError(
                    "S3 object is not a valid ZIP file.", logger
                )

            # Extract entirely in memory; S3 is object storage, so we "upload"
            # extracted leaves individually rather than creating directories.
            with zipfile.ZipFile(BytesIO(zip_bytes)) as zip_ref:
                for fileinfo in zip_ref.infolist():
                    if fileinfo.is_dir():
                        # 'Directories' in ZIPs do not map to S3 objects.
                        continue
                    file_data = zip_ref.read(fileinfo.filename)
                    target_key = f"{destination_s3_key}/{fileinfo.filename}"

                    logger.info("Uploading extracted file to S3: %s", target_key)
                    self._client.put_object(
                        Bucket=self.s3_config.bucket_name,
                        Key=target_key,
                        Body=file_data,
                    )

            s3_uri = f"s3://{self.s3_config.bucket_name}/{destination_s3_key}"
            logger.info("Extraction and streaming to S3 complete: %s", s3_uri)
            return s3_uri
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to extract and stream ZIP to S3.")
            raise TextSummarizerError(e, logger) from e

    def stream_url_to_s3(
        self, url: str, s3_key: str, validate_zip: bool = True
    ) -> str:
        """Stream the bytes from a remote URL directly into S3.

        Optionally validate ZIP signature if the destination key ends with
        ".zip".

        Args:
            url (str): Source URL to fetch bytes from.
            s3_key (str): Destination key to write in S3.
            validate_zip (bool): Whether to validate a ZIP signature.

        Returns:
            str: s3:// URI of the uploaded object.

        Raises:
            TextSummarizerError: If the download or upload fails.
        """
        try:
            logger.info("Downloading content from URL for S3: %s", url)

            # We request streaming to support large files; for simplicity we
            # still read into memory once. If extremely large files are common,
            # switch to chunked reads + multipart upload.
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status()
            file_content = response.content

            # Optional header validation avoids uploading a wrong file type
            # when the caller expects a ZIP pointed by s3_key suffix.
            if validate_zip and s3_key.endswith(".zip"):
                if not file_content.startswith(b"PK"):
                    logger.error(
                        "Downloaded file does not have a valid ZIP signature."
                    )
                    raise TextSummarizerError(
                        "Downloaded file is not a valid ZIP archive.", logger
                    )

            # Single PUT writes the object; for huge files, consider multipart.
            self._client.put_object(
                Bucket=self.s3_config.bucket_name,
                Key=s3_key,
                Body=file_content,
            )
            s3_uri = f"s3://{self.s3_config.bucket_name}/{s3_key}"
            logger.info("Successfully streamed URL to S3: %s", s3_uri)
            return s3_uri
        except Exception as e:  # noqa: BLE001
            logger.error("Error streaming URL to S3.")
            raise TextSummarizerError(e, logger) from e

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _parse_s3_uri(self, s3_uri: str) -> tuple[str, str]:
        """Parse an S3 URI (s3://bucket/key) into (bucket, key).

        Why:
            Many helpers accept either a full s3:// URI or just a key. Central
            parsing keeps behavior consistent and concentrates validation in
            one place.

        Args:
            s3_uri (str): Full S3 URI.

        Returns:
            tuple[str, str]: (bucket, key)

        Raises:
            TextSummarizerError: If the URI is malformed.
        """
        try:
            # Enforce the correct scheme to avoid ambiguous parsing of strings
            # that merely *contain* "s3://".
            if not s3_uri.startswith("s3://"):
                raise ValueError(f"Invalid S3 URI: {s3_uri}")

            # Remove the scheme and split once: 'bucket/key...'
            parts = s3_uri[5:].split("/", 1)

            # A valid URI must provide both a bucket and a non-empty key.
            if len(parts) != 2 or not parts[0] or not parts[1]:
                raise ValueError(f"Invalid S3 URI: {s3_uri}")

            return parts[0], parts[1]
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to parse S3 URI: %s", s3_uri)
            raise TextSummarizerError(e, logger) from e
