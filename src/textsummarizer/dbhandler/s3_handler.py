import os
import io
from io import BytesIO, StringIO
from pathlib import Path
import zipfile
import requests

import boto3
import joblib
import numpy as np
import pandas as pd
import yaml
from botocore.exceptions import ClientError

from src.textsummarizer.dbhandler.base_handler import DBHandler
from src.textsummarizer.entity.config_entity import S3HandlerConfig
from src.textsummarizer.exception.exception import TextSummarizerError
from src.textsummarizer.logging import logger

class S3Handler(DBHandler):
    """
    AWS S3 Handler for file, directory, and DataFrame operations.
    """

    def __init__(self, config: S3HandlerConfig) -> None:
        try:
            self.s3_config = config
            self._client = boto3.client("s3", region_name=self.s3_config.aws_region)
            logger.info(
                "S3Handler initialized for bucket '%s' in region '%s'",
                self.s3_config.bucket_name,
                self.s3_config.aws_region,
            )
        except Exception as e:
            logger.info("Failed to initialize S3 client.")
            raise TextSummarizerError(e, logger) from e

    def __enter__(self) -> "S3Handler":
        # Re-create the client if it was closed before (for robustness)
        if self._client is None:
            self._client = boto3.client("s3", region_name=self.s3_config.aws_region)
            logger.info("S3Handler client re-initialized on context enter.")
        logger.info("S3Handler context entered.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
        logger.info("S3Handler context exited.")

    def close(self) -> None:
        try:
            self._client = None
            logger.info("S3Handler resources released.")
        except Exception as e:
            logger.error("Error while closing S3Handler: %s", str(e))

    def upload_file(self, local_path: Path, s3_key: str) -> str:
        try:
            if not local_path.is_file():
                raise FileNotFoundError(f"Local file not found: {local_path.as_posix()}")
            self._client.upload_file(
                Filename=str(local_path),
                Bucket=self.s3_config.bucket_name,
                Key=s3_key,
            )
            s3_uri = f"s3://{self.s3_config.bucket_name}/{s3_key}"
            logger.info("Uploaded: %s -> %s", local_path.as_posix(), s3_uri)
            return s3_uri
        except ClientError as e:
            logger.info("AWS ClientError during file upload: %s", str(e))
            raise TextSummarizerError(e, logger) from e
        except Exception as e:
            logger.info("Unexpected error during file upload: %s", str(e))
            raise TextSummarizerError(e, logger) from e

    def sync_directory(self, local_dir: Path, s3_prefix: str) -> None:
        try:
            if not local_dir.is_dir():
                raise NotADirectoryError(f"Local directory not found: {local_dir.as_posix()}")

            logger.info(
                "Starting directory sync: %s -> s3://%s/%s",
                local_dir.as_posix(),
                self.s3_config.bucket_name,
                s3_prefix,
            )

            for root, _, files in os.walk(local_dir):
                for file in files:
                    local_file_path = Path(root) / file
                    relative_path = local_file_path.relative_to(local_dir)
                    remote_key = f"{s3_prefix}/{relative_path.as_posix()}"
                    self.upload_file(local_file_path, remote_key)

            logger.info(
                "Directory successfully synced: %s -> s3://%s/%s",
                local_dir.as_posix(),
                self.s3_config.bucket_name,
                s3_prefix,
            )
        except Exception as e:
            logger.info("Directory sync to S3 failed.")
            raise TextSummarizerError(e, logger) from e

    def load_csv(self, s3_uri: str) -> pd.DataFrame:
        try:
            bucket, key = self._parse_s3_uri(s3_uri)
            obj = self._client.get_object(Bucket=bucket, Key=key)
            return pd.read_csv(obj["Body"])
        except Exception as e:
            logger.info("Failed to load CSV from S3.")
            raise TextSummarizerError(e, logger) from e

    def stream_csv(self, df: pd.DataFrame, s3_key: str) -> str:
        try:
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
        except Exception as e:
            logger.info("Failed to stream CSV to S3.")
            raise TextSummarizerError(e, logger) from e

    def stream_yaml(self, data: dict, s3_key: str) -> str:
        def _convert(obj):
            if isinstance(obj, dict):
                return {k: _convert(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [_convert(v) for v in obj]
            if isinstance(obj, tuple):
                return tuple(_convert(v) for v in obj)
            if isinstance(obj, np.generic):
                return obj.item()
            return obj

        try:
            python_data = _convert(data)
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
        except Exception as e:
            logger.info("Failed to stream YAML to S3.")
            raise TextSummarizerError(e, logger) from e

    def _parse_s3_uri(self, s3_uri: str) -> tuple[str, str]:
        if not s3_uri.startswith("s3://"):
            raise ValueError(f"Invalid S3 URI: {s3_uri}")
        parts = s3_uri[5:].split("/", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid S3 URI: {s3_uri}")
        return parts[0], parts[1]

    def stream_object(self, obj: object, s3_key: str) -> str:
        try:
            buf = BytesIO()
            joblib.dump(obj, buf)
            buf.seek(0)
            self._client.put_object(
                Bucket=self.s3_config.bucket_name, Key=s3_key, Body=buf.read()
            )
            uri = f"s3://{self.s3_config.bucket_name}/{s3_key}"
            logger.info("Streamed object to: %s", uri)
            return uri
        except Exception as e:
            logger.info("Failed to stream object to S3.")
            raise TextSummarizerError(e, logger) from e

    def stream_npy(self, array: np.ndarray, s3_key: str) -> str:
        try:
            buf = BytesIO()
            np.save(buf, array, allow_pickle=False)
            buf.seek(0)
            self._client.put_object(
                Bucket=self.s3_config.bucket_name, Key=s3_key, Body=buf.read()
            )
            uri = f"s3://{self.s3_config.bucket_name}/{s3_key}"
            logger.info("Streamed .npy to: %s", uri)
            return uri
        except Exception as e:
            logger.info("Failed to stream .npy to S3.")
            raise TextSummarizerError(e, logger) from e

    def load_npy(self, s3_uri: str) -> np.ndarray:
        try:
            bucket, key = self._parse_s3_uri(s3_uri)
            resp = self._client.get_object(Bucket=bucket, Key=key)
            data = resp["Body"].read()
            buf = BytesIO(data)
            buf.seek(0)
            arr = np.load(buf, allow_pickle=False)
            logger.info("Loaded .npy from S3: %s", s3_uri)
            return arr
        except Exception as e:
            logger.info("Failed to load .npy from S3.")
            raise TextSummarizerError(e, logger) from e

    def load_object(self, s3_uri: str) -> object:
        try:
            bucket, key = self._parse_s3_uri(s3_uri)
            resp = self._client.get_object(Bucket=bucket, Key=key)
            data = resp["Body"].read()
            buf = BytesIO(data)
            buf.seek(0)
            obj = joblib.load(buf)
            logger.info("Loaded object from S3: %s", s3_uri)
            return obj
        except Exception as e:
            logger.info("Failed to load object from S3: %s", s3_uri)
            raise TextSummarizerError(e, logger) from e

    def stream_df_as_csv(self, df: pd.DataFrame, s3_key: str) -> str:
        try:
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
        except Exception as e:
            logger.info("Failed to stream DataFrame as CSV to S3.")
            raise TextSummarizerError(e, logger) from e

    def extract_and_stream_zip_to_s3(self, source_zip_s3_key: str, destination_s3_key: str) -> str:
        """
        Downloads a ZIP file from S3, extracts its contents in memory,
        and uploads extracted files to S3 under the given prefix.
        """
        try:
            logger.info(f"Downloading ZIP from S3: {source_zip_s3_key}")
            s3_obj = self._client.get_object(
                Bucket=self.s3_config.bucket_name, Key=source_zip_s3_key
            )
            zip_bytes = s3_obj['Body'].read()
            # Minimal zip header validation
            if not zip_bytes.startswith(b'PK'):
                logger.error("S3 object is not a valid ZIP file (missing ZIP signature).")
                raise TextSummarizerError("S3 object is not a valid ZIP file.", logger)

            with zipfile.ZipFile(BytesIO(zip_bytes)) as zip_ref:
                for fileinfo in zip_ref.infolist():
                    if fileinfo.is_dir():
                        continue  # Skip directories
                    file_data = zip_ref.read(fileinfo.filename)
                    target_key = f"{destination_s3_key}/{fileinfo.filename}"
                    logger.info(f"Uploading extracted file to S3: {target_key}")
                    self._client.put_object(
                        Bucket=self.s3_config.bucket_name,
                        Key=target_key,
                        Body=file_data,
                    )

            s3_uri = f"s3://{self.s3_config.bucket_name}/{destination_s3_key}"
            logger.info(f"Extraction and streaming to S3 complete: {s3_uri}")
            return s3_uri
        except Exception as e:
            logger.info("Failed to extract and stream ZIP to S3.")
            raise TextSummarizerError(e, logger) from e

    def stream_url_to_s3(self, url: str, s3_key: str, validate_zip: bool = True) -> str:
        """
        Streams the content from a remote URL directly to S3 (buffered in memory).
        Optionally validates ZIP header if file is zip.

        Args:
            url (str): The source URL to stream from.
            s3_key (str): The S3 key to upload the content to.
            validate_zip (bool): If True, validates ZIP file header for .zip files.

        Returns:
            str: The S3 URI of the uploaded file.

        Raises:
            TextSummarizerError: On failure to download or upload.
        """
        try:
            logger.info(f"Downloading content from URL for S3: {url}")
            response = requests.get(url, stream=True)
            response.raise_for_status()

            # Read all bytes into memory
            file_content = response.content

            # Optional: ZIP file validation
            if validate_zip and s3_key.endswith('.zip'):
                if not file_content.startswith(b'PK'):
                    logger.error("Downloaded file does not have ZIP signature.")
                    raise TextSummarizerError("Downloaded file is not a valid ZIP archive.", logger)

            # Upload to S3
            self._client.put_object(
                Bucket=self.s3_config.bucket_name,
                Key=s3_key,
                Body=file_content,
            )
            s3_uri = f"s3://{self.s3_config.bucket_name}/{s3_key}"
            logger.info(f"Successfully streamed URL to S3: {s3_uri}")
            return s3_uri

        except Exception as e:
            logger.error(f"Error streaming URL to S3: {e}")
            raise TextSummarizerError(e, logger) from e

    def load_from_source(self, source: str) -> pd.DataFrame:
        """
        Loads data from a specified source (e.g., S3, local file) into a DataFrame.

        Args:
            source (str): The source path or URI to load data from.

        Returns:
            pd.DataFrame: The loaded DataFrame.

        Raises:
            TextSummarizerError: On failure to load data.
        """
        try:
            if source.startswith("s3://"):
                # Load from S3
                s3_key = source[len("s3://"):].strip("/")
                logger.info(f"Loading DataFrame from S3: {s3_key}")
                obj = self._client.get_object(Bucket=self.s3_config.bucket_name, Key=s3_key)
                df = pd.read_csv(BytesIO(obj['Body'].read()))
            else:
                # Load from local file
                logger.info(f"Loading DataFrame from local file: {source}")
                df = pd.read_csv(source)

            logger.info("DataFrame loaded successfully.")
            return df

        except Exception as e:
            logger.error(f"Error loading DataFrame: {e}")
            raise TextSummarizerError(e, logger) from e