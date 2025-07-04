import os
from io import BytesIO, StringIO
from pathlib import Path

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

    This class provides a comprehensive interface for interacting with S3,
    including file uploads, directory synchronization, and in-memory streaming
    of various data formats like CSV, YAML, and serialized Python objects.
    """

    def __init__(self, config: S3HandlerConfig) -> None:
        """
        Initializes the S3Handler with the given configuration.

        Args:
            config (S3HandlerConfig): Configuration for the S3 handler.
        """
        try:
            self.config = config
            self._client = boto3.client("s3", region_name=self.config.aws_region)
            logger.info(
                "S3Handler initialized for bucket '%s' in region '%s'",
                self.config.bucket_name,
                self.config.aws_region,
            )
        except Exception as e:
            logger.info("Failed to initialize S3 client.")
            raise TextSummarizerError(e, logger) from e

    def __enter__(self) -> "S3Handler":
        """Enter the runtime context."""
        logger.info("S3Handler context entered.")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the runtime context."""
        logger.info("S3Handler context exited.")

    def close(self) -> None:
        """Close any open connections (no-op for S3)."""
        logger.info("S3Handler.close() called. No persistent connection to close.")

    def load_from_source(self) -> pd.DataFrame:
        """Load data from the primary source (not implemented for S3)."""
        raise NotImplementedError("S3Handler does not support load_from_source directly.")

    def upload_file(self, local_path: Path, s3_key: str) -> None:
        """
        Upload a single local file to S3.

        Args:
            local_path (Path): The path to the local file.
            s3_key (str): The target S3 key.
        """
        try:
            if not local_path.is_file():
                raise FileNotFoundError(f"Local file not found: {local_path.as_posix()}")

            self._client.upload_file(
                Filename=str(local_path),
                Bucket=self.config.bucket_name,
                Key=s3_key,
            )
            logger.info(
                "Uploaded: %s -> s3://%s/%s",
                local_path.as_posix(),
                self.config.bucket_name,
                s3_key,
            )
        except ClientError as e:
            logger.info("AWS ClientError during file upload: %s", str(e))
            raise TextSummarizerError(e, logger) from e
        except Exception as e:
            logger.info("Unexpected error during file upload: %s", str(e))
            raise TextSummarizerError(e, logger) from e

    def sync_directory(self, local_dir: Path, s3_prefix: str) -> None:
        """
        Recursively upload a local directory to an S3 prefix.

        Args:
            local_dir (Path): The local directory to sync.
            s3_prefix (str): The target S3 prefix.
        """
        try:
            if not local_dir.is_dir():
                raise NotADirectoryError(f"Local directory not found: {local_dir.as_posix()}")

            logger.info(
                "Starting directory sync: %s -> s3://%s/%s",
                local_dir.as_posix(),
                self.config.bucket_name,
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
                self.config.bucket_name,
                s3_prefix,
            )
        except Exception as e:
            logger.info("Directory sync to S3 failed.")
            raise TextSummarizerError(e, logger) from e

    def load_csv(self, s3_uri: str) -> pd.DataFrame:
        """
        Load a CSV file from S3 into a pandas DataFrame.

        Args:
            s3_uri (str): The S3 URI of the CSV file.

        Returns:
            pd.DataFrame: The loaded DataFrame.
        """
        try:
            bucket, key = self._parse_s3_uri(s3_uri)
            obj = self._client.get_object(Bucket=bucket, Key=key)
            return pd.read_csv(obj["Body"])
        except Exception as e:
            logger.info("Failed to load CSV from S3.")
            raise TextSummarizerError(e, logger) from e

    def stream_csv(self, df: pd.DataFrame, s3_key: str) -> str:
        """
        Stream a DataFrame as a CSV to S3 without writing to a local file.

        Args:
            df (pd.DataFrame): The DataFrame to stream.
            s3_key (str): The target S3 key.

        Returns:
            str: The S3 URI of the created object.
        """
        try:
            buf = StringIO()
            df.to_csv(buf, index=False)
            buf.seek(0)
            self._client.put_object(
                Bucket=self.config.bucket_name,
                Key=s3_key,
                Body=buf.getvalue().encode("utf-8")
            )
            s3_uri = f"s3://{self.config.bucket_name}/{s3_key}"
            logger.info(f"Streamed CSV to: {s3_uri}")
            return s3_uri
        except Exception as e:
            logger.info("Failed to stream CSV to S3.")
            raise TextSummarizerError(e, logger) from e

    def stream_yaml(self, data: dict, s3_key: str) -> str:
        """
        Stream a dictionary as a YAML file to S3.

        Args:
            data (dict): The dictionary to stream.
            s3_key (str): The target S3 key.

        Returns:
            str: The S3 URI of the created object.
        """
        def _convert(obj):
            if isinstance(obj, dict):
                return { _convert(k): _convert(v) for k, v in obj.items() }
            if isinstance(obj, list):
                return [ _convert(v) for v in obj ]
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
                Bucket=self.config.bucket_name,
                Key=s3_key,
                Body=buf.getvalue().encode("utf-8"),
            )

            s3_uri = f"s3://{self.config.bucket_name}/{s3_key}"
            logger.info(f"Streamed YAML to: {s3_uri}")
            return s3_uri

        except Exception as e:
            logger.info("Failed to stream YAML to S3.")
            raise TextSummarizerError(e, logger) from e

    def _parse_s3_uri(self, s3_uri: str) -> tuple[str, str]:
        """
        Parse an S3 URI into its bucket and key components.

        Args:
            s3_uri (str): The S3 URI (e.g., "s3://bucket/key").

        Returns:
            tuple[str, str]: A tuple containing the bucket and key.
        """
        if not s3_uri.startswith("s3://"):
            raise ValueError(f"Invalid S3 URI: {s3_uri}")
        parts = s3_uri[5:].split("/", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid S3 URI: {s3_uri}")
        return parts[0], parts[1]

    def stream_object(self, obj: object, s3_key: str) -> str:
        """
        Serialize and stream a Python object to S3 using joblib.

        Args:
            obj (object): The object to serialize.
            s3_key (str): The target S3 key.

        Returns:
            str: The S3 URI of the created object.
        """
        try:
            buf = BytesIO()
            joblib.dump(obj, buf)
            buf.seek(0)

            self._client.put_object(
                Bucket=self.config.bucket_name,
                Key=s3_key,
                Body=buf.read(),
            )

            uri = f"s3://{self.config.bucket_name}/{s3_key}"
            logger.info("Streamed object to: %s", uri)
            return uri

        except Exception as e:
            logger.info("Failed to stream object to S3.")
            raise TextSummarizerError(e, logger) from e

    def stream_npy(self, array: np.ndarray, s3_key: str) -> str:
        """
        Serialize and stream a NumPy array to S3 in .npy format.

        Args:
            array (np.ndarray): The NumPy array to stream.
            s3_key (str): The target S3 key.

        Returns:
            str: The S3 URI of the created object.
        """
        try:
            buf = BytesIO()
            np.save(buf, array, allow_pickle=False)
            buf.seek(0)

            self._client.put_object(
                Bucket=self.config.bucket_name,
                Key=s3_key,
                Body=buf.read(),
            )

            uri = f"s3://{self.config.bucket_name}/{s3_key}"
            logger.info("Streamed .npy to: %s", uri)
            return uri

        except Exception as e:
            logger.info("Failed to stream .npy to S3.")
            raise TextSummarizerError(e, logger) from e

    def load_npy(self, s3_uri: str) -> np.ndarray:
        """
        Load a .npy-serialized NumPy array from S3.

        Args:
            s3_uri (str): The S3 URI of the .npy file.

        Returns:
            np.ndarray: The deserialized NumPy array.
        """
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
        """
        Load a joblib-serialized object from S3.

        Args:
            s3_uri (str): The S3 URI of the serialized object.

        Returns:
            object: The deserialized Python object.
        """
        try:
            bucket, key = self._parse_s3_uri(s3_uri)
            resp = self._client.get_object(Bucket=bucket, Key=key)
            data = resp['Body'].read()

            buf = BytesIO(data)
            buf.seek(0)
            obj = joblib.load(buf)

            logger.info("Loaded object from S3: %s", s3_uri)
            return obj

        except Exception as e:
            logger.info("Failed to load object from S3: %s", s3_uri)
            raise TextSummarizerError(e, logger) from e

    def stream_df_as_csv(self, df: pd.DataFrame, s3_key: str) -> str:
        """
        Stream a pandas DataFrame as a CSV to S3.

        Args:
            df (pd.DataFrame): The DataFrame to stream.
            s3_key (str): The target S3 key.

        Returns:
            str: The S3 URI of the created object.
        """
        try:
            buf = BytesIO()
            df.to_csv(buf, index=False)
            buf.seek(0)

            self._client.put_object(
                Bucket=self.config.bucket_name,
                Key=s3_key,
                Body=buf.read(),
                ContentType='text/csv',
            )

            uri = f"s3://{self.config.bucket_name}/{s3_key}"
            logger.info("Streamed DataFrame as CSV to: %s", uri)
            return uri

        except Exception as e:
            logger.info("Failed to stream DataFrame as CSV to S3.")
            raise TextSummarizerError(e, logger) from e
