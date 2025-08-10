"""Data ingestion component.

Prepares datasets for both local and cloud (S3) workflows by:
1) downloading a ZIP archive to local storage and/or directly to S3,
2) extracting its contents locally and/or directly in S3, and
3) returning an artifact with all relevant paths and URIs.

Design intent:
- Single orchestration point that coordinates reusable I/O helpers
  (see utils.core: `download_file`, `extract_zip`) and any cloud handler.
- Clear, production-grade logging around each boundary (download/extract/copy).
- Fail-fast with contextual errors wrapped in the project custom exception.
- Predictable return shapes: attributes are always present (may be None).
- Idempotent where reasonable (e.g., DVC mirror is cleaned before re-copy).
"""

from pathlib import Path
import posixpath
import shutil

from box import ConfigBox

from src.textsummarizer.dbhandler.base_handler import DBHandler
from src.textsummarizer.entity.artifact_entity import DataIngestionArtifact
from src.textsummarizer.entity.config_entity import DataIngestionConfig
from src.textsummarizer.exception.exception import TextSummarizerError
from src.textsummarizer.logging import logger
from src.textsummarizer.utils.core import download_file, extract_zip


class DataIngestion:
    """Coordinate dataset download, extraction, and artifact creation.

    Args:
        config (DataIngestionConfig): Configuration values for ingestion.
        backup_handler (DBHandler | None): Optional handler used for S3 ops.
    """

    def __init__(
        self,
        config: DataIngestionConfig,
        backup_handler: DBHandler | None = None,
    ) -> None:
        # Hold config and optional S3 handler (used as a context manager).
        self.ingestion_config = config
        self.backup_handler = backup_handler

        # Feature flags determine whether local and/or S3 operations run.
        self.local_enabled = self.ingestion_config.local_enabled
        self.s3_enabled = self.ingestion_config.s3_enabled

    def _download_data(self) -> ConfigBox:
        """Download the dataset ZIP locally and/or stream it to S3.

        Returns:
            ConfigBox: With fields:
                - raw_filepath (Path | None): Local ZIP path if downloaded.
                - raw_s3_uri (str | None): S3 URI if streamed to S3.

        Raises:
            TextSummarizerError: If any download step fails.
        """
        # Initialize results with explicit keys for predictable attributes.
        results = ConfigBox(
            raw_filepath=None,
            raw_s3_uri=None,
        )

        try:
            url = self.ingestion_config.source_url

            # Local download via shared utility with retries and logging.
            if self.local_enabled:
                logger.info(
                    "Downloading ZIP locally to: %s",
                    self.ingestion_config.raw_filepath,
                )
                download_file(
                    url=url,
                    download_path=self.ingestion_config.raw_filepath,
                )
                results.raw_filepath = self.ingestion_config.raw_filepath

            # Optional S3 streaming (avoids large local temp files).
            if self.s3_enabled and self.backup_handler:
                logger.info(
                    "Streaming ZIP directly to S3 with key: %s",
                    self.ingestion_config.raw_s3_key,
                )
                # Use the handler as a context manager to guarantee cleanup.
                with self.backup_handler as handler:
                    s3_uri = handler.stream_url_to_s3(
                        url=url,
                        s3_key=self.ingestion_config.raw_s3_key,
                    )
                results.raw_s3_uri = s3_uri

            return results

        except Exception as e:  # noqa: BLE001
            logger.error("Failed while downloading or streaming the ZIP dataset.")
            raise TextSummarizerError(e, logger) from e

    def _extract_zip(self) -> ConfigBox:
        """Extract the dataset ZIP locally and/or in S3.

        Returns:
            ConfigBox: With fields:
                - ingested_filepath (Path | None): Local dataset directory path.
                - dvc_ingested_filepath (Path | None): Local DVC mirror path.
                - ingested_s3_uri (str | None): S3 URI to the dataset directory.
                - dvc_ingested_s3_uri (str | None): S3 URI to the DVC mirror.

        Raises:
            TextSummarizerError: If any extraction step fails.
        """
        # Keep attribute presence explicit and stable for callers.
        results = ConfigBox(
            ingested_filepath=None,
            dvc_ingested_filepath=None,
            ingested_s3_uri=None,
            dvc_ingested_s3_uri=None,
        )

        try:
            # Local extraction via shared utility for consistency and reuse.
            if self.local_enabled:
                zip_path = self.ingestion_config.raw_filepath
                ingested_dir = self.ingestion_config.ingested_dir

                logger.info("Extracting ZIP locally to: %s", ingested_dir)
                # Centralized error handling in utils.core.extract_zip.
                extract_zip(zip_path=zip_path, extract_to=ingested_dir, label="ZIP")
                logger.info("Local extraction complete.")

                # Point to the dataset subdirectory within the extracted folder.
                results.ingested_filepath = (
                    ingested_dir / self.ingestion_config.dataset_name
                )

                # Mirror extracted data into the DVC-tracked directory.
                dvc_ingested = self.ingestion_config.dvc_ingested_dir

                # Ensure parent exists to avoid copy failures on first run.
                dvc_ingested.parent.mkdir(parents=True, exist_ok=True)

                # Clean any old copy to keep the mirror deterministic/idempotent.
                if dvc_ingested.exists():
                    shutil.rmtree(dvc_ingested)

                # Copy the entire extracted directory (not just the subfolder).
                shutil.copytree(ingested_dir, dvc_ingested)

                results.dvc_ingested_filepath = (
                    dvc_ingested / self.ingestion_config.dataset_name
                )

            # S3 extraction: performed remotely by the handler implementation.
            if self.s3_enabled and self.backup_handler:
                logger.info(
                    "Extracting ZIP in S3: %s -> %s",
                    self.ingestion_config.raw_s3_key,
                    self.ingestion_config.ingested_s3_key,
                )
                with self.backup_handler as handler:
                    ingested_s3_uri = handler.extract_and_stream_zip_to_s3(
                        source_zip_s3_key=self.ingestion_config.raw_s3_key,
                        destination_s3_key=self.ingestion_config.ingested_s3_key,
                    )
                    # Use POSIX joins so S3 URIs are consistent across platforms.
                    results.ingested_s3_uri = posixpath.join(
                        ingested_s3_uri, self.ingestion_config.dataset_name
                    )

                    # Optionally upload the local DVC mirror to S3.
                    # Guard on local availability to avoid errors in S3-only mode.
                    if (
                        self.ingestion_config.dvc_ingested_s3_key
                        and self.local_enabled
                        and self.ingestion_config.dvc_ingested_dir.exists()
                    ):
                        dvc_ingested_s3_uri = handler.upload_dir(
                            local_dir=self.ingestion_config.dvc_ingested_dir,
                            s3_prefix=self.ingestion_config.dvc_ingested_s3_key,
                        )
                        results.dvc_ingested_s3_uri = posixpath.join(
                            dvc_ingested_s3_uri, self.ingestion_config.dataset_name
                        )

            return results

        except Exception as e:  # noqa: BLE001
            logger.error("Failed during ZIP extraction step.")
            raise TextSummarizerError(e, logger) from e

    def run_ingestion(self) -> DataIngestionArtifact:
        """Run the full ingestion process: download, extract, and produce artifacts.

        Returns:
            DataIngestionArtifact: Local paths and S3 URIs produced by ingestion.

        Raises:
            TextSummarizerError: If any stage fails.
        """
        try:
            logger.info("Starting data ingestion pipeline...")

            # Step 1: Download (local and/or S3).
            download_info = self._download_data()

            # Step 2: Extract (local and/or S3).
            extract_info = self._extract_zip()

            # Step 3: Build artifact with exactly the paths/URIs produced above.
            artifact = DataIngestionArtifact(
                raw_filepath=(
                    Path(download_info.raw_filepath)
                    if download_info.raw_filepath
                    else None
                ),
                raw_s3_uri=download_info.raw_s3_uri,
                ingested_filepath=(
                    Path(extract_info.ingested_filepath)
                    if extract_info.ingested_filepath
                    else None
                ),
                dvc_ingested_filepath=(
                    Path(extract_info.dvc_ingested_filepath)
                    if extract_info.dvc_ingested_filepath
                    else None
                ),
                ingested_s3_uri=extract_info.ingested_s3_uri,
                dvc_ingested_s3_uri=extract_info.dvc_ingested_s3_uri,
            )

            logger.info("Data ingestion complete: %s", artifact)
            return artifact

        except Exception as e:  # noqa: BLE001
            logger.error("Data ingestion pipeline failed.")
            raise TextSummarizerError(e, logger) from e
