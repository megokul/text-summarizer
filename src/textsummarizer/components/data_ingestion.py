import zipfile
import urllib.request as request
from pathlib import Path

from src.textsummarizer.entity.config_entity import DataIngestionConfig
from src.textsummarizer.exception.exception import TextSummarizerError
from src.textsummarizer.logging import logger
from src.textsummarizer.dbhandler.s3_handler import S3Handler


class DataIngestion:
    """
    Handles downloading and extracting the dataset,
    and optionally uploading to S3 and saving for DVC tracking.
    """

    def __init__(
        self,
        ingestion_config: DataIngestionConfig,
        backup_handler: S3Handler | None = None
    ) -> None:
        self.ingestion_config = ingestion_config
        self.backup_handler = backup_handler

    def download_file(self) -> None:
        try:
            file_path = self.ingestion_config.local_data_file
            source_url = self.ingestion_config.source_url

            if "github.com" in source_url and "/blob/" in source_url:
                source_url = source_url.replace("/blob/", "/raw/")
                logger.info(f"Converted GitHub URL to raw URL: {source_url}")

            if not file_path.exists():
                file_path.parent.mkdir(parents=True, exist_ok=True)
                request.urlretrieve(url=source_url, filename=str(file_path))
                logger.info(f"Downloaded dataset to: {file_path}")
            else:
                logger.info(f"Dataset already exists at: {file_path}, skipping download.")

        except Exception as e:
            logger.error("Error during file download.")
            raise TextSummarizerError(e, logger) from e

    def extract_zip_file(self) -> None:
        try:
            logger.info(f"Extracting ZIP to: {self.ingestion_config.unzip_dir}")
            self.ingestion_config.unzip_dir.mkdir(parents=True, exist_ok=True)

            with zipfile.ZipFile(self.ingestion_config.local_data_file, "r") as zip_ref:
                zip_ref.extractall(self.ingestion_config.unzip_dir)

            logger.info(f"Extraction complete: {self.ingestion_config.unzip_dir}")

        except Exception as e:
            logger.error("Error during ZIP extraction.")
            raise TextSummarizerError(e, logger) from e

    def backup_and_save(self) -> None:
        """
        Save extracted data to DVC path, and optionally to S3.
        """
        try:
            src = self.ingestion_config.unzip_dir
            dvc_dst = self.ingestion_config.dvc_data_dir

            # Copy extracted files to DVC directory
            dvc_dst.mkdir(parents=True, exist_ok=True)
            for item in src.glob("*"):
                target_path = dvc_dst / item.name
                target_path.write_bytes(item.read_bytes())
                logger.info(f"Copied to DVC directory: {target_path}")

            # If S3 backup is enabled, upload
            if self.ingestion_config.s3_enabled and self.backup_handler:
                for item in dvc_dst.glob("*"):
                    s3_key = f"{self.ingestion_config.s3_prefix}/{item.name}"
                    self.backup_handler.upload_file(local_path=item, s3_key=s3_key)

        except Exception as e:
            logger.error("Error during backup and DVC save.")
            raise TextSummarizerError(e, logger) from e

    def run_ingestion(self) -> None:
        try:
            logger.info("=== Starting Data Ingestion ===")
            self.download_file()
            self.extract_zip_file()

            if self.ingestion_config.local_enabled or self.ingestion_config.s3_enabled:
                self.backup_and_save()

            logger.info("=== Data Ingestion Complete ===")

        except Exception as e:
            raise TextSummarizerError(e, logger) from e
