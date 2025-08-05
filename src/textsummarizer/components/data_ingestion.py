import zipfile
import shutil
from pathlib import Path
from typing import Optional
import posixpath

from src.textsummarizer.entity.artifact_entity import DataIngestionArtifact
from src.textsummarizer.entity.config_entity import DataIngestionConfig
from src.textsummarizer.exception.exception import TextSummarizerError
from src.textsummarizer.logging import logger
from src.textsummarizer.dbhandler.base_handler import DBHandler
from src.textsummarizer.utils.core import download_file
from box import ConfigBox

class DataIngestion:
    """Orchestrates data downloading, extraction, and persistence."""

    def __init__(
        self,
        config: DataIngestionConfig,
        backup_handler: Optional[DBHandler] = None,
    ) -> None:
        self.ingestion_config = config
        self.backup_handler = backup_handler
        self.local_enabled = self.ingestion_config.local_enabled
        self.s3_enabled = self.ingestion_config.s3_enabled

    def _download_data(self) -> dict:
        results = {
            "raw_filepath": None,
            "raw_s3_uri": None,
        }
        url = self.ingestion_config.source_url

        if self.local_enabled:
            logger.info("Downloading ZIP locally to: %s", self.ingestion_config.raw_filepath)
            download_file(
                url=url,
                download_path=self.ingestion_config.raw_filepath,
            )
            results["raw_filepath"] = self.ingestion_config.raw_filepath

        if self.s3_enabled and self.backup_handler:
            logger.info("Streaming ZIP to S3: %s", self.ingestion_config.raw_s3_key)
            with self.backup_handler as handler:
                s3_uri = handler.stream_url_to_s3(
                    url=url,
                    s3_key=self.ingestion_config.raw_s3_key,
                )
            results["raw_s3_uri"] = s3_uri

        return ConfigBox(results)

    def _extract_zip(self) -> dict:
        results = {
            "ingested_filepath": None,
            "dvc_ingested_filepath": None,
            "ingested_s3_uri": None,
            "dvc_ingested_s3_uri": None,
        }
        try:
            # Local extraction
            if self.local_enabled:
                zip_path = self.ingestion_config.raw_filepath
                ingested_dir = self.ingestion_config.ingested_dir
                ingested_dir.mkdir(parents=True, exist_ok=True)
                logger.info("Extracting ZIP locally to: %s", ingested_dir)
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(ingested_dir)
                logger.info("Extraction complete.")
                results["ingested_filepath"] = ingested_dir / self.ingestion_config.dataset_name

                # DVC-tracked ingested data
                dvc_ingested = self.ingestion_config.dvc_ingested_dir
                if dvc_ingested.exists():
                    shutil.rmtree(dvc_ingested)
                shutil.copytree(ingested_dir, dvc_ingested)
                results["dvc_ingested_filepath"] = dvc_ingested / self.ingestion_config.dataset_name

            # S3 extraction
            if self.s3_enabled and self.backup_handler:
                logger.info("Extracting ZIP in S3: %s -> %s", self.ingestion_config.raw_s3_key, self.ingestion_config.ingested_s3_key)
                with self.backup_handler as handler:
                    ingested_s3_uri = handler.extract_and_stream_zip_to_s3(
                        source_zip_s3_key=self.ingestion_config.raw_s3_key,
                        destination_s3_key=self.ingestion_config.ingested_s3_key,
                    )

                    results["ingested_s3_uri"] = posixpath.join(ingested_s3_uri, self.ingestion_config.dataset_name)

                    # DVC ingested S3
                    if self.ingestion_config.dvc_ingested_s3_key:
                        dvc_ingested_s3_uri = handler.upload_dir(
                            local_dir=self.ingestion_config.dvc_ingested_dir,
                            s3_prefix=self.ingestion_config.dvc_ingested_s3_key,
                        )
                        results["dvc_ingested_s3_uri"] = posixpath.join(dvc_ingested_s3_uri, self.ingestion_config.dataset_name)

            return ConfigBox(results)

        except Exception as e:
            logger.info("Error during ZIP extraction")
            raise TextSummarizerError(e, logger) from e

    def run_ingestion(self) -> DataIngestionArtifact:
        try:
            logger.info("Starting data ingestion...")
            download_info = self._download_data()
            extract_info = self._extract_zip()
            artifact = DataIngestionArtifact(
                raw_filepath=Path(download_info.raw_filepath).absolute() if download_info.raw_filepath else None,
                raw_s3_uri=download_info.raw_s3_uri,
                ingested_filepath=Path(extract_info.ingested_filepath).absolute() if extract_info.ingested_filepath else None,
                dvc_ingested_filepath=Path(extract_info.dvc_ingested_filepath).absolute() if extract_info.dvc_ingested_filepath else None,
                ingested_s3_uri=extract_info.ingested_s3_uri,
                dvc_ingested_s3_uri=extract_info.dvc_ingested_s3_uri,
            )
            logger.info(f"Data ingestion complete: {artifact}")
            return artifact

        except Exception as e:
            logger.info("Data ingestion pipeline failed")
            raise TextSummarizerError(e, logger) from e