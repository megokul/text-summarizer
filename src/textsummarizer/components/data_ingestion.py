import zipfile
from pathlib import Path
import pandas as pd
import shutil

from src.textsummarizer.entity.config_entity import DataIngestionConfig
from src.textsummarizer.entity.artifact_entity import DataIngestionArtifact
from src.textsummarizer.exception.exception import TextSummarizerError
from src.textsummarizer.logging import logger
from src.textsummarizer.dbhandler.s3_handler import S3Handler
from src.textsummarizer.utils.core import save_to_csv, download_file


class DataIngestion:
    """
    Production-grade data ingestion component:
    - Downloads dataset with retries
    - Extracts ZIP contents into DataFrame
    - Saves to local + DVC path
    - Optionally uploads to S3
    - Returns DataIngestionArtifact with all paths/URIs
    """

    def __init__(
        self,
        ingestion_config: DataIngestionConfig,
        backup_handler: S3Handler | None = None
    ) -> None:
        self.ingestion_config = ingestion_config
        self.backup_handler = backup_handler

    def _extract_zip_to_dataframe(self) -> pd.DataFrame:
        """
        Extracts the first CSV file inside the ZIP archive into a DataFrame.
        """
        try:
            zip_path = self.ingestion_config.raw_filepath
            logger.info(f"Extracting ZIP file: {zip_path}")

            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                csv_files = [name for name in zip_ref.namelist() if name.endswith('.csv')]
                if not csv_files:
                    raise ValueError("No CSV file found in ZIP archive.")
                csv_name = csv_files[0]
                logger.info(f"Found CSV in ZIP: {csv_name}")

                with zip_ref.open(csv_name) as f:
                    df = pd.read_csv(f)

            logger.info("Extraction successful. DataFrame created.")
            return df

        except Exception as e:
            logger.error("Error during ZIP extraction.")
            raise TextSummarizerError(e, logger) from e

    def _persist_data(self, df: pd.DataFrame) -> dict:
        """
        Saves data to local + DVC + S3 (based on config) and returns file path/URI dictionary.
        """
        try:
            conf = self.ingestion_config
            paths = {
                "raw_filepath": None,
                "dvc_raw_filepath": None,
                "ingested_filepath": None,
                "raw_s3_uri": None,
                "dvc_raw_s3_uri": None,
                "ingested_s3_uri": None,
            }

            if conf.local_enabled:
                logger.info("Saving files locally...")
                paths["raw_filepath"] = conf.raw_filepath

                # Copy raw ZIP to DVC path
                conf.dvc_raw_filepath.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(conf.raw_filepath, conf.dvc_raw_filepath)
                paths["dvc_raw_filepath"] = conf.dvc_raw_filepath

                # Save ingested DataFrame
                save_to_csv(df, conf.ingested_filepath, label="Ingested Data")
                paths["ingested_filepath"] = conf.ingested_filepath

            if conf.s3_enabled and self.backup_handler:
                logger.info("Uploading files to S3...")
                with self.backup_handler as handler:
                    paths["raw_s3_uri"] = handler.upload_file(
                        conf.raw_filepath, conf.raw_s3_key
                    )
                    paths["dvc_raw_s3_uri"] = handler.upload_file(
                        conf.dvc_raw_filepath, conf.dvc_raw_s3_key
                    )
                    paths["ingested_s3_uri"] = handler.stream_csv(
                        df, conf.ingested_s3_key
                    )

            return paths

        except Exception as e:
            logger.error("Error during file persistence.")
            raise TextSummarizerError(e, logger) from e

    def run_ingestion(self) -> DataIngestionArtifact:
        """
        Executes full ingestion pipeline:
        - Downloads
        - Extracts
        - Saves
        - Returns artifact
        """
        try:
            logger.info("=== Starting Data Ingestion ===")
            download_file(url=self.ingestion_config.source_url, raw_filepath=self.ingestion_config.raw_filepath)
            df = self._extract_zip_to_dataframe()
            persisted = self._persist_data(df)

            artifact = DataIngestionArtifact(**persisted)
            logger.info(f"Ingestion completed.\nArtifact:\n{artifact}")
            return artifact

        except Exception as e:
            raise TextSummarizerError(e, logger) from e
