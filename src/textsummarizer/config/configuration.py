import os
from pathlib import Path

from src.textsummarizer.constants.constants import (
    ARTIFACTS_ROOT,
    CONFIG_FILENAME,
    CONFIG_ROOT,
    DVC_RAW_SUBDIR,
    DVC_ROOT,
    INGEST_INGESTED_SUBDIR,
    INGEST_RAW_SUBDIR,
    INGEST_ROOT,
    PARAMS_FILENAME,
)
from src.textsummarizer.entity.config_entity import (
    DataIngestionConfig,
    S3HandlerConfig,
)
from src.textsummarizer.utils.core import read_yaml
from src.textsummarizer.utils.timestamp import get_utc_timestamp
from src.textsummarizer.logging import logger


class ConfigurationManager:
    """
    Manages the loading and provision of all project configurations.

    This class reads configuration files (config.yaml, params.yaml) and
    provides methods to access structured configuration objects for different
    pipeline components. It ensures that configurations are loaded once and
    accessed consistently.
    """
    _global_timestamp: str = None

    def __init__(self) -> None:
        """
        Initializes the ConfigurationManager by loading all necessary configs
        and setting up artifact directories.
        """
        logger.info("Initializing ConfigurationManager.")
        self._init_artifacts()
        self._load_configs()
        logger.info("ConfigurationManager initialized successfully.")

    def _init_artifacts(self) -> None:
        """
        Initializes artifact directories with a consistent, cached timestamp.
        This ensures all artifacts from a single run are stored together.
        """
        if ConfigurationManager._global_timestamp is None:
            ConfigurationManager._global_timestamp = get_utc_timestamp()
            logger.info(f"Generated global timestamp: {ConfigurationManager._global_timestamp}")
        
        timestamp = ConfigurationManager._global_timestamp
        self.artifacts_root = Path(ARTIFACTS_ROOT) / timestamp
        self.artifacts_root.mkdir(parents=True, exist_ok=True)
        logger.info(f"Artifacts root directory set to: {self.artifacts_root}")

    def _load_configs(self) -> None:
        """
        Loads configuration files (config.yaml, params.yaml) into memory.
        """
        logger.info("Loading configuration files.")
        config_root = Path(CONFIG_ROOT)
        config_filepath = config_root / CONFIG_FILENAME
        params_filepath = config_root / PARAMS_FILENAME

        self.config = read_yaml(config_filepath)
        self.params = read_yaml(params_filepath)
        logger.info("Configuration files loaded successfully.")

    def get_data_ingestion_config(self) -> DataIngestionConfig:
        """
        Creates and returns the DataIngestionConfig.

        This method extracts the data ingestion settings from the loaded
        configurations and constructs a DataIngestionConfig object.

        Returns:
            DataIngestionConfig: A dataclass object containing all settings
                                 for the data ingestion component.
        """
        logger.info("Creating data ingestion configuration.")
        ingestion_config = self.config.data_ingestion
        data_backup_config = self.config.data_backup

        # Define paths for data ingestion artifacts
        root_dir = self.artifacts_root / INGEST_ROOT
        raw_filepath = root_dir / INGEST_RAW_SUBDIR / ingestion_config.raw_data_filename
        dvc_raw_filepath = Path(DVC_ROOT) / DVC_RAW_SUBDIR / ingestion_config.raw_data_filename
        ingested_filepath = root_dir / INGEST_INGESTED_SUBDIR / ingestion_config.ingested_data_filename
        dvc_ingested_filepath = Path(DVC_ROOT) / INGEST_INGESTED_SUBDIR / ingestion_config.ingested_data_filename

        # Create the configuration object
        data_ingestion_config = DataIngestionConfig(
            root_dir=root_dir,
            source_url=ingestion_config.source_URL,
            raw_filepath=raw_filepath,
            dvc_raw_filepath=dvc_raw_filepath,
            ingested_filepath=ingested_filepath,
            dvc_ingested_filepath=dvc_ingested_filepath,
            local_enabled=data_backup_config.local_enabled,
            s3_enabled=data_backup_config.s3_enabled,
        )
        logger.info(f"Data ingestion configuration created: {data_ingestion_config}")
        return data_ingestion_config

    def get_s3_handler_config(self) -> S3HandlerConfig:
        """
        Creates and returns the S3HandlerConfig.

        Returns:
            S3HandlerConfig: Configuration object for the S3 handler.
        """
        logger.info("Creating S3 handler configuration.")
        s3_config = self.config.s3_handler
        root_dir = self.artifacts_root / "s3_handler"
        aws_region = os.getenv("AWS_REGION")

        s3_handler_config = S3HandlerConfig(
            root_dir=root_dir,
            bucket_name=s3_config.bucket_name,
            aws_region=aws_region,
        )
        logger.info(f"S3 handler configuration created: {s3_handler_config}")
        return s3_handler_config
