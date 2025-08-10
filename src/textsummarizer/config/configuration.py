"""Central configuration manager for the text summarization project.

Loads strongly-typed configuration objects for every pipeline component from
on-disk YAML files and constructs deterministic artifact directory layouts.

Design intent:
- Single source of truth: Parse YAML once, expose typed config objects for
  each component to avoid drift and duplicated parsing logic.
- Run-scoped artifacts: Cache a global UTC timestamp so all outputs for a run
  land under the same folder, improving reproducibility and cleanup.
- Predictable I/O: Read two YAML files (``config.yaml`` and ``params.yaml``)
  and build dataclass-like configs without side effects beyond ensuring the
  artifacts root exists.
- Robustness: Provide contextual logging and wrap failures in
  ``TextSummarizerError`` to standardize error handling across components.
"""

import os
from pathlib import Path

from src.textsummarizer.constants.constants import (
    ARTIFACTS_ROOT,
    CONFIG_FILENAME,
    CONFIG_ROOT,
    DVC_RAW_SUBDIR,
    DVC_ROOT,
    EVALUATION_REPORT_SUBDIR,
    EVALUATION_ROOT,
    FINAL_MODEL_ROOT,
    FINAL_MODEL_SUBDIR,
    FINAL_TOKENIZER_SUBDIR,
    INGEST_INGESTED_SUBDIR,
    INGEST_RAW_SUBDIR,
    INGEST_ROOT,
    PARAMS_FILENAME,
    PREDICTION_ROOT,
    TRAINER_MODEL_SUBDIR,
    TRAINER_ROOT,
    TRAINER_TOKENIZER_SUBDIR,
    TRANSFORM_ROOT,
    TRANSFORM_TOKENIZED_SUBDIR,
)
from src.textsummarizer.entity.config_entity import (
    DataIngestionConfig,
    DataTransformationConfig,
    ModelEvaluationConfig,
    ModelTrainerConfig,
    PredictionConfig,
    S3HandlerConfig,
)
from src.textsummarizer.exception.exception import TextSummarizerError
from src.textsummarizer.logging import logger
from src.textsummarizer.utils.core import read_yaml
from src.textsummarizer.utils.timestamp import get_utc_timestamp


class ConfigurationManager:
    """Manage loading and provisioning of all project configurations.

    Responsibilities:
    - Create a run-scoped artifacts root using a cached timestamp.
    - Load ``config.yaml`` and ``params.yaml`` exactly once at startup.
    - Construct typed configuration objects for each pipeline component.

    Notes:
        The timestamp is cached at the *class* level so multiple instances
        created within the same process still reference the same run folder.
    """

    # Class-level variable caches the "run timestamp" across instances.
    _global_timestamp: str = None

    def __init__(self) -> None:
        """Initialize the configuration manager.

        Returns:
            None

        Raises:
            TextSummarizerError: If initialization fails.
        """
        try:
            logger.info("Initializing ConfigurationManager.")
            # Prepare the artifacts root for this run (idempotent).
            self._init_artifacts()
            # Load YAML configs into memory (ConfigBox objects).
            self._load_configs()
            logger.info("ConfigurationManager initialized successfully.")
        except Exception as e:  # noqa: BLE001
            # Log at error (not exception) per project convention.
            logger.error("Failed to initialize ConfigurationManager.")
            raise TextSummarizerError(e, logger) from e
        return None

    def _init_artifacts(self) -> None:
        """Initialize artifact directories using a cached run timestamp.

        Returns:
            None

        Raises:
            TextSummarizerError: If directory creation fails.
        """
        try:
            # If a timestamp hasn't been set for this process, generate one now.
            if ConfigurationManager._global_timestamp is None:
                # Generate a sortable UTC timestamp string (e.g., 20250101T120000Z).
                ConfigurationManager._global_timestamp = get_utc_timestamp()
                logger.info(
                    "Generated global timestamp: %s",
                    ConfigurationManager._global_timestamp,
                )

            # Pull the class-level timestamp for this instance.
            timestamp = ConfigurationManager._global_timestamp

            # Compose the run-scoped artifacts root, e.g., artifacts/<timestamp>.
            self.artifacts_root = Path(ARTIFACTS_ROOT) / timestamp

            # Ensure the directory exists; idempotent if it already does.
            self.artifacts_root.mkdir(parents=True, exist_ok=True)
            logger.info("Artifacts root directory: %s", self.artifacts_root)
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to initialize artifact directories.")
            raise TextSummarizerError(e, logger) from e
        return None

    def _load_configs(self) -> None:
        """Load ``config.yaml`` and ``params.yaml`` into memory.

        Returns:
            None

        Raises:
            TextSummarizerError: If YAML files cannot be read.
        """
        try:
            logger.info("Loading configuration files.")

            # Base directory for YAML configs (typically ./config/).
            config_root = Path(CONFIG_ROOT)

            # Exact file locations for configuration and parameters.
            config_filepath = config_root / CONFIG_FILENAME
            params_filepath = config_root / PARAMS_FILENAME

            # read_yaml returns ConfigBox so we can use dot notation throughout.
            self.config = read_yaml(config_filepath)
            self.params = read_yaml(params_filepath)

            logger.info("Configuration files loaded successfully.")
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to load configuration files.")
            raise TextSummarizerError(e, logger) from e
        return None

    def get_data_ingestion_config(self) -> DataIngestionConfig:
        """Build and return the configuration for the data ingestion component.

        Returns:
            DataIngestionConfig: Structured ingestion configuration.

        Raises:
            TextSummarizerError: If configuration construction fails.
        """
        try:
            logger.info("Creating data ingestion configuration.")

            # Extract source sections from config.yaml (using dot notation).
            ingestion_config = self.config.data_ingestion
            data_backup_config = self.config.data_backup

            # Compose the run-scoped root for ingestion artifacts.
            root_dir = self.artifacts_root / INGEST_ROOT

            # Local raw ZIP path under the run-scoped folder.
            raw_filepath = (
                root_dir / INGEST_RAW_SUBDIR / ingestion_config.raw_data_filename
            )

            # DVC raw path: lives under a shared DVC root, not run-scoped.
            dvc_raw_filepath = (
                Path(DVC_ROOT) / DVC_RAW_SUBDIR / ingestion_config.raw_data_filename
            )

            # Local extracted dataset folder for this run.
            ingested_dir = root_dir / INGEST_INGESTED_SUBDIR

            # DVC extracted dataset mirror (stable path for versioning).
            dvc_ingested_dir = Path(DVC_ROOT) / INGEST_INGESTED_SUBDIR

            # Create the dataclass-like config object with all resolved paths.
            config_obj = DataIngestionConfig(
                root_dir=root_dir,
                source_url=ingestion_config.source_URL,
                raw_filepath=raw_filepath,
                dvc_raw_filepath=dvc_raw_filepath,
                ingested_dir=ingested_dir,
                dvc_ingested_dir=dvc_ingested_dir,
                local_enabled=data_backup_config.local_enabled,
                s3_enabled=data_backup_config.s3_enabled,
                dataset_name=ingestion_config.dataset_name,
            )

            logger.info("DataIngestionConfig created.")
            return config_obj
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to create data ingestion configuration.")
            raise TextSummarizerError(e, logger) from e

    def get_s3_handler_config(self) -> S3HandlerConfig:
        """Build and return the configuration for the S3 handler.

        Returns:
            S3HandlerConfig: Structured S3 configuration.

        Raises:
            TextSummarizerError: If configuration construction fails.
        """
        try:
            logger.info("Creating S3 handler configuration.")

            # Pull the S3 section with bucket details.
            s3_config = self.config.s3_handler

            # Place any S3-related temp artifacts/logs under the run root.
            root_dir = self.artifacts_root / "s3_handler"

            # Region is pulled from environment for deployment flexibility.
            aws_region = os.getenv("AWS_REGION")

            # Build the S3 handler config object.
            config_obj = S3HandlerConfig(
                root_dir=root_dir,
                bucket_name=s3_config.bucket_name,
                aws_region=aws_region,
            )

            logger.info("S3HandlerConfig created.")
            return config_obj
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to create S3 handler configuration.")
            raise TextSummarizerError(e, logger) from e

    def get_data_transformation_config(self) -> DataTransformationConfig:
        """Build and return the configuration for data transformation.

        Returns:
            DataTransformationConfig: Structured transformation configuration.

        Raises:
            TextSummarizerError: If configuration construction fails.
        """
        try:
            logger.info("Creating data transformation configuration.")

            # Parameters for transformation (tokenizer + limits) live in params.
            params = self.params.data_transformation

            # Run-scoped transformation root and dataset dirs.
            root_dir = self.artifacts_root / TRANSFORM_ROOT
            tokenized_dataset_dir = root_dir / TRANSFORM_TOKENIZED_SUBDIR

            # DVC mirror lives under a stable global DVC root.
            dvc_tokenized_dataset_dir = Path(DVC_ROOT) / TRANSFORM_TOKENIZED_SUBDIR

            # Data backup modes (local/S3) come from config.yaml.
            data_backup_config = self.config.data_backup

            # Build the transformation config object using explicit values.
            config_obj = DataTransformationConfig(
                root_dir=root_dir,
                tokenized_dataset_dir=tokenized_dataset_dir,
                dvc_tokenized_dataset_dir=dvc_tokenized_dataset_dir,
                tokenizer_name=params.tokenizer.pretrained_model_name,
                max_input_length=params.tokenizer.max_input_length,
                max_target_length=params.tokenizer.max_target_length,
                local_enabled=data_backup_config.local_enabled,
                s3_enabled=data_backup_config.s3_enabled,
            )

            logger.info("DataTransformationConfig created.")
            return config_obj
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to create data transformation configuration.")
            raise TextSummarizerError(e, logger) from e

    def get_model_trainer_config(self) -> ModelTrainerConfig:
        """Build and return the configuration for the model trainer.

        Returns:
            ModelTrainerConfig: Structured trainer configuration.

        Raises:
            TextSummarizerError: If configuration construction fails.
        """
        try:
            logger.info("Creating model trainer configuration.")

            # Static trainer settings (model checkpoint) from config.yaml.
            config = self.config.model_trainer

            # Training hyperparameters come from params.yaml.
            params = self.params.model_trainer.training_arguments

            # Run-scoped trainer root; holds working model/tokenizer outputs.
            root_dir = self.artifacts_root / TRAINER_ROOT
            model_dir = root_dir / TRAINER_MODEL_SUBDIR
            tokenizer_dir = root_dir / TRAINER_TOKENIZER_SUBDIR

            # Final promoted locations: stable paths that inference references.
            final_model_dir = Path(FINAL_MODEL_ROOT) / FINAL_MODEL_SUBDIR
            final_tokenizer_dir = Path(FINAL_MODEL_ROOT) / FINAL_TOKENIZER_SUBDIR

            # Data backup (local/S3) flags.
            data_backup_config = self.config.data_backup

            # Build the trainer config with explicit, typed fields.
            config_obj = ModelTrainerConfig(
                root_dir=root_dir,
                model_ckpt=config.model_ckpt,
                num_train_epochs=params.num_train_epochs,
                warmup_steps=params.warmup_steps,
                per_device_train_batch_size=params.per_device_train_batch_size,
                per_device_eval_batch_size=params.per_device_eval_batch_size,
                weight_decay=params.weight_decay,
                logging_steps=params.logging_steps,
                learning_rate=params.learning_rate,
                eval_strategy=params.eval_strategy,
                eval_steps=params.eval_steps,
                save_steps=params.save_steps,
                gradient_accumulation_steps=params.gradient_accumulation_steps,
                local_enabled=data_backup_config.local_enabled,
                s3_enabled=data_backup_config.s3_enabled,
                final_model_dir=final_model_dir,
                final_tokenizer_dir=final_tokenizer_dir,
                model_dir=model_dir,
                tokenizer_dir=tokenizer_dir,
            )

            logger.info("ModelTrainerConfig created.")
            return config_obj
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to create model trainer configuration.")
            raise TextSummarizerError(e, logger) from e

    def get_model_evaluation_config(self) -> ModelEvaluationConfig:
        """Build and return the configuration for model evaluation.

        Returns:
            ModelEvaluationConfig: Structured evaluation configuration.

        Raises:
            TextSummarizerError: If configuration construction fails.
        """
        try:
            logger.info("Creating model evaluation configuration.")

            # Eval sinks come from config.yaml; dynamic params from params.yaml.
            evaluation_config = self.config.model_evaluation
            eval_params = self.params.model_evaluation

            # Tokenizer-derived limits ensure evaluation matches preprocessing.
            tokenizer_params = self.params.data_transformation.tokenizer

            # Reuse generation defaults from prediction to avoid drift.
            pred_params = self.params.prediction

            # Run-scoped evaluation root and the report file path.
            root_dir = self.artifacts_root / EVALUATION_ROOT
            eval_report_filepath = (
                root_dir / EVALUATION_REPORT_SUBDIR / evaluation_config.report_filename
            )

            # Backup targets (local/S3) flags.
            data_backup_config = self.config.data_backup

            # Assemble the evaluation config object.
            config_obj = ModelEvaluationConfig(
                root_dir=root_dir,
                eval_report_filepath=eval_report_filepath,
                eval_params=eval_params,
                max_input_length=tokenizer_params.max_input_length,
                max_target_length=tokenizer_params.max_target_length,
                length_penalty=pred_params.length_penalty,
                num_beams=pred_params.num_beams,
                local_enabled=data_backup_config.local_enabled,
                s3_enabled=data_backup_config.s3_enabled,
            )

            logger.info("ModelEvaluationConfig created.")
            return config_obj
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to create model evaluation configuration.")
            raise TextSummarizerError(e, logger) from e

    def get_prediction_config(self) -> PredictionConfig:
        """Build and return the configuration for the prediction component.

        Returns:
            PredictionConfig: Structured prediction configuration.

        Raises:
            TextSummarizerError: If configuration construction fails.
        """
        try:
            logger.info("Creating prediction configuration.")

            # Stable final artifact locations for inference consumption.
            root_dir = Path(PREDICTION_ROOT)
            model_dir = Path(FINAL_MODEL_ROOT) / FINAL_MODEL_SUBDIR
            tokenizer_dir = Path(FINAL_MODEL_ROOT) / FINAL_TOKENIZER_SUBDIR

            # Backup flags (local/S3).
            data_backup_config = self.config.data_backup

            # Derive generation parameters from params.yaml.
            transform_params = self.params.data_transformation
            prediction_params = self.params.prediction

            # Build the prediction config using explicit fields.
            config_obj = PredictionConfig(
                root_dir=root_dir,
                model_dir=model_dir,
                tokenizer_dir=tokenizer_dir,
                local_enabled=data_backup_config.local_enabled,
                s3_enabled=data_backup_config.s3_enabled,
                max_input_length=transform_params.tokenizer.max_input_length,
                max_target_length=transform_params.tokenizer.max_target_length,
                num_beams=prediction_params.num_beams,
                length_penalty=prediction_params.length_penalty,
                no_repeat_ngram_size=prediction_params.no_repeat_ngram_size,
                batch_size=prediction_params.batch_size,
                early_stopping=prediction_params.early_stopping,
                device=prediction_params.device,
            )

            logger.info("PredictionConfig created.")
            return config_obj
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to create prediction configuration.")
            raise TextSummarizerError(e, logger) from e
