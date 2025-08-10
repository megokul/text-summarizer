# FILE: src/textsummarizer/pipeline/training_pipeline.py
"""End-to-end training pipeline orchestration.

This module wires together configuration loading, optional S3 integration,
data ingestion, transformation, model training, and evaluation.

# Design intent
- Config-driven orchestration: No hidden defaults; every component reads
  behavior from typed config entities produced by ``ConfigurationManager``.
- Backend-agnostic storage: Local-first with optional S3 mirroring, delegated
  to ``S3Handler`` so the pipeline logic stays clean.
- Robust logging and failures: Log meaningful progress and wrap all errors in
  the project-specific ``TextSummarizerError`` with context just before raising.
"""

from pathlib import Path  # noqa: F401 (kept for parity and future use)

# Third-party imports
from dotenv import load_dotenv
import mlflow
from dagshub import dagshub_logger  # noqa: F401 (kept for future logging scopes)
import dagshub

# Local imports
from src.textsummarizer.components.data_ingestion import DataIngestion
from src.textsummarizer.components.data_transformation import DataTransformation
from src.textsummarizer.components.model_evaluation import ModelEvaluation
from src.textsummarizer.components.model_trainer import ModelTrainer
from src.textsummarizer.config.configuration import ConfigurationManager
from src.textsummarizer.dbhandler.s3_handler import S3Handler
from src.textsummarizer.exception.exception import TextSummarizerError
from src.textsummarizer.logging import logger

# Ensure environment variables (e.g., AWS credentials, MLflow settings) are
# available before any code might rely on them.
load_dotenv(override=True)

# Initialize DagsHub MLflow integration *early* so MLflow hooks are active
# throughout the pipeline (e.g., Trainer's "report_to=mlflow").
# NOTE: This is a no-op if already initialized by the environment.
dagshub.init("text-summarizer", repo_owner="megokul")


class TrainingPipeline:
    """Orchestrate the full model training lifecycle.

    Responsibilities:
    - Construct component configs via ``ConfigurationManager``.
    - Initialize optional S3 handler (used by components for uploads/downloads).
    - Execute ingestion → transformation → training → evaluation in order.
    - Log progress at each stage and wrap failures into ``TextSummarizerError``.
    """

    def __init__(self) -> None:
        """Initialize the pipeline with a shared configuration manager.

        Returns:
            None

        Raises:
            TextSummarizerError: If configuration manager initialization fails.
        """
        try:
            logger.info("Initializing TrainingPipeline...")
            # The configuration manager also standardizes run timestamps so that
            # all artifacts land under a single, consistent run folder.
            self.config_manager = ConfigurationManager()
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to initialize TrainingPipeline.")
            raise TextSummarizerError(e, logger) from e
        return None

    def run_pipeline(self) -> None:
        """Execute the full training pipeline end-to-end.

        Steps:
            1) Build S3 handler (optional) based on configuration flags.
            2) Run data ingestion (download + extract + artifact creation).
            3) Run data transformation (tokenization + artifact creation).
            4) Run model training (optionally logs to MLflow).
            5) Run model evaluation (compute metrics and save report).

        Returns:
            None

        Raises:
            TextSummarizerError: If any stage of the pipeline fails.
        """
        try:
            logger.info("========== Training Pipeline Started ==========")

            # -----------------------------------------------------------------
            # Step 1: Setup S3 handler from configuration.
            # We keep S3 interactions behind a single handler to preserve a
            # backend-agnostic pipeline; local-only runs simply skip S3.
            # -----------------------------------------------------------------
            s3_config = self.config_manager.get_s3_handler_config()
            s3_handler = S3Handler(config=s3_config)

            # -----------------------------------------------------------------
            # Step 2: Data ingestion
            # Orchestrates source download and extraction (local and/or S3).
            # Produces a ``DataIngestionArtifact`` with concrete locations.
            # -----------------------------------------------------------------
            data_ingestion_config = self.config_manager.get_data_ingestion_config()
            data_ingestion = DataIngestion(
                config=data_ingestion_config,
                backup_handler=s3_handler,
            )
            data_ingestion_artifact = data_ingestion.run_ingestion()
            logger.info("Data Ingestion Artifact: %s", data_ingestion_artifact)

            # -----------------------------------------------------------------
            # Step 3: Data transformation
            # Converts raw dataset to tokenized Hugging Face datasets ready for
            # training/evaluation. Produces a transformation artifact.
            # -----------------------------------------------------------------
            data_transformation_config = (
                self.config_manager.get_data_transformation_config()
            )
            data_transformation = DataTransformation(
                config=data_transformation_config,
                ingestion_artifact=data_ingestion_artifact,
                backup_handler=s3_handler,
            )
            data_transformation_artifact = data_transformation.run_transformation()
            logger.info(
                "Data Transformation Artifact: %s",
                data_transformation_artifact,
            )

            # -----------------------------------------------------------------
            # Step 4: Model training
            # Trains a seq2seq model using HF Trainer. If configured, metrics
            # and parameters are reported to MLflow.
            # -----------------------------------------------------------------
            model_trainer_config = self.config_manager.get_model_trainer_config()
            model_trainer = ModelTrainer(
                config=model_trainer_config,
                transformation_artifact=data_transformation_artifact,
                backup_handler=s3_handler,
            )
            model_trainer_artifact = model_trainer.train()
            logger.info("Model Trainer Artifact: %s", model_trainer_artifact)

            # -----------------------------------------------------------------
            # Step 5: Model evaluation
            # Computes metrics (e.g., ROUGE) over the test split and persists a
            # YAML report locally and/or to S3.
            # -----------------------------------------------------------------
            model_evaluation_config = (
                self.config_manager.get_model_evaluation_config()
            )
            model_evaluation = ModelEvaluation(
                config=model_evaluation_config,
                trainer_artifact=model_trainer_artifact,
                transformation_artifact=data_transformation_artifact,
                backup_handler=s3_handler,
            )
            model_evaluation_artifact = model_evaluation.run_evaluation()
            logger.info(
                "Model Evaluation Artifact: %s", model_evaluation_artifact
            )

            logger.info("========== Training Pipeline Completed ==========")
            return None

        except Exception as e:  # noqa: BLE001
            logger.error("TrainingPipeline failed.")
            raise TextSummarizerError(e, logger) from e
