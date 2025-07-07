from src.textsummarizer.config.configuration import ConfigurationManager
from src.textsummarizer.exception.exception import TextSummarizerError
from src.textsummarizer.dbhandler.s3_handler import S3Handler
from src.textsummarizer.logging import logger

from src.textsummarizer.components.data_ingestion import DataIngestion
from src.textsummarizer.components.data_transformation import DataTransformation
# from src.textsummarizer.components.model_trainer import ModelTrainer
# from src.textsummarizer.components.model_evaluation import ModelEvaluation


class TrainingPipeline:
    """
    Orchestrates the entire model training process.
    """
    def __init__(self):
        try:
            logger.info("Initializing TrainingPipeline...")
            self.config_manager = ConfigurationManager()
        except Exception as e:
            raise TextSummarizerError(e, "Failed to initialize TrainingPipeline.") from e

    def run_pipeline(self):
        """
        Executes the full training pipeline from data ingestion to evaluation.
        """
        try:
            logger.info("========== Training Pipeline Started ==========")

            # Step 1: Setup configurations and S3 handler
            s3_config = self.config_manager.get_s3_handler_config()
            s3_handler = S3Handler(config=s3_config)

            # Step 2: Run data ingestion
            data_ingestion_config = self.config_manager.get_data_ingestion_config()
            data_ingestion = DataIngestion(
                ingestion_config=data_ingestion_config,
                backup_handler=s3_handler,
            )
            data_ingestion_artifact = data_ingestion.run_ingestion()
            logger.info(f"Data Ingestion Artifact: {data_ingestion_artifact}")

            # Step 3: Run data transformation
            if data_ingestion_artifact:
                data_transformation_config = self.config_manager.get_data_transformation_config()
                data_transformation = DataTransformation(
                    transformation_config=data_transformation_config,
                    ingestion_artifact=data_ingestion_artifact,
                )
                data_transformation_artifact = data_transformation.run_transformation()
                logger.info(f"Data Transformation Artifact: {data_transformation_artifact}")
            else:
                logger.warning("Data validation failed. Skipping subsequent steps.")
                return

            # # Step 5: Run model training
            # model_trainer_config = self.config_manager.get_model_trainer_config()
            # model_trainer = ModelTrainer(
            #     trainer_config=model_trainer_config,
            #     transformation_artifact=data_transformation_artifact,
            # )
            # model_trainer_artifact = model_trainer.run_training()
            # logger.info(f"Model Trainer Artifact: {model_trainer_artifact}")

            # # Step 6: Run model evaluation
            # model_evaluation_config = self.config_manager.get_model_evaluation_config()
            # model_evaluation = ModelEvaluation(
            #     evaluation_config=model_evaluation_config,
            #     trainer_artifact=model_trainer_artifact,
            #     transformation_artifact=data_transformation_artifact
            # )
            # model_evaluation_artifact = model_evaluation.run_evaluation()
            # logger.info(f"Model Evaluation Artifact: {model_evaluation_artifact}")

            logger.info("========== Training Pipeline Completed ==========")

        except Exception as e:
            raise TextSummarizerError(e, "TrainingPipeline failed.") from e
