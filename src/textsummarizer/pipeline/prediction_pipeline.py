from pathlib import Path
from typing import List

import pandas as pd

from src.textsummarizer.config.configuration import ConfigurationManager
from src.textsummarizer.dbhandler.s3_handler import S3Handler
from src.textsummarizer.components.model_prediction import ModelPrediction
from src.textsummarizer.exception.exception import TextSummarizerError
from src.textsummarizer.logging import logger
from src.textsummarizer.utils.core import read_csv

from dotenv import load_dotenv
load_dotenv(override=True)

class PredictionPipeline:
    def __init__(self) -> None:
        try:
            logger.info("Initializing PredictionPipeline...")
            self.config_manager = ConfigurationManager()
        except Exception as e:
            # Keep the project’s error type
            raise TextSummarizerError(e, logger) from e

    def run_pipeline(
        self,
        input_texts: str | list[str] | None = None,  # <-- accept str or list[str]
        input_file: Path | None = None,
    ) -> list[str]:
        """
        Run prediction end-to-end:
          1) Load config + optional S3 handler
          2) Initialize ModelPrediction
          3) Resolve inputs (from string, list, or file)
          4) Predict summaries
          5) Save predictions via component (local/S3 per config)

        Returns:
          list[str]: generated summaries in order.
        """
        try:
            logger.info("========== Prediction Pipeline Started ==========")

            # 1) Load prediction configuration + S3 handler config
            prediction_config = self.config_manager.get_prediction_config()
            s3_config = self.config_manager.get_s3_handler_config()
            s3_handler = S3Handler(s3_config) if prediction_config.s3_enabled else None

            # 2) Initialize model predictor (this should load model/tokenizer per config)
            predictor = ModelPrediction(
                prediction_config=prediction_config,
                backup_handler=s3_handler,
            )

            # 3) Prepare input data (STRICTLY config-driven; no defaults)
            if input_texts is not None:
                logger.info("Using provided in-memory texts for prediction.")

                # Normalize to list[str] without exploding a single str into characters
                if isinstance(input_texts, str):
                    texts: List[str] = [input_texts]
                elif isinstance(input_texts, (list, tuple)):
                    texts = [str(t) for t in input_texts]
                else:
                    raise TextSummarizerError(
                        "input_texts must be a str or list[str].",
                        logger,
                    )

            elif input_file is not None:
                logger.info("Loading input texts from file: %s", input_file)
                df: pd.DataFrame = read_csv(Path(input_file))

                # Column name must be provided in params via PredictionConfig (no fallbacks)
                input_col = prediction_config.input_text_column  # must exist in config
                if input_col not in df.columns:
                    raise TextSummarizerError(
                        f"Configured input_text_column='{input_col}' not found in {input_file}.",
                        logger,
                    )
                texts = df[input_col].astype(str).tolist()

            else:
                raise TextSummarizerError("No input data provided for prediction.", logger)

            # Basic validation before calling model
            texts = [t if t is not None else "" for t in texts]
            empty_idx = [i for i, t in enumerate(texts) if not str(t).strip()]
            if empty_idx:
                # Fail early with actionable info
                raise TextSummarizerError(
                    f"Input contains {len(empty_idx)} empty text(s). Example indices: {empty_idx[:10]}",
                    logger,
                )

            # 4) Run prediction
            summaries = predictor.predict(texts)

            # 5) Save predictions (component handles local/S3 per config)
            predictor.save_predictions(summaries)

            logger.info("========== Prediction Pipeline Completed ==========")
            return summaries.loc[0, "summary"]

        except Exception as e:
            raise TextSummarizerError(e, logger) from e
