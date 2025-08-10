# FILE: src/textsummarizer/pipeline/prediction_pipeline.py
"""End-to-end prediction pipeline for text summarization.

This module wires together configuration loading, optional S3 integration,
model/tokenizer initialization, input resolution (string/list/CSV), batch
prediction, and result persistence.

# Design intent
- Config-driven: No hidden defaults. All behavior flows from configuration
  objects constructed by ``ConfigurationManager``.
- Backend-agnostic: Local-first with optional S3 support, delegated to
  ``S3Handler`` and the ``ModelPrediction`` component.
- Reliability: All failures are logged with context and re-raised as the
  project-specific ``TextSummarizerError`` to keep error handling uniform.
"""

from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from src.textsummarizer.components.model_prediction import ModelPrediction
from src.textsummarizer.config.configuration import ConfigurationManager
from src.textsummarizer.dbhandler.s3_handler import S3Handler
from src.textsummarizer.exception.exception import TextSummarizerError
from src.textsummarizer.logging import logger
from src.textsummarizer.utils.core import read_csv

# Load environment variables before any code may rely on them (e.g., AWS creds).
load_dotenv(override=True)


class PredictionPipeline:
    """Coordinate config, model init, input resolution, prediction, and saving.

    Typical usage:
        pipeline = PredictionPipeline()
        df = pipeline.run_pipeline(input_texts=["short dialogue...", ...])

    Args:
        None

    Raises:
        TextSummarizerError: If initialization fails.
    """

    def __init__(self) -> None:
        try:
            logger.info("Initializing PredictionPipeline...")
            # Build the configuration manager once; it caches a global timestamp
            # so all artifacts created during this process are grouped together.
            self.config_manager = ConfigurationManager()
        except Exception as e:  # noqa: BLE001
            # Wrap any failure in the project-specific exception with context.
            logger.error("Failed to initialize PredictionPipeline.")
            raise TextSummarizerError(e, logger) from e
        return None

    def run_pipeline(
        self,
        input_texts: str | list[str] | None = None,
        input_file: Path | None = None,
        text_column: str | None = None,
    ) -> pd.DataFrame:
        """Execute the full prediction flow and return a DataFrame of results.

        Steps:
            1) Build prediction + S3 configs.
            2) Initialize the ``ModelPrediction`` component.
            3) Resolve inputs (string/list *or* CSV file with a target column).
            4) Generate summaries via the component.
            5) Persist outputs (local/S3) per configuration.

        Exactly one of ``input_texts`` or ``input_file`` must be provided.

        Args:
            input_texts (str | list[str] | None): A single string or a list of
                strings to summarize. If a single string is provided, it is
                wrapped as a single-item list in order.
            input_file (Path | None): Path to a CSV containing input texts.
            text_column (str | None): Column name in ``input_file`` holding
                the texts. Required if ``input_file`` is provided.

        Returns:
            pd.DataFrame: Two columns: ``text`` (input) and ``summary`` (output).

        Raises:
            TextSummarizerError: If any stage fails.
        """
        try:
            logger.info("========== Prediction Pipeline Started ==========")

            # -----------------------------------------------------------------
            # 1) Load configuration and allocate optional S3 handler.
            # -----------------------------------------------------------------
            prediction_config = self.config_manager.get_prediction_config()
            s3_config = self.config_manager.get_s3_handler_config()

            # Only instantiate S3 handler when explicitly enabled. This keeps
            # dependencies (AWS creds) optional for pure-local runs.
            s3_handler = (
                S3Handler(s3_config) if prediction_config.s3_enabled else None
            )

            # -----------------------------------------------------------------
            # 2) Initialize the prediction component. This encapsulates
            #    model/tokenizer loading (local-first, S3 fallback).
            # -----------------------------------------------------------------
            predictor = ModelPrediction(
                prediction_config=prediction_config,
                backup_handler=s3_handler,
            )

            # -----------------------------------------------------------------
            # 3) Resolve input data source into a list[str].
            # -----------------------------------------------------------------
            if input_texts is not None and input_file is not None:
                # Disallow ambiguous usage; caller must choose one source.
                raise TextSummarizerError(
                    "Provide either `input_texts` or `input_file`, not both.",
                    logger,
                )

            if input_texts is not None:
                logger.info("Using provided in-memory texts for prediction.")
                # Normalize without exploding a single string into characters.
                if isinstance(input_texts, str):
                    texts = [input_texts]
                elif isinstance(input_texts, list | tuple):
                    texts = [str(t) for t in input_texts]
                else:
                    raise TextSummarizerError(
                        "`input_texts` must be str or list[str].", logger
                    )

            elif input_file is not None:
                logger.info(
                    "Loading input texts from CSV file: %s",
                    Path(input_file).as_posix(),
                )
                df: pd.DataFrame = read_csv(Path(input_file))

                # The column name must be explicit. We do not guess defaults
                # to avoid accidentally summarizing the wrong field.
                if not text_column:
                    raise TextSummarizerError(
                        "When `input_file` is provided, `text_column` is required.",
                        logger,
                    )
                if text_column not in df.columns:
                    raise TextSummarizerError(
                        (
                            f"Column '{text_column}' not found in file: "
                            f"{Path(input_file).as_posix()}. "
                            f"Available columns: {list(df.columns)}"
                        ),
                        logger,
                    )
                texts = df[text_column].astype(str).tolist()
            else:
                raise TextSummarizerError(
                    "No input data provided. Supply `input_texts` or `input_file`.",
                    logger,
                )

            # Guard against empty/blank items. This is a common data quality
            # issue and results in wasted generation calls.
            texts = [t if t is not None else "" for t in texts]
            empty_idx = [i for i, t in enumerate(texts) if not str(t).strip()]
            if empty_idx:
                raise TextSummarizerError(
                    (
                        f"Input contains {len(empty_idx)} empty text(s). "
                        f"Example indices: {empty_idx[:10]}"
                    ),
                    logger,
                )

            # -----------------------------------------------------------------
            # 4) Run prediction (component returns a DataFrame).
            # -----------------------------------------------------------------
            results_df = predictor.predict(texts)

            # -----------------------------------------------------------------
            # 5) Persist results per configuration (local and/or S3).
            # -----------------------------------------------------------------
            local_path, s3_key_or_uri = predictor.save_predictions(results_df)
            logger.info(
                "Predictions saved. local=%s, s3=%s",
                local_path.as_posix() if local_path else "None",
                s3_key_or_uri if s3_key_or_uri else "None",
            )

            logger.info("========== Prediction Pipeline Completed ==========")
            return results_df

        except Exception as e:  # noqa: BLE001
            logger.error("Prediction pipeline failed.")
            raise TextSummarizerError(e, logger) from e
