"""Prediction component for the text summarization pipeline.

Runs batch predictions using a config-driven approach:
- Prefer local model/tokenizer if paths exist.
- If local artifacts are absent, download them from S3 using the injected
  ``DBHandler`` and temporary directories.
- Accept inputs as an iterable of strings, a pandas Series, or a DataFrame
  column, then return a DataFrame of texts and summaries.
- Save predictions locally and/or to S3 based on configuration.

Design intent:
- Keep orchestration here and delegate model I/O to ``TextSummarizerModel`` and
  cloud operations to the injected ``DBHandler``. This separation reduces
  complexity and improves testability.
- Provide production-grade, contextual logging around each stage so failures are
  easy to diagnose.
- Wrap all failures in ``TextSummarizerError`` to surface consistent,
  project-specific exceptions to callers.
"""

from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd

from src.textsummarizer.dbhandler.base_handler import DBHandler
from src.textsummarizer.entity.config_entity import PredictionConfig
from src.textsummarizer.exception.exception import TextSummarizerError
from src.textsummarizer.inference.estimator import TextSummarizerModel
from src.textsummarizer.logging import logger


class ModelPrediction:
    """Config-driven prediction runner.

    Responsibilities:
    - Resolve model/tokenizer locations (local-first, optional S3 fallback).
    - Normalize inputs (list/Series/DataFrame) into strings to summarize.
    - Run batched generation through ``TextSummarizerModel``.
    - Persist predictions to local/S3 sinks per configuration.
    """

    def __init__(
        self,
        prediction_config: PredictionConfig,
        backup_handler: DBHandler | None = None,
    ) -> None:
        """Initialize the prediction runner.

        Args:
            prediction_config (PredictionConfig): Configuration controlling
                artifact locations, generation params, and sinks.
            backup_handler (DBHandler | None): Optional handler for S3 I/O.

        Returns:
            None

        Raises:
            TextSummarizerError: If initialization fails for any reason.
        """
        try:
            logger.info("Initializing ModelPrediction component.")
            self.prediction_config = prediction_config
            self.backup_handler = backup_handler

            # Timestamp is used to make outputs idempotent and auditable; each
            # run writes to a unique subfolder (both local and S3).
            self.timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

            # Temporary directories used only when we pull artifacts from S3.
            # We keep handles on the instance so we can clean them up in __del__.
            self._tmp_model_dir: TemporaryDirectory[str] | None = None
            self._tmp_tok_dir: TemporaryDirectory[str] | None = None

            # Create the underlying summarizer instance (local-first load).
            self.summarizer: TextSummarizerModel
            self._init_summarizer()
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to initialize ModelPrediction.")
            raise TextSummarizerError(e, logger) from e

    def __del__(self) -> None:
        """Best-effort cleanup of any temporary directories used for S3 pulls.

        Returns:
            None
        """
        # We intentionally swallow any cleanup errors — shutdown should be quiet.
        try:
            if self._tmp_model_dir is not None:
                self._tmp_model_dir.cleanup()
            if self._tmp_tok_dir is not None:
                self._tmp_tok_dir.cleanup()
        except Exception:  # noqa: BLE001
            # No logging here to avoid noisy destructor-time logs.
            pass
        return None

    # ---------------------------------------------------------------------
    # Model/tokenizer loading (local-first, optional S3 fallback)
    # ---------------------------------------------------------------------
    def _init_summarizer(self) -> None:
        """Instantiate ``TextSummarizerModel`` from local or S3 sources.

        Resolution order:
        1) Use local directories if present (fastest, cheapest).
        2) If local is unavailable and S3 is enabled, download both artifacts
           to temporary directories, then load from those paths.

        Returns:
            None

        Raises:
            TextSummarizerError: If artifacts cannot be resolved or loaded.
        """
        try:
            pc = self.prediction_config

            # We require an explicit device (e.g., "cuda" or "cpu") to make the
            # compute target obvious in logs and configuration.
            if not pc.device:
                raise TextSummarizerError(
                    "`device` must be provided in PredictionConfig.",
                    logger,
                )

            # Determine if local artifacts are usable. We guard against None
            # and verify the directories exist to avoid cryptic load errors.
            local_model_ok = pc.model_dir is not None and Path(pc.model_dir).exists()
            local_tok_ok = (
                pc.tokenizer_dir is not None and Path(pc.tokenizer_dir).exists()
            )

            if local_model_ok and local_tok_ok:
                # Happy path: everything is already on disk.
                logger.info(
                    "Loading summarizer from local paths: model=%s, tokenizer=%s",
                    Path(pc.model_dir).as_posix(),
                    Path(pc.tokenizer_dir).as_posix(),
                )
                self.summarizer = TextSummarizerModel(
                    model_dir=Path(pc.model_dir),
                    tokenizer_dir=Path(pc.tokenizer_dir),
                    device=pc.device,
                    max_input_length=pc.max_input_length,
                    max_target_length=pc.max_target_length,
                    num_beams=pc.num_beams,
                    length_penalty=pc.length_penalty,
                    no_repeat_ngram_size=pc.no_repeat_ngram_size,
                    early_stopping=pc.early_stopping,
                )
                logger.info("Summarizer loaded from local directories.")
                return None

            logger.info(
                "Local model/tokenizer not found — attempting S3 load if configured."
            )

            # S3 fallback requires: flag enabled, handler present, and both keys.
            if not pc.s3_enabled:
                raise TextSummarizerError(
                    "Local model/tokenizer not available and s3_enabled=False.",
                    logger,
                )
            if self.backup_handler is None:
                raise TextSummarizerError(
                    "S3 loading requested but backup_handler is None.",
                    logger,
                )
            if pc.model_s3_key is None or pc.tokenizer_s3_key is None:
                raise TextSummarizerError(
                    "S3 loading requested but model_s3_key/tokenizer_s3_key "
                    "not set in PredictionConfig.",
                    logger,
                )

            # Allocate temp directories so downloads do not pollute the repo and
            # to ensure a clean lifecycle (deleted in __del__).
            self._tmp_model_dir = TemporaryDirectory()
            self._tmp_tok_dir = TemporaryDirectory()
            tmp_model_path = Path(self._tmp_model_dir.name)
            tmp_tok_path = Path(self._tmp_tok_dir.name)

            # Stream directories from S3 into the temporary locations.
            with self.backup_handler as handler:
                logger.info("Downloading model dir from S3 key: %s", pc.model_s3_key)
                handler.download_dir(pc.model_s3_key, tmp_model_path)
                logger.info(
                    "Downloading tokenizer dir from S3 key: %s", pc.tokenizer_s3_key
                )
                handler.download_dir(pc.tokenizer_s3_key, tmp_tok_path)

            # Load the summarizer from the freshly downloaded directories.
            self.summarizer = TextSummarizerModel(
                model_dir=tmp_model_path,
                tokenizer_dir=tmp_tok_path,
                device=pc.device,
                max_input_length=pc.max_input_length,
                max_target_length=pc.max_target_length,
                num_beams=pc.num_beams,
                length_penalty=pc.length_penalty,
                no_repeat_ngram_size=pc.no_repeat_ngram_size,
                early_stopping=pc.early_stopping,
            )
            logger.info("Summarizer loaded from S3 into temporary directories.")
            return None

        except Exception as e:  # noqa: BLE001
            logger.error("Failed to initialize summarizer.")
            raise TextSummarizerError(e, logger) from e

    # ---------------------------------------------------------------------
    # Prediction
    # ---------------------------------------------------------------------
    def predict(
        self,
        inputs: pd.DataFrame | pd.Series | list[str],
        text_column: str | None = None,
    ) -> pd.DataFrame:
        """Run batch predictions and return a DataFrame of results.

        Input normalization rules:
        - DataFrame: use ``text_column`` parameter if provided; otherwise use
          ``PredictionConfig.text_column``. Column must exist.
        - Series: values are treated as input texts.
        - list[str]: values are treated as input texts.

        Args:
            inputs (DataFrame | Series | list[str]): Inputs to summarize.
            text_column (str | None): Optional column name (for DataFrame input).

        Returns:
            pd.DataFrame: Two columns: ``text`` and ``summary``.

        Raises:
            TextSummarizerError: If input types are invalid or inference fails.
        """
        try:
            pc = self.prediction_config

            # Normalize inputs into a list[str]. Having a single canonical format
            # simplifies batching logic and error handling below.
            if isinstance(inputs, pd.DataFrame):
                # Caller-provided column overrides config; otherwise use config.
                col = text_column or pc.text_column
                if not col:
                    raise TextSummarizerError(
                        "When passing a DataFrame, 'text_column' must be provided "
                        "or set in PredictionConfig.text_column.",
                        logger,
                    )
                if col not in inputs.columns:
                    raise TextSummarizerError(
                        f"Column '{col}' not found in DataFrame.", logger
                    )
                texts = inputs[col].astype(str).tolist()
            elif isinstance(inputs, pd.Series):
                texts = inputs.astype(str).tolist()
            elif isinstance(inputs, list):
                texts = [str(x) for x in inputs]
            else:
                raise TextSummarizerError(
                    "inputs must be a DataFrame, Series, or list[str].",
                    logger,
                )

            if not texts:
                raise TextSummarizerError("No input texts provided.", logger)

            # Process in batches to avoid OOM on long inputs and to keep
            # inference latency predictable across large datasets.
            summaries: list[str] = []
            bs = int(pc.batch_size)
            for i in range(0, len(texts), bs):
                chunk = texts[i : i + bs]
                summaries.extend(self.summarizer.batch_predict(chunk))

            # Return a tidy DataFrame for downstream storage or analysis.
            df = pd.DataFrame({"text": texts, "summary": summaries})
            logger.info("Generated %d summaries.", len(df))
            return df

        except Exception as e:  # noqa: BLE001
            logger.error("Prediction failed.")
            raise TextSummarizerError(e, logger) from e

    # ---------------------------------------------------------------------
    # Save predictions
    # ---------------------------------------------------------------------
    def save_predictions(self, df: pd.DataFrame) -> tuple[Path | None, str | None]:
        """Persist predictions to local disk and/or S3.

        Local write:
            <root_dir>/<timestamp>/predictions.csv

        S3 write:
            <root_s3_key>/<timestamp>/predictions.csv

        Args:
            df (pd.DataFrame): Predictions with columns ``text`` and ``summary``.

        Returns:
            tuple[Path | None, str | None]: (local_path, s3_key_or_uri)

        Raises:
            TextSummarizerError: If saving fails or configuration is incomplete.
        """
        try:
            pc = self.prediction_config
            local_path: Path | None = None
            s3_key_or_uri: str | None = None
            filename = "predictions.csv"

            # -----------------------------
            # Local save (idempotent paths)
            # -----------------------------
            if pc.local_enabled:
                # Create a distinct timestamp folder for every run so old outputs
                # are never clobbered and are easy to audit.
                out_dir = pc.root_dir / self.timestamp
                out_dir.mkdir(parents=True, exist_ok=True)
                local_path = out_dir / filename

                # Use explicit UTF-8 to avoid platform-dependent defaults.
                df.to_csv(local_path, index=False, encoding="utf-8")
                logger.info("Predictions saved locally at %s", local_path.as_posix())

            # -----------------------------
            # S3 save (streaming write)
            # -----------------------------
            if pc.s3_enabled:
                if self.backup_handler is None:
                    raise TextSummarizerError(
                        "S3 save enabled but backup_handler is None.",
                        logger,
                    )
                # We expect a configured S3 “root” prefix; do not invent a
                # fallback path. If it is missing, bail with a clear message.
                if not pc.root_s3_key:
                    raise TextSummarizerError(
                        "`root_s3_key` must be set in PredictionConfig when "
                        "s3_enabled=True.",
                        logger,
                    )
                # Compose the final S3 key under the timestamped subfolder.
                s3_key = f"{pc.root_s3_key.rstrip('/')}/{self.timestamp}/{filename}"
                with self.backup_handler as handler:
                    handler.stream_df_as_csv(df, s3_key)
                s3_key_or_uri = s3_key
                logger.info("Predictions saved to S3 at %s", s3_key)

            return local_path, s3_key_or_uri

        except Exception as e:  # noqa: BLE001
            logger.error("Failed to save predictions.")
            raise TextSummarizerError(e, logger) from e
