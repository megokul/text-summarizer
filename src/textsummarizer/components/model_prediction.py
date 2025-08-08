from pathlib import Path
from datetime import datetime, timezone
from tempfile import TemporaryDirectory

import pandas as pd

from src.textsummarizer.entity.config_entity import PredictionConfig
from src.textsummarizer.dbhandler.base_handler import DBHandler
from src.textsummarizer.exception.exception import TextSummarizerError
from src.textsummarizer.logging import logger
from src.textsummarizer.inference.estimator import TextSummarizerModel


class ModelPrediction:
    """
    Config-driven prediction runner.
    - Prefer local model/tokenizer if paths exist.
    - If not found locally, download from S3 (requires s3 keys + backup_handler).
    - Predicts on iterable text / Series / DataFrame column.
    - Saves predictions to local and/or S3 per config.
    """

    def __init__(
        self,
        prediction_config: PredictionConfig,
        backup_handler: DBHandler | None = None,
    ) -> None:
        try:
            logger.info("Initializing ModelPrediction.")
            self.prediction_config = prediction_config
            self.backup_handler = backup_handler
            self.timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

            # temp dirs (only if we pull from S3)
            self._tmp_model_dir: TemporaryDirectory[str] | None = None
            self._tmp_tok_dir: TemporaryDirectory[str] | None = None

            self._init_summarizer()
        except Exception as e:
            logger.exception("Failed to initialize ModelPrediction.")
            raise TextSummarizerError(e, logger) from e

    def __del__(self) -> None:
        # best-effort cleanup of temp dirs (if used)
        try:
            if self._tmp_model_dir is not None:
                self._tmp_model_dir.cleanup()
            if self._tmp_tok_dir is not None:
                self._tmp_tok_dir.cleanup()
        except Exception:
            pass

    # ------------------- Load model/tokenizer (local-first) -------------------

    def _init_summarizer(self) -> None:
        try:
            pc = self.prediction_config

            if not hasattr(pc, "device") or not pc.device:
                raise TextSummarizerError("`device` must be provided in PredictionConfig.", logger)

            # 1) Try local if paths exist
            local_model_ok = pc.model_dir is not None and Path(pc.model_dir).exists()
            local_tok_ok = pc.tokenizer_dir is not None and Path(pc.tokenizer_dir).exists()

            if local_model_ok and local_tok_ok:
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
                return

            logger.info("Local model/tokenizer not found — attempting S3 load if configured.")

            # 2) Fall back to S3 if enabled + keys provided
            if not pc.s3_enabled:
                raise TextSummarizerError(
                    "Local model/tokenizer not available and s3_enabled=False.", logger
                )
            if self.backup_handler is None:
                raise TextSummarizerError(
                    "S3 loading requested but backup_handler is None.", logger
                )
            if pc.model_s3_key is None or pc.tokenizer_s3_key is None:
                raise TextSummarizerError(
                    "S3 loading requested but model_s3_key/tokenizer_s3_key not set in PredictionConfig.",
                    logger,
                )

            self._tmp_model_dir = TemporaryDirectory()
            self._tmp_tok_dir = TemporaryDirectory()
            tmp_model_path = Path(self._tmp_model_dir.name)
            tmp_tok_path = Path(self._tmp_tok_dir.name)

            with self.backup_handler as handler:
                logger.info("Downloading model dir from S3 key: %s", pc.model_s3_key)
                handler.download_dir(pc.model_s3_key, tmp_model_path)
                logger.info("Downloading tokenizer dir from S3 key: %s", pc.tokenizer_s3_key)
                handler.download_dir(pc.tokenizer_s3_key, tmp_tok_path)

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
        except Exception as e:
            logger.exception("Failed to initialize summarizer.")
            raise TextSummarizerError(e, logger) from e

    # ------------------- Prediction -------------------

    def predict(
        self,
        inputs: pd.DataFrame | pd.Series | list[str],
        text_column: str | None = None,
    ) -> pd.DataFrame:
        try:
            pc = self.prediction_config

            if isinstance(inputs, pd.DataFrame):
                col = text_column or getattr(pc, "text_column", None)
                if not col:
                    raise TextSummarizerError(
                        "When passing a DataFrame, 'text_column' must be provided "
                        "or set in PredictionConfig.text_column.",
                        logger,
                    )
                if col not in inputs.columns:
                    raise TextSummarizerError(f"Column '{col}' not found in DataFrame.", logger)
                texts = inputs[col].astype(str).tolist()
            elif isinstance(inputs, pd.Series):
                texts = inputs.astype(str).tolist()
            elif isinstance(inputs, list):
                texts = [str(x) for x in inputs]
            else:
                raise TextSummarizerError(
                    "inputs must be a DataFrame, Series, or list[str].", logger
                )

            if not texts:
                raise TextSummarizerError("No input texts provided.", logger)

            summaries: list[str] = []
            bs = pc.batch_size
            for i in range(0, len(texts), bs):
                chunk = texts[i : i + bs]
                summaries.extend(self.summarizer.batch_predict(chunk))

            df = pd.DataFrame({"text": texts, "summary": summaries})
            logger.info("Generated %d summaries.", len(df))
            return df
        except Exception as e:
            logger.exception("Prediction failed.")
            raise TextSummarizerError(e, logger) from e

    # ------------------- Save Predictions -------------------

    def save_predictions(self, df: pd.DataFrame) -> tuple[Path | None, str | None]:
        try:
            pc = self.prediction_config
            local_path: Path | None = None
            s3_key_or_uri: str | None = None
            filename = "predictions.csv"

            # Local save (if enabled)
            if pc.local_enabled:
                out_dir = pc.root_dir / self.timestamp
                out_dir.mkdir(parents=True, exist_ok=True)
                local_path = out_dir / filename
                df.to_csv(local_path, index=False, encoding="utf-8")
                logger.info("Predictions saved locally at %s", local_path.as_posix())

            # S3 save (if enabled)
            if pc.s3_enabled:
                if self.backup_handler is None:
                    raise TextSummarizerError(
                        "S3 save enabled but backup_handler is None.", logger
                    )
                if not getattr(pc, "root_s3_key", None):
                    raise TextSummarizerError(
                        "`root_s3_key` must be set in PredictionConfig when s3_enabled=True.",
                        logger,
                    )
                s3_key = f"{pc.root_s3_key.rstrip('/')}/{self.timestamp}/{filename}"
                with self.backup_handler as handler:
                    handler.stream_df_as_csv(df, s3_key)
                s3_key_or_uri = s3_key
                logger.info("Predictions saved to S3 at %s", s3_key)

            return local_path, s3_key_or_uri
        except Exception as e:
            logger.exception("Failed to save predictions.")
            raise TextSummarizerError(e, logger) from e
