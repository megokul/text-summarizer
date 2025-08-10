"""Model evaluation component for the text summarization pipeline.

Evaluates a seq2seq model on the tokenized **test** split using configurable
metrics (e.g., ROUGE), and persists a concise YAML report locally and/or to S3.

Design intent:
- Orchestrate evaluation while delegating I/O to Hugging Face libraries and the
  injected ``DBHandler`` for cloud operations. This keeps concerns separated,
  reduces complexity, and improves testability.
- Provide detailed, contextual logging around each stage (load → infer →
  aggregate → save). We log the intent before acting and the outcome after,
  which simplifies diagnosing failures in production.
- Wrap all failures in the project-specific ``TextSummarizerError`` to deliver
  consistent error semantics to callers.
- Return a predictable artifact with both local and remote report locations
  (or ``None`` when disabled) so downstream code can branch deterministically.
"""

from pathlib import Path
import tempfile

import evaluate
import pandas as pd
import torch
from datasets import DatasetDict, load_from_disk
from tqdm import tqdm
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

from src.textsummarizer.dbhandler.base_handler import DBHandler
from src.textsummarizer.entity.artifact_entity import (
    DataTransformationArtifact,
    ModelEvaluationArtifact,
    ModelTrainerArtifact,
)
from src.textsummarizer.entity.config_entity import ModelEvaluationConfig
from src.textsummarizer.exception.exception import TextSummarizerError
from src.textsummarizer.logging import logger
from src.textsummarizer.utils.core import save_to_yaml


class ModelEvaluation:
    """Coordinate model evaluation and persist a compact metric report.

    Responsibilities:
        - Load tokenizer/model from local disk or S3 (via the injected handler).
        - Load the tokenized dataset (prefer local; S3 fallback).
        - Generate predictions in batches to avoid OOM.
        - Compute metrics using the ``evaluate`` library.
        - Save a YAML report to configured sinks.
    """

    def __init__(
        self,
        config: ModelEvaluationConfig,
        trainer_artifact: ModelTrainerArtifact,
        transformation_artifact: DataTransformationArtifact | None = None,
        backup_handler: DBHandler | None = None,
    ) -> None:
        """Initialize the evaluator with configuration and inputs.

        Args:
            config (ModelEvaluationConfig): Evaluation config with sinks and a
                namespaced ``eval_params`` object (ConfigBox-like).
            trainer_artifact (ModelTrainerArtifact): Paths/URIs to the trained
                model and tokenizer artifacts.
            transformation_artifact (DataTransformationArtifact | None): Optional
                dataset locations produced by the transformation step.
            backup_handler (DBHandler | None): Optional handler for S3 I/O.

        Returns:
            None
        """
        # Persist references for reuse across helper methods; this reduces the
        # need to pass many parameters around and keeps call sites clean.
        self.eval_config = config
        self.trainer_artifact = trainer_artifact
        self.transformation_artifact = transformation_artifact
        self.backup_handler = backup_handler

        # Store evaluation parameters as a single namespaced object. Per request,
        # we access all fields as attributes, e.g. `self.params.batch_size`.
        # No getattr() or dict .get() fallbacks.
        self.params = self.eval_config.eval_params

    # ---------------------------------------------------------------------
    # Loaders: tokenizer / model / dataset (local first, then S3 if enabled)
    # ---------------------------------------------------------------------
    def _load_tokenizer(self) -> AutoTokenizer:
        """Load a tokenizer from local disk or download it from S3.

        Returns:
            AutoTokenizer: The loaded tokenizer.

        Raises:
            TextSummarizerError: If no valid location is configured or load fails.
        """
        try:
            # Prefer local: avoids network I/O, cheaper and faster in most cases.
            if self.eval_config.local_enabled and self.trainer_artifact.tokenizer_dir:
                tokenizer_path = self.trainer_artifact.tokenizer_dir
                logger.info("Loading tokenizer (local): %s", tokenizer_path)

                # Guard against misconfigured paths to avoid cryptic HF errors.
                if not tokenizer_path.exists():
                    raise TextSummarizerError(
                        f"Tokenizer dir not found at: {tokenizer_path}", logger
                    )
                return AutoTokenizer.from_pretrained(tokenizer_path)

            # Fall back to S3 when enabled and a handler is available.
            if (
                self.eval_config.s3_enabled
                and self.trainer_artifact.tokenizer_s3_uri
                and self.backup_handler
            ):
                tokenizer_s3_uri = self.trainer_artifact.tokenizer_s3_uri
                logger.info("Downloading tokenizer dir (S3): %s", tokenizer_s3_uri)

                # Use a temporary directory for isolation and automatic cleanup.
                with self.backup_handler as handler, tempfile.TemporaryDirectory() as td:  # noqa: E501
                    tmp_path = Path(td)
                    handler.download_dir(tokenizer_s3_uri, tmp_path)
                    logger.info("Tokenizer downloaded to temp dir. Loading...")
                    return AutoTokenizer.from_pretrained(tmp_path.as_posix())

            # If neither sink provided a path/URI, we cannot proceed.
            raise TextSummarizerError(
                "No valid tokenizer location found for loading.", logger
            )

        except Exception as e:  # noqa: BLE001
            logger.error("Failed to load tokenizer.")
            raise TextSummarizerError(e, logger) from e

    def _load_model(self, device: str) -> AutoModelForSeq2SeqLM:
        """Load a model from local disk or download it from S3.

        Args:
            device (str): Target device (``'cuda'`` or ``'cpu'``).

        Returns:
            AutoModelForSeq2SeqLM: The loaded model on the requested device.

        Raises:
            TextSummarizerError: If no valid location is configured or load fails.
        """
        try:
            # Prefer local model for the same reasons as tokenizer.
            if self.eval_config.local_enabled and self.trainer_artifact.trained_model_dir:  # noqa: E501
                model_path = Path(self.trainer_artifact.trained_model_dir)
                logger.info("Loading model (local): %s", model_path)

                if not model_path.exists():
                    raise TextSummarizerError(
                        f"Model dir not found at: {model_path}", logger
                    )
                return AutoModelForSeq2SeqLM.from_pretrained(
                    model_path
                ).to(device)

            # Otherwise, download the model directory from S3 to a temp folder.
            if (
                self.eval_config.s3_enabled
                and self.trainer_artifact.model_s3_uri
                and self.backup_handler
            ):
                model_s3_uri = self.trainer_artifact.model_s3_uri
                logger.info("Downloading model dir (S3): %s", model_s3_uri)

                with self.backup_handler as handler, tempfile.TemporaryDirectory() as td:  # noqa: E501
                    tmp_path = Path(td)
                    handler.download_dir(model_s3_uri, tmp_path)
                    logger.info("Model downloaded to temp dir. Loading...")
                    return AutoModelForSeq2SeqLM.from_pretrained(
                        tmp_path.as_posix()
                    ).to(device)

            raise TextSummarizerError(
                "No valid model location found for loading.", logger
            )

        except Exception as e:  # noqa: BLE001
            logger.error("Failed to load model.")
            raise TextSummarizerError(e, logger) from e

    def _load_dataset(self) -> DatasetDict:
        """Load the tokenized dataset (we read the test split for evaluation).

        Returns:
            DatasetDict: Dataset dictionary containing at least a ``test`` split.

        Raises:
            TextSummarizerError: If the dataset is missing or malformed.
        """
        try:
            # Prefer local for latency/cost reasons.
            if (
                self.eval_config.local_enabled
                and self.transformation_artifact
                and self.transformation_artifact.tokenized_dataset_dir
            ):
                dataset_path = Path(
                    self.transformation_artifact.tokenized_dataset_dir
                )
                logger.info("Loading DatasetDict (local): %s", dataset_path)

                # Hugging Face persists a sentinel file; check it for integrity.
                if not dataset_path.exists() or not (
                    dataset_path / "dataset_dict.json"
                ).exists():
                    raise TextSummarizerError(
                        f"Expected DatasetDict structure not found at: {dataset_path}",  # noqa: E501
                        logger,
                    )

                # Use a file:// URI for OS-agnostic behavior across platforms.
                dataset = load_from_disk("file://" + dataset_path.as_posix())
                if not isinstance(dataset, DatasetDict):
                    raise TextSummarizerError(
                        f"Loaded dataset is not a DatasetDict: {type(dataset)}",
                        logger,
                    )
                logger.info("Loaded dataset from local disk.")
                return dataset

            # S3 fallback if configured; download entire dataset directory first.
            if (
                self.eval_config.s3_enabled
                and self.transformation_artifact
                and self.transformation_artifact.tokenized_dataset_s3_uri
                and self.backup_handler
            ):
                s3_uri = self.transformation_artifact.tokenized_dataset_s3_uri
                logger.info("Downloading DatasetDict (S3): %s", s3_uri)

                with self.backup_handler as handler, tempfile.TemporaryDirectory() as td:  # noqa: E501
                    tmp_path = Path(td)
                    handler.download_dir(s3_uri, tmp_path)
                    logger.info("Dataset downloaded to temp dir. Loading...")
                    dataset = load_from_disk(tmp_path.as_posix())

                    if not isinstance(dataset, DatasetDict):
                        raise TextSummarizerError(
                            f"S3-loaded dataset is not a DatasetDict: {type(dataset)}",
                            logger,
                        )
                    logger.info("Loaded dataset from temp S3 dir.")
                    return dataset

            # No viable source → error out with context for the caller.
            raise TextSummarizerError(
                "No valid dataset location found for loading.", logger
            )

        except Exception as e:  # noqa: BLE001
            logger.error("Failed to load dataset.")
            raise TextSummarizerError(e, logger) from e

    # ---------------------------------------------------------------------
    # Metric calculation helpers
    # ---------------------------------------------------------------------
    def _generate_batch_chunks(self, data: list, batch_size: int) -> list:
        """Yield contiguous slices of ``data`` of size ``batch_size``.

        We stream batches for stable memory and for progress reporting with
        ``tqdm``. This keeps evaluation reliable on modest accelerators.

        Args:
            data (list): Sequence to be chunked (e.g., list of strings).
            batch_size (int): Maximum chunk size.

        Returns:
            list: A generator of data slices.

        Raises:
            None
        """
        # Iterate by fixed-size windows; the final chunk may be shorter.
        for i in range(0, len(data), batch_size):
            yield data[i : i + batch_size]
        # Explicit "no value" return per project convention.
        return None

    def _get_metric(self, name: str) -> tuple[object, dict[str, object]]:
        """Load a metric by name with options from config.

        Args:
            name (str): Metric name recognized by the ``evaluate`` library.

        Returns:
            tuple[object, dict[str, object]]: The loaded metric and its options.

        Raises:
            TextSummarizerError: If the metric cannot be loaded.
        """
        try:
            # Pull per-metric options directly from the namespaced params object.
            options: dict[str, object] = self.params.metric_options.get(name, {})  # type: ignore[attr-defined]  # noqa: E501
            logger.info("Loading metric: %s with options: %s", name, options)

            # Metrics (e.g., "rouge") are fetched from the `evaluate` registry.
            metric = evaluate.load(name)
            return metric, options

        except Exception as e:  # noqa: BLE001
            logger.error("Failed to load metric: %s", name)
            raise TextSummarizerError(e, logger) from e

    def _calculate_metrics(
        self,
        dataset,
        metric,
        metric_options: dict[str, object],
        model: AutoModelForSeq2SeqLM,
        tokenizer: AutoTokenizer,
        batch_size: int,
        device: str,
        column_text: str,
        column_summary: str,
    ) -> dict[str, object] | float:
        """Compute metric scores on the provided dataset split.

        We evaluate in small batches to fit device memory and to show progress.
        Generation uses parameters provided via ``self.params``.

        Args:
            dataset: HF split-like object (indexable) with text fields.
            metric: An instance returned by ``evaluate.load(name)``.
            metric_options (dict[str, object]): Options for ``metric.compute``.
            model (AutoModelForSeq2SeqLM): Model to evaluate.
            tokenizer (AutoTokenizer): Paired tokenizer.
            batch_size (int): Batch size for generation.
            device (str): Device on which to run inference.
            column_text (str): Source text column name.
            column_summary (str): Reference summary column name.

        Returns:
            dict[str, object] | float: Computed score(s) from the metric.

        Raises:
            TextSummarizerError: If generation or computation fails.
        """
        try:
            # Materialize lists once to avoid re-indexing the HF dataset many times.
            article_batches = list(
                self._generate_batch_chunks(dataset[column_text], batch_size)
            )
            target_batches = list(
                self._generate_batch_chunks(dataset[column_summary], batch_size)
            )
            logger.info(
                "Evaluating on %d batches (batch_size=%d)",
                len(article_batches),
                batch_size,
            )

            # Iterate in lockstep over inputs and references with a progress bar.
            for article_batch, target_batch in tqdm(
                zip(article_batches, target_batches),
                total=len(article_batches),
                desc="Evaluating batches",
            ):
                # Encode the source batch; padding='max_length' yields dense
                # tensors and avoids a custom collator here.
                inputs = tokenizer(
                    article_batch,
                    max_length=int(self.eval_config.max_input_length),  # type: ignore[attr-defined]  # noqa: E501
                    truncation=True,
                    padding="max_length",
                    return_tensors="pt",
                )

                # Run generation without autograd; move tensors to the target device.
                with torch.no_grad():
                    summaries = model.generate(
                        input_ids=inputs["input_ids"].to(device),
                        attention_mask=inputs["attention_mask"].to(device),
                        length_penalty=float(self.eval_config.length_penalty),  # type: ignore[attr-defined]  # noqa: E501
                        num_beams=int(self.eval_config.num_beams),  # type: ignore[attr-defined]  # noqa: E501
                        max_length=int(self.eval_config.max_target_length),  # type: ignore[attr-defined]  # noqa: E501
                    )

                # Decode predicted token IDs into strings, skipping special tokens.
                decoded_summaries = [
                    tokenizer.decode(
                        s, skip_special_tokens=True, clean_up_tokenization_spaces=True
                    )
                    for s in summaries
                ]

                # Normalize any pathological empty string to a single space; some
                # metrics are sensitive to empties.
                decoded_summaries = [d if d else " " for d in decoded_summaries]

                # Accumulate predictions and references for this batch.
                metric.add_batch(
                    predictions=decoded_summaries, references=target_batch
                )

            # Compute aggregate scores after all batches are processed.
            score = metric.compute(**metric_options)
            logger.info("Evaluation metrics computed: %s", score)
            return score

        except Exception as e:  # noqa: BLE001
            logger.error("Failed during metric computation.")
            raise TextSummarizerError(e, logger) from e

    # ---------------------------------------------------------------------
    # Report persistence
    # ---------------------------------------------------------------------
    def _save_report(self, df: pd.DataFrame) -> tuple[Path | None, str | None]:
        """Save evaluation report to local YAML and/or S3.

        We convert a single-row dataframe into a flat mapping to keep YAML
        concise. For multi-row dataframes, a ``dict[list]`` is used.

        Args:
            df (pd.DataFrame): Dataframe with metric columns.

        Returns:
            tuple[Path | None, str | None]: (local_yaml_path, s3_yaml_uri)

        Raises:
            TextSummarizerError: If saving fails for either sink.
        """
        # Convert the DataFrame into a compact, YAML-friendly structure.
        if len(df) == 1:
            report_dict: dict[str, object] = df.iloc[0].to_dict()
        else:
            report_dict = df.to_dict(orient="list")

        local_path: Path | None = None
        s3_uri: str | None = None

        try:
            # Local save: helper ensures parent directories exist and logs actions.
            if self.eval_config.local_enabled:
                local_path = Path(self.eval_config.eval_report_filepath)
                save_to_yaml(
                    report_dict,
                    local_path,
                    label="Model Evaluation Metrics (YAML)",
                )
                logger.info("Saved evaluation metrics YAML: %s", local_path)

            # S3 save: stream YAML to a key using the injected handler.
            if self.eval_config.s3_enabled:
                if self.backup_handler is None:
                    raise TextSummarizerError(
                        "S3 saving enabled but backup_handler is None.", logger
                    )
                s3_key = self.eval_config.eval_report_s3_key
                if not s3_key:
                    raise TextSummarizerError(
                        "S3 saving enabled but eval_report_s3_key is missing.",
                        logger,
                    )

                with self.backup_handler as handler:
                    s3_uri = handler.stream_yaml(report_dict, s3_key)
                    logger.info("Uploaded evaluation metrics YAML to S3: %s", s3_uri)

            return local_path, s3_uri

        except Exception as e:  # noqa: BLE001
            logger.error("Failed to save evaluation report.")
            raise TextSummarizerError(e, logger) from e

    # ---------------------------------------------------------------------
    # Main evaluation flow
    # ---------------------------------------------------------------------
    def run_evaluation(self) -> ModelEvaluationArtifact:
        """Execute the evaluation pipeline end-to-end.

        Steps:
            1) Load tokenizer and model.
            2) Load dataset and select the test split.
            3) Optionally subset for faster evaluation (per config).
            4) Compute each configured metric on generated summaries.
            5) Save a YAML report locally and/or to S3.
            6) Return a structured artifact with produced locations.

        Returns:
            ModelEvaluationArtifact: Paths/URIs to the saved evaluation report.

        Raises:
            TextSummarizerError: If any stage fails.
        """
        try:
            # Choose device based on CUDA availability; we log this for traceability.
            device = "cuda" if torch.cuda.is_available() else "cpu"
            logger.info("Device used for evaluation: %s", device)

            # Load model components and the dataset with the same conventions used
            # in other components for consistency.
            tokenizer = self._load_tokenizer()
            model = self._load_model(device=device)
            dataset = self._load_dataset()

            # Evaluate on the "test" split; log size to surface silent data issues.
            test_data = dataset["test"]
            logger.info("Test split loaded with %d samples.", len(test_data))

            # Optional subsetting for quick smoke tests or CI runs.
            if (
                self.params.subset_size is not None
                and self.params.subset_size > 0
            ):
                logger.info(
                    "Evaluating on subset: first %d samples.",
                    self.params.subset_size,
                )
                test_data = test_data.select(range(int(self.params.subset_size)))
            else:
                logger.info("Evaluating on entire test set.")

            # Compute metrics one-by-one; if one fails, logs identify which.
            all_scores: dict[str, object] = {}
            for metric_name in self.params.metrics:
                metric, metric_options = self._get_metric(metric_name)
                logger.info("Running evaluation for metric: %s", metric_name)

                score = self._calculate_metrics(
                    dataset=test_data,
                    metric=metric,
                    metric_options=metric_options,
                    model=model,
                    tokenizer=tokenizer,
                    batch_size=int(self.params.batch_size),
                    device=device,
                    column_text=str(self.params.column_text),
                    column_summary=str(self.params.column_summary),
                )

                # Normalize shapes: dict metrics → namespace keys; scalar ok as-is.
                if isinstance(score, dict):
                    for k, v in score.items():
                        key = f"{metric_name}_{k}" if k != metric_name else k
                        all_scores[key] = v
                else:
                    all_scores[metric_name] = score

            logger.info("Final evaluation scores: %s", all_scores)

            # Persist results and return a structured artifact for downstream steps.
            df = pd.DataFrame(all_scores, index=["model"])
            local_report_path, s3_report_uri = self._save_report(df)
            logger.info(
                "Evaluation report saved: local=%s, s3=%s",
                local_report_path,
                s3_report_uri,
            )

            return ModelEvaluationArtifact(
                eval_report_filepath=local_report_path,
                eval_report_s3_uri=s3_report_uri,
            )

        except Exception as e:  # noqa: BLE001
            logger.error("Model evaluation failed.")
            raise TextSummarizerError(e, logger) from e
