import tempfile
from pathlib import Path
from typing import Optional, Any, Dict

import evaluate
import pandas as pd
import torch
from datasets import load_from_disk, DatasetDict
from tqdm import tqdm
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

from src.textsummarizer.dbhandler.base_handler import DBHandler
from src.textsummarizer.entity.artifact_entity import (
    ModelTrainerArtifact,
    DataTransformationArtifact,
    ModelEvaluationArtifact,
)
from src.textsummarizer.entity.config_entity import ModelEvaluationConfig
from src.textsummarizer.exception.exception import TextSummarizerError
from src.textsummarizer.logging import logger
from src.textsummarizer.utils.core import save_to_csv


class ModelEvaluation:
    """
    Handles model evaluation using the configured metrics and saves the result.
    Returns a ModelEvaluationArtifact containing report paths.
    """

    def __init__(
        self,
        config: ModelEvaluationConfig,
        trainer_artifact: ModelTrainerArtifact,
        transformation_artifact: DataTransformationArtifact | None = None,
        backup_handler: DBHandler | None = None,
    ):
        self.eval_config = config
        self.trainer_artifact = trainer_artifact
        self.transformation_artifact = transformation_artifact
        self.backup_handler = backup_handler

        # Pull dynamic params
        params = getattr(self.eval_config, "eval_params", {})
        self.metric_names = params.get("metrics", ["rouge1", "rouge2", "rougeL", "rougeLsum"])
        self.batch_size = params.get("batch_size", 8)
        self.subset_size = params.get("subset_size", None)
        self.metric_options = params.get("metric_options", {})
        self.column_text = params.get("column_text", "dialogue")
        self.column_summary = params.get("column_summary", "summary")

        # Generation params (prefer explicit eval params; fall back to transformation defaults)
        self.max_input_length = params.get("max_input_length", 1024)
        self.max_target_length = params.get("max_target_length", 128)
        self.length_penalty = params.get("length_penalty", 0.8)
        self.num_beams = params.get("num_beams", 8)

    # -------- Loader Functions (local + S3 via context) --------
    def _load_tokenizer(self):
        try:
            if self.eval_config.local_enabled and self.trainer_artifact.tokenizer_dir:
                tokenizer_path = self.trainer_artifact.tokenizer_dir
                logger.info("Loading tokenizer from local path: %s", tokenizer_path)
                if not tokenizer_path.exists():
                    raise TextSummarizerError(f"Tokenizer dir not found at: {tokenizer_path}", logger)
                return AutoTokenizer.from_pretrained(tokenizer_path)

            if self.eval_config.s3_enabled and self.trainer_artifact.tokenizer_s3_uri and self.backup_handler:
                tokenizer_s3_uri = self.trainer_artifact.tokenizer_s3_uri
                logger.info("Downloading tokenizer dir from S3: %s", tokenizer_s3_uri)
                with self.backup_handler as handler, tempfile.TemporaryDirectory() as temp_dir:
                    temp_dir_path = Path(temp_dir)
                    handler.download_dir(tokenizer_s3_uri, temp_dir_path)
                    tokenizer = AutoTokenizer.from_pretrained(str(temp_dir_path))
                    logger.info("Loaded tokenizer from temp S3 dir.")
                    return tokenizer

            raise TextSummarizerError("No valid tokenizer location found for loading.", logger)
        except Exception as e:
            logger.error("Failed to load tokenizer.")
            raise TextSummarizerError(e, logger) from e

    def _load_model(self, device: str):
        try:
            if self.eval_config.local_enabled and self.trainer_artifact.trained_model_dir:
                model_path = Path(self.trainer_artifact.trained_model_dir)
                logger.info("Loading model from local path: %s", model_path)
                if not model_path.exists():
                    raise TextSummarizerError(f"Model dir not found at: {model_path}", logger)
                return AutoModelForSeq2SeqLM.from_pretrained(model_path).to(device)

            if self.eval_config.s3_enabled and self.trainer_artifact.model_s3_uri and self.backup_handler:
                model_s3_uri = self.trainer_artifact.model_s3_uri
                logger.info("Downloading model dir from S3: %s", model_s3_uri)
                with self.backup_handler as handler, tempfile.TemporaryDirectory() as temp_dir:
                    temp_dir_path = Path(temp_dir)
                    handler.download_dir(model_s3_uri, temp_dir_path)
                    model = AutoModelForSeq2SeqLM.from_pretrained(str(temp_dir_path)).to(device)
                    logger.info("Loaded model from temp S3 dir.")
                    return model

            raise TextSummarizerError("No valid model location found for loading.", logger)
        except Exception as e:
            logger.error("Failed to load model.")
            raise TextSummarizerError(e, logger) from e

    def _load_dataset(self) -> DatasetDict:
        try:
            if self.eval_config.local_enabled and self.transformation_artifact and self.transformation_artifact.tokenized_dataset_dir:
                dataset_path = Path(self.transformation_artifact.tokenized_dataset_dir)
                logger.info("Loading DatasetDict from local path: %s", dataset_path)
                if not dataset_path.exists() or not (dataset_path / "dataset_dict.json").exists():
                    raise TextSummarizerError(
                        f"Expected DatasetDict structure not found at: {dataset_path}", logger
                    )
                dataset = load_from_disk("file://" + dataset_path.as_posix())
                if not isinstance(dataset, DatasetDict):
                    raise TextSummarizerError(
                        f"Loaded dataset is not a DatasetDict: {type(dataset)}", logger
                    )
                logger.info("Loaded dataset from disk.")
                return dataset

            if self.eval_config.s3_enabled and self.transformation_artifact and self.transformation_artifact.tokenized_dataset_s3_uri and self.backup_handler:
                s3_uri = self.transformation_artifact.tokenized_dataset_s3_uri
                logger.info("Downloading DatasetDict from S3: %s", s3_uri)
                with self.backup_handler as handler, tempfile.TemporaryDirectory() as temp_dir:
                    temp_dir_path = Path(temp_dir)
                    handler.download_dir(s3_uri, temp_dir_path)
                    dataset = load_from_disk(str(temp_dir_path))
                    if not isinstance(dataset, DatasetDict):
                        raise TextSummarizerError(
                            f"S3-loaded dataset is not a DatasetDict: {type(dataset)}", logger
                        )
                    logger.info("Loaded dataset from temp S3 dir.")
                    return dataset

            raise TextSummarizerError("No valid dataset location found for loading.", logger)
        except Exception as e:
            logger.error("Failed to load dataset")
            raise TextSummarizerError(e, logger) from e

    # ------------------- Metric Calculation -------------------
    def _generate_batch_chunks(self, data, batch_size: int):
        for i in range(0, len(data), batch_size):
            yield data[i: i + batch_size]

    def _get_metric(self, name: str) -> tuple[Any, Dict[str, Any]]:
        """Load evaluation metric and apply options from params.yaml if available."""
        options: Dict[str, Any] = self.metric_options.get(name, {})
        logger.info("Loading metric: %s with options: %s", name, options)
        metric = evaluate.load(name)
        return metric, options

    def _calculate_metrics(
        self,
        dataset,
        metric,
        metric_options,
        model,
        tokenizer,
        batch_size: int = 8,
        device: str = "cuda" if torch.cuda.is_available() else "cpu",
        column_text: str = "dialogue",
        column_summary: str = "summary",
    ):
        article_batches = list(self._generate_batch_chunks(dataset[column_text], batch_size))
        target_batches = list(self._generate_batch_chunks(dataset[column_summary], batch_size))
        logger.info("Evaluating on %d batches (batch_size=%d)", len(article_batches), batch_size)

        for article_batch, target_batch in tqdm(
            zip(article_batches, target_batches), total=len(article_batches), desc="Evaluating batches"
        ):
            inputs = tokenizer(
                article_batch,
                max_length=self.max_input_length,
                truncation=True,
                padding="max_length",
                return_tensors="pt",
            )
            with torch.no_grad():
                summaries = model.generate(
                    input_ids=inputs["input_ids"].to(device),
                    attention_mask=inputs["attention_mask"].to(device),
                    length_penalty=self.length_penalty,
                    num_beams=self.num_beams,
                    max_length=self.max_target_length,
                )
            decoded_summaries = [
                tokenizer.decode(s, skip_special_tokens=True, clean_up_tokenization_spaces=True)
                for s in summaries
            ]
            decoded_summaries = [d.replace("", " ") for d in decoded_summaries]
            metric.add_batch(predictions=decoded_summaries, references=target_batch)

        score = metric.compute(**metric_options)
        logger.info("Evaluation metrics computed: %s", score)
        return score

    # ------------------- Report Saving -------------------
    def _save_report(self, df: pd.DataFrame) -> tuple[Path | None, str | None]:
        """
        Save evaluation report as YAML locally and/or to S3 (strictly via config).
        """
        # Flatten the single-row DF to a flat dict; multi-row -> dict of lists
        if len(df) == 1:
            report_dict = df.iloc[0].to_dict()
        else:
            report_dict = df.to_dict(orient="list")

        local_path: Path | None = None
        s3_uri: str | None = None

        # Local save (strict)
        if self.eval_config.local_enabled:
            local_path = Path(self.eval_config.eval_report_filepath)
            from src.textsummarizer.utils.core import save_to_yaml
            save_to_yaml(report_dict, local_path, label="Model Evaluation Metrics (YAML)")
            logger.info("Saved evaluation metrics YAML to %s", local_path)

        # S3 save (strict)
        if self.eval_config.s3_enabled:
            if self.backup_handler is None:
                raise TextSummarizerError(
                    "S3 saving enabled but backup_handler is None.", logger
                )
            s3_key = self.eval_config.eval_report_s3_key
            if not s3_key:
                raise TextSummarizerError(
                    "S3 saving enabled but eval_report_s3_key is missing.", logger
                )
            with self.backup_handler as handler:
                s3_uri = handler.stream_yaml(report_dict, s3_key)
                logger.info("Uploaded evaluation metrics YAML to S3: %s", s3_uri)

        return local_path, s3_uri


    # ------------------- Main Evaluation Flow -------------------
    def run_evaluation(self) -> ModelEvaluationArtifact:
        """
        Loads model, tokenizer, dataset; computes and saves metrics;
        returns ModelEvaluationArtifact with report paths.
        """
        try:
            device = "cuda" if torch.cuda.is_available() else "cpu"
            logger.info("Device used for evaluation: %s", device)

            tokenizer = self._load_tokenizer()
            model = self._load_model(device=device)
            dataset = self._load_dataset()
            test_data = dataset["test"]
            logger.info("Test split loaded with %d samples.", len(test_data))

            # Optional subsetting for faster eval
            if self.subset_size is not None and self.subset_size > 0:
                logger.info("Evaluating on subset: first %d samples.", self.subset_size)
                test_data = test_data.select(range(self.subset_size))
            else:
                logger.info("Evaluating on entire test set.")

            all_scores: Dict[str, Any] = {}
            for metric_name in self.metric_names:
                metric, metric_options = self._get_metric(metric_name)
                logger.info("Running evaluation for metric: %s", metric_name)
                score = self._calculate_metrics(
                    dataset=test_data,
                    metric=metric,
                    metric_options=metric_options,
                    model=model,
                    tokenizer=tokenizer,
                    batch_size=self.batch_size,
                    device=device,
                    column_text=self.column_text,
                    column_summary=self.column_summary,
                )
                if isinstance(score, dict):
                    for k, v in score.items():
                        all_scores[f"{metric_name}_{k}" if k != metric_name else k] = v
                else:
                    all_scores[metric_name] = score

            logger.info("Final evaluation scores: %s", all_scores)

            df = pd.DataFrame(all_scores, index=["pegasus"])
            local_report_path, s3_report_uri = self._save_report(df)
            logger.info("Evaluation report saved: local=%s, s3=%s", local_report_path, s3_report_uri)

            return ModelEvaluationArtifact(
                eval_report_filepath=local_report_path,
                eval_report_s3_uri=s3_report_uri,
            )
        except Exception as e:
            logger.error("Model evaluation failed.")
            raise TextSummarizerError(e, logger) from e
