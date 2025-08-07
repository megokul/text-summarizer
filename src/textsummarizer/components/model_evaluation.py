import torch
import pandas as pd
from tqdm import tqdm
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer
from datasets import load_from_disk, DatasetDict
import evaluate
from pathlib import Path
from src.textsummarizer.entity.config_entity import ModelEvaluationConfig
from src.textsummarizer.entity.artifact_entity import (
    ModelTrainerArtifact,
    DataTransformationArtifact,
    ModelEvaluationArtifact,
)
from src.textsummarizer.dbhandler.base_handler import DBHandler
from src.textsummarizer.exception.exception import TextSummarizerError
from src.textsummarizer.logging import logger
from src.textsummarizer.utils.core import save_to_csv
from typing import Optional, Any, Dict

class ModelEvaluation:
    """
    Handles model evaluation using the configured metrics and saves the result.
    Returns a ModelEvaluationArtifact containing report paths.
    """
    def __init__(
        self,
        config: ModelEvaluationConfig,
        trainer_artifact: ModelTrainerArtifact,
        transformation_artifact: DataTransformationArtifact = None,
        backup_handler: Optional[DBHandler] = None,
    ):
        self.eval_config = config
        self.trainer_artifact = trainer_artifact
        self.transformation_artifact = transformation_artifact
        self.backup_handler = backup_handler

        # Grab all evaluation settings from config/params (batch size, subset size, metric options, etc)
        params = getattr(self.eval_config, "eval_params", {})
        self.metric_names = params.get("metrics", ["rouge1", "rouge2", "rougeL", "rougeLsum"])
        self.batch_size = params.get("batch_size", 8)
        self.subset_size = params.get("subset_size", None)
        self.metric_options = params.get("metric_options", {})
        self.column_text = params.get("column_text", "dialogue")
        self.column_summary = params.get("column_summary", "summary")

        # Generation params (defaults can be overridden by config)
        self.max_input_length = params.get("max_input_length", None)
        self.max_target_length = params.get("max_target_length", None)
        self.length_penalty = params.get("length_penalty", 0.8)
        self.num_beams = params.get("num_beams", 8)
        # Try to pick from data_transformation params if not present here
        # (you can adjust this to fit your config loading pattern)
        if self.max_input_length is None and hasattr(self.eval_config, "transformation_params"):
            self.max_input_length = self.eval_config.transformation_params.get("max_input_length", 1024)
        if self.max_target_length is None and hasattr(self.eval_config, "transformation_params"):
            self.max_target_length = self.eval_config.transformation_params.get("max_target_length", 128)
        if self.max_input_length is None:
            self.max_input_length = 1024
        if self.max_target_length is None:
            self.max_target_length = 128

    # ------------------- Loader Functions -------------------
    def _load_tokenizer(self):
        try:
            if self.eval_config.local_enabled and self.trainer_artifact.tokenizer_dir:
                tokenizer_path = self.trainer_artifact.tokenizer_dir
                logger.info(f"Loading tokenizer from local path: {tokenizer_path}")
                if not tokenizer_path.exists():
                    raise TextSummarizerError(
                        f"Tokenizer dir not found at: {tokenizer_path}", logger
                    )
                tokenizer = AutoTokenizer.from_pretrained(tokenizer_path)
                logger.info("Loaded tokenizer from local disk.")
                return tokenizer

            if self.eval_config.s3_enabled and self.trainer_artifact.tokenizer_s3_uri:
                tokenizer_s3_uri = self.trainer_artifact.tokenizer_s3_uri
                logger.info(f"Loading tokenizer from S3 URI: {tokenizer_s3_uri}")
                tokenizer = AutoTokenizer.from_pretrained(tokenizer_s3_uri)
                logger.info("Loaded tokenizer from S3.")
                return tokenizer

            raise TextSummarizerError("No valid tokenizer location found for loading.", logger)

        except Exception as e:
            logger.error("Failed to load tokenizer.")
            raise TextSummarizerError(e, logger) from e

    def _load_model(self, device):
        try:
            if self.eval_config.local_enabled and self.trainer_artifact.trained_model_dir:
                model_path = Path(self.trainer_artifact.trained_model_dir)
                logger.info(f"Loading model from local path: {model_path}")
                if not model_path.exists():
                    raise TextSummarizerError(
                        f"Model dir not found at: {model_path}", logger
                    )
                model = AutoModelForSeq2SeqLM.from_pretrained(model_path).to(device)
                logger.info("Loaded model from local disk.")
                return model

            if self.eval_config.s3_enabled and self.trainer_artifact.model_s3_uri:
                model_s3_uri = self.trainer_artifact.model_s3_uri
                logger.info(f"Loading model from S3 URI: {model_s3_uri}")
                model = AutoModelForSeq2SeqLM.from_pretrained(model_s3_uri).to(device)
                logger.info("Loaded model from S3.")
                return model

            raise TextSummarizerError("No valid model location found for loading.", logger)

        except Exception as e:
            logger.error("Failed to load model.")
            raise TextSummarizerError(e, logger) from e

    def _load_dataset(self) -> DatasetDict:
        try:
            if self.eval_config.local_enabled and self.transformation_artifact.tokenized_dataset_dir:
                dataset_path = Path(self.transformation_artifact.tokenized_dataset_dir)
                logger.info(f"Loading DatasetDict from local path: {dataset_path}")
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

            if self.eval_config.s3_enabled and self.transformation_artifact.tokenized_dataset_s3_uri:
                s3_uri = self.transformation_artifact.tokenized_dataset_s3_uri
                logger.info(f"Loading DatasetDict from S3 URI: {s3_uri}")
                dataset = load_from_disk(s3_uri)
                if not isinstance(dataset, DatasetDict):
                    raise TextSummarizerError(
                        f"S3-loaded dataset is not a DatasetDict: {type(dataset)}", logger
                    )
                logger.info("Loaded dataset from S3.")
                return dataset

            raise TextSummarizerError("No valid dataset location found for loading.", logger)

        except Exception as e:
            logger.error("Failed to load dataset")
            raise TextSummarizerError(e, logger) from e

    # ------------------- Metric Calculation -------------------
    def _generate_batch_chunks(self, data, batch_size):
        for i in range(0, len(data), batch_size):
            yield data[i : i + batch_size]

    def _get_metric(self, name: str) -> Any:
        """Load evaluation metric and apply options from params.yaml if available."""
        options: Dict[str, Any] = self.metric_options.get(name, {})
        logger.info(f"Loading metric: {name} with options: {options}")
        metric = evaluate.load(name)
        # Some metrics (like rouge) allow options when computing
        return metric, options

    def _calculate_metrics(
        self,
        dataset,
        metric,
        metric_options,
        model,
        tokenizer,
        batch_size=8,
        device="cuda" if torch.cuda.is_available() else "cpu",
        column_text="dialogue",
        column_summary="summary",
    ):
        article_batches = list(self._generate_batch_chunks(dataset[column_text], batch_size))
        target_batches = list(self._generate_batch_chunks(dataset[column_summary], batch_size))
        logger.info(f"Evaluating on {len(article_batches)} batches (batch_size={batch_size})")

        for article_batch, target_batch in tqdm(
            zip(article_batches, target_batches), total=len(article_batches), desc="Evaluating batches",
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
        logger.info(f"Evaluation metrics computed: {score}")
        return score

    # ------------------- Report Saving -------------------
    def _save_report(self, df: pd.DataFrame) -> (Path | None, str | None):
        """
        Save evaluation report locally and/or to S3, return tuple of (local_path, s3_uri).
        """
        local_path = Path(self.eval_config.eval_report_filepath) if self.eval_config.local_enabled else None
        s3_uri = None

        # Save locally if enabled
        if self.eval_config.local_enabled and local_path is not None:
            save_to_csv(df, local_path, label="Model Evaluation Metrics")
            logger.info(f"Saved evaluation metrics to {local_path}")

        # Save to S3 if enabled
        if self.eval_config.s3_enabled and self.backup_handler is not None and local_path is not None:
            with self.backup_handler as handler:
                # S3 key follows the report local path
                s3_key = local_path.as_posix()
                s3_uri = handler.upload_file(local_path, s3_key)
                logger.info(f"Uploaded evaluation metrics to S3: {s3_uri}")

        return local_path, s3_uri


    # ------------------- Main Evaluation Flow -------------------
    def run_evaluation(self) -> ModelEvaluationArtifact:
        """
        Loads model, tokenizer, dataset; computes and saves metrics;
        returns ModelEvaluationArtifact with report paths.
        """
        try:
            device = "cuda" if torch.cuda.is_available() else "cpu"
            logger.info(f"Device used for evaluation: {device}")

            tokenizer = self._load_tokenizer()
            model = self._load_model(device=device)
            dataset = self._load_dataset()
            test_data = dataset["test"]
            logger.info(f"Test split loaded with {len(test_data)} samples.")

            # Support evaluating only a subset (for fast tests)
            if self.subset_size is not None and self.subset_size > 0:
                logger.info(f"Evaluating on subset: first {self.subset_size} samples.")
                test_data = test_data.select(range(self.subset_size))
            else:
                logger.info("Evaluating on entire test set.")

            # Evaluate and compute scores for all requested metrics
            all_scores = {}
            for metric_name in self.metric_names:
                metric, metric_options = self._get_metric(metric_name)
                logger.info(f"Running evaluation for metric: {metric_name}")
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
                # Some metrics (like ROUGE) return multiple keys, so flatten if necessary
                if isinstance(score, dict):
                    for k, v in score.items():
                        all_scores[f"{metric_name}_{k}" if k != metric_name else k] = v
                else:
                    all_scores[metric_name] = score

            logger.info(f"Final evaluation scores: {all_scores}")

            # Save report and return artifact
            df = pd.DataFrame(all_scores, index=["pegasus"])
            local_report_path, s3_report_uri = self._save_report(df)
            logger.info(f"Evaluation report saved: local={local_report_path}, s3={s3_report_uri}")

            return ModelEvaluationArtifact(
                eval_report_filepath=local_report_path,
                eval_report_s3_uri=s3_report_uri,
            )

        except Exception as e:
            logger.error("Model evaluation failed.")
            raise TextSummarizerError(e, logger) from e

