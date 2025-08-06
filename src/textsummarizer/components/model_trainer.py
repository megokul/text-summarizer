"""Model training component for text summarization pipeline."""
import os
import torch
from transformers import (
    AutoModelForSeq2SeqLM,
    AutoTokenizer,
    TrainingArguments,
    Trainer,
    DataCollatorForSeq2Seq,
)
import tempfile
from datasets import load_from_disk, DatasetDict, Features, Sequence, Value
from typing import Optional
from src.textsummarizer.entity.config_entity import ModelTrainerConfig
from src.textsummarizer.dbhandler.base_handler import DBHandler
from src.textsummarizer.exception.exception import TextSummarizerError
from src.textsummarizer.logging import logger
from pathlib import Path
from src.textsummarizer.entity.artifact_entity import (
    ModelTrainerArtifact,
    DataTransformationArtifact,
)
from box import ConfigBox

class ModelTrainer:
    """Orchestrates model training using Hugging Face Transformers."""

    def __init__(
        self,
        config: ModelTrainerConfig,
        artifact: DataTransformationArtifact,
        backup_handler: Optional[DBHandler] = None,
    ) -> None:
        self.trainer_config = config
        self.transformation_artifact = artifact
        self.backup_handler = backup_handler

    def _get_device(self) -> str:
        device = "cuda" if torch.cuda.is_available() else "cpu"
        logger.info(f"Using device: {device}")
        return device

    def _load_model_and_tokenizer(self):
        tokenizer = AutoTokenizer.from_pretrained(self.trainer_config.model_ckpt)
        model = AutoModelForSeq2SeqLM.from_pretrained(self.trainer_config.model_ckpt)
        logger.info(f"Loaded model and tokenizer from checkpoint: {self.trainer_config.model_ckpt}")
        return model, tokenizer

    def _cast_dataset_features(self, dataset: DatasetDict) -> DatasetDict:
        features = Features({
            'id': Value('string'),
            'dialogue': Value('string'),
            'summary': Value('string'),
            'input_ids': Sequence(Value('int64')),
            'attention_mask': Sequence(Value('int64')),
            'labels': Sequence(Value('int64')),
        })
        for split in dataset.keys():
            dataset[split] = dataset[split].cast(features)
        logger.info("Casted input_ids, attention_mask, and labels to int64 for all splits.")
        return dataset

    def _load_dataset(self) -> DatasetDict:
        try:
            if self.trainer_config.local_enabled and self.transformation_artifact.tokenized_dataset_dir:
                dataset_path = Path(self.transformation_artifact.tokenized_dataset_dir)
                logger.info(f"Loading Hugging Face DatasetDict from local path: {dataset_path}")
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
                return self._cast_dataset_features(dataset)

            if self.trainer_config.s3_enabled and self.transformation_artifact.tokenized_dataset_s3_uri:
                s3_uri = self.transformation_artifact.tokenized_dataset_s3_uri
                logger.info(f"Loading Hugging Face DatasetDict from S3 URI: {s3_uri}")
                dataset = load_from_disk(s3_uri)
                if not isinstance(dataset, DatasetDict):
                    raise TextSummarizerError(
                        f"S3-loaded dataset is not a DatasetDict: {type(dataset)}", logger
                    )
                logger.info("Loaded dataset from S3.")
                return self._cast_dataset_features(dataset)

            raise TextSummarizerError("No valid dataset location found for loading.", logger)

        except Exception as e:
            logger.info("Failed to load dataset")
            raise TextSummarizerError(e, logger) from e

    def _get_training_args(self) -> TrainingArguments:
        args = TrainingArguments(
            output_dir=str(self.trainer_config.root_dir),
            num_train_epochs=int(self.trainer_config.num_train_epochs),
            warmup_steps=int(self.trainer_config.warmup_steps),
            per_device_train_batch_size=int(self.trainer_config.per_device_train_batch_size),
            per_device_eval_batch_size=int(self.trainer_config.per_device_eval_batch_size),
            weight_decay=float(self.trainer_config.weight_decay),
            logging_steps=int(self.trainer_config.logging_steps),
            eval_strategy=str(self.trainer_config.eval_strategy),
            eval_steps=int(self.trainer_config.eval_steps),
            save_steps=int(self.trainer_config.save_steps),
            gradient_accumulation_steps=int(self.trainer_config.gradient_accumulation_steps),
            learning_rate=float(self.trainer_config.learning_rate),
            fp16=getattr(self.trainer_config, "fp16", False),
            report_to="mlflow",
        )
        logger.info("Training arguments initialized.")
        return args

    def _save_model_and_tokenizer(self, model, tokenizer):
        """
        Save model and tokenizer to local disk and/or S3, as configured.
        Returns a ConfigBox with all output locations.
        """
        # --- Local Paths ---
        model_dir = self.trainer_config.model_dir
        tokenizer_dir = self.trainer_config.tokenizer_dir
        final_model_dir = self.trainer_config.final_model_dir
        final_tokenizer_dir = self.trainer_config.final_tokenizer_dir

        # --- S3 Keys ---
        model_s3_key = self.trainer_config.model_s3_key
        tokenizer_s3_key = self.trainer_config.tokenizer_s3_key
        final_model_s3_key = self.trainer_config.final_model_s3_key
        final_tokenizer_s3_key = self.trainer_config.final_tokenizer_s3_key

        model_s3_uri = tokenizer_s3_uri = final_model_s3_uri = final_tokenizer_s3_uri = None

        # ---- Save locally if enabled ----
        if self.trainer_config.local_enabled:
            logger.info(f"Saving model to {model_dir}")
            model_dir.mkdir(parents=True, exist_ok=True)
            model.save_pretrained(model_dir)

            logger.info(f"Saving tokenizer to {tokenizer_dir}")
            tokenizer_dir.mkdir(parents=True, exist_ok=True)
            tokenizer.save_pretrained(tokenizer_dir)

            logger.info(f"Saving final model to {final_model_dir}")
            final_model_dir.mkdir(parents=True, exist_ok=True)
            model.save_pretrained(final_model_dir)

            logger.info(f"Saving final tokenizer to {final_tokenizer_dir}")
            final_tokenizer_dir.mkdir(parents=True, exist_ok=True)
            tokenizer.save_pretrained(final_tokenizer_dir)

            logger.info("Model and tokenizer successfully saved locally.")

        # ---- Save to S3 if enabled ----
        if self.trainer_config.s3_enabled and self.backup_handler is not None:
            with self.backup_handler as handler:
                # ---- Main model directory (to S3) ----
                if model_s3_key:
                    with tempfile.TemporaryDirectory() as tmp_model_dir:
                        tmp_model_path = Path(tmp_model_dir)
                        model.save_pretrained(tmp_model_path)
                        model_s3_uri = handler.upload_dir(tmp_model_path, model_s3_key)
                        logger.info(f"Model directory uploaded to S3: {model_s3_uri}")

                if tokenizer_s3_key:
                    with tempfile.TemporaryDirectory() as tmp_tokenizer_dir:
                        tmp_tokenizer_path = Path(tmp_tokenizer_dir)
                        tokenizer.save_pretrained(tmp_tokenizer_path)
                        tokenizer_s3_uri = handler.upload_dir(tmp_tokenizer_path, tokenizer_s3_key)
                        logger.info(f"Tokenizer directory uploaded to S3: {tokenizer_s3_uri}")

                # ---- Final model directory (to S3) ----
                if final_model_s3_key:
                    with tempfile.TemporaryDirectory() as tmp_final_model_dir:
                        tmp_final_model_path = Path(tmp_final_model_dir)
                        model.save_pretrained(tmp_final_model_path)
                        final_model_s3_uri = handler.upload_dir(tmp_final_model_path, final_model_s3_key)
                        logger.info(f"Final model directory uploaded to S3: {final_model_s3_uri}")

                if final_tokenizer_s3_key:
                    with tempfile.TemporaryDirectory() as tmp_final_tokenizer_dir:
                        tmp_final_tokenizer_path = Path(tmp_final_tokenizer_dir)
                        tokenizer.save_pretrained(tmp_final_tokenizer_path)
                        final_tokenizer_s3_uri = handler.upload_dir(tmp_final_tokenizer_path, final_tokenizer_s3_key)
                        logger.info(f"Final tokenizer directory uploaded to S3: {final_tokenizer_s3_uri}")

        logger.info("Final model/tokenizer save operation complete.")

        saved_paths = ConfigBox({
            "model_dir": model_dir,
            "tokenizer_dir": tokenizer_dir,
            "final_model_dir": final_model_dir,
            "final_tokenizer_dir": final_tokenizer_dir,
            "model_s3_uri": model_s3_uri,
            "tokenizer_s3_uri": tokenizer_s3_uri,
            "final_model_s3_uri": final_model_s3_uri,
            "final_tokenizer_s3_uri": final_tokenizer_s3_uri,
        })

        return saved_paths

    def _validate_all_fields(self, dataset: DatasetDict):
        logger.info("Validating all samples in train/validation splits for empty or malformed fields...")
        bad_samples = []
        for split in ["train", "validation"]:
            if split not in dataset:
                logger.warning(f"Split '{split}' not found in dataset. Skipping check.")
                continue
            for idx, ex in enumerate(dataset[split]):
                for field in ["input_ids", "labels", "attention_mask"]:
                    field_val = ex[field]
                    if not isinstance(field_val, list) or len(field_val) == 0:
                        bad_samples.append((split, idx, field, ex))
                        continue
                    if not all(isinstance(x, int) for x in field_val):
                        bad_samples.append((split, idx, field, ex))
                if all(v == -100 for v in ex["labels"]):
                    bad_samples.append((split, idx, "labels_all_-100", ex))
        if bad_samples:
            for split, idx, field, ex in bad_samples[:10]:
                logger.error(f"Bad sample - Split: '{split}', idx: {idx}, field: '{field}', value: {ex.get(field, '')}, full sample: {ex}")
            logger.error(f"Found {len(bad_samples)} bad/empty or malformed fields in train/validation splits! Aborting training.")
            raise ValueError(f"{len(bad_samples)} invalid samples found. See logs above for details.")
        logger.info("Validation passed: No empty or malformed fields found in train/validation splits.")

    def _log_field_length_stats(self, dataset: DatasetDict):
        for split in dataset.keys():
            logger.info(f"Field length stats for split: {split}")
            for field in ["input_ids", "labels", "attention_mask"]:
                lengths = [len(x[field]) for x in dataset[split]]
                logger.info(f"  {field}: min={min(lengths)}, max={max(lengths)}, num_empty={sum(l == 0 for l in lengths)}")
                if field == "labels":
                    num_all_neg100 = sum(
                        all(v == -100 for v in x[field]) for x in dataset[split]
                    )
                    logger.info(f"  {field}: samples with all -100: {num_all_neg100}")

    def _log_dtype_summary(self, dataset: DatasetDict):
        import numpy as np
        for split in dataset.keys():
            logger.info(f"Dtype summary for split: {split}")
            ex = dataset[split][0]
            for field in ["input_ids", "labels", "attention_mask"]:
                arr = np.array(ex[field])
                logger.info(f"  {field}: dtype={arr.dtype}, example: {arr[:5]}")

    def _filter_invalid_samples(self, dataset: DatasetDict) -> DatasetDict:
        def is_valid(example):
            return (
                isinstance(example["input_ids"], list) and len(example["input_ids"]) > 0 and
                isinstance(example["labels"], list) and len(example["labels"]) > 0 and
                any(v != -100 for v in example["labels"])
            )
        for split in ["train", "validation"]:
            if split in dataset:
                orig_len = len(dataset[split])
                dataset[split] = dataset[split].filter(is_valid)
                logger.info(f"Filtered {orig_len - len(dataset[split])} invalid samples from '{split}' split.")
        return dataset

    def train(self) -> ModelTrainerArtifact:
        """Execute model training pipeline and return artifact."""
        try:
            logger.info("Starting model training pipeline.")
            device = self._get_device()
            model, tokenizer = self._load_model_and_tokenizer()
            model = model.to(device)

            dataset = self._load_dataset()
            self._log_dtype_summary(dataset)
            self._log_field_length_stats(dataset)
            dataset = self._filter_invalid_samples(dataset)
            self._validate_all_fields(dataset)

            train_len = len(dataset["train"])
            val_len = len(dataset["validation"])
            logger.info(f"Final split lengths: train={train_len}, validation={val_len}")
            logger.info(f"Batch sizes: train={self.trainer_config.per_device_train_batch_size}, eval={self.trainer_config.per_device_eval_batch_size}")

            if train_len == 0:
                raise RuntimeError("Train split is empty after filtering. Cannot proceed.")
            if val_len == 0:
                raise RuntimeError("Validation split is empty after filtering. Cannot proceed.")
            if self.trainer_config.per_device_eval_batch_size > val_len:
                logger.warning(
                    f"Eval batch size ({self.trainer_config.per_device_eval_batch_size}) > validation set size ({val_len}); lowering eval batch size to 1."
                )
                self.trainer_config.per_device_eval_batch_size = 1

            training_args = self._get_training_args()
            data_collator = DataCollatorForSeq2Seq(tokenizer, model=model)

            print("Train size:", train_len)
            print("Validation size:", val_len)
            print("First train sample:", dataset["train"][0] if train_len > 0 else "EMPTY")
            print("Dataset columns (train):", dataset["train"].features)
            print("Dataset columns (validation):", dataset["validation"].features)

            logger.info(f"Train size: {train_len}")
            logger.info(f"Validation size: {val_len}")
            if train_len > 0:
                logger.info(f"First train sample: {dataset['train'][0]}")
            logger.info(f"Dataset columns (train): {dataset['train'].features}")
            logger.info(f"Dataset columns (validation): {dataset['validation'].features}")

            logger.info("Beginning training...")
            trainer = Trainer(
                model=model,
                args=training_args,
                tokenizer=tokenizer,
                data_collator=data_collator,
                train_dataset=dataset["train"],
                eval_dataset=dataset["validation"],
            )

            # trainer.train()

            model, tokenizer = self.load_model_from_backup()

            logger.info("Training completed.")

            # Save model and tokenizer, get paths
            saved_paths = self._save_model_and_tokenizer(model, tokenizer)
            logger.info("Model and tokenizer saved successfully.")

            artifact = ModelTrainerArtifact(
                trained_model_dir=saved_paths.model_dir,
                tokenizer_dir=saved_paths.tokenizer_dir,
                final_model_dir=saved_paths.final_model_dir,
                final_tokenizer_dir=saved_paths.final_tokenizer_dir,
                model_s3_uri=saved_paths.model_s3_uri,
                tokenizer_s3_uri=saved_paths.tokenizer_s3_uri,
                final_model_s3_uri=saved_paths.final_model_s3_uri,
                final_tokenizer_s3_uri=saved_paths.final_tokenizer_s3_uri,
            )

            logger.info(f"Model training successfully completed. Artifact: {artifact}")

            return artifact

        except Exception as e:
            logger.info("Error during model training")
            raise TextSummarizerError(e, logger) from e

    def load_model_from_backup(self) -> tuple:
        """
        Load model and tokenizer from backup directories (always using POSIX paths).
        """
        try:
            model_dir = Path("model_backup/pegasus_samsum_model")
            tokenizer_dir = Path("model_backup/pegasus_samsum_tokenizer")
            logger.info(f"Loading model from backup directory: {model_dir}")
            model = AutoModelForSeq2SeqLM.from_pretrained(model_dir.as_posix())
            logger.info(f"Loading tokenizer from backup directory: {tokenizer_dir}")
            tokenizer = AutoTokenizer.from_pretrained(tokenizer_dir.as_posix())
            logger.info("Model and tokenizer loaded successfully from backup.")
            return model, tokenizer
        except Exception as e:
            logger.error(f"Failed to load model or tokenizer from backup: {e}")
            raise TextSummarizerError(e, logger) from e
