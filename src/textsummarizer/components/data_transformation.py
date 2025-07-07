# src/textsummarizer/components/data_transformation.py

import os
import pandas as pd
from io import StringIO
from transformers import AutoTokenizer
from datasets import load_dataset
from src.textsummarizer.entity.config_entity import DataTransformationConfig
from src.textsummarizer.entity.artifact_entity import DataIngestionArtifact, DataTransformationArtifact
from src.textsummarizer.logging import logger
from src.textsummarizer.exception.exception import TextSummarizerError
from src.textsummarizer.dbhandler.s3_handler import S3Handler


class DataTransformation:
    def __init__(
        self,
        transformation_config: DataTransformationConfig,
        ingestion_artifact: DataIngestionArtifact,
        backup_handler: S3Handler | None = None,
    ) -> None:
        self.config = transformation_config
        self.ingestion_artifact = ingestion_artifact
        self.backup_handler = backup_handler
        self.tokenizer = AutoTokenizer.from_pretrained(self.config.tokenizer_name)

    def _convert_examples_to_features(self, example_batch: dict) -> dict:
        try:
            inputs = self.tokenizer(
                example_batch["dialogue"],
                max_length=self.config.max_input_length,
                truncation=True,
                padding="max_length",
            )
            with self.tokenizer.as_target_tokenizer():
                targets = self.tokenizer(
                    example_batch["summary"],
                    max_length=self.config.max_target_length,
                    truncation=True,
                    padding="max_length",
                )
            return {
                "input_ids": inputs["input_ids"],
                "attention_mask": inputs["attention_mask"],
                "labels": targets["input_ids"],
            }
        except Exception as e:
            raise TextSummarizerError(e, logger) from e

    def _split_and_tokenize(self, dataset):
        try:
            logger.info("Splitting dataset into train, val, test...")
            train_valtest = dataset.train_test_split(
                test_size=(1.0 - self.config.train_size),
                seed=self.config.random_state,
            )
            val_test = train_valtest["test"].train_test_split(
                test_size=self.config.test_size / (self.config.test_size + self.config.val_size),
                seed=self.config.random_state,
            )

            return {
                "train": train_valtest["train"].map(self._convert_examples_to_features, batched=True),
                "val": val_test["train"].map(self._convert_examples_to_features, batched=True),
                "test": val_test["test"].map(self._convert_examples_to_features, batched=True),
            }
        except Exception as e:
            raise TextSummarizerError(e, logger) from e

    def _save_locally(self, datasets: dict) -> tuple:
        try:
            if not self.config.local_enabled:
                logger.info("Local saving skipped (config.local_enabled = False)")
                return None, None, None

            logger.info("Saving tokenized datasets locally...")
            datasets["train"].to_csv(self.config.train_filepath, index=False)
            datasets["val"].to_csv(self.config.val_filepath, index=False)
            datasets["test"].to_csv(self.config.test_filepath, index=False)

            return (
                self.config.train_filepath,
                self.config.val_filepath,
                self.config.test_filepath,
            )
        except Exception as e:
            raise TextSummarizerError(e, logger) from e

    def _stream_to_s3(self, datasets: dict) -> tuple:
        try:
            if not self.config.s3_enabled or not self.backup_handler:
                logger.info("S3 upload skipped (s3_enabled is False or no handler provided).")
                return None, None, None

            logger.info("Streaming tokenized datasets to S3...")
            with self.backup_handler as handler:
                train_uri = handler.stream_csv(datasets["train"].to_pandas(), self.config.train_s3_key)
                val_uri = handler.stream_csv(datasets["val"].to_pandas(), self.config.val_s3_key)
                test_uri = handler.stream_csv(datasets["test"].to_pandas(), self.config.test_s3_key)

            return train_uri, val_uri, test_uri
        except Exception as e:
            raise TextSummarizerError(e, logger) from e

    def run_transformation(self) -> DataTransformationArtifact:
        try:
            logger.info("=== Starting Data Transformation ===")
            raw_dataset = load_dataset("csv", data_files=str(self.ingestion_artifact.ingested_filepath))
            tokenized = self._split_and_tokenize(raw_dataset["train"])

            train_fp, val_fp, test_fp = self._save_locally(tokenized)
            train_s3, val_s3, test_s3 = self._stream_to_s3(tokenized)

            logger.info("=== Data Transformation Complete ===")

            return DataTransformationArtifact(
                train_filepath=train_fp,
                val_filepath=val_fp,
                test_filepath=test_fp,
                train_s3_uri=train_s3,
                val_s3_uri=val_s3,
                test_s3_uri=test_s3,
            )

        except Exception as e:
            raise TextSummarizerError(e, logger) from e
