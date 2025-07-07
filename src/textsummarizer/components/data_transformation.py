import os
from transformers import AutoTokenizer
from datasets import load_dataset
from src.textsummarizer.entity.config_entity import (
    DataTransformationConfig,
    DataIngestionArtifact,
    DataTransformationArtifact,
)
from src.textsummarizer.logging import logger
from src.textsummarizer.exception.exception import TextSummarizerError


class DataTransformation:
    def __init__(
        self,
        config: DataTransformationConfig,
        data_ingestion_artifact: DataIngestionArtifact,
    ) -> None:
        self.config = config
        self.data_ingestion_artifact = data_ingestion_artifact
        self.tokenizer = AutoTokenizer.from_pretrained(self.config.tokenizer_name)

    def _convert_examples_to_features(self, example_batch: dict) -> dict:
        """
        Tokenizes input and target text into model-ready features.
        """
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

    def initiate_data_transformation(self) -> DataTransformationArtifact:
        """
        Runs full preprocessing: loading, splitting, tokenizing, and saving.
        """
        try:
            logger.info("Starting data transformation step.")

            # Load ingested CSV dataset from path
            dataset = load_dataset("csv", data_files=str(self.data_ingestion_artifact.ingested_filepath))
            full_dataset = dataset["train"]

            logger.info("Performing stratified or random data split.")
            train_valtest = full_dataset.train_test_split(
                test_size=(1.0 - self.config.train_size),
                seed=self.config.random_state,
            )
            val_test_split = train_valtest["test"].train_test_split(
                test_size=self.config.test_size / (self.config.test_size + self.config.val_size),
                seed=self.config.random_state,
            )

            train_ds = train_valtest["train"]
            val_ds = val_test_split["train"]
            test_ds = val_test_split["test"]

            logger.info("Tokenizing datasets...")
            train_ds = train_ds.map(self._convert_examples_to_features, batched=True)
            val_ds = val_ds.map(self._convert_examples_to_features, batched=True)
            test_ds = test_ds.map(self._convert_examples_to_features, batched=True)

            logger.info("Saving transformed datasets to disk.")
            train_ds.to_csv(self.config.train_filepath, index=False)
            val_ds.to_csv(self.config.val_filepath, index=False)
            test_ds.to_csv(self.config.test_filepath, index=False)

            logger.info("Data transformation complete.")

            return DataTransformationArtifact(
                transformed_data_path=str(self.config.root_dir)
            )

        except Exception as e:
            raise TextSummarizerError(e, logger) from e
