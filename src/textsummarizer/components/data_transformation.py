import os
import shutil
from transformers import AutoTokenizer
from datasets import load_from_disk, DatasetDict
from src.textsummarizer.entity.artifact_entity import DataTransformationArtifact, DataIngestionArtifact
from src.textsummarizer.entity.config_entity import DataTransformationConfig
from src.textsummarizer.logging import logger
from src.textsummarizer.exception.exception import TextSummarizerError
from typing import Optional
from pathlib import Path
import numpy as np

class DataTransformation:
    def __init__(
        self,
        config: DataTransformationConfig,
        artifact: DataIngestionArtifact,
        backup_handler: Optional[object] = None,
    ):
        self.transformation_config = config
        self.ingestion_artifact = artifact
        self.backup_handler = backup_handler
        self.tokenizer = AutoTokenizer.from_pretrained(self.transformation_config.tokenizer_name)

    def _load_data(self) -> DatasetDict:
        """
        Loads dataset from S3 or local disk depending on config flags.
        Assumes the saved format is Hugging Face DatasetDict with splits (train, val, test).
        """
        try:
            # Load from local path
            if self.transformation_config.local_enabled and self.ingestion_artifact.ingested_filepath:
                dataset_path = Path(self.ingestion_artifact.ingested_filepath)
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
                return dataset

            # Load directly from s3://... URI (saved in artifact)
            if self.transformation_config.s3_enabled and self.ingestion_artifact.ingested_s3_uri:
                s3_uri = self.ingestion_artifact.ingested_s3_uri
                logger.info(f"Loading Hugging Face DatasetDict from S3 URI: {s3_uri}")

                dataset = load_from_disk(s3_uri)
                if not isinstance(dataset, DatasetDict):
                    raise TextSummarizerError(
                        f"S3-loaded dataset is not a DatasetDict: {type(dataset)}", logger
                    )
                return dataset

            raise TextSummarizerError("No valid dataset location found for loading.", logger)

        except Exception as e:
            logger.info("Failed to load dataset")
            raise TextSummarizerError(e, logger) from e

    def _tokenize_data(self, dataset: DatasetDict) -> DatasetDict:
        """
        Tokenizes dialogue and summary fields for all splits in the dataset.
        """
        try:
            def convert_examples_to_features(example_batch):
                input_encodings = self.tokenizer(
                    example_batch['dialogue'],
                    max_length=self.transformation_config.max_input_length,
                    truncation=True,
                )
                with self.tokenizer.as_target_tokenizer():
                    target_encodings = self.tokenizer(
                        example_batch['summary'],
                        max_length=self.transformation_config.max_target_length,
                        truncation=True,
                    )
                return {
                    'input_ids': input_encodings['input_ids'],
                    'attention_mask': input_encodings['attention_mask'],
                    'labels': target_encodings['input_ids'],
                }

            logger.info("Tokenizing dataset splits...")
            tokenized = dataset.map(convert_examples_to_features, batched=True)
            logger.info("Tokenization complete.")

            # Run additional quality checks
            self.check_empty_fields(tokenized)
            self.check_input_attention_length_match(tokenized)

            return tokenized

        except Exception as e:
            logger.info("Error during batch tokenization")
            raise TextSummarizerError(e, logger) from e

    def check_empty_fields(self, tokenized_dataset, fields=('input_ids', 'labels', 'attention_mask')):
        """
        Checks and logs empty fields in all splits.
        """
        try:
            any_empty = False
            for split in tokenized_dataset.keys():
                for idx, example in enumerate(tokenized_dataset[split]):
                    for field in fields:
                        if isinstance(example[field], list) and len(example[field]) == 0:
                            msg = (
                                f"EMPTY FIELD detected in split '{split}', index {idx}, field '{field}'. Sample: {example}"
                            )
                            logger.error(msg)
                            any_empty = True
            if not any_empty:
                logger.info("No empty fields detected in any split.")
        except Exception as e:
            logger.error(f"Error during empty field check: {e}")

    def check_input_attention_length_match(self, tokenized_dataset: DatasetDict):
        """
        Checks and logs if input_ids and attention_mask are not the same length in every record.
        Raises an exception if any mismatch is found.
        """
        try:
            all_ok = True
            for split in tokenized_dataset.keys():
                for idx, example in enumerate(tokenized_dataset[split]):
                    input_len = len(example['input_ids'])
                    attn_len = len(example['attention_mask'])
                    if input_len != attn_len:
                        msg = (
                            f"LENGTH MISMATCH in split '{split}', idx {idx}: "
                            f"input_ids length = {input_len}, attention_mask length = {attn_len}. "
                            f"input_ids (first 10): {example['input_ids'][:10]}, "
                            f"attention_mask (first 10): {example['attention_mask'][:10]}"
                        )
                        logger.error(msg)
                        print(msg)
                        all_ok = False
            if all_ok:
                msg = "All input_ids and attention_mask fields are the same length in every split."
                logger.info(msg)
                print(msg)
            else:
                raise TextSummarizerError("Mismatch between input_ids and attention_mask lengths detected!", logger)
        except Exception as e:
            logger.error(f"Error during input/attention mask length check: {e}")
            raise

    def _save_all(self, tokenized_dataset: DatasetDict):
        """
        Handles saving locally (and to DVC) and/or to S3, based on config flags.
        Returns all paths/URIs.
        """
        dataset_dir = None
        dvc_dataset_dir = None
        tokenized_dataset_s3_key = None
        dvc_tokenized_dataset_s3_key = None

        try:
            # Save locally and to DVC
            if self.transformation_config.local_enabled:
                dataset_dir = self.transformation_config.tokenized_dataset_dir
                os.makedirs(dataset_dir, exist_ok=True)
                tokenized_dataset.save_to_disk(dataset_dir.as_posix())
                logger.info(f"Saved tokenized dataset locally: {dataset_dir}")

                dvc_dataset_dir = self.transformation_config.dvc_tokenized_dataset_dir
                if dvc_dataset_dir.exists():
                    shutil.rmtree(dvc_dataset_dir)
                shutil.copytree(dataset_dir, dvc_dataset_dir)
                logger.info(f"DVC dataset copy created: {dvc_dataset_dir}")

            # Upload to S3
            if self.transformation_config.s3_enabled and self.backup_handler:
                tokenized_dataset_s3_key = self.transformation_config.tokenized_dataset_s3_key
                dvc_tokenized_dataset_s3_key = self.transformation_config.dvc_tokenized_dataset_s3_key
                with self.backup_handler as handler:
                    if tokenized_dataset_s3_key:
                        tokenized_dataset.save_to_disk(tokenized_dataset_s3_key)
                        logger.info(f"Saved tokenized dataset directly to S3: {tokenized_dataset_s3_key}")

                    if dvc_tokenized_dataset_s3_key:
                        tokenized_dataset.save_to_disk(dvc_tokenized_dataset_s3_key)
                        logger.info(f"Uploaded DVC tokenized dataset to S3: {dvc_tokenized_dataset_s3_key}")

            return dataset_dir, dvc_dataset_dir, tokenized_dataset_s3_key, dvc_tokenized_dataset_s3_key

        except Exception as e:
            logger.info("Error during saving outputs (local/DVC/S3)")
            raise TextSummarizerError(e, logger) from e

    def print_column_dtypes(self, tokenized_dataset: DatasetDict, fields=('input_ids', 'labels', 'attention_mask')):
        """
        Print and log dtype of specified fields for all splits in tokenized_dataset.
        """
        try:
            for split in tokenized_dataset.keys():
                msg = f"\nSplit: {split}"
                print(msg)
                logger.info(msg)
                for field in fields:
                    arr = np.array(tokenized_dataset[split][0][field])
                    field_msg = f"  {field}: dtype={arr.dtype} (example: {arr[:5]})"
                    print(field_msg)
                    logger.info(field_msg)
        except Exception as e:
            err_msg = f"Error during dtype check: {e}"
            print(err_msg)
            logger.info(err_msg)

    def run_transformation(self) -> DataTransformationArtifact:
        """
        Pipeline for full data transformation:
        - Load dataset
        - Tokenize
        - Save locally/DVC/S3
        - Print dtype summary
        - Return artifact
        """
        try:
            logger.info("Starting data transformation...")

            dataset = self._load_data()
            tokenized_dataset = self._tokenize_data(dataset)
            tokenized_dataset_dir, dvc_tokenized_dataset_dir, tokenized_dataset_s3_uri, dvc_tokenized_dataset_s3_uri = self._save_all(tokenized_dataset)

            # Print dtypes for debugging
            self.print_column_dtypes(tokenized_dataset)

            artifact = DataTransformationArtifact(
                tokenized_dataset_dir=tokenized_dataset_dir,
                dvc_tokenized_dataset_dir=dvc_tokenized_dataset_dir,
                tokenized_dataset_s3_uri=tokenized_dataset_s3_uri,
                dvc_tokenized_dataset_s3_uri=dvc_tokenized_dataset_s3_uri,
            )

            logger.info(f"Data transformation complete: {artifact}")
            return artifact

        except Exception as e:
            logger.info("Data transformation failed")
            raise TextSummarizerError(e, logger) from e