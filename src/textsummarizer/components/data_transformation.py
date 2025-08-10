"""Data transformation component.

Prepares tokenized datasets for seq2seq training by:
1) loading a Hugging Face ``DatasetDict`` from local disk and/or S3,
2) tokenizing input/target text with the configured tokenizer, and
3) saving the tokenized dataset locally, in a DVC mirror, and/or to S3.

Design intent:
- Keep orchestration here; delegate low-level I/O to the injected ``DBHandler``
  (cloud) and to Hugging Face utilities (local). This separation keeps the code
  testable and limits surface area for bugs.
- Provide production-grade logging at each boundary (load → tokenize → save),
  logging both *intent* and *outcome*, with explicit counts per split to help
  spot silent data loss.
- Fail fast with meaningful context; before raising, log a concise error and
  then wrap the original exception in ``TextSummarizerError`` so upstream
  callers receive consistent, project-specific failures.
- Predictable artifacts: all fields are present in the returned artifact; if a
  target sink (e.g., S3) is disabled, the corresponding field is ``None``.
- Strict configuration: S3 upload targets must be **bare keys** (object
  prefixes). We intentionally avoid auto-fixing ``s3://`` URIs to surface
  misconfigurations early.
"""

from pathlib import Path
import shutil
import tempfile

from datasets import DatasetDict, load_from_disk
from transformers import AutoTokenizer

from src.textsummarizer.dbhandler.base_handler import DBHandler
from src.textsummarizer.entity.artifact_entity import (
    DataIngestionArtifact,
    DataTransformationArtifact,
)
from src.textsummarizer.entity.config_entity import DataTransformationConfig
from src.textsummarizer.exception.exception import TextSummarizerError
from src.textsummarizer.logging import logger


class DataTransformation:
    """Transform raw dataset into tokenized form suitable for training.

    This component is intentionally *thin*: it coordinates loading a saved
    ``DatasetDict``, applies tokenizer-driven mapping, and persists the result
    to the configured sinks (local/DVC/S3). All heavyweight logic (tokenizer
    behavior, dataset serialization, cloud I/O) is delegated to well-tested
    libraries or injected adapters.
    """

    def __init__(
        self,
        config: DataTransformationConfig,
        ingestion_artifact: DataIngestionArtifact,
        backup_handler: DBHandler | None = None,
    ) -> None:
        """Initialize the transformer with configuration and inputs.

        Args:
            config (DataTransformationConfig): Transformation configuration
                (tokenizer name, max lengths, output locations, flags).
            ingestion_artifact (DataIngestionArtifact): Output from the
                ingestion stage pointing to the source ``DatasetDict`` location.
            backup_handler (DBHandler | None): Optional handler for S3 uploads.
                Passed as a context manager to guarantee cleanup.

        Returns:
            None
        """
        # Store references for use across helpers (explicit dependencies).
        self.transformation_config = config
        self.ingestion_artifact = ingestion_artifact
        self.backup_handler = backup_handler

        # Build tokenizer once; reuse per split to avoid repeated downloads and
        # to ensure we use identical tokenization settings across the run.
        self.tokenizer = AutoTokenizer.from_pretrained(
            self.transformation_config.tokenizer_name
        )

    def _load_data(self) -> DatasetDict:
        """Load a saved Hugging Face ``DatasetDict`` from local disk or S3.

        Preference order:
        1) Local location, if enabled and present.
        2) S3 location, if enabled and present.

        Returns:
            DatasetDict: Dataset with expected splits (e.g., train/validation/test).

        Raises:
            TextSummarizerError: If no valid source is configured, the on-disk
                structure is invalid, or loading fails.
        """
        try:
            # Prefer local when available; it's faster and cheaper than S3.
            if (
                self.transformation_config.local_enabled
                and self.ingestion_artifact.ingested_filepath
            ):
                dataset_path = Path(self.ingestion_artifact.ingested_filepath)
                logger.info("Loading DatasetDict (local): %s", dataset_path)

                # Guard against partial/incorrect directories early. The
                # presence of 'dataset_dict.json' is a reliable HF sentinel.
                if not dataset_path.exists() or not (
                    dataset_path / "dataset_dict.json"
                ).exists():
                    raise TextSummarizerError(
                        f"Expected DatasetDict structure not found at: {dataset_path}",
                        logger,
                    )

                # Use a URI-like path form on Windows/macOS/Linux consistently.
                dataset = load_from_disk("file://" + dataset_path.as_posix())
                if not isinstance(dataset, DatasetDict):
                    raise TextSummarizerError(
                        f"Loaded dataset is not a DatasetDict: {type(dataset)}",
                        logger,
                    )

                # Log splits and counts to detect unexpected shape changes.
                for split in dataset.keys():
                    logger.info("Loaded split '%s' with %d records.", split, len(dataset[split]))  # noqa: E501
                return dataset

            # Fall back to S3 if configured; supports cloud-only environments.
            if (
                self.transformation_config.s3_enabled
                and self.ingestion_artifact.ingested_s3_uri
            ):
                s3_uri = self.ingestion_artifact.ingested_s3_uri
                logger.info("Loading DatasetDict (S3): %s", s3_uri)

                dataset = load_from_disk(s3_uri)
                if not isinstance(dataset, DatasetDict):
                    raise TextSummarizerError(
                        f"S3-loaded dataset is not a DatasetDict: {type(dataset)}",
                        logger,
                    )

                for split in dataset.keys():
                    logger.info(
                        "Loaded split '%s' with %d records (S3).",
                        split,
                        len(dataset[split]),
                    )
                return dataset

            # No viable source configured or present.
            raise TextSummarizerError(
                "No valid dataset location found for loading.",
                logger,
            )

        except Exception as e:  # noqa: BLE001
            logger.error("Failed to load dataset.")
            raise TextSummarizerError(e, logger) from e

    def _convert_examples_to_features(self, example_batch: dict) -> dict:
        """Convert a batch of raw examples into tokenized model features.

        The method is defined on the class (not nested) to keep it importable
        and picklable by Hugging Face Datasets when enabling multiprocessing
        (e.g., ``dataset.map(..., num_proc=4)``). It also allows straightforward
        unit testing of tokenization behavior.

        Args:
            example_batch (dict): Batch containing fields ``dialogue`` and
                ``summary`` as lists of strings.

        Returns:
            dict: Tokenized fields aligned with the Trainer API:
                - ``input_ids`` (list[list[int]])
                - ``attention_mask`` (list[list[int]])
                - ``labels`` (list[list[int]])

        Raises:
            KeyError: If expected fields are missing in the batch.
        """
        # Tokenize source inputs (dialogue). We enforce truncation at a
        # configured maximum to avoid oversized sequences during collation.
        input_enc = self.tokenizer(
            example_batch["dialogue"],
            max_length=self.transformation_config.max_input_length,
            truncation=True,
        )

        # Tokenize targets (summary). The legacy `as_target_tokenizer()` call is
        # retained to preserve behavior across tokenizers that treat targets
        # differently (e.g., seq2seq models).
        with self.tokenizer.as_target_tokenizer():  # noqa: SIM117
            target_enc = self.tokenizer(
                example_batch["summary"],
                max_length=self.transformation_config.max_target_length,
                truncation=True,
            )

        # Explicit field names match the Trainer and model forward kwargs.
        return {
            "input_ids": input_enc["input_ids"],
            "attention_mask": input_enc["attention_mask"],
            "labels": target_enc["input_ids"],
        }

    def _tokenize_data(self, dataset: DatasetDict) -> DatasetDict:
        """Apply tokenization across all dataset splits.

        We deliberately keep the map function simple and stateless beyond access
        to ``self.tokenizer`` and configuration. Any error here should fail fast
        with context so upstream can decide to retry or inspect inputs.

        Args:
            dataset (DatasetDict): Loaded dataset containing text fields.

        Returns:
            DatasetDict: Tokenized dataset with fields required by the Trainer.

        Raises:
            TextSummarizerError: If tokenization fails for any split.
        """
        try:
            logger.info("Tokenizing dataset splits...")
            for split in dataset.keys():
                logger.info("Pre-tokenization count for '%s': %d", split, len(dataset[split]))  # noqa: E501

            # ``batched=True`` performs vectorized tokenization, which is
            # significantly faster and consistent with HF best practices.
            tokenized = dataset.map(self._convert_examples_to_features, batched=True)

            for split in tokenized.keys():
                logger.info(
                    "Post-tokenization count for '%s': %d",
                    split,
                    len(tokenized[split]),
                )

            logger.info("Tokenization complete.")
            return tokenized

        except Exception as e:  # noqa: BLE001
            logger.error("Error during batch tokenization.")
            raise TextSummarizerError(e, logger) from e

    def _save_all(
        self, tokenized_dataset: DatasetDict
    ) -> tuple[Path | None, Path | None, str | None, str | None]:
        """Persist the tokenized dataset to configured sinks.

        Local saves create (or replace) two copies:
        - primary local directory (working copy)
        - DVC mirror (stable copy for data versioning)

        S3 saves upload a folder-structured dataset using the injected
        ``DBHandler``. **Strict policy**: configuration must provide *bare*
        object keys (no ``s3://`` prefix). We fail early if misconfigured.

        Args:
            tokenized_dataset (DatasetDict): Dataset after tokenization.

        Returns:
            tuple[Path | None, Path | None, str | None, str | None]:
                (local_dir, dvc_local_dir, tokenized_s3_uri, dvc_tokenized_s3_uri)

        Raises:
            TextSummarizerError: If saving locally or uploading to S3 fails.
        """
        # Initialize outputs so the caller gets a predictable artifact structure.
        dataset_dir: Path | None = None
        dvc_dataset_dir: Path | None = None
        tokenized_dataset_s3_uri: str | None = None
        dvc_tokenized_dataset_s3_uri: str | None = None

        try:
            # ----------------------------
            # Local and DVC disk saves
            # ----------------------------
            if self.transformation_config.local_enabled:
                dataset_dir = self.transformation_config.tokenized_dataset_dir

                # Create the target directory explicitly before saving to avoid
                # relying on implicit directory creation inside libraries.
                dataset_dir.mkdir(parents=True, exist_ok=True)

                # Save entire DatasetDict folder (preserves HF metadata/splits).
                tokenized_dataset.save_to_disk(dataset_dir.as_posix())
                logger.info("Saved tokenized dataset locally: %s", dataset_dir)

                # Keep the DVC mirror deterministic/idempotent: remove any old
                # copy first to avoid accidental leftovers from prior runs.
                dvc_dataset_dir = self.transformation_config.dvc_tokenized_dataset_dir
                if dvc_dataset_dir.exists():
                    shutil.rmtree(dvc_dataset_dir)
                dvc_dataset_dir.parent.mkdir(parents=True, exist_ok=True)
                shutil.copytree(dataset_dir, dvc_dataset_dir)
                logger.info("DVC dataset copy created: %s", dvc_dataset_dir)

            # ----------------------------
            # S3 uploads (strict bare keys)
            # ----------------------------
            if self.transformation_config.s3_enabled and self.backup_handler:
                tokenized_key = self.transformation_config.tokenized_dataset_s3_key
                dvc_tokenized_key = (
                    self.transformation_config.dvc_tokenized_dataset_s3_key
                )

                # Enforce strict bare-key policy. We intentionally do not
                # transform values here so misconfigurations surface clearly.
                if tokenized_key and tokenized_key.startswith("s3://"):
                    logger.error(
                        "tokenized_dataset_s3_key must be a bare object key, got: %s",
                        tokenized_key,
                    )
                    raise TextSummarizerError(
                        "Invalid tokenized_dataset_s3_key: provide a bare S3 key "
                        "(no 's3://bucket/').",
                        logger,
                    )
                if dvc_tokenized_key and dvc_tokenized_key.startswith("s3://"):
                    logger.error(
                        "dvc_tokenized_dataset_s3_key must be a bare object key, got: %s",  # noqa: E501
                        dvc_tokenized_key,
                    )
                    raise TextSummarizerError(
                        "Invalid dvc_tokenized_dataset_s3_key: provide a bare S3 key "
                        "(no 's3://bucket/').",
                        logger,
                    )

                # Use a temporary directory to assemble an upload payload that is
                # independent from local/DVC targets (avoids racing on writes).
                with self.backup_handler as handler, tempfile.TemporaryDirectory() as td:  # noqa: E501
                    tmp_path = Path(td)
                    tokenized_dataset.save_to_disk(tmp_path.as_posix())

                    if tokenized_key:
                        tokenized_dataset_s3_uri = handler.upload_dir(
                            tmp_path, tokenized_key
                        )
                        logger.info(
                            "Uploaded tokenized dataset folder to S3: %s",
                            tokenized_dataset_s3_uri,
                        )

                    if dvc_tokenized_key:
                        dvc_tokenized_dataset_s3_uri = handler.upload_dir(
                            tmp_path, dvc_tokenized_key
                        )
                        logger.info(
                            "Uploaded DVC tokenized dataset folder to S3: %s",
                            dvc_tokenized_dataset_s3_uri,
                        )

            return (
                dataset_dir,
                dvc_dataset_dir,
                tokenized_dataset_s3_uri,
                dvc_tokenized_dataset_s3_uri,
            )

        except Exception as e:  # noqa: BLE001
            logger.error("Failed during dataset save/upload.")
            raise TextSummarizerError(e, logger) from e

    def run_transformation(self) -> DataTransformationArtifact:
        """Execute the transformation pipeline end-to-end.

        Steps:
            1) Load dataset (local or S3) with structural checks and split counts.
            2) Tokenize using the configured tokenizer and max lengths.
            3) Save locally and to DVC, and/or upload to S3 with strict key checks.
            4) Return a structured artifact with all produced paths/URIs.

        Returns:
            DataTransformationArtifact: Paths/URIs of saved tokenized datasets.

        Raises:
            TextSummarizerError: If any stage fails (load, tokenize, save).
        """
        try:
            logger.info("Starting data transformation...")

            # Keep each stage focused; smaller failure domains aid debugging.
            dataset = self._load_data()
            tokenized_dataset = self._tokenize_data(dataset)
            (
                tokenized_dataset_dir,
                dvc_tokenized_dataset_dir,
                tokenized_dataset_s3_uri,
                dvc_tokenized_dataset_s3_uri,
            ) = self._save_all(tokenized_dataset)

            # Construct a stable artifact so downstream stages can rely on the
            # presence/absence of outputs based solely on configuration flags.
            artifact = DataTransformationArtifact(
                tokenized_dataset_dir=tokenized_dataset_dir,
                dvc_tokenized_dataset_dir=dvc_tokenized_dataset_dir,
                tokenized_dataset_s3_uri=tokenized_dataset_s3_uri,
                dvc_tokenized_dataset_s3_uri=dvc_tokenized_dataset_s3_uri,
            )

            logger.info("Data transformation complete: %s", artifact)
            return artifact

        except Exception as e:  # noqa: BLE001
            logger.error("Data transformation failed.")
            raise TextSummarizerError(e, logger) from e
