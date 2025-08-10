"""Model training component for the text summarization pipeline.

Coordinates loading tokenized datasets, preparing a seq2seq model/tokenizer,
(optionally) running training, and saving artifacts locally and/or to S3.

Design intent:
- Centralize orchestration; delegate heavy lifting to Transformers/Datasets and
  the injected ``DBHandler`` for cloud I/O. This separation limits complexity
  here and makes it easier to test each boundary independently.
- Provide production-grade logging around each phase (load → train → save),
  logging both *intent* and *outcome*. This helps diagnose failures quickly.
- Wrap failures in ``TextSummarizerError`` with contextual logging so callers
  receive project-specific exceptions.
- **No added strict validation or fallbacks for S3 keys.** We pass the keys
  exactly as provided in configuration to the ``DBHandler``. This preserves the
  original behavior (no fail-fast checks and no auto-normalization).
- Return a predictable artifact containing all produced paths and URIs. Any
  disabled sink returns ``None`` for its field, so downstream code can branch
  deterministically without KeyErrors.
"""

from pathlib import Path
import tempfile

import torch
from box import ConfigBox
from datasets import DatasetDict, Features, Sequence, Value, load_from_disk
from transformers import (
    AutoModelForSeq2SeqLM,
    AutoTokenizer,
    DataCollatorForSeq2Seq,
    Trainer,
    TrainingArguments,
)

from src.textsummarizer.dbhandler.base_handler import DBHandler
from src.textsummarizer.entity.artifact_entity import (
    DataTransformationArtifact,
    ModelTrainerArtifact,
)
from src.textsummarizer.entity.config_entity import ModelTrainerConfig
from src.textsummarizer.exception.exception import TextSummarizerError
from src.textsummarizer.logging import logger


class ModelTrainer:
    """Orchestrate model training using Hugging Face Transformers."""

    def __init__(
        self,
        config: ModelTrainerConfig,
        transformation_artifact: DataTransformationArtifact,
        backup_handler: DBHandler | None = None,
    ) -> None:
        """Initialize trainer with configuration and inputs.

        Args:
            config (ModelTrainerConfig): Training hyperparameters and I/O config.
            transformation_artifact (DataTransformationArtifact): Output from the
                transformation stage with dataset locations.
            backup_handler (DBHandler | None): Optional cloud handler for S3 ops.

        Returns:
            None
        """
        # Keep dependencies on the instance so helpers can access them without
        # passing many parameters around. This reduces call-site noise.
        self.trainer_config = config
        self.transformation_artifact = transformation_artifact
        self.backup_handler = backup_handler

    def _get_device(self) -> str:
        """Select compute device based on CUDA availability.

        Returns:
            str: ``"cuda"`` if a GPU is available, otherwise ``"cpu"``.
        """
        # Prefer GPU when available for speed; otherwise fall back to CPU.
        device = "cuda" if torch.cuda.is_available() else "cpu"
        logger.info("Using device: %s", device)
        return device

    def _load_model_and_tokenizer(
        self,
    ) -> tuple[AutoModelForSeq2SeqLM, AutoTokenizer]:
        """Load model and tokenizer from the configured checkpoint.

        Loading both from the same checkpoint guarantees compatible vocabs and
        special tokens, which is critical for seq2seq models.

        Returns:
            tuple[AutoModelForSeq2SeqLM, AutoTokenizer]: Loaded model/tokenizer.

        Raises:
            TextSummarizerError: If the checkpoint fails to load.
        """
        try:
            # Keep tokenizer/model aligned to the same checkpoint to avoid
            # subtle tokenization mismatches (e.g., different special tokens).
            tokenizer = AutoTokenizer.from_pretrained(self.trainer_config.model_ckpt)
            model = AutoModelForSeq2SeqLM.from_pretrained(
                self.trainer_config.model_ckpt
            )
            logger.info(
                "Loaded model/tokenizer from checkpoint: %s",
                self.trainer_config.model_ckpt,
            )
            return model, tokenizer
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to load model/tokenizer from checkpoint.")
            raise TextSummarizerError(e, logger) from e

    def _cast_dataset_features(self, dataset: DatasetDict) -> DatasetDict:
        """Cast dataset numeric fields to consistent dtypes for training.

        Why cast?
        - Tokenization pipelines can yield Python lists whose integer widths
          vary by platform or intermediate steps. We normalize to int64 so the
          collator and model see predictable shapes/dtypes.

        Args:
            dataset (DatasetDict): Tokenized dataset.

        Returns:
            DatasetDict: Dataset with standardized feature types.

        Raises:
            TextSummarizerError: If casting fails.
        """
        try:
            # Define a canonical schema for our training columns.
            features = Features(
                {
                    "id": Value("string"),
                    "dialogue": Value("string"),
                    "summary": Value("string"),
                    "input_ids": Sequence(Value("int64")),
                    "attention_mask": Sequence(Value("int64")),
                    "labels": Sequence(Value("int64")),
                }
            )
            # Cast per split to avoid re-materializing the whole dataset.
            for split in dataset.keys():
                dataset[split] = dataset[split].cast(features)

            logger.info(
                "Casted input_ids, attention_mask, and labels to int64 for all "
                "splits."
            )
            return dataset
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to cast dataset features to int64.")
            raise TextSummarizerError(e, logger) from e

    def _load_dataset(self) -> DatasetDict:
        """Load the tokenized dataset from local disk or S3.

        Preference order:
        1) Local location if enabled and present (faster/cheaper).
        2) S3 location if enabled and present (supports cloud-only runs).

        Returns:
            DatasetDict: Tokenized dataset with splits.

        Raises:
            TextSummarizerError: If no valid source exists or loading fails.
        """
        try:
            # Prefer local disk to reduce cloud costs and latency.
            if (
                self.trainer_config.local_enabled
                and self.transformation_artifact.tokenized_dataset_dir
            ):
                dataset_path = Path(self.transformation_artifact.tokenized_dataset_dir)
                logger.info("Loading DatasetDict (local): %s", dataset_path)

                # Validate persisted DatasetDict layout early to fail fast on
                # misconfigured paths or half-written directories.
                if not dataset_path.exists() or not (
                    dataset_path / "dataset_dict.json"
                ).exists():
                    raise TextSummarizerError(
                        f"Expected DatasetDict structure not found at: {dataset_path}",
                        logger,
                    )

                # Use file:// form for path normalization across OSes.
                dataset = load_from_disk("file://" + dataset_path.as_posix())
                if not isinstance(dataset, DatasetDict):
                    raise TextSummarizerError(
                        f"Loaded dataset is not a DatasetDict: {type(dataset)}",
                        logger,
                    )
                logger.info("Loaded dataset from local disk.")
                return self._cast_dataset_features(dataset)

            # Fall back to S3 when local is disabled/unavailable.
            if (
                self.trainer_config.s3_enabled
                and self.transformation_artifact.tokenized_dataset_s3_uri
            ):
                s3_uri = self.transformation_artifact.tokenized_dataset_s3_uri
                logger.info("Loading DatasetDict (S3): %s", s3_uri)

                dataset = load_from_disk(s3_uri)
                if not isinstance(dataset, DatasetDict):
                    raise TextSummarizerError(
                        f"S3-loaded dataset is not a DatasetDict: {type(dataset)}",
                        logger,
                    )
                logger.info("Loaded dataset from S3.")
                return self._cast_dataset_features(dataset)

            # No viable source configured or present.
            raise TextSummarizerError(
                "No valid dataset location found for loading.",
                logger,
            )
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to load tokenized dataset.")
            raise TextSummarizerError(e, logger) from e

    def _get_training_args(self) -> TrainingArguments:
        """Create ``TrainingArguments`` from configuration.

        NOTE: Per your environment/tests, the parameter name is ``eval_strategy``
        (not the usual Transformers ``evaluation_strategy``). We map your config
        field directly to ``eval_strategy`` to prevent a runtime TypeError.

        Returns:
            TrainingArguments: Prepared arguments for ``Trainer``.

        Raises:
            TextSummarizerError: If arguments cannot be constructed.
        """
        try:
            # Keep conversions explicit to avoid accidental type mismatches from
            # YAML/Boxed configs (e.g., strings where ints are expected).
            args = TrainingArguments(
                output_dir=str(self.trainer_config.root_dir),
                num_train_epochs=int(self.trainer_config.num_train_epochs),
                warmup_steps=int(self.trainer_config.warmup_steps),
                per_device_train_batch_size=int(
                    self.trainer_config.per_device_train_batch_size
                ),
                per_device_eval_batch_size=int(
                    self.trainer_config.per_device_eval_batch_size
                ),
                weight_decay=float(self.trainer_config.weight_decay),
                logging_steps=int(self.trainer_config.logging_steps),
                # IMPORTANT: honor your environment's requirement.
                eval_strategy=str(self.trainer_config.eval_strategy),
                eval_steps=int(self.trainer_config.eval_steps),
                save_steps=int(self.trainer_config.save_steps),
                gradient_accumulation_steps=int(
                    self.trainer_config.gradient_accumulation_steps
                ),
                learning_rate=float(self.trainer_config.learning_rate),
                fp16=bool(getattr(self.trainer_config, "fp16", False)),
                report_to="mlflow",
            )
            logger.info("Training arguments initialized.")
            return args
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to initialize TrainingArguments.")
            raise TextSummarizerError(e, logger) from e

    def _save_model_and_tokenizer(
        self,
        model: AutoModelForSeq2SeqLM,
        tokenizer: AutoTokenizer,
    ) -> ConfigBox:
        """Save model/tokenizer locally and/or upload to S3.

        Local saves create two copies (working + final). S3 uploads mirror both
        copies using directory uploads so each artifact is self-contained.

        Args:
            model (AutoModelForSeq2SeqLM): Trained (or loaded) model.
            tokenizer (AutoTokenizer): Tokenizer aligned with the model.

        Returns:
            ConfigBox: Paths and URIs for all saved artifacts.

        Raises:
            TextSummarizerError: If any save or upload step fails.
        """
        try:
            # --- Local Paths (used as-is; no additional validation here) ---
            model_dir = self.trainer_config.model_dir
            tokenizer_dir = self.trainer_config.tokenizer_dir
            final_model_dir = self.trainer_config.final_model_dir
            final_tokenizer_dir = self.trainer_config.final_tokenizer_dir

            # --- S3 Keys (passed through to the handler exactly as provided) ---
            model_s3_key = self.trainer_config.model_s3_key
            tokenizer_s3_key = self.trainer_config.tokenizer_s3_key
            final_model_s3_key = self.trainer_config.final_model_s3_key
            final_tokenizer_s3_key = self.trainer_config.final_tokenizer_s3_key

            # Collect S3 URIs for the return value; start with Nones so the
            # caller's artifact shape is stable regardless of enabled sinks.
            model_s3_uri = None
            tokenizer_s3_uri = None
            final_model_s3_uri = None
            final_tokenizer_s3_uri = None

            # -----------------------------
            # Local: save model/tokenizer
            # -----------------------------
            if self.trainer_config.local_enabled:
                # Create folders explicitly to avoid relying on implicit create.
                logger.info("Saving model to %s", model_dir)
                model_dir.mkdir(parents=True, exist_ok=True)
                model.save_pretrained(model_dir)

                logger.info("Saving tokenizer to %s", tokenizer_dir)
                tokenizer_dir.mkdir(parents=True, exist_ok=True)
                tokenizer.save_pretrained(tokenizer_dir)

                # Produce a "final" copy that represents the trained state.
                logger.info("Saving final model to %s", final_model_dir)
                final_model_dir.mkdir(parents=True, exist_ok=True)
                model.save_pretrained(final_model_dir)

                logger.info("Saving final tokenizer to %s", final_tokenizer_dir)
                final_tokenizer_dir.mkdir(parents=True, exist_ok=True)
                tokenizer.save_pretrained(final_tokenizer_dir)

                logger.info("Local save of model/tokenizer complete.")

            # -----------------------------
            # S3: upload directories
            # -----------------------------
            if self.trainer_config.s3_enabled and self.backup_handler is not None:
                # IMPORTANT: We intentionally **do not** enforce bare-key checks
                # or perform URI normalization here. Keys/URIs are forwarded to
                # the handler unchanged to preserve original behavior.
                with self.backup_handler as handler:
                    if model_s3_key:
                        with tempfile.TemporaryDirectory() as td:
                            tmp_path = Path(td)
                            model.save_pretrained(tmp_path)
                            model_s3_uri = handler.upload_dir(tmp_path, model_s3_key)
                            logger.info(
                                "Uploaded model directory to S3: %s", model_s3_uri
                            )

                    if tokenizer_s3_key:
                        with tempfile.TemporaryDirectory() as td:
                            tmp_path = Path(td)
                            tokenizer.save_pretrained(tmp_path)
                            tokenizer_s3_uri = handler.upload_dir(
                                tmp_path, tokenizer_s3_key
                            )
                            logger.info(
                                "Uploaded tokenizer directory to S3: %s",
                                tokenizer_s3_uri,
                            )

                    if final_model_s3_key:
                        with tempfile.TemporaryDirectory() as td:
                            tmp_path = Path(td)
                            model.save_pretrained(tmp_path)
                            final_model_s3_uri = handler.upload_dir(
                                tmp_path, final_model_s3_key
                            )
                            logger.info(
                                "Uploaded final model directory to S3: %s",
                                final_model_s3_uri,
                            )

                    if final_tokenizer_s3_key:
                        with tempfile.TemporaryDirectory() as td:
                            tmp_path = Path(td)
                            tokenizer.save_pretrained(tmp_path)
                            final_tokenizer_s3_uri = handler.upload_dir(
                                tmp_path, final_tokenizer_s3_key
                            )
                            logger.info(
                                "Uploaded final tokenizer directory to S3: %s",
                                final_tokenizer_s3_uri,
                            )

            logger.info("Final model/tokenizer save operation complete.")

            saved_paths = ConfigBox(
                {
                    "model_dir": model_dir,
                    "tokenizer_dir": tokenizer_dir,
                    "final_model_dir": final_model_dir,
                    "final_tokenizer_dir": final_tokenizer_dir,
                    "model_s3_uri": model_s3_uri,
                    "tokenizer_s3_uri": tokenizer_s3_uri,
                    "final_model_s3_uri": final_model_s3_uri,
                    "final_tokenizer_s3_uri": final_tokenizer_s3_uri,
                }
            )
            return saved_paths

        except Exception as e:  # noqa: BLE001
            logger.error("Failed while saving/uploading model/tokenizer.")
            raise TextSummarizerError(e, logger) from e

    def train(self) -> ModelTrainerArtifact:
        """Execute the training workflow and return a trainer artifact.

        The default behavior in this project currently *skips* the actual call
        to ``trainer.train()`` and instead loads a pre-trained backup model/
        tokenizer. This is common during pipeline bring-up or when CI budgets
        cannot sustain full training. If/when training is re-enabled, we keep
        the structure here so the switch is one line.

        Returns:
            ModelTrainerArtifact: Paths/URIs of saved model/tokenizer artifacts.

        Raises:
            TextSummarizerError: If any stage of the workflow fails.
        """
        try:
            logger.info("Starting model training pipeline.")

            # 1) Device setup — record what accelerator we're using for traceability.
            device = self._get_device()

            # 2) Load model/tokenizer — keep them aligned to the same checkpoint.
            model, tokenizer = self._load_model_and_tokenizer()
            model = model.to(device)

            # 3) Load dataset — already tokenized in a prior stage.
            dataset = self._load_dataset()

            # 4) Prepare training arguments — pulled from config for reproducibility.
            training_args = self._get_training_args()

            # 5) Collator — handles dynamic padding and label shifting for seq2seq.
            data_collator = DataCollatorForSeq2Seq(tokenizer, model=model)

            # 6) Create Trainer — keeps train/eval loops, logging, and checkpointing.
            trainer = Trainer(
                model=model,
                args=training_args,
                tokenizer=tokenizer,
                data_collator=data_collator,
                train_dataset=dataset["train"],
                eval_dataset=dataset["validation"],
            )

            # 7) Training — intentionally disabled for now. This preserves the
            #    pipeline structure while avoiding long-running training in CI.
            # trainer.train()
            logger.info("Training skipped — using backup model for now.")

            # Use in production
            # trainer.train()

            # 8) Development shortcut — load from prebuilt backup assets.
            model, tokenizer = self.load_model_from_backup()

            # 9) Persist results to configured sinks (local/S3).
            saved_paths = self._save_model_and_tokenizer(model, tokenizer)

            # 10) Return a structured artifact with all locations.
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

            logger.info("Model training successfully completed: %s", artifact)
            return artifact

        except Exception as e:  # noqa: BLE001
            logger.error("Error during model training.")
            raise TextSummarizerError(e, logger) from e

    def load_model_from_backup(self) -> tuple[AutoModelForSeq2SeqLM, AutoTokenizer]:
        """Load a pretrained model/tokenizer from local backup directories.

        This path avoids network calls at this stage and gives deterministic
        artifacts for downstream steps (e.g., evaluation). Paths are converted
        to POSIX to neutralize platform differences.

        Returns:
            tuple[AutoModelForSeq2SeqLM, AutoTokenizer]: Loaded backup objects.

        Raises:
            TextSummarizerError: If backup folders are missing or load fails.
        """
        try:
            model_dir = Path("model_backup/pegasus_samsum_model")
            tokenizer_dir = Path("model_backup/pegasus_samsum_tokenizer")

            logger.info("Loading model from backup directory: %s", model_dir)
            model = AutoModelForSeq2SeqLM.from_pretrained(model_dir.as_posix())

            logger.info("Loading tokenizer from backup directory: %s", tokenizer_dir)
            tokenizer = AutoTokenizer.from_pretrained(tokenizer_dir.as_posix())

            logger.info("Model and tokenizer loaded successfully from backup.")
            return model, tokenizer
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to load model/tokenizer from backup.")
            raise TextSummarizerError(e, logger) from e
