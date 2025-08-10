# FILE: src/textsummarizer/entity/config_entity.py
"""Configuration entities for the text summarization pipeline.

This module defines small, typed dataclasses that describe configuration inputs
for each pipeline component (ingestion, transformation, training, evaluation,
and prediction), plus the S3 handler.

Design intent:
- Strong typing: Keep shapes explicit so misconfigurations surface early.
- Path normalization: Convert all path-like fields to ``pathlib.Path`` in
  ``__post_init__`` so downstream code can rely on consistent types.
- Readability: Provide Google-style docstrings and rich ``__repr__`` for
  log-friendly inspection of active settings. **All paths are rendered using
  POSIX formatting** (``Path(...).as_posix()``) for cross-platform consistency.
- Simplicity: These classes are pure data holders—no I/O is performed here.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict


# =============================================================================
# Data Ingestion
# =============================================================================


@dataclass
class DataIngestionConfig:
    """Configuration for the data ingestion component.

    Attributes:
        root_dir (Path): Root folder for this component's artifacts.
        source_url (str): HTTP/HTTPS URL for the raw dataset archive/file.
        raw_filepath (Path): Local filesystem path where the raw file is saved.
        dvc_raw_filepath (Path): DVC-tracked mirror of ``raw_filepath``.
        ingested_dir (Path): Local directory where the archive is extracted.
        dvc_ingested_dir (Path): DVC-tracked mirror of ``ingested_dir``.
        local_enabled (bool): Whether to write artifacts to local disk.
        s3_enabled (bool): Whether to mirror artifacts to S3.
        dataset_name (str): Expected top-level dataset folder name after extract.
    """

    root_dir: Path
    source_url: str
    raw_filepath: Path
    dvc_raw_filepath: Path
    ingested_dir: Path
    dvc_ingested_dir: Path
    local_enabled: bool
    s3_enabled: bool
    dataset_name: str

    def __post_init__(self) -> None:
        """Normalize path-like attributes to ``Path`` instances.

        Why:
            Callers may pass strings (e.g., from YAML). Converting here keeps
            the rest of the codebase free from repetitive conversions.
        """
        # Normalize every path-like attribute exactly once at construction.
        self.root_dir = Path(self.root_dir)
        self.raw_filepath = Path(self.raw_filepath)
        self.dvc_raw_filepath = Path(self.dvc_raw_filepath)
        self.ingested_dir = Path(self.ingested_dir)
        self.dvc_ingested_dir = Path(self.dvc_ingested_dir)
        return None

    @property
    def raw_s3_key(self) -> str:
        """Compute the S3 object key for the **raw** file.

        Returns:
            str: POSIX-style key derived from ``raw_filepath``.
        """
        # Mirror local structure in S3 by using the POSIX path string.
        return self.raw_filepath.as_posix()

    @property
    def dvc_raw_s3_key(self) -> str:
        """Compute the S3 object key for the **DVC raw** file.

        Returns:
            str: POSIX-style key derived from ``dvc_raw_filepath``.
        """
        return self.dvc_raw_filepath.as_posix()

    @property
    def ingested_s3_key(self) -> str:
        """Compute the S3 object key for the **ingested** directory.

        Returns:
            str: POSIX-style key derived from ``ingested_dir``.
        """
        return self.ingested_dir.as_posix()

    @property
    def dvc_ingested_s3_key(self) -> str:
        """Compute the S3 object key for the **DVC ingested** directory.

        Returns:
            str: POSIX-style key derived from ``dvc_ingested_dir``.
        """
        return self.dvc_ingested_dir.as_posix()

    def __repr__(self) -> str:
        """Return a log-friendly, multi-line summary of the configuration.

        Returns:
            str: Pretty-printed configuration values (paths as POSIX).
        """
        parts = [
            "\nData Ingestion Config:",
            f"  - Root Dir:             {self.root_dir.as_posix()}",
            f"  - Source URL:           {self.source_url}",
            f"  - Raw Data Path:        {self.raw_filepath.as_posix()}",
            f"  - DVC Raw Data Path:    {self.dvc_raw_filepath.as_posix()}",
            f"  - Ingested Data Path:   {self.ingested_dir.as_posix()}",
            f"  - DVC Ingested Path:    {self.dvc_ingested_dir.as_posix()}",
            f"  - Local Save Enabled:   {self.local_enabled}",
            f"  - S3 Upload Enabled:    {self.s3_enabled}",
            f"  - Raw S3 Key:           {self.raw_s3_key}",
            f"  - DVC Raw S3 Key:       {self.dvc_raw_s3_key}",
            f"  - Ingested S3 Key:      {self.ingested_s3_key}",
            f"  - DVC Ingested S3 Key:  {self.dvc_ingested_s3_key}",
            f"  - Dataset Name:         {self.dataset_name}",
        ]
        return "\n".join(parts)


# =============================================================================
# S3 Handler
# =============================================================================


@dataclass
class S3HandlerConfig:
    """Configuration for the S3 handler.

    Attributes:
        root_dir (Path): Root folder to stage temporary S3-related artifacts.
        bucket_name (str): AWS S3 bucket name.
        aws_region (str): AWS region for the bucket.
    """

    root_dir: Path
    bucket_name: str
    aws_region: str

    def __post_init__(self) -> None:
        """Normalize path-like attributes to ``Path`` instances."""
        self.root_dir = Path(self.root_dir)
        return None

    def __repr__(self) -> str:
        """Return a log-friendly, multi-line summary of the configuration.

        Returns:
            str: Pretty-printed configuration values (paths as POSIX).
        """
        return (
            "\nS3 Handler Config:\n"
            f"  - Root Dir:              {self.root_dir.as_posix()}\n"
            f"  - Bucket Name:           {self.bucket_name}\n"
            f"  - AWS Region:            {self.aws_region}\n"
        )


# =============================================================================
# Data Transformation
# =============================================================================


@dataclass
class DataTransformationConfig:
    """Configuration for the data transformation component.

    Attributes:
        root_dir (Path): Root directory for transformation artifacts.
        tokenizer_name (str): Hugging Face tokenizer name/checkpoint.
        max_input_length (int): Max encoder input tokens.
        max_target_length (int): Max decoder target tokens.
        tokenized_dataset_dir (Path): Local path to save tokenized dataset.
        dvc_tokenized_dataset_dir (Path): DVC mirror for tokenized dataset.
        local_enabled (bool): Whether to write artifacts locally.
        s3_enabled (bool): Whether to mirror artifacts to S3.
    """

    root_dir: Path
    tokenizer_name: str
    max_input_length: int
    max_target_length: int

    tokenized_dataset_dir: Path
    dvc_tokenized_dataset_dir: Path

    local_enabled: bool
    s3_enabled: bool

    def __post_init__(self) -> None:
        """Normalize path-like attributes to ``Path`` instances."""
        self.root_dir = Path(self.root_dir)
        self.tokenized_dataset_dir = Path(self.tokenized_dataset_dir)
        self.dvc_tokenized_dataset_dir = Path(self.dvc_tokenized_dataset_dir)
        return None

    @property
    def tokenized_dataset_s3_key(self) -> str:
        """Derive the S3 object key for the tokenized dataset directory.

        Returns:
            str: POSIX-style key derived from ``tokenized_dataset_dir``.
        """
        return self.tokenized_dataset_dir.as_posix()

    @property
    def dvc_tokenized_dataset_s3_key(self) -> str:
        """Derive the S3 object key for the DVC tokenized dataset directory.

        Returns:
            str: POSIX-style key derived from ``dvc_tokenized_dataset_dir``.
        """
        return self.dvc_tokenized_dataset_dir.as_posix()

    def __repr__(self) -> str:
        """Return a log-friendly, multi-line summary of the configuration.

        Returns:
            str: Pretty-printed configuration values (paths as POSIX).
        """
        parts = [
            "\nData Transformation Config:",
            f"  - Root Dir:                     {self.root_dir.as_posix()}",
            f"  - Tokenizer Name:               {self.tokenizer_name}",
            f"  - Max Input Length:             {self.max_input_length}",
            f"  - Max Target Length:            {self.max_target_length}",
            f"  - Tokenized Data Dir:           "
            f"{self.tokenized_dataset_dir.as_posix()}",
            f"  - DVC Tokenized Data Dir:       "
            f"{self.dvc_tokenized_dataset_dir.as_posix()}",
            f"  - Tokenized Dataset S3 Key:     {self.tokenized_dataset_s3_key}",
            f"  - DVC Tokenized S3 Key:         {self.dvc_tokenized_dataset_s3_key}",
            f"  - Local Save Enabled:           {self.local_enabled}",
            f"  - S3 Upload Enabled:            {self.s3_enabled}",
        ]
        return "\n".join(parts)


# =============================================================================
# Model Trainer
# =============================================================================


@dataclass
class ModelTrainerConfig:
    """Configuration for the model training component.

    Attributes:
        root_dir (Path): Root directory for trainer artifacts.
        model_dir (Path): Directory where intermediate model checkpoints save.
        tokenizer_dir (Path): Directory where tokenizer is saved.
        model_ckpt (str): Base model checkpoint identifier.
        num_train_epochs (int): Training epochs.
        warmup_steps (int): LR scheduler warmup steps.
        per_device_train_batch_size (int): Train batch size per device.
        per_device_eval_batch_size (int): Eval batch size per device.
        weight_decay (float): Weight decay value.
        logging_steps (int): Logging frequency (steps).
        eval_strategy (str): HF evaluation strategy (e.g., 'steps', 'epoch').
        learning_rate (float): Optimizer learning rate.
        eval_steps (int): Evaluation frequency (steps).
        save_steps (float): Checkpoint save frequency (steps).
        gradient_accumulation_steps (int): Accumulation steps.
        final_model_dir (Path): Directory for final exported model.
        final_tokenizer_dir (Path): Directory for final exported tokenizer.
        local_enabled (bool): Whether to save locally.
        s3_enabled (bool): Whether to upload/mirror to S3.
    """

    root_dir: Path
    model_dir: Path
    tokenizer_dir: Path
    model_ckpt: str
    num_train_epochs: int
    warmup_steps: int
    per_device_train_batch_size: int
    per_device_eval_batch_size: int
    weight_decay: float
    logging_steps: int
    eval_strategy: str
    learning_rate: float
    eval_steps: int
    save_steps: float
    gradient_accumulation_steps: int
    final_model_dir: Path
    final_tokenizer_dir: Path
    local_enabled: bool
    s3_enabled: bool

    def __post_init__(self) -> None:
        """Normalize path-like attributes to ``Path`` instances."""
        self.root_dir = Path(self.root_dir)
        self.model_dir = Path(self.model_dir)
        self.tokenizer_dir = Path(self.tokenizer_dir)
        self.final_model_dir = Path(self.final_model_dir)
        self.final_tokenizer_dir = Path(self.final_tokenizer_dir)
        return None

    @property
    def model_s3_key(self) -> str:
        """Derive the S3 object key for the model directory.

        Returns:
            str: POSIX-style key derived from ``model_dir``.
        """
        return self.model_dir.as_posix()

    @property
    def tokenizer_s3_key(self) -> str:
        """Derive the S3 object key for the tokenizer directory.

        Returns:
            str: POSIX-style key derived from ``tokenizer_dir``.
        """
        return self.tokenizer_dir.as_posix()

    @property
    def final_model_s3_key(self) -> str:
        """Derive the S3 object key for the final model directory.

        Returns:
            str: POSIX-style key derived from ``final_model_dir``.
        """
        return self.final_model_dir.as_posix()

    @property
    def final_tokenizer_s3_key(self) -> str:
        """Derive the S3 object key for the final tokenizer directory.

        Returns:
            str: POSIX-style key derived from ``final_tokenizer_dir``.
        """
        return self.final_tokenizer_dir.as_posix()

    def __repr__(self) -> str:
        """Return a log-friendly, multi-line summary of the configuration.

        Returns:
            str: Pretty-printed configuration values (paths as POSIX).
        """
        parts = [
            "\nModel Trainer Config:",
            f"  - Root Dir:                     {self.root_dir.as_posix()}",
            f"  - Model Dir:                    {self.model_dir.as_posix()}",
            f"  - Model S3 Key:                 {self.model_s3_key}",
            f"  - Tokenizer Dir:                {self.tokenizer_dir.as_posix()}",
            f"  - Tokenizer S3 Key:             {self.tokenizer_s3_key}",
            f"  - Final Model Dir:              {self.final_model_dir.as_posix()}",
            f"  - Final Model S3 Key:           {self.final_model_s3_key}",
            f"  - Final Tokenizer Dir:          "
            f"{self.final_tokenizer_dir.as_posix()}",
            f"  - Final Tokenizer S3 Key:       {self.final_tokenizer_s3_key}",
            f"  - Model Checkpoint:             {self.model_ckpt}",
            f"  - Number of Training Epochs:    {self.num_train_epochs}",
            f"  - Warmup Steps:                 {self.warmup_steps}",
            f"  - Learning Rate:                {self.learning_rate}",
            f"  - Per Device Train Batch Size:  {self.per_device_train_batch_size}",
            f"  - Per Device Eval Batch Size:   {self.per_device_eval_batch_size}",
            f"  - Weight Decay:                 {self.weight_decay}",
            f"  - Logging Steps:                {self.logging_steps}",
            f"  - Evaluation Strategy:          {self.eval_strategy}",
            f"  - Eval Steps:                   {self.eval_steps}",
            f"  - Save Steps:                   {self.save_steps}",
            f"  - Gradient Accumulation Steps:  "
            f"{self.gradient_accumulation_steps}",
            f"  - Local Save Enabled:           {self.local_enabled}",
            f"  - S3 Upload Enabled:            {self.s3_enabled}",
        ]
        return "\n".join(parts)


# =============================================================================
# Model Evaluation
# =============================================================================


@dataclass
class ModelEvaluationConfig:
    """Configuration for the model evaluation component.

    Attributes:
        root_dir (Path): Root directory for evaluation artifacts.
        eval_report_filepath (Path): Local path for the evaluation report file.
        local_enabled (bool): Whether to write the report locally.
        s3_enabled (bool): Whether to upload the report to S3.
        eval_params (Dict[str, Any]): Metric configuration (names/options/etc.).
        max_input_length (int): Tokenizer max length for inputs during eval.
        max_target_length (int): Generated summary max length during eval.
        length_penalty (float): Generation penalty for longer sequences.
        num_beams (int): Beam search width for generation.
    """

    root_dir: Path
    eval_report_filepath: Path
    local_enabled: bool
    s3_enabled: bool
    eval_params: Dict[str, Any]
    max_input_length: int
    max_target_length: int
    length_penalty: float
    num_beams: int

    def __post_init__(self) -> None:
        """Normalize path-like attributes to ``Path`` instances."""
        self.root_dir = Path(self.root_dir)
        self.eval_report_filepath = Path(self.eval_report_filepath)
        return None

    @property
    def eval_report_s3_key(self) -> str:
        """Derive the S3 object key for the evaluation report file.

        Returns:
            str: POSIX-style key derived from ``eval_report_filepath``.
        """
        return self.eval_report_filepath.as_posix()

    def __repr__(self) -> str:
        """Return a log-friendly, multi-line summary of the configuration.

        Returns:
            str: Pretty-printed configuration values (paths as POSIX).
        """
        return (
            "\nModel Evaluation Config:"
            f"\n  - Root Dir:               {self.root_dir.as_posix()}"
            f"\n  - Max Input Length:       {self.max_input_length}"
            f"\n  - Max Target Length:      {self.max_target_length}"
            f"\n  - Length Penalty:         {self.length_penalty}"
            f"\n  - Num Beams:              {self.num_beams}"
            f"\n  - Eval Report Filepath:   "
            f"{self.eval_report_filepath.as_posix()}"
            f"\n  - Eval Report S3 Key:     {self.eval_report_s3_key}"
            f"\n  - Local Save Enabled:     {self.local_enabled}"
            f"\n  - S3 Upload Enabled:      {self.s3_enabled}"
        )


# =============================================================================
# Prediction
# =============================================================================


@dataclass
class PredictionConfig:
    """Configuration for the prediction/inference component.

    Attributes:
        root_dir (Path): Root folder to store prediction outputs.
        model_dir (Path | None): Local directory of the model to load, if any.
        tokenizer_dir (Path | None): Local directory of the tokenizer to load.
        local_enabled (bool): Whether to save predictions locally.
        s3_enabled (bool): Whether to upload predictions to S3.
        max_input_length (int): Max input length during generation.
        max_target_length (int): Max generated summary length.
        num_beams (int): Beam width for generation.
        length_penalty (float): Penalty to discourage long sequences.
        no_repeat_ngram_size (int): N-gram repeat constraint.
        device (str): Torch device string (e.g., "cpu", "cuda").
        batch_size (int): Batch size for batched inference.
        early_stopping (bool): Whether to use early stopping during generation.
    """

    root_dir: Path
    model_dir: Path | None
    tokenizer_dir: Path | None
    local_enabled: bool
    s3_enabled: bool
    max_input_length: int
    max_target_length: int
    num_beams: int
    length_penalty: float
    no_repeat_ngram_size: int
    device: str
    batch_size: int
    early_stopping: bool

    def __post_init__(self) -> None:
        """Normalize path-like attributes to ``Path`` instances."""
        self.root_dir = Path(self.root_dir)
        # Model/tokenizer can be None when loading from S3; only convert if set.
        self.model_dir = Path(self.model_dir) if self.model_dir is not None else None
        self.tokenizer_dir = (
            Path(self.tokenizer_dir) if self.tokenizer_dir is not None else None
        )
        return None

    @property
    def root_s3_key(self) -> str:
        """Derive the S3 object key root for prediction outputs.

        Returns:
            str: POSIX-style key derived from ``root_dir``.
        """
        return self.root_dir.as_posix()

    @property
    def model_s3_key(self) -> str | None:
        """Derive the S3 object key for the model directory (if present).

        Returns:
            str | None: POSIX-style key or ``None`` if ``model_dir`` is ``None``.
        """
        return self.model_dir.as_posix() if self.model_dir is not None else None

    @property
    def tokenizer_s3_key(self) -> str | None:
        """Derive the S3 object key for the tokenizer directory (if present).

        Returns:
            str | None: POSIX-style key or ``None`` if ``tokenizer_dir`` is
                ``None``.
        """
        return (
            self.tokenizer_dir.as_posix() if self.tokenizer_dir is not None else None
        )

    def __repr__(self) -> str:
        """Return a log-friendly, multi-line summary of the configuration.

        Returns:
            str: Pretty-printed configuration values (paths as POSIX).
        """
        model_dir_str = self.model_dir.as_posix() if self.model_dir else "None"
        tokenizer_dir_str = (
            self.tokenizer_dir.as_posix() if self.tokenizer_dir else "None"
        )

        return (
            "\nPrediction Config:"
            f"\n  - Root Dir:               {self.root_dir.as_posix()}"
            f"\n  - Root S3 Key:            {self.root_s3_key}"
            f"\n  - Model Dir:              {model_dir_str}"
            f"\n  - Tokenizer Dir:          {tokenizer_dir_str}"
            f"\n  - Model S3 Key:           {self.model_s3_key}"
            f"\n  - Tokenizer S3 Key:       {self.tokenizer_s3_key}"
            f"\n  - Local Save Enabled:     {self.local_enabled}"
            f"\n  - S3 Upload Enabled:      {self.s3_enabled}"
            f"\n  - Max Input Length:       {self.max_input_length}"
            f"\n  - Max Target Length:      {self.max_target_length}"
            f"\n  - Num Beams:              {self.num_beams}"
            f"\n  - Length Penalty:         {self.length_penalty}"
            f"\n  - No Repeat Ngram Size:   {self.no_repeat_ngram_size}"
            f"\n  - Device:                 {self.device}"
            f"\n  - Batch Size:             {self.batch_size}"
            f"\n  - Early Stopping:         {self.early_stopping}"
        )
