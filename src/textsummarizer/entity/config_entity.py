from dataclasses import dataclass
from pathlib import Path
from box import ConfigBox

@dataclass
class DataIngestionConfig:
    root_dir: Path
    source_url: str
    raw_filepath: Path
    dvc_raw_filepath: Path
    ingested_dir: Path
    dvc_ingested_dir: Path
    ingested_dir: Path
    dvc_ingested_dir: Path
    local_enabled: bool
    s3_enabled: bool
    dataset_name: str
    dataset_name: str

    def __post_init__(self) -> None:
        """Ensure all path-like attributes are Path objects."""
        self.root_dir = Path(self.root_dir)
        self.raw_filepath = Path(self.raw_filepath)
        self.dvc_raw_filepath = Path(self.dvc_raw_filepath)
        self.ingested_dir = Path(self.ingested_dir)
        self.dvc_ingested_dir = Path(self.dvc_ingested_dir)
        self.ingested_dir = Path(self.ingested_dir)
        self.dvc_ingested_dir = Path(self.dvc_ingested_dir)

    @property
    def raw_s3_key(self) -> str:
        """Generate the S3 key for the raw data file."""
        return self.raw_filepath.as_posix()

    @property
    def dvc_raw_s3_key(self) -> str:
        """Generate the S3 key for the DVC raw data file."""
        return self.dvc_raw_filepath.as_posix()

    @property
    def ingested_s3_key(self) -> str:
        """Generate the S3 key for the ingested data file."""
        return self.ingested_dir.as_posix()

        return self.ingested_dir.as_posix()

    @property
    def dvc_ingested_s3_key(self) -> str:
        """Generate the S3 key for the DVC ingested data file."""
        return self.dvc_ingested_dir.as_posix()
        return self.dvc_ingested_dir.as_posix()

    def __repr__(self) -> str:
        """Return a formatted string representation of the configuration."""
        parts = [
            "\nData Ingestion Config:",
            f"  - Root Dir:             {self.root_dir}",
            f"  - Source URL:           {self.source_url}",
            f"  - Raw Data Path:        {self.raw_filepath}",
            f"  - DVC Raw Data Path:    {self.dvc_raw_filepath}",
            f"  - Ingested Data Path:   {self.ingested_dir}",
            f"  - DVC Ingested Path:    {self.dvc_ingested_dir}",
            f"  - Ingested Data Path:   {self.ingested_dir}",
            f"  - DVC Ingested Path:    {self.dvc_ingested_dir}",
            f"  - Local Save Enabled:   {self.local_enabled}",
            f"  - S3 Upload Enabled:    {self.s3_enabled}",
            f"  - Raw S3 Key:           {self.raw_s3_key}",
            f"  - DVC Raw S3 Key:       {self.dvc_raw_s3_key}",
            f"  - Ingested S3 Key:      {self.ingested_s3_key}",
            f"  - DVC Ingested S3 Key:  {self.dvc_ingested_s3_key}",
            f"  - Dataset Name:         {self.dataset_name}",
            f"  - Dataset Name:         {self.dataset_name}",
        ]
        return "\n".join(parts)

@dataclass
class S3HandlerConfig:
    root_dir: Path
    bucket_name: str
    aws_region: str

    def __post_init__(self) -> None:
        self.root_dir = Path(self.root_dir)

    def __repr__(self) -> str:
        return (
            "\nS3 Handler Config:\n"
            f"  - Root Dir:              {self.root_dir}\n"
            f"  - Bucket Name:           {self.bucket_name}\n"
            f"  - AWS Region:            {self.aws_region}\n"
        )


@dataclass
class DataTransformationConfig:
    root_dir: Path
    tokenizer_name: str
    max_input_length: int
    max_target_length: int

    tokenized_dataset_dir: Path
    dvc_tokenized_dataset_dir: Path

    tokenized_dataset_dir: Path
    dvc_tokenized_dataset_dir: Path

    local_enabled: bool
    s3_enabled: bool
    local_enabled: bool
    s3_enabled: bool

    def __post_init__(self) -> None:
        """Ensure all path-like attributes are Path objects."""
        """Ensure all path-like attributes are Path objects."""
        self.root_dir = Path(self.root_dir)
        self.tokenized_dataset_dir = Path(self.tokenized_dataset_dir)
        self.dvc_tokenized_dataset_dir = Path(self.dvc_tokenized_dataset_dir)

    @property
    def tokenized_dataset_s3_key(self) -> str:
        """S3 key for the tokenized dataset."""
        return self.tokenized_dataset_dir.as_posix()

    @property
    def dvc_tokenized_dataset_s3_key(self) -> str:
        """S3 key for the DVC tokenized dataset."""
        return self.dvc_tokenized_dataset_dir.as_posix()

    def __repr__(self) -> str:
        parts = [
            "\nData Transformation Config:",
            f"  - Root Dir:                     {self.root_dir}",
            f"  - Tokenizer Name:               {self.tokenizer_name}",
            f"  - Max Input Length:             {self.max_input_length}",
            f"  - Max Target Length:            {self.max_target_length}",
            f"  - Tokenized Data Dir:           {self.tokenized_dataset_dir}",
            f"  - DVC Tokenized Data Dir:       {self.dvc_tokenized_dataset_dir}",
            f"  - Tokenized Dataset S3 Key:     {self.tokenized_dataset_s3_key}",
            f"  - DVC Tokenized Dataset S3 Key: {self.dvc_tokenized_dataset_s3_key}",
            f"  - Local Save Enabled:           {self.local_enabled}",
            f"  - S3 Upload Enabled:            {self.s3_enabled}",
            f"  - Root Dir:                     {self.root_dir}",
            f"  - Tokenizer Name:               {self.tokenizer_name}",
            f"  - Max Input Length:             {self.max_input_length}",
            f"  - Max Target Length:            {self.max_target_length}",
            f"  - Tokenized Data Dir:           {self.tokenized_dataset_dir}",
            f"  - DVC Tokenized Data Dir:       {self.dvc_tokenized_dataset_dir}",
            f"  - Tokenized Dataset S3 Key:     {self.tokenized_dataset_s3_key}",
            f"  - DVC Tokenized Dataset S3 Key: {self.dvc_tokenized_dataset_s3_key}",
            f"  - Local Save Enabled:           {self.local_enabled}",
            f"  - S3 Upload Enabled:            {self.s3_enabled}",
        ]
        return "\n".join(parts)



@dataclass
class ModelTrainerConfig:
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
        """Ensure all path-like attributes are Path objects."""
        self.root_dir = Path(self.root_dir)
        self.model_dir = Path(self.model_dir)
        self.tokenizer_dir = Path(self.tokenizer_dir)
        self.final_model_dir = Path(self.final_model_dir)
        self.final_tokenizer_dir = Path(self.final_tokenizer_dir)

    @property
    def model_s3_key(self) -> str:
        """S3 key for the model."""
        return self.model_dir.as_posix()

    @property
    def tokenizer_s3_key(self) -> str:
        """S3 key for the tokenizer."""
        return self.tokenizer_dir.as_posix()

    @property
    def final_model_s3_key(self) -> str:
        """S3 key for the final model."""
        return self.final_model_dir.as_posix()

    @property
    def final_tokenizer_s3_key(self) -> str:
        """S3 key for the final tokenizer."""
        return self.final_tokenizer_dir.as_posix()

    def __repr__(self) -> str:
        parts = [
            "\nModel Trainer Config:",
            f"  - Root Dir:                     {self.root_dir}",
            f"  - Model Dir:                    {self.model_dir}",
            f"  - Tokenizer Dir:                {self.tokenizer_dir}",
            f"  - Final Model Dir:              {self.final_model_dir}",
            f"  - Final Tokenizer Dir:          {self.final_tokenizer_dir}",
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
            f"  - Gradient Accumulation Steps:  {self.gradient_accumulation_steps}",
            f"  - Local Save Enabled:           {self.local_enabled}",
            f"  - S3 Upload Enabled:            {self.s3_enabled}",
        ]
        return "\n".join(parts)

@dataclass
class ModelEvaluationConfig:
    root_dir: Path
    report_path: Path
