from dataclasses import dataclass
from pathlib import Path
from box import ConfigBox

@dataclass
class DataIngestionConfig:
    root_dir: Path
    source_url: str
    raw_filepath: Path
    dvc_raw_filepath: Path
    ingested_filepath: Path
    dvc_ingested_filepath: Path
    local_enabled: bool
    s3_enabled: bool

    def __post_init__(self) -> None:
        """Ensure all path-like attributes are Path objects."""
        self.root_dir = Path(self.root_dir)
        self.raw_filepath = Path(self.raw_filepath)
        self.dvc_raw_filepath = Path(self.dvc_raw_filepath)
        self.ingested_filepath = Path(self.ingested_filepath)
        self.dvc_ingested_filepath = Path(self.dvc_ingested_filepath)

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
        return self.ingested_filepath.as_posix()
        
    @property
    def dvc_ingested_s3_key(self) -> str:
        """Generate the S3 key for the DVC ingested data file."""
        return self.dvc_ingested_filepath.as_posix()

    def __repr__(self) -> str:
        """Return a formatted string representation of the configuration."""
        parts = [
            "\nData Ingestion Config:",
            f"  - Root Dir:             {self.root_dir}",
            f"  - Source URL:           {self.source_url}",
            f"  - Raw Data Path:        {self.raw_filepath}",
            f"  - DVC Raw Data Path:    {self.dvc_raw_filepath}",
            f"  - Ingested Data Path:   {self.ingested_filepath}",
            f"  - DVC Ingested Path:    {self.dvc_ingested_filepath}",
            f"  - Local Save Enabled:   {self.local_enabled}",
            f"  - S3 Upload Enabled:    {self.s3_enabled}",
            f"  - Raw S3 Key:           {self.raw_s3_key}",
            f"  - DVC Raw S3 Key:       {self.dvc_raw_s3_key}",
            f"  - Ingested S3 Key:      {self.ingested_s3_key}",
            f"  - DVC Ingested S3 Key:  {self.dvc_ingested_s3_key}",
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

    train_filepath: Path
    val_filepath: Path
    test_filepath: Path

    train_size: float
    val_size: float
    test_size: float
    random_state: int
    stratify: bool

    local_enabled: bool
    s3_enabled: bool

    def __post_init__(self) -> None:
        """Ensure all path-like attributes are Path objects."""
        self.root_dir = Path(self.root_dir)
        self.train_filepath = Path(self.train_filepath)
        self.val_filepath = Path(self.val_filepath)
        self.test_filepath = Path(self.test_filepath)

    @property
    def train_s3_key(self) -> str:
        """S3 key for the train dataset."""
        return self.train_filepath.as_posix()

    @property
    def val_s3_key(self) -> str:
        """S3 key for the validation dataset."""
        return self.val_filepath.as_posix()

    @property
    def test_s3_key(self) -> str:
        """S3 key for the test dataset."""
        return self.test_filepath.as_posix()

    def __repr__(self) -> str:
        parts = [
            "\nData Transformation Config:",
            f"  - Root Dir:              {self.root_dir}",
            f"  - Tokenizer Name:        {self.tokenizer_name}",
            f"  - Max Input Length:      {self.max_input_length}",
            f"  - Max Target Length:     {self.max_target_length}",
            f"  - Train Filepath:        {self.train_filepath}",
            f"  - Val Filepath:          {self.val_filepath}",
            f"  - Test Filepath:         {self.test_filepath}",
            f"  - Train Size:            {self.train_size}",
            f"  - Val Size:              {self.val_size}",
            f"  - Test Size:             {self.test_size}",
            f"  - Random State:          {self.random_state}",
            f"  - Stratify:              {self.stratify}",
            f"  - Local Save Enabled:    {self.local_enabled}",
            f"  - S3 Upload Enabled:     {self.s3_enabled}",
            f"  - Train S3 Key:          {self.train_s3_key}",
            f"  - Val S3 Key:            {self.val_s3_key}",
            f"  - Test S3 Key:           {self.test_s3_key}",
        ]
        return "\n".join(parts)



@dataclass
class ModelTrainerConfig:
    root_dir: Path
    model_path: Path
    report_path: Path
    inference_model_path: Path

@dataclass
class ModelEvaluationConfig:
    root_dir: Path
    report_path: Path
