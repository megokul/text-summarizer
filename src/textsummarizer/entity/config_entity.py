from dataclasses import dataclass
from pathlib import Path

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
