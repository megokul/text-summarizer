from dataclasses import dataclass
from pathlib import Path
from typing import Optional

@dataclass(frozen=True)
class DataIngestionArtifact:
    raw_filepath: Path | None = None
    dvc_raw_filepath: Path | None = None
    ingested_filepath: Path | None = None
    dvc_ingested_filepath: Path | None = None
    raw_s3_uri: str | None = None
    dvc_raw_s3_uri: str | None = None
    ingested_s3_uri: str | None = None
    dvc_ingested_s3_uri: str | None = None

    def __repr__(self) -> str:
        raw_local_str = self.raw_filepath.as_posix() if self.raw_filepath else "None"
        dvc_raw_local_str = self.dvc_raw_filepath.as_posix() if self.dvc_raw_filepath else "None"
        ingested_local_str = self.ingested_filepath.as_posix() if self.ingested_filepath else "None"
        dvc_ingested_local_str = self.dvc_ingested_filepath.as_posix() if self.dvc_ingested_filepath else "None"

        raw_s3_str = self.raw_s3_uri if self.raw_s3_uri else "None"
        dvc_raw_s3_str = self.dvc_raw_s3_uri if self.dvc_raw_s3_uri else "None"
        ingested_s3_str = self.ingested_s3_uri if self.ingested_s3_uri else "None"
        dvc_ingested_s3_str = self.dvc_ingested_s3_uri if self.dvc_ingested_s3_uri else "None"

        return (
            "\nData Ingestion Artifact:\n"
            f"  - Raw Local Path:          '{raw_local_str}'\n"
            f"  - DVC Raw Local Path:      '{dvc_raw_local_str}'\n"
            f"  - Ingested Local Path:     '{ingested_local_str}'\n"
            f"  - DVC Ingested Local Path: '{dvc_ingested_local_str}'\n"
            f"  - Raw S3 URI:              '{raw_s3_str}'\n"
            f"  - DVC Raw S3 URI:          '{dvc_raw_s3_str}'\n"
            f"  - Ingested S3 URI:         '{ingested_s3_str}'"
            f"  - DVC Ingested S3 URI:     '{dvc_ingested_s3_str}'\n"
        )


@dataclass(frozen=True)
class DataTransformationArtifact:
    tokenized_dataset_dir: Path | None = None
    dvc_tokenized_dataset_dir: Path | None = None  
    tokenized_dataset_s3_uri: str | None = None
    dvc_tokenized_dataset_s3_uri: str | None = None
    
    def __repr__(self) -> str:  
        tokenized_local_str = self.tokenized_dataset_dir.as_posix() if self.tokenized_dataset_dir else "None"
        dvc_tokenized_local_str = self.dvc_tokenized_dataset_dir.as_posix() if self.dvc_tokenized_dataset_dir else "None"

        tokenized_s3_str = self.tokenized_dataset_s3_uri if self.tokenized_dataset_s3_uri else "None"
        dvc_tokenized_s3_str = self.dvc_tokenized_dataset_s3_uri if self.dvc_tokenized_dataset_s3_uri else "None"

        return (
            "\nData Transformation Artifact:\n"
            f"  - Tokenized Local Path:          '{tokenized_local_str}'\n"
            f"  - DVC Tokenized Local Path:      '{dvc_tokenized_local_str}'\n"
            f"  - Tokenized S3 URI:              '{tokenized_s3_str}'\n"
            f"  - DVC Tokenized S3 URI:          '{dvc_tokenized_s3_str}'\n"
        )

@dataclass(frozen=True)
class ModelTrainerArtifact:
    model_filepath: Path | None = None
    model_s3_uri: str | None = None

    def __repr__(self) -> str:
        def fmt(p): return p.as_posix() if isinstance(p, Path) else "None"
        def fmt_uri(u): return u if u else "None"

        return (
            "\nModel Trainer Artifact:\n"
            f"  - Model Filepath:        '{fmt(self.model_filepath)}'\n"
            f"  - Model S3 URI:          '{fmt_uri(self.model_s3_uri)}'\n"
        )