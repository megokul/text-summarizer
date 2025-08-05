from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from typing import Optional

@dataclass(frozen=True)
class DataIngestionArtifact:
    raw_filepath: Path | None = None
    dvc_raw_filepath: Path | None = None
    ingested_filepath: Path | None = None
    dvc_ingested_filepath: Path | None = None
    dvc_ingested_filepath: Path | None = None
    raw_s3_uri: str | None = None
    dvc_raw_s3_uri: str | None = None
    ingested_s3_uri: str | None = None
    dvc_ingested_s3_uri: str | None = None
    dvc_ingested_s3_uri: str | None = None

    def __repr__(self) -> str:
        raw_local_str = self.raw_filepath.as_posix() if self.raw_filepath else "None"
        dvc_raw_local_str = self.dvc_raw_filepath.as_posix() if self.dvc_raw_filepath else "None"
        ingested_local_str = self.ingested_filepath.as_posix() if self.ingested_filepath else "None"
        dvc_ingested_local_str = self.dvc_ingested_filepath.as_posix() if self.dvc_ingested_filepath else "None"
        dvc_ingested_local_str = self.dvc_ingested_filepath.as_posix() if self.dvc_ingested_filepath else "None"

        raw_s3_str = self.raw_s3_uri if self.raw_s3_uri else "None"
        dvc_raw_s3_str = self.dvc_raw_s3_uri if self.dvc_raw_s3_uri else "None"
        ingested_s3_str = self.ingested_s3_uri if self.ingested_s3_uri else "None"
        dvc_ingested_s3_str = self.dvc_ingested_s3_uri if self.dvc_ingested_s3_uri else "None"
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
    trained_model_dir: Path | None = None
    tokenizer_dir: Path | None = None
    final_model_dir: Path | None = None
    final_tokenizer_dir: Path | None = None
    model_s3_uri: str | None = None
    tokenizer_s3_uri: str | None = None
    final_model_s3_uri: str | None = None
    final_tokenizer_s3_uri: str | None = None

    def __repr__(self) -> str:
        model_local_str = self.trained_model_dir.as_posix() if self.trained_model_dir else "None"
        tokenizer_local_str = self.tokenizer_dir.as_posix() if self.tokenizer_dir else "None"
        final_model_local_str = self.final_model_dir.as_posix() if self.final_model_dir else "None"
        final_tokenizer_local_str = self.final_tokenizer_dir.as_posix() if self.final_tokenizer_dir else "None"
        model_s3_str = self.model_s3_uri if self.model_s3_uri else "None"
        tokenizer_s3_str = self.tokenizer_s3_uri if self.tokenizer_s3_uri else "None"
        final_model_s3_str = self.final_model_s3_uri if self.final_model_s3_uri else "None"
        final_tokenizer_s3_str = self.final_tokenizer_s3_uri if self.final_tokenizer_s3_uri else "None"

        return (
            "\nModel Trainer Artifact:\n"
            f"  - Trained Model Local Path:          '{model_local_str}'\n"
            f"  - Tokenizer Local Path:              '{tokenizer_local_str}'\n"
            f"  - Final Model Local Path:            '{final_model_local_str}'\n"
            f"  - Final Tokenizer Local Path:        '{final_tokenizer_local_str}'\n"
            f"  - Model S3 URI:                      '{model_s3_str}'\n"
            f"  - Tokenizer S3 URI:                  '{tokenizer_s3_str}'\n"
            f"  - Final Model S3 URI:                '{final_model_s3_str}'\n"
            f"  - Final Tokenizer S3 URI:            '{final_tokenizer_s3_str}'\n"
        )