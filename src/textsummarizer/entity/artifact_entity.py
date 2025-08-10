# FILE: src/textsummarizer/entity/artifact_entity.py
"""Artifact entity definitions for the text summarization pipeline.

This module declares small, immutable dataclasses that capture the outputs
("artifacts") produced by each pipeline stage. These artifacts provide a clear,
typed contract between components (e.g., ingestion → transformation → training),
and keep I/O details (local paths vs. S3 URIs) explicit and discoverable.

# Design intent
- Immutability: Artifacts are frozen dataclasses so downstream code treats them
  as read-only contracts rather than mutable state.
- Explicitness: Each possible output location (local path and/or S3 URI) is
  represented with a dedicated field and uses `| None` when unavailable.
- Developer ergonomics: Custom `__repr__` implementations format artifacts in a
  concise, human-readable block for fast log inspection.
"""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DataIngestionArtifact:
    """Record outputs produced by the data ingestion stage.

    Attributes:
        raw_filepath (Path | None): Local path to the downloaded raw ZIP/file.
        dvc_raw_filepath (Path | None): DVC-tracked mirror of the raw file.
        ingested_filepath (Path | None): Local path to extracted dataset root.
        dvc_ingested_filepath (Path | None): DVC-tracked mirror of extraction.
        raw_s3_uri (str | None): S3 URI of the uploaded raw ZIP/file.
        dvc_raw_s3_uri (str | None): S3 URI of the DVC raw mirror (if any).
        ingested_s3_uri (str | None): S3 URI to extracted dataset root.
        dvc_ingested_s3_uri (str | None): S3 URI to DVC ingested mirror.
    """

    raw_filepath: Path | None = None
    dvc_raw_filepath: Path | None = None
    ingested_filepath: Path | None = None
    dvc_ingested_filepath: Path | None = None
    raw_s3_uri: str | None = None
    dvc_raw_s3_uri: str | None = None
    ingested_s3_uri: str | None = None
    dvc_ingested_s3_uri: str | None = None

    def __repr__(self) -> str:
        """Return a compact, log-friendly multiline representation.

        Returns:
            str: Pretty-printed summary with both local paths and S3 URIs.
        """
        # Convert optional Paths to POSIX strings for consistent cross-platform
        # logging. Fallback to "None" for absent values to keep alignment.
        raw_local_str = (
            self.raw_filepath.as_posix() if self.raw_filepath else "None"
        )
        dvc_raw_local_str = (
            self.dvc_raw_filepath.as_posix() if self.dvc_raw_filepath else "None"
        )
        ingested_local_str = (
            self.ingested_filepath.as_posix() if self.ingested_filepath else "None"
        )
        dvc_ingested_local_str = (
            self.dvc_ingested_filepath.as_posix()
            if self.dvc_ingested_filepath
            else "None"
        )

        # URIs are already strings; fall back to "None" when absent.
        raw_s3_str = self.raw_s3_uri if self.raw_s3_uri else "None"
        dvc_raw_s3_str = self.dvc_raw_s3_uri if self.dvc_raw_s3_uri else "None"
        ingested_s3_str = self.ingested_s3_uri if self.ingested_s3_uri else "None"
        dvc_ingested_s3_str = (
            self.dvc_ingested_s3_uri if self.dvc_ingested_s3_uri else "None"
        )

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
    """Record outputs produced by the data transformation stage.

    Attributes:
        tokenized_dataset_dir (Path | None): Local path to tokenized dataset.
        dvc_tokenized_dataset_dir (Path | None): DVC mirror of tokenized data.
        tokenized_dataset_s3_uri (str | None): S3 URI of tokenized dataset.
        dvc_tokenized_dataset_s3_uri (str | None): S3 URI of DVC mirror.
    """

    tokenized_dataset_dir: Path | None = None
    dvc_tokenized_dataset_dir: Path | None = None
    tokenized_dataset_s3_uri: str | None = None
    dvc_tokenized_dataset_s3_uri: str | None = None

    def __repr__(self) -> str:
        """Return a compact, log-friendly multiline representation.

        Returns:
            str: Pretty-printed summary with both local paths and S3 URIs.
        """
        tokenized_local_str = (
            self.tokenized_dataset_dir.as_posix()
            if self.tokenized_dataset_dir
            else "None"
        )
        dvc_tokenized_local_str = (
            self.dvc_tokenized_dataset_dir.as_posix()
            if self.dvc_tokenized_dataset_dir
            else "None"
        )

        tokenized_s3_str = (
            self.tokenized_dataset_s3_uri
            if self.tokenized_dataset_s3_uri
            else "None"
        )
        dvc_tokenized_s3_str = (
            self.dvc_tokenized_dataset_s3_uri
            if self.dvc_tokenized_dataset_s3_uri
            else "None"
        )

        return (
            "\nData Transformation Artifact:\n"
            f"  - Tokenized Local Path:          '{tokenized_local_str}'\n"
            f"  - DVC Tokenized Local Path:      '{dvc_tokenized_local_str}'\n"
            f"  - Tokenized S3 URI:              '{tokenized_s3_str}'\n"
            f"  - DVC Tokenized S3 URI:          '{dvc_tokenized_s3_str}'\n"
        )


@dataclass(frozen=True)
class ModelTrainerArtifact:
    """Record outputs produced by the model training stage.

    Attributes:
        trained_model_dir (Path | None): Local path to trained model dir.
        tokenizer_dir (Path | None): Local path to tokenizer dir.
        final_model_dir (Path | None): Local path to final model dir.
        final_tokenizer_dir (Path | None): Local path to final tokenizer dir.
        model_s3_uri (str | None): S3 URI to trained model dir.
        tokenizer_s3_uri (str | None): S3 URI to tokenizer dir.
        final_model_s3_uri (str | None): S3 URI to final model dir.
        final_tokenizer_s3_uri (str | None): S3 URI to final tokenizer dir.
    """

    trained_model_dir: Path | None = None
    tokenizer_dir: Path | None = None
    final_model_dir: Path | None = None
    final_tokenizer_dir: Path | None = None
    model_s3_uri: str | None = None
    tokenizer_s3_uri: str | None = None
    final_model_s3_uri: str | None = None
    final_tokenizer_s3_uri: str | None = None

    def __repr__(self) -> str:
        """Return a compact, log-friendly multiline representation.

        Returns:
            str: Pretty-printed summary with both local paths and S3 URIs.
        """
        model_local_str = (
            self.trained_model_dir.as_posix()
            if self.trained_model_dir
            else "None"
        )
        tokenizer_local_str = (
            self.tokenizer_dir.as_posix() if self.tokenizer_dir else "None"
        )
        final_model_local_str = (
            self.final_model_dir.as_posix() if self.final_model_dir else "None"
        )
        final_tokenizer_local_str = (
            self.final_tokenizer_dir.as_posix()
            if self.final_tokenizer_dir
            else "None"
        )

        model_s3_str = self.model_s3_uri if self.model_s3_uri else "None"
        tokenizer_s3_str = (
            self.tokenizer_s3_uri if self.tokenizer_s3_uri else "None"
        )
        final_model_s3_str = (
            self.final_model_s3_uri if self.final_model_s3_uri else "None"
        )
        final_tokenizer_s3_str = (
            self.final_tokenizer_s3_uri
            if self.final_tokenizer_s3_uri
            else "None"
        )

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


@dataclass(frozen=True)
class ModelEvaluationArtifact:
    """Record outputs produced by the model evaluation stage.

    Attributes:
        eval_report_filepath (Path): Local YAML/CSV (etc.) path for the report.
        eval_report_s3_uri (str): S3 URI where the report was uploaded.
    """

    eval_report_filepath: Path
    eval_report_s3_uri: str

    def __repr__(self) -> str:
        """Return a compact, log-friendly multiline representation.

        Returns:
            str: Pretty-printed summary with local report path and S3 URI.
        """
        return (
            "\nModel Evaluation Artifact:\n"
            f"  - Report File: '{self.eval_report_filepath}'\n"
            f"  - Report S3 URI: '{self.eval_report_s3_uri}'\n"
        )
