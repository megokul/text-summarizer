from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class DataIngestionArtifact:
    raw_filepath: Path | None = None
    dvc_raw_filepath: Path | None = None
    ingested_filepath: Path | None = None
    raw_s3_uri: str | None = None
    dvc_raw_s3_uri: str | None = None
    ingested_s3_uri: str | None = None

    def __repr__(self) -> str:
        raw_local_str = self.raw_filepath.as_posix() if self.raw_filepath else "None"
        dvc_raw_local_str = self.dvc_raw_filepath.as_posix() if self.dvc_raw_filepath else "None"
        ingested_local_str = self.ingested_filepath.as_posix() if self.ingested_filepath else "None"

        raw_s3_str = self.raw_s3_uri if self.raw_s3_uri else "None"
        dvc_raw_s3_str = self.dvc_raw_s3_uri if self.dvc_raw_s3_uri else "None"
        ingested_s3_str = self.ingested_s3_uri if self.ingested_s3_uri else "None"

        return (
            "\nData Ingestion Artifact:\n"
            f"  - Raw Local Path:        '{raw_local_str}'\n"
            f"  - DVC Raw Local Path:    '{dvc_raw_local_str}'\n"
            f"  - Ingested Local Path:   '{ingested_local_str}'\n"
            f"  - Raw S3 URI:            '{raw_s3_str}'\n"
            f"  - DVC Raw S3 URI:        '{dvc_raw_s3_str}'\n"
            f"  - Ingested S3 URI:       '{ingested_s3_str}'"
        )


from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class DataTransformationArtifact:
    train_filepath: Optional[Path] = None
    val_filepath: Optional[Path] = None
    test_filepath: Optional[Path] = None

    train_s3_uri: Optional[str] = None
    val_s3_uri: Optional[str] = None
    test_s3_uri: Optional[str] = None

    def __repr__(self) -> str:
        def fmt(p): return p.as_posix() if isinstance(p, Path) else "None"
        def fmt_uri(u): return u if u else "None"

        return (
            "\nData Transformation Artifact:\n"
            f"  - Train Filepath:        '{fmt(self.train_filepath)}'\n"
            f"  - Val Filepath:          '{fmt(self.val_filepath)}'\n"
            f"  - Test Filepath:         '{fmt(self.test_filepath)}'\n"
            f"  - Train S3 URI:          '{fmt_uri(self.train_s3_uri)}'\n"
            f"  - Val S3 URI:            '{fmt_uri(self.val_s3_uri)}'\n"
            f"  - Test S3 URI:           '{fmt_uri(self.test_s3_uri)}'\n"
        )
