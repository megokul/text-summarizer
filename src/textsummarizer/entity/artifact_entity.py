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


@dataclass(frozen=True)
class DataTransformationArtifact:
    transformed_data_path: str
