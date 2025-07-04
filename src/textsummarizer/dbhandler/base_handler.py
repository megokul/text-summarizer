from abc import ABC, abstractmethod
from pathlib import Path
import pandas as pd

from src.textsummarizer.exception.exception import TextSummarizerError
from src.textsummarizer.logging import logger


class DBHandler(ABC):
    """
    Abstract base class for all database and storage handlers.

    This class defines a common interface for interacting with various data
    sources like PostgreSQL, MongoDB, S3, or local files, ensuring
    consistent behavior across the application.
    """

    def __enter__(self) -> "DBHandler":
        """Enter the runtime context."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Exit the runtime context and ensure resources are closed.
        """
        try:
            self.close()
        except Exception as e:
            logger.info("An error occurred while closing the DBHandler.")
            raise TextSummarizerError(e, logger) from e

    @abstractmethod
    def close(self) -> None:
        """
        Close any open connections or resources.
        """
        pass

    @abstractmethod
    def load_from_source(self) -> pd.DataFrame:
        """
        Load data from the primary source (e.g., a database table or a specific file).
        """
        pass

    def load_from_csv(self, source: Path) -> pd.DataFrame:
        """
        Load data from a CSV file into a pandas DataFrame.

        Args:
            source (Path): The path to the CSV file.

        Returns:
            pd.DataFrame: The loaded DataFrame.
        """
        try:
            df = pd.read_csv(source)
            logger.info(f"DataFrame loaded successfully from CSV: {source.as_posix()}")
            return df
        except Exception as e:
            logger.info(f"Failed to load DataFrame from CSV: '{source.as_posix()}'")
            raise TextSummarizerError(e, logger) from e
