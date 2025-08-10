"""Abstract base interface for storage/database handlers.

This module defines a minimal, consistent interface for components that read
and write data to external systems (e.g., S3, relational/NoSQL databases,
local files). Concrete implementations should inherit from ``DBHandler`` and
provide the required behaviors.

Design intent:
    - Enforce a common contract across heterogeneous backends so the rest of
      the pipeline (ingestion, transformation, evaluation, etc.) can remain
      backend-agnostic.
    - Provide a context manager protocol to guarantee timely resource cleanup
      (connections, file handles, temp dirs) without leaking details to call
      sites.
    - Centralize small, reusable helpers (e.g., CSV loading) with consistent
      logging and exception semantics using the project-specific
      ``TextSummarizerError`` wrapper.
"""

from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd

from src.textsummarizer.exception.exception import TextSummarizerError
from src.textsummarizer.logging import logger


class DBHandler(ABC):
    """Abstract base class for all database and storage handlers.

    Concrete subclasses may represent systems like PostgreSQL, MongoDB, S3,
    or even the local filesystem. The goal is to present a uniform surface
    area to upstream components.
    """

    # --------------------------------------------------------------------- #
    # Context manager protocol
    # --------------------------------------------------------------------- #
    def __enter__(self) -> "DBHandler":
        """Enter the runtime context.

        Returns:
            DBHandler: The handler instance itself so callers can use ``as``.
        """
        # Returning self allows: ``with handler as h: ...``
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb,  # traceback type intentionally unannotated to avoid importing
    ) -> None:
        """Exit the runtime context and ensure resources are closed.

        We always call ``close()`` to release connections or handles. Any
        exception raised by ``close()`` is wrapped in ``TextSummarizerError``
        to maintain consistent error semantics across the project.

        Args:
            exc_type (type[BaseException] | None): Exception type if raised.
            exc_val (BaseException | None): Exception instance if raised.
            exc_tb: Traceback object if raised.

        Returns:
            None

        Raises:
            TextSummarizerError: If closing resources fails.
        """
        try:
            # Always attempt cleanup; call sites should not need to remember.
            self.close()
        except Exception as e:  # noqa: BLE001
            logger.error("An error occurred while closing the DBHandler.")
            raise TextSummarizerError(e, logger) from e
        return None

    # --------------------------------------------------------------------- #
    # Required interface
    # --------------------------------------------------------------------- #
    @abstractmethod
    def close(self) -> None:
        """Close any open connections or resources.

        Implementations must release anything acquired (DB sessions, sockets,
        temp directories, etc.). This method is called by ``__exit__`` and may
        be called by users explicitly as well.

        Returns:
            None
        """
        # Abstract; subclasses must implement and explicitly return None.
        return None

    @abstractmethod
    def load_from_source(self) -> pd.DataFrame:
        """Load data from the primary source.

        This should encapsulate the *canonical* way the handler retrieves data
        (e.g., a query from a database, reading a directory in object storage,
        or opening a local file).

        Returns:
            pd.DataFrame: Tabular data loaded from the handler's source.
        """
        # Abstract; subclasses must implement and return a DataFrame.
        raise NotImplementedError

    # --------------------------------------------------------------------- #
    # Shared helper(s)
    # --------------------------------------------------------------------- #
    def load_from_csv(self, source: Path) -> pd.DataFrame:
        """Load a CSV file into a pandas DataFrame with consistent logging.

        This provides a narrow, predictable CSV reader so all components share
        the same behavior and error semantics. It intentionally avoids exotic
        parsing options; callers that need advanced behavior should implement
        it in a subclass to keep this base helper simple and reliable.

        Args:
            source (Path): Path to the CSV file.

        Returns:
            pd.DataFrame: The loaded DataFrame.

        Raises:
            TextSummarizerError: If the CSV cannot be read for any reason.
        """
        try:
            # Read the CSV using pandas' default heuristics. We do not attempt
            # dtype inference overrides here to keep the helper generic.
            df = pd.read_csv(source)

            # Log success with the resolved POSIX path for cross-platform clarity.
            logger.info("Loaded DataFrame from CSV: %s", source.as_posix())
            return df
        except Exception as e:  # noqa: BLE001
            # Provide context (path attempted) before raising the project error.
            logger.error(
                "Failed to load DataFrame from CSV: %s", source.as_posix()
            )
            raise TextSummarizerError(e, logger) from e
