# FILE: src/textsummarizer/logging/app_logger.py
"""Centralized logger setup utilities.

This module provides:
- A memory-buffered S3 log handler that keeps a single object up-to-date.
- A `setup_logger` function that configures a project-wide logger with
  console, optional local file, and optional S3 handlers.

Design intent:
- Single source of truth for logging configuration across the project.
- Idempotent setup that avoids duplicate handlers on repeated calls.
- Zero surprise file layout: logs/<UTC>/UTC.log for both local and S3.
- Fail closed with uniform project exceptions while logging meaningful context.
"""

from __future__ import annotations  # TODO(clarify): Explain what this does

# Stdlib imports
import logging
import sys
from io import BytesIO
from pathlib import Path

# Third-party imports
import boto3
from box import ConfigBox
from yaml import safe_load

# Local imports
from src.textsummarizer.constants.constants import (
    CONFIG_FILENAME,
    CONFIG_ROOT,
    LOGS_ROOT,
)
from src.textsummarizer.exception.exception import TextSummarizerError
from src.textsummarizer.utils.timestamp import get_utc_timestamp


class LogHandler(logging.Handler):
    """In-memory S3 log sink that overwrites a single object on each emit.

    This handler:
    - Buffers all lines in memory so the remote object represents the full log
      at any point in time (helpful for tailing or post-mortems).
    - Uploads (PUT) the *entire* buffer on each emit. This is simpler than
      multipart append semantics and keeps object state consistent.
    - Avoids writing any local files when S3 logging is enabled.

    Notes:
        - We rely on ``logging.Handler.handleError`` inside ``emit`` to avoid
          recursive logging during failures (which could cause infinite loops).
    """

    def __init__(self, bucket: str, key: str, level: int = logging.NOTSET) -> None:
        """Initialize the S3 log handler.

        Args:
            bucket (str): Target S3 bucket name.
            key (str): Object key under which logs are written.
            level (int): Logging level for this handler.

        Returns:
            None
        """
        # Initialize base handler with the desired level.
        super().__init__(level)

        # Store immutable S3 target info.
        self.bucket = bucket
        self.key = key

        # Buffer holds the entire log file in memory. This makes each upload
        # idempotent: the result object is always the full current log.
        self.buffer = BytesIO()

        # Create a low-level S3 client (boto3 manages connection pooling).
        self.s3 = boto3.client("s3")

        # Keep formatting consistent with other project handlers.
        self.setFormatter(
            logging.Formatter(
                "[%(asctime)s] - %(levelname)s - %(module)s - %(message)s"
            )
        )
        return None

    def emit(self, record: logging.LogRecord) -> None:
        """Write a log record to the in-memory buffer and upload to S3.

        Args:
            record (logging.LogRecord): The record to format and write.

        Returns:
            None
        """
        try:
            # Format the record and append a newline for readability.
            line = self.format(record) + "\n"

            # Write to the end of the buffer (append mode).
            self.buffer.write(line.encode("utf-8"))

            # Reset pointer to the beginning for upload.
            self.buffer.seek(0)

            # Upload the entire buffer content. PUT is idempotent for the
            # object contents: after this call, the object reflects all logs.
            self.s3.put_object(
                Bucket=self.bucket,
                Key=self.key,
                Body=self.buffer.getvalue(),
            )

            # Move pointer back to end so subsequent writes append correctly.
            self.buffer.seek(0, 2)
        except Exception:  # noqa: BLE001
            # Delegate to logging internals; prevents recursive logging here.
            self.handleError(record)
        return None


def setup_logger(name: str = "app_logger", level: int = logging.DEBUG) -> logging.Logger:
    """Create or retrieve a configured logger with console/file/S3 handlers.

    Behavior:
        - Always attaches a console (stdout) handler.
        - If ``local_enabled`` in config: writes to logs/<UTC>/<UTC>.log.
        - If ``s3_enabled`` in config: writes to s3://<bucket>/logs/<UTC>/<UTC>.log.
        - Idempotent: re-calling will not duplicate identical handlers.

    Args:
        name (str): Logger name to create/retrieve.
        level (int): Logging level to apply to the logger and new handlers.

    Returns:
        logging.Logger: The configured logger instance.

    Raises:
        TextSummarizerError: If configuration or handler setup fails.
    """
    try:
        # Ensure stdout speaks UTF-8 where possible. In some environments
        # (e.g., redirected streams), reconfigure may be unavailable; fail-soft.
        try:
            sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            # Best-effort only; do not block logger setup if unavailable.
            pass

        # Use a UTC timestamp so concurrent machines do not clash on local time.
        timestamp = get_utc_timestamp()

        # Read project config (YAML) once to determine logging sinks.
        config_path = Path(CONFIG_ROOT) / CONFIG_FILENAME
        with config_path.open("r", encoding="utf-8") as file:
            # ConfigBox provides attribute-style access (dot notation).
            config = ConfigBox(safe_load(file))

        # Toggle sinks from config. These flags allow environment-driven
        # control: local vs. S3 logging, or both.
        local_enabled = bool(config.data_backup.local_enabled)
        s3_enabled = bool(config.data_backup.s3_enabled)
        bucket = str(config.s3_handler.bucket_name)

        # Create/retrieve the named logger.
        logger = logging.getLogger(name)
        logger.setLevel(level)

        # ------------------------------------------------------------------
        # 1) Console handler (always present)
        # ------------------------------------------------------------------
        # Avoid adding a duplicate console handler for the same stream.
        has_console = any(
            isinstance(h, logging.StreamHandler) and getattr(h, "stream", None) is sys.stdout
            for h in logger.handlers
        )
        if not has_console:
            ch = logging.StreamHandler(sys.stdout)
            ch.setLevel(level)
            ch.setFormatter(
                logging.Formatter(
                    "[%(asctime)s] - %(levelname)s - %(module)s - %(message)s"
                )
            )
            logger.addHandler(ch)

        # ------------------------------------------------------------------
        # 2) Local file handler (optional)
        # ------------------------------------------------------------------
        if local_enabled:
            # Each run writes to its own timestamped subdirectory for easy
            # cleanup and auditability.
            log_dir = Path(LOGS_ROOT) / timestamp
            log_dir.mkdir(parents=True, exist_ok=True)
            log_filepath = log_dir / f"{timestamp}.log"

            # Avoid duplicate FileHandlers pointing at the same file.
            has_file = any(
                isinstance(h, logging.FileHandler)
                and getattr(h, "baseFilename", None) == str(log_filepath)
                for h in logger.handlers
            )
            if not has_file:
                fh = logging.FileHandler(log_filepath, encoding="utf-8")
                fh.setLevel(level)
                fh.setFormatter(
                    logging.Formatter(
                        "[%(asctime)s] - %(levelname)s - %(module)s - %(message)s"
                    )
                )
                logger.addHandler(fh)

        # ------------------------------------------------------------------
        # 3) S3 handler (optional)
        # ------------------------------------------------------------------
        if s3_enabled and bucket:
            log_s3_key = f"{LOGS_ROOT}/{timestamp}/{timestamp}.log"

            # Avoid adding multiple S3 handlers. This keeps uploads bounded.
            has_s3 = any(isinstance(h, LogHandler) for h in logger.handlers)
            if not has_s3:
                s3h = LogHandler(bucket=bucket, key=log_s3_key, level=level)
                logger.addHandler(s3h)

        return logger

    except Exception as e:  # noqa: BLE001
        # Surface a clear failure path for upstream initialization code.
        # Note: We cannot use a global logger here since setup may be failing.
        raise TextSummarizerError(e, None) from e
