import logging
import sys
from io import BytesIO
from pathlib import Path

import boto3

from box import ConfigBox
from yaml import safe_load

from src.textsummarizer.constants.constants import (
    CONFIG_ROOT,
    CONFIG_FILENAME,
    LOGS_ROOT,
)
from src.textsummarizer.utils.timestamp import get_utc_timestamp


class LogHandler(logging.Handler):
    """
    Buffers all log lines in memory and, on each emit, PUTs the full buffer
    to S3 so that the object is always up-to-date. No local file is ever written.
    """
    def __init__(self, bucket: str, key: str, level: int = logging.NOTSET) -> None:
        super().__init__(level)
        self.bucket = bucket
        self.key = key
        self.buffer = BytesIO()
        self.s3 = boto3.client("s3")
        self.setFormatter(logging.Formatter(
            "[%(asctime)s] - %(levelname)s - %(module)s - %(message)s"
        ))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            line = self.format(record) + "\n"
            self.buffer.write(line.encode("utf-8"))
            # rewind to beginning before upload
            self.buffer.seek(0)
            self.s3.put_object(
                Bucket=self.bucket,
                Key=self.key,
                Body=self.buffer.getvalue()
            )
            # move buffer pointer to end for next write
            self.buffer.seek(0, 2)
        except Exception:
            self.handleError(record)

def setup_logger(name: str = "app_logger", level: int = logging.DEBUG) -> logging.Logger:
    """
    Configure and return a logger that:
      - Always writes to stdout.
      - If local_enabled: writes to LOGS_ROOT/<ts>/<ts>.log on disk.
      - If s3_enabled: streams to S3 under s3://<bucket>/logs/<ts>/<ts>.log.
      - If both flags are True, does both.
    """
    # ensure UTF-8 on console
    sys.stdout.reconfigure(encoding="utf-8")
    timestamp = get_utc_timestamp()

    # load YAML as ConfigBox
    config_path = Path(CONFIG_ROOT) / CONFIG_FILENAME
    with config_path.open("r", encoding="utf-8") as file:
        config = ConfigBox(safe_load(file))

    # flags
    local_enabled = config.data_backup.local_enabled
    s3_enabled = config.data_backup.s3_enabled
    bucket = config.s3_handler.s3_bucket

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # 1) Console handler (always)
    if not any(isinstance(h, logging.StreamHandler) and h.stream is sys.stdout
               for h in logger.handlers):
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(level)
        ch.setFormatter(logging.Formatter(
            "[%(asctime)s] - %(levelname)s - %(module)s - %(message)s"
        ))
        logger.addHandler(ch)

    # 2) Local file handler
    if local_enabled:
        log_dir = Path(LOGS_ROOT) / timestamp
        log_dir.mkdir(parents=True, exist_ok=True)
        log_filepath = log_dir / f"{timestamp}.log"

        if not any(isinstance(h, logging.FileHandler)
                   and h.baseFilename == str(log_filepath)
                   for h in logger.handlers):
            fh = logging.FileHandler(log_filepath, encoding="utf-8")
            fh.setLevel(level)
            fh.setFormatter(logging.Formatter(
                "[%(asctime)s] - %(levelname)s - %(module)s - %(message)s"
            ))
            logger.addHandler(fh)

    # 3) S3 handler
    if s3_enabled and bucket:
        log_s3_key = f"{LOGS_ROOT}/{timestamp}/{timestamp}.log"
        if not any(isinstance(h, LogHandler) for h in logger.handlers):
            s3h = LogHandler(bucket=bucket, key=log_s3_key, level=level)
            logger.addHandler(s3h)

    return logger
