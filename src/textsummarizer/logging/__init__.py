"""
Centralized logger for the textsummarizer project.

Provides a reusable `logger` instance configured with:
- UTC timestamped log directory and file
- File + stream handlers
- DEBUG level logging by default
"""

import logging
from .app_logger import setup_logger

# Static logger name and level
LOGGER_NAME = "textsummarizer_logger"
LOG_LEVEL = logging.DEBUG

# Initialize logger once and share across the project
logger = setup_logger(name=LOGGER_NAME, level=LOG_LEVEL)
