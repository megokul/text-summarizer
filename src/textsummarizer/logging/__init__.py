# FILE: src/textsummarizer/logging/__init__.py
"""Centralized logger initialization for the TextSummarizer project.

Design intent:
--------------
- Create a **single reusable logger** instance for the entire project so that
  all modules log in a consistent format and location.
- Configure both **file logging** (UTC timestamped) and **console logging**
  so developers get real-time feedback during execution and an archived log
  for debugging/auditing.
- Default to DEBUG level for maximum verbosity during development.
- Respect environment variables loaded from `.env` so logging behavior can be
  tuned per deployment environment without code changes.

Key features:
-------------
- **UTC timestamped log directory**: Each run gets its own timestamped file.
- **Dual handlers**: StreamHandler for console, FileHandler for disk.
- **Configurable log level**: Controlled by a constant here or via env vars.
"""

# Load environment variables from .env before any other imports use them.
from dotenv import load_dotenv

load_dotenv(override=True)

import logging  # noqa: E402  # (import after dotenv so env vars are ready)
from .app_logger import setup_logger  # noqa: E402

# ---------------------------------------------------------------------------
# Logger configuration constants
# ---------------------------------------------------------------------------

# Name used for the logger object; modules will log via `logger = logging.getLogger(LOGGER_NAME)`
LOGGER_NAME: str = "textsummarizer_logger"

# Default log level; can be overridden via environment variables if needed.
LOG_LEVEL: int = logging.DEBUG

# ---------------------------------------------------------------------------
# Global logger instance
# ---------------------------------------------------------------------------

# Initialize the logger using the centralized setup function.
# This function is responsible for creating handlers, formatting, and ensuring
# the logger writes to both stdout and the UTC timestamped log file.
logger = setup_logger(name=LOGGER_NAME, level=LOG_LEVEL)
