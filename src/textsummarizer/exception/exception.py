"""Project-specific exception with rich context capture and safe logging.

This module defines `TextSummarizerError`, a wrapper for underlying exceptions
that standardizes error reporting and logging across the project.

Key features:
- Precise origin (file, line, function) from the deepest traceback frame.
- Logs using the *original* traceback to preserve stack accuracy.
- Preserves chaining (`__cause__`, `__context__`) and `__notes__` (Python 3.11+).
- Optional capture of (redacted) locals via `include_locals=True`.
- Structured export helpers: `.to_dict()` and `.to_json()` for diagnostics.

External behavior intentionally remains stable:
- Same constructor first two parameters (`error`, `logger`).
- Same `__str__` message format as your original class.
"""

from __future__ import annotations

# Standard library imports
import json
import traceback
import types
import uuid
from logging import Logger
from typing import Any

__all__ = ["TextSummarizerError"]

# Alias for Python's traceback type
TracebackType = types.TracebackType


def _walk_to_last_tb(tb: TracebackType | None) -> TracebackType | None:
    """Return the deepest traceback node in a chain, if any."""
    cur = tb
    while cur and cur.tb_next:  # follow the chain until the last node
        cur = cur.tb_next
    return cur


def _looks_sensitive(key: str) -> bool:
    """Heuristically determine if a variable name may contain secrets."""
    k = key.lower()
    return any(s in k for s in (
        "password", "passwd", "secret", "token", "apikey", "api_key", "auth", "key"
    ))


class TextSummarizerError(Exception):
    """Wrap and log an underlying exception with maximum useful context."""

    def __init__(self, error: Exception, logger: Logger, *, include_locals: bool = False) -> None:
        """
        Args:
            error: The underlying exception to wrap.
            logger: A configured logger instance.
            include_locals: If True, capture (redacted) locals in the stored traceback.
        """
        # Set base Exception message to the original error string
        super().__init__(str(error))
        self.message: str = str(error)
        self.logger: Logger = logger

        # Generate a stable UUID for this error occurrence — useful for tracing in logs
        self.error_id: str = str(uuid.uuid4())

        # Preserve Python's built-in exception chaining metadata
        self.__cause__ = error.__cause__
        self.__context__ = error.__context__
        self.__suppress_context__ = getattr(error, "__suppress_context__", False)
        # Python 3.11+ supports __notes__ for extra developer hints
        self.notes: list[str] | None = getattr(error, "__notes__", None)

        # Get the deepest frame where the exception occurred
        last_tb = _walk_to_last_tb(error.__traceback__)
        if last_tb and last_tb.tb_frame:
            self.line: int | None = last_tb.tb_lineno  # exact line number
            self.file: str = last_tb.tb_frame.f_code.co_filename  # file path
            self.function: str | None = last_tb.tb_frame.f_code.co_name  # function name
        else:
            # Fallback values if no traceback info is available
            self.line = None
            self.file = "Unknown"
            self.function = None

        # Store a structured traceback object — can capture locals if requested
        self._tb_exc: traceback.TracebackException = traceback.TracebackException.from_exception(
            error, capture_locals=include_locals
        )

        # If locals were captured, redact any that look sensitive
        if include_locals:
            try:
                for frame in self._tb_exc.stack:
                    if hasattr(frame, "locals") and isinstance(frame.locals, dict):
                        frame.locals = {
                            k: ("<redacted>" if _looks_sensitive(k) else v)
                            for k, v in frame.locals.items()
                        }
            except Exception as redact_err:  # noqa: BLE001
                print(f"Local redaction failed in TextSummarizerError [{self.error_id}]: {redact_err}")

        # Log the error using the original traceback (not the wrapper's)
        try:
            self.logger.error(
                str(self),  # formatted string version of the error
                exc_info=(type(error), error, error.__traceback__),  # preserve original traceback
                extra={
                    "error_id": self.error_id,
                    "origin_file": self.file,
                    "origin_line": self.line,
                    "origin_function": self.function,
                },
                stacklevel=1,  # ensure log points to caller location
            )
        except Exception as log_error:  # noqa: BLE001
            # Logging failures should never break the program flow
            print(f"Logging failed inside TextSummarizerError [{self.error_id}]: {log_error}")

    def __str__(self) -> str:
        """Return a formatted message including file, line, and original text."""
        return (
            f"Error occurred in file [{self.file}], "
            f"line [{self.line}], "
            f"message: [{self.message}]"
        )

    def __repr__(self) -> str:
        """Return a developer-friendly representation with key attributes."""
        return (
            f"{self.__class__.__name__}("
            f"message={self.message!r}, file={self.file!r}, "
            f"line={self.line!r}, function={self.function!r}, "
            f"error_id={self.error_id!r})"
        )

    def to_dict(self, *, include_locals: bool = False) -> dict[str, object]:
        """
        Return a JSON-serializable dictionary with rich error context.

        Args:
            include_locals: If True and available, include (redacted) locals.

        Returns:
            Dictionary of error details suitable for telemetry or debugging.
        """
        stack: list[dict[str, object]] = []
        for frame in self._tb_exc.stack:
            entry: dict[str, object] = {
                "file": frame.filename,
                "line": frame.lineno,
                "function": frame.name,
                "code_context": (frame.line or "").strip() if frame.line else None,
            }
            # Optionally include locals captured at the time of error
            if include_locals and hasattr(frame, "locals") and isinstance(frame.locals, dict):
                entry["locals"] = dict(frame.locals)
            stack.append(entry)

        return {
            "error_id": self.error_id,
            "message": self.message,
            "type": type(self).__name__,
            "origin": {
                "file": self.file,
                "line": self.line,
                "function": self.function,
            },
            "stack": stack,
            "has_cause": self.__cause__ is not None,
            "has_context": self.__context__ is not None,
            "suppress_context": self.__suppress_context__,
            "notes": list(self.notes) if self.notes else None,
        }

    def to_json(self, *, include_locals: bool = False) -> str:
        """
        Return a JSON string of `.to_dict()` for storage or transport.

        Args:
            include_locals: If True, include (redacted) locals in JSON.

        Returns:
            JSON string representation of the error details.
        """
        return json.dumps(self.to_dict(include_locals=include_locals), ensure_ascii=False)
