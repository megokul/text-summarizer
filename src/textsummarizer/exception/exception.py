import sys
from types import TracebackType
from logging import Logger


class TextSummarizerError(Exception):
    """
    Custom exception for the Text Summarizer project.

    Automatically captures:
    - Original exception message
    - Filename and line number from the traceback
    - Logs the formatted error using an injected logger
    """

    def __init__(self, error: Exception, logger: Logger) -> None:
        # Set the core message
        super().__init__(str(error))
        self.message: str = str(error)
        self.logger: Logger = logger

        # Extract traceback info from current exception context
        _, _, tb = sys.exc_info()
        tb: TracebackType | None

        # Safely capture line number and file
        self.line: int | None = tb.tb_lineno if tb and tb.tb_lineno else None
        self.file: str = tb.tb_frame.f_code.co_filename if tb and tb.tb_frame else "Unknown"

        # Log the error using injected logger with traceback
        try:
            self.logger.error(str(self), exc_info=True)
        except Exception as log_error:
            print(f"Logging failed inside TextSummarizerError: {log_error}")

    def __str__(self) -> str:
        return (
            f"Error occurred in file [{self.file}], "
            f"line [{self.line}], "
            f"message: [{self.message}]"
        )
