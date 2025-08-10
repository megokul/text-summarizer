"""Utility for generating and caching a single UTC timestamp per run.

This ensures that all components in the pipeline (logs, reports, artifacts)
share the exact same timestamp for a given execution, aiding traceability
and reproducibility.
"""

from datetime import datetime, timezone

# ---------------------------------------------------------------------------- #
# Module-level cache variable to store the generated timestamp.
# Once generated, it is reused for all subsequent calls during the same run.
# ---------------------------------------------------------------------------- #
_timestamp_cache: str | None = None


def get_utc_timestamp() -> str:
    """Generate and cache a UTC timestamp string for the current run.

    The first time this function is called, it generates a timestamp in the
    format ``YYYY_MM_DDTHH_MM_SSZ`` (e.g., ``2025_08_10T14_22_05Z``).
    This timestamp is stored in a module-level cache and returned for all
    subsequent calls within the same application run, ensuring consistency
    across logs, reports, and artifact naming.

    Returns:
        str: The cached UTC timestamp string.

    Example:
        >>> ts1 = get_utc_timestamp()
        >>> ts2 = get_utc_timestamp()
        >>> ts1 == ts2
        True
    """
    global _timestamp_cache

    # Timestamp format: year, month, day, time in 24-hour format, ending with Z (UTC)
    timestamp_format = "%Y_%m_%dT%H_%M_%SZ"

    # Generate timestamp only once; reuse for the rest of the run.
    if _timestamp_cache is None:
        _timestamp_cache = datetime.now(timezone.utc).strftime(timestamp_format)

    return _timestamp_cache
