from datetime import datetime, timezone

# Global cache for the timestamp to ensure it's generated only once per run
_timestamp_cache: str | None = None

def get_utc_timestamp() -> str:
    """
    Generates and caches a UTC timestamp string.

    The timestamp is created upon the first call and then reused for all
    subsequent calls within the same application run. This ensures consistency
    across logs and artifacts.

    Returns:
        str: A timestamp in the format 'YYYY_MM_DDTHH_MM_SSZ'.
    """
    # Define the desired timestamp format
    format="%Y_%m_%dT%H_%M_%SZ"
    
    global _timestamp_cache
    # Generate the timestamp only if the cache is empty
    if _timestamp_cache is None:
        _timestamp_cache = datetime.now(timezone.utc).strftime(format)
    
    return _timestamp_cache
