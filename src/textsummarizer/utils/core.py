# FILE: src/textsummarizer/utils/core.py
"""Core utilities for the Text Summarizer project.

Purpose
-------
Provide reusable helpers for:
- Reading and writing YAML / JSON / CSV.
- Persisting and restoring Python objects and NumPy arrays.
- Downloading remote files with retries.
- Extracting ZIP archives.
- Converting NumPy/pandas objects to plain Python for clean serialization.

Design intent
-------------
- Centralize I/O so logging and error handling are consistent.
- Fail fast with precise context; wrap errors using ``TextSummarizerError``.
- Keep behavior explicit and predictable; functions that don't yield values
  return ``None`` explicitly.
- Imports are ordered: stdlib → third-party → local.
"""

# ------------------------------- Standard Library --------------------------- #
from pathlib import Path  # Cross-platform filesystem paths
from time import sleep  # Backoff timing for retry loops
from types import NoneType  # For functions that explicitly return None
import json  # JSON serialization for configs/artifacts
import zipfile  # ZIP archive handling

# --------------------------------- 3rd Party ------------------------------- #
from box import ConfigBox  # Dict-like with dot access
from box.exceptions import BoxKeyError, BoxTypeError, BoxValueError
from ensure import ensure_annotations  # Lightweight runtime type checks
import joblib  # Efficient object (de)serialization
import numpy as np  # Numerical arrays and dtypes
import pandas as pd  # DataFrames/Series
import requests  # HTTP client
import yaml  # YAML read/write

# ----------------------------------- Local --------------------------------- #
from src.textsummarizer.exception.exception import TextSummarizerError
from src.textsummarizer.logging import logger


# --------------------------------------------------------------------------- #
# YAML
# --------------------------------------------------------------------------- #
@ensure_annotations
def read_yaml(path_to_yaml: Path) -> ConfigBox:
    """Load a YAML file into a dot-accessible structure.

    Args:
        path_to_yaml (Path): Path to the YAML file.

    Returns:
        ConfigBox: Parsed YAML content with attribute-style access.

    Raises:
        TextSummarizerError: File missing, empty, or parse failure.
    """
    # Pre-check for existence to avoid deep stack traces from loaders.
    if not path_to_yaml.exists():
        logger.error("YAML file not found: '%s'", path_to_yaml.as_posix())
        raise TextSummarizerError(
            FileNotFoundError(
                f"YAML file not found: '{path_to_yaml.as_posix()}'"
            ),
            logger,
        )

    try:
        # Use explicit encoding for portability.
        with path_to_yaml.open("r", encoding="utf-8") as file:
            # safe_load avoids executing arbitrary YAML tags.
            content = yaml.safe_load(file)
    except (BoxValueError, BoxTypeError, BoxKeyError, yaml.YAMLError) as e:
        logger.error("Failed to parse YAML from: '%s'", path_to_yaml.as_posix())
        raise TextSummarizerError(e, logger) from e
    except Exception as e:  # noqa: BLE001
        logger.error(
            "Unexpected error while reading YAML from: '%s'",
            path_to_yaml.as_posix(),
        )
        raise TextSummarizerError(e, logger) from e

    # Treat empty YAML as an error (prevents None leaking downstream).
    if content is None:
        logger.error(
            "YAML file is empty or improperly formatted: '%s'",
            path_to_yaml.as_posix(),
        )
        raise TextSummarizerError(
            ValueError(
                "YAML file is empty or improperly formatted: "
                f"'{path_to_yaml.as_posix()}'"
            ),
            logger,
        )

    logger.info("YAML successfully loaded from: '%s'", path_to_yaml.as_posix())
    return ConfigBox(content)


# --------------------------------------------------------------------------- #
# CSV
# --------------------------------------------------------------------------- #
@ensure_annotations
def save_to_csv(df: pd.DataFrame, *paths: Path, label: str) -> NoneType:
    """Write a DataFrame to one or more CSV files (overwrite if present).

    Args:
        df (pd.DataFrame): Tabular data to save.
        *paths (Path): One or more destination file paths.
        label (str): Short label used in logs.

    Returns:
        None: This function does not return a value.

    Raises:
        TextSummarizerError: Any write failure.
    """
    try:
        for path in paths:
            path = Path(path)  # Normalize string inputs to Path

            # Ensure parent directory exists prior to open/write.
            if not path.parent.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                logger.info(
                    "Created directory for %s: '%s'",
                    label,
                    path.parent.as_posix(),
                )
            else:
                logger.info(
                    "Directory already exists for %s: '%s'",
                    label,
                    path.parent.as_posix(),
                )

            # Exclude index to keep files compact and consistent.
            df.to_csv(path, index=False)
            logger.info("%s saved to: '%s'", label, path.as_posix())

        return None
    except Exception as e:  # noqa: BLE001
        # Use last attempted path for context (safe; contains no data).
        logger.error("Failed to save CSV to: '%s'", path.as_posix())
        raise TextSummarizerError(e, logger) from e


@ensure_annotations
def read_csv(filepath: Path) -> pd.DataFrame:
    """Read a CSV file into a DataFrame.

    Args:
        filepath (Path): Path to the CSV file.

    Returns:
        pd.DataFrame: Loaded table.

    Raises:
        TextSummarizerError: File missing or read failure.
    """
    # Fail fast with a clear message if the file is not found.
    if not filepath.exists():
        logger.error("CSV file not found: '%s'", filepath.as_posix())
        raise TextSummarizerError(
            FileNotFoundError(f"CSV file not found: '{filepath.as_posix()}'"),
            logger,
        )

    try:
        df = pd.read_csv(filepath)
        logger.info("CSV file read successfully from: '%s'", filepath.as_posix())
        return df
    except Exception as e:  # noqa: BLE001
        logger.error("Failed to read CSV from: '%s'", filepath.as_posix())
        raise TextSummarizerError(e, logger) from e


# --------------------------------------------------------------------------- #
# YAML / JSON WRITERS
# --------------------------------------------------------------------------- #
@ensure_annotations
def save_to_yaml(data: dict, *paths: Path, label: str) -> NoneType:
    """Write a Python dict to YAML (UTF-8).

    Converts non-primitive types before serialization.

    Args:
        data (dict): Mapping to serialize.
        *paths (Path): One or more destination files.
        label (str): Short label used in logs.

    Returns:
        None: This function does not return a value.

    Raises:
        TextSummarizerError: Any write failure.
    """
    try:
        # Convert numpy/pandas objects to plain Python for clean YAML.
        data = to_python(data)

        for path in paths:
            path = Path(path)

            # Create parent directories if missing.
            if not path.parent.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                logger.info(
                    "Created directory for %s: '%s'",
                    label,
                    path.parent.as_posix(),
                )
            else:
                logger.info(
                    "Directory already exists for %s: '%s'",
                    label,
                    path.parent.as_posix(),
                )

            # safe_dump avoids arbitrary object tags.
            with path.open("w", encoding="utf-8") as file:
                yaml.safe_dump(data, file, sort_keys=False, allow_unicode=True)

            logger.info("%s saved to: '%s'", label, path.as_posix())

        return None
    except Exception as e:  # noqa: BLE001
        logger.error("Failed to save YAML to: '%s'", path.as_posix())
        raise TextSummarizerError(e, logger) from e


@ensure_annotations
def save_to_json(data: dict, *paths: Path, label: str) -> NoneType:
    """Save a dictionary to JSON (UTF-8, pretty-printed).

    Args:
        data (dict): Data to serialize.
        *paths (Path): One or more destination files.
        label (str): Short label used in logs.

    Returns:
        None: This function does not return a value.

    Raises:
        TextSummarizerError: Any write failure.
    """
    try:
        for path in paths:
            path = Path(path)

            # Ensure directory exists to avoid open() errors.
            if not path.parent.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                logger.info(
                    "Created directory for %s: '%s'",
                    label,
                    path.parent.as_posix(),
                )
            else:
                logger.info(
                    "Directory already exists for %s: '%s'",
                    label,
                    path.parent.as_posix(),
                )

            # indent=4 improves readability and diffs.
            with path.open("w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)

            logger.info("%s saved to: '%s'", label, path.as_posix())

        return None
    except Exception as e:  # noqa: BLE001
        logger.error("Failed to save JSON to: '%s'", path.as_posix())
        raise TextSummarizerError(e, logger) from e


# --------------------------------------------------------------------------- #
# OBJECT / ARRAY SERIALIZATION
# --------------------------------------------------------------------------- #
@ensure_annotations
def save_object(obj: object, *paths: Path, label: str) -> NoneType:
    """Serialize a Python object with joblib to one or more paths.

    Args:
        obj (object): Python object to serialize.
        *paths (Path): Destination file paths.
        label (str): Short label used in logs.

    Returns:
        None: This function does not return a value.

    Raises:
        TextSummarizerError: Serialization or write failure.
    """
    try:
        for path in paths:
            path = Path(path)

            # Ensure parent directories exist first.
            if not path.parent.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                logger.info(
                    "Created directory for %s: '%s'",
                    label,
                    path.parent.as_posix(),
                )
            else:
                logger.info(
                    "Directory already exists for %s: '%s'",
                    label,
                    path.parent.as_posix(),
                )

            # joblib is efficient for numpy-heavy objects and ML models.
            joblib.dump(obj, path)
            logger.info("%s saved to: '%s'", label, path.as_posix())

        return None
    except Exception as e:  # noqa: BLE001
        logger.error("Failed to save %s to: '%s'", label, path.as_posix())
        raise TextSummarizerError(e, logger) from e


@ensure_annotations
def save_array(array: np.ndarray | pd.Series, *paths: Path, label: str) -> NoneType:
    """Save a NumPy array or pandas Series to `.npy` files.

    Args:
        array (np.ndarray | pd.Series): Data to save.
        *paths (Path): Destination file paths.
        label (str): Short label used in logs.

    Returns:
        None: This function does not return a value.

    Raises:
        TextSummarizerError: Any write failure.
    """
    try:
        # Convert Series/list-like into a concrete ndarray.
        array = np.asarray(array)

        for path in paths:
            path = Path(path)

            # Ensure parent directory exists.
            if not path.parent.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                logger.info(
                    "Created directory for %s: '%s'",
                    label,
                    path.parent.as_posix(),
                )
            else:
                logger.info(
                    "Directory already exists for %s: '%s'",
                    label,
                    path.parent.as_posix(),
                )

            # Save in NumPy's portable binary format.
            np.save(path, array)
            logger.info("%s saved to: '%s'", label, path.as_posix())

        return None
    except Exception as e:  # noqa: BLE001
        logger.error("Failed to save %s to: '%s'", label, path.as_posix())
        raise TextSummarizerError(e, logger) from e


@ensure_annotations
def load_array(path: Path, label: str) -> np.ndarray:
    """Load a NumPy array from a `.npy` file.

    Args:
        path (Path): Path to `.npy`.
        label (str): Short label used in logs.

    Returns:
        np.ndarray: Loaded array.

    Raises:
        TextSummarizerError: Any read/deserialize failure.
    """
    try:
        path = Path(path)  # Normalize potential string inputs.

        # Friendly message if missing (NumPy's error is less clear).
        if not path.exists():
            raise FileNotFoundError(
                f"{label} file not found at path: '{path.as_posix()}'"
            )

        array = np.load(path)
        logger.info("%s loaded successfully from: '%s'", label, path.as_posix())
        return array
    except Exception as e:  # noqa: BLE001
        logger.error("Failed to load %s from: '%s'", label, path.as_posix())
        raise TextSummarizerError(e, logger) from e


@ensure_annotations
def load_object(path: Path, label: str) -> object:
    """Load a serialized object via joblib.

    Args:
        path (Path): Source file path.
        label (str): Short label used in logs.

    Returns:
        object: Deserialized Python object.

    Raises:
        TextSummarizerError: Any read/deserialize failure.
    """
    try:
        path = Path(path)

        # Check existence explicitly to format a clear error.
        if not path.exists():
            raise FileNotFoundError(
                f"{label} not found at: '{path.as_posix()}'"
            )

        # joblib.load efficiently deserializes objects saved with joblib.dump.
        obj = joblib.load(path)
        logger.info("%s loaded from: '%s'", label, path.as_posix())
        return obj
    except Exception as e:  # noqa: BLE001
        logger.error("Failed to load %s from: '%s'", label, path.as_posix())
        raise TextSummarizerError(e, logger) from e


# --------------------------------------------------------------------------- #
# DOWNLOADS
# --------------------------------------------------------------------------- #
@ensure_annotations
def download_file(
    url: str,
    download_path: Path,
    retries: int = 3,
    delay: float = 2.0,
) -> NoneType:
    """Download a file with retry support.

    Converts GitHub blob URLs to raw content automatically.

    Args:
        url (str): Source URL.
        download_path (Path): Destination file path.
        retries (int): Number of retry attempts before failing.
        delay (float): Delay (seconds) between retry attempts.

    Returns:
        None: This function does not return a value.

    Raises:
        TextSummarizerError: If all retry attempts fail.
    """
    # Transform GitHub "blob" URLs to "raw" URLs for direct content fetch.
    if "github.com" in url and "/blob/" in url:
        url = url.replace("/blob/", "/raw/")
        logger.info("Converted GitHub blob URL to raw URL: %s", url)

    # Fast-path exit if the file already exists (idempotent behavior).
    if download_path.exists():
        logger.info(
            "Dataset already exists at: %s. Skipping download.",
            download_path.as_posix(),
        )
        return None  # Explicit: no download performed.

    # Ensure parent directories exist to avoid FileNotFoundError on open().
    download_path.parent.mkdir(parents=True, exist_ok=True)

    # Attempt the download with basic linear backoff between attempts.
    for attempt in range(1, retries + 1):
        try:
            logger.info(
                "Attempting to download dataset (Attempt %d/%d)...",
                attempt,
                retries,
            )
            # stream=True to avoid loading the entire file in memory.
            response = requests.get(url, stream=True, timeout=60)
            response.raise_for_status()  # Raise for non-2xx statuses.

            # Write chunks to file to keep memory usage bounded.
            with download_path.open("wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  # Guard against keep-alive chunks
                        f.write(chunk)

            logger.info(
                "Download successful. File saved to: %s",
                download_path.as_posix(),
            )
            return None  # Completed successfully; exit early.

        except Exception as e:  # noqa: BLE001
            logger.warning("Download attempt %d failed: %s", attempt, e)
            # Sleep only between attempts, not after the final failure.
            if attempt < retries:
                sleep(delay)
            else:
                logger.error("Error during dataset download after retries.")
                raise TextSummarizerError(e, logger) from e


# --------------------------------------------------------------------------- #
# ARCHIVES
# --------------------------------------------------------------------------- #
@ensure_annotations
def extract_zip(zip_path: Path, extract_to: Path, label: str) -> NoneType:
    """Extract a ZIP archive into the given directory.

    Creates the destination directory if it does not exist.

    Args:
        zip_path (Path): Path to the .zip file to extract.
        extract_to (Path): Destination directory.
        label (str): Short label used in logs.

    Returns:
        None: This function does not return a value.

    Raises:
        TextSummarizerError: Any extraction failure.
    """
    try:
        # Validate that the input archive exists; avoid obscure ZipFile errors.
        if not zip_path.exists():
            raise FileNotFoundError(
                f"ZIP file not found: '{zip_path.as_posix()}'"
            )

        # Make sure the target directory exists to receive extracted contents.
        if not extract_to.exists():
            extract_to.mkdir(parents=True, exist_ok=True)

        # Use the stdlib ZipFile; context manager ensures proper closure.
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_to)

        logger.info("%s extracted to: %s", label, extract_to.as_posix())
        return None
    except Exception as e:  # noqa: BLE001
        logger.error(
            "Failed to extract %s to: %s", label, extract_to.as_posix()
        )
        raise TextSummarizerError(e, logger) from e


# --------------------------------------------------------------------------- #
# CONVERSIONS
# --------------------------------------------------------------------------- #
@ensure_annotations
def to_python(obj: object) -> object:
    """Recursively convert NumPy/pandas objects to plain Python types.

    Helps ensure clean JSON/YAML serialization, since many libraries
    cannot handle NumPy/pandas types directly.

    Args:
        obj (object): Arbitrary input object (possibly nested).

    Returns:
        object: Structure composed of base Python types only.
    """
    # Treat NA-like values uniformly.
    if isinstance(obj, float) and pd.isna(obj):
        return None

    # Convert numpy scalar types to built-in Python scalars.
    if isinstance(obj, np.generic):
        return obj.item()

    # Convert pandas Timestamp/Timedelta to ISO-8601 strings.
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    if isinstance(obj, pd.Timedelta):
        return obj.isoformat()

    # Handle common container types recursively.
    if isinstance(obj, dict):
        return {k: to_python(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple, set)):
        seq = [to_python(v) for v in obj]
        # Preserve original container type except for sets (serialize as list).
        return type(obj)(seq) if not isinstance(obj, set) else list(seq)

    # Convert arrays and pandas containers to serializable equivalents.
    if isinstance(obj, np.ndarray):
        return obj.tolist()

    if isinstance(obj, pd.Series):
        return obj.apply(to_python).tolist()

    if isinstance(obj, pd.DataFrame):
        # Convert each cell, then export rows as a list of dicts ("records").
        return obj.applymap(to_python).to_dict(orient="records")

    # Already a base Python type.
    return obj
