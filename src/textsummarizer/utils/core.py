import pandas as pd
import requests
from time import sleep
from pathlib import Path
from box import ConfigBox
from box.exceptions import BoxValueError, BoxTypeError, BoxKeyError
from ensure import ensure_annotations
import yaml
import json
import numpy as np
import pandas as pd
import joblib

from src.textsummarizer.logging import logger
from src.textsummarizer.exception.exception import TextSummarizerError


@ensure_annotations
def read_yaml(path_to_yaml: Path) -> ConfigBox:
    """
    Load a YAML file and return its contents as a ConfigBox for dot-access.

    Raises:
        TextSummarizerError: If the file is missing, corrupted, or unreadable.
    """
    # Check if the YAML file exists before attempting to read
    if not path_to_yaml.exists():
        logger.info(f"YAML file not found: '{path_to_yaml.as_posix()}'")
        raise TextSummarizerError(FileNotFoundError(f"YAML file not found: '{path_to_yaml.as_posix()}'"), logger)

    try:
        # Open and read the YAML file
        with path_to_yaml.open("r", encoding="utf-8") as file:
            content = yaml.safe_load(file)
    except (BoxValueError, BoxTypeError, BoxKeyError, yaml.YAMLError) as e:
        # Handle parsing errors
        logger.info(f"Failed to parse YAML from: '{path_to_yaml.as_posix()}'")
        raise TextSummarizerError(e, logger) from e
    except Exception as e:
        # Handle other unexpected errors
        logger.info(f"Unexpected error while reading YAML from: '{path_to_yaml.as_posix()}'")
        raise TextSummarizerError(e, logger) from e

    # Check for empty YAML file
    if content is None:
        logger.info(f"YAML file is empty or improperly formatted: '{path_to_yaml.as_posix()}'")
        raise TextSummarizerError(ValueError(f"YAML file is empty or improperly formatted: '{path_to_yaml.as_posix()}'"), logger)

    logger.info(f"YAML successfully loaded from: '{path_to_yaml.as_posix()}'")
    return ConfigBox(content)


@ensure_annotations
def save_to_csv(df: pd.DataFrame, *paths: Path, label: str):
    """Saves a DataFrame to one or more CSV files."""
    try:
        # Iterate over all provided paths
        for path in paths:
            path = Path(path)
            # Create parent directory if it doesn't exist
            if not path.parent.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created directory for {label}: '{path.parent.as_posix()}'")
            else:
                logger.info(f"Directory already exists for {label}: '{path.parent.as_posix()}'")

            # Save DataFrame to CSV
            df.to_csv(path, index=False)
            logger.info(f"{label} saved to: '{path.as_posix()}'")
    except Exception as e:
        # Log and raise error on failure
        logger.info(f"Failed to save CSV to: '{path.as_posix()}'")
        raise TextSummarizerError(e, logger) from e


@ensure_annotations
def read_csv(filepath: Path) -> pd.DataFrame:
    """
    Read a CSV file into a Pandas DataFrame.

    Raises:
        TextSummarizerError: If the file is missing, corrupted, or unreadable.
    """
    # Check if the CSV file exists
    if not filepath.exists():
        logger.info(f"CSV file not found: '{filepath.as_posix()}'")
        raise TextSummarizerError(FileNotFoundError(f"CSV file not found: '{filepath.as_posix()}'"), logger)

    try:
        # Read CSV into DataFrame
        df = pd.read_csv(filepath)
        logger.info(f"CSV file read successfully from: '{filepath.as_posix()}'")
        return df
    except Exception as e:
        # Log and raise error on failure
        logger.info(f"Failed to read CSV from: '{filepath.as_posix()}'")
        raise TextSummarizerError(e, logger) from e

@ensure_annotations
def save_to_yaml(data: dict, *paths: Path, label: str):
    """
    Write a dict out to YAML, always using UTF-8.
    """
    try:
        # Iterate over all provided paths
        for path in paths:
            path = Path(path)
            # Create parent directory if it doesn't exist
            if not path.parent.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created directory for {label}: '{path.parent.as_posix()}'")
            else:
                logger.info(f"Directory already exists for {label}: '{path.parent.as_posix()}'")

            # Write UTF-8 encoded YAML file
            with open(path, "w", encoding="utf-8") as file:
                yaml.dump(data, file, sort_keys=False)

            logger.info(f"{label} saved to: '{path.as_posix()}'")
    except Exception as e:
        # Log and raise error on failure
        logger.info(f"Failed to save YAML to: '{path.as_posix()}'")
        raise TextSummarizerError(e, logger) from e

@ensure_annotations
def save_to_json(data: dict, *paths: Path, label: str):
    """Saves a dictionary to one or more JSON files."""
    try:
        # Iterate over all provided paths
        for path in paths:
            path = Path(path)
            # Create parent directory if it doesn't exist
            if not path.parent.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created directory for {label}: '{path.parent.as_posix()}'")
            else:
                logger.info(f"Directory already exists for {label}: '{path.parent.as_posix()}'")

            # Write UTF-8 encoded JSON file
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)

            logger.info(f"{label} saved to: '{path.as_posix()}'")
    except Exception as e:
        # Log and raise error on failure
        logger.info(f"Failed to save JSON to: '{path.as_posix()}'")
        raise TextSummarizerError(e, logger) from e


@ensure_annotations
def save_object(obj: object, *paths: Path, label: str):
    """
    Saves a serializable object using joblib to the specified path.

    Args:
        obj (object): The object to serialize.
        path (Path): The path to save the object.
        label (str): Label used for logging context.
    """
    try:
        # Iterate over all provided paths
        for path in paths:
            path = Path(path)
            # Create parent directory if it doesn't exist
            if not path.parent.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created directory for {label}: '{path.parent.as_posix()}'")
            else:
                logger.info(f"Directory already exists for {label}: '{path.parent.as_posix()}'")

            # Serialize and save the object
            joblib.dump(obj, path)
            logger.info(f"{label} saved to: '{path.as_posix()}'")

    except Exception as e:
        # Log and raise error on failure
        logger.info(f"Failed to save {label} to: '{path.as_posix()}'")
        raise TextSummarizerError(e, logger) from e


@ensure_annotations
def save_array(array: np.ndarray | pd.Series, *paths: Path, label: str):
    """
    Saves a NumPy array or pandas Series to the specified paths in `.npy` format.

    Args:
        array (Union[np.ndarray, pd.Series]): Data to save.
        *paths (Path): One or more file paths.
        label (str): Label for logging.
    """
    try:
        # Ensure data is a NumPy array
        array = np.asarray(array)

        # Iterate over all provided paths
        for path in paths:
            path = Path(path)

            # Create parent directory if it doesn't exist
            if not path.parent.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created directory for {label}: '{path.parent.as_posix()}'")
            else:
                logger.info(f"Directory already exists for {label}: '{path.parent.as_posix()}'")

            # Save array to .npy file
            np.save(path, array)
            logger.info(f"{label} saved to: '{path.as_posix()}'")

    except Exception as e:
        # Log and raise error on failure
        logger.info(f"Failed to save {label} to: '{path.as_posix()}'")
        raise TextSummarizerError(e, logger) from e


@ensure_annotations
def load_array(path: Path, label: str) -> np.ndarray:
    """
    Loads a NumPy array from the specified `.npy` file path.

    Args:
        path (Path): Path to the `.npy` file.
        label (str): Label for logging.

    Returns:
        np.ndarray: Loaded NumPy array.
    """
    try:
        path = Path(path)

        # Check if the file exists
        if not path.exists():
            raise FileNotFoundError(f"{label} file not found at path: '{path.as_posix()}'")

        # Load array from .npy file
        array = np.load(path)
        logger.info(f"{label} loaded successfully from: '{path.as_posix()}'")
        return array

    except Exception as e:
        # Log and raise error on failure
        logger.info(f"Failed to load {label} from: '{path.as_posix()}'")
        raise TextSummarizerError(e, logger) from e

@ensure_annotations
def load_object(path: Path, label: str):
    """
    Loads a serialized object from the specified path using joblib.

    Args:
        path (Path): The path to the serialized object.
        label (str): Label used for logging context.

    Returns:
        Any: The deserialized object.
    """
    try:
        path = Path(path)
        # Check if the file exists
        if not path.exists():
            raise FileNotFoundError(f"{label} not found at: '{path.as_posix()}'")
        
        # Deserialize and load the object
        obj = joblib.load(path)
        logger.info(f"{label} loaded from: '{path.as_posix()}'")
        return obj

    except Exception as e:
        # Log and raise error on failure
        logger.info(f"Failed to load {label} from: '{path.as_posix()}'")
        raise TextSummarizerError(e, logger) from e


def download_file(url: str, raw_filepath: Path, retries: int = 3, delay: float = 2.0) -> None:
    """
    Downloads dataset from configured URL with retries.
    Converts GitHub blob URLs to raw content.
    """
    if "github.com" in url and "/blob/" in url:
        url = url.replace("/blob/", "/raw/")
        logger.info(f"Converted GitHub blob URL to raw URL: {url}")

    if raw_filepath.exists():
        logger.info(f"Dataset already exists at: {raw_filepath}. Skipping download.")
        return

    raw_filepath.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Attempting to download dataset (Attempt {attempt}/{retries})...")
            response = requests.get(url, stream=True)
            response.raise_for_status()

            with open(raw_filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            logger.info(f"Download successful. File saved to: {raw_filepath}")
            return

        except Exception as e:
            logger.warning(f"Download attempt {attempt} failed: {e}")
            if attempt < retries:
                sleep(delay)
            else:
                logger.error("Error during dataset download after retries.")
                raise TextSummarizerError(e, logger) from e
