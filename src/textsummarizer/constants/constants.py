"""
Global constants for the Text Summarizer project.

These constants define:
- Configuration file locations
- Directory structures for logs, artifacts, data, and models
- Standardized subdirectory names for each pipeline stage
- Dataset label conventions

Design notes:
    - Centralizing constants ensures consistent usage across the project.
    - Changing a constant here updates it globally.
    - All paths are relative; final absolute paths are constructed dynamically
      in the ConfigurationManager or relevant components.

"""

# ==============================================================================
# CONFIGURATION CONSTANTS
# ==============================================================================
# Directory containing configuration YAML files
CONFIG_ROOT = "config"

# Filenames for configuration, parameters, schema, and templates
CONFIG_FILENAME = "config.yaml"
PARAMS_FILENAME = "params.yaml"
SCHEMA_FILENAME = "schema.yaml"
TEMPLATES_FILENAME = "templates.yaml"

# ==============================================================================
# LOGGING CONSTANTS
# ==============================================================================
# Root directory where log files will be stored
LOGS_ROOT = "logs"

# ==============================================================================
# ARTIFACTS AND DATA HANDLING CONSTANTS
# ==============================================================================
# Root directory for all generated artifacts (scoped per pipeline run)
ARTIFACTS_ROOT = "artifacts"

# Directories for handler components (e.g., database/S3 backup handlers)
POSTGRES_HANDLER_ROOT = "mongo_handler"  # TODO(clarify): Should this be postgres_handler?
S3_HANDLER_ROOT = "s3_handler"

# ---------------------------
# Data Ingestion Directories
# ---------------------------
# Root folder for data ingestion artifacts
INGEST_ROOT = "data_ingestion"
# Subdirectory for storing downloaded raw data files (e.g., ZIPs)
INGEST_RAW_SUBDIR = "raw_data"
# Subdirectory for storing extracted/cleaned ingested datasets
INGEST_INGESTED_SUBDIR = "ingested_data"

# ---------------------------
# DVC Data Directories
# ---------------------------
# Root folder for DVC-managed data (stable across runs)
DVC_ROOT = "data"
# Subfolder for raw datasets in DVC
DVC_RAW_SUBDIR = "raw"
# Subfolder for validated datasets in DVC
DVC_VALIDATED_SUBDIR = "validated"
# Subfolder for transformed datasets in DVC
DVC_TRANSFORMED_SUBDIR = "transformed"

# ==============================================================================
# DATA VALIDATION CONSTANTS
# ==============================================================================
# Root directory for validation outputs
VALID_ROOT = "data_validation"
# Subdirectory for storing validated datasets
VALID_VALIDATED_SUBDIR = "validated"
# Subdirectory for storing validation reports
VALID_REPORTS_SUBDIR = "reports"

# ==============================================================================
# DATASET LABELS
# ==============================================================================
# Standard dataset split labels (used in transformation, training, evaluation)
X_TRAIN_LABEL = "X_train"
Y_TRAIN_LABEL = "y_train"
X_VAL_LABEL = "X_val"
Y_VAL_LABEL = "y_val"
X_TEST_LABEL = "X_test"
Y_TEST_LABEL = "y_test"

# Consistent split name strings
TRAIN_LABEL = "train"
VAL_LABEL = "val"
TEST_LABEL = "test"

# ==============================================================================
# DATA TRANSFORMATION CONSTANTS
# ==============================================================================
# Root directory for transformation outputs (tokenization, feature prep)
TRANSFORM_ROOT = "data_transformation"
# Subdirectory for tokenized datasets
TRANSFORM_TOKENIZED_SUBDIR = "tokenized_data"

# ==============================================================================
# MODEL TRAINING CONSTANTS
# ==============================================================================
# Root directory for training artifacts (checkpoints, logs)
TRAINER_ROOT = "model_trainer"
# Subfolder for storing trained model weights
TRAINER_MODEL_SUBDIR = "pegasus_samsum_model"
# Subfolder for storing trained tokenizer artifacts
TRAINER_TOKENIZER_SUBDIR = "pegasus_samsum_tokenizer"

# ==============================================================================
# INFERENCE AND EVALUATION CONSTANTS
# ==============================================================================
# ---------------------------
# Final Model Directories
# ---------------------------
# Root directory for finalized/promoted models
FINAL_MODEL_ROOT = "final_model"
# Subfolder for storing the final trained model
FINAL_MODEL_SUBDIR = "model"
# Subfolder for storing the final tokenizer
FINAL_TOKENIZER_SUBDIR = "tokenizer"

# ---------------------------
# Model Evaluation Directories
# ---------------------------
# Root directory for evaluation outputs
EVALUATION_ROOT = "model_evaluation"
# Subdirectory for evaluation reports (metrics, comparisons)
EVALUATION_REPORT_SUBDIR = "reports"

# ---------------------------
# Prediction Outputs
# ---------------------------
# Root directory for storing model predictions
PREDICTION_ROOT = "predictions"
