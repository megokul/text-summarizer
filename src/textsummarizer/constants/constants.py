# ==============================================================================
# CONFIGURATION CONSTANTS
# ==============================================================================
# Directories and filenames for configuration files
CONFIG_ROOT = "config"
CONFIG_FILENAME = "config.yaml"
PARAMS_FILENAME = "params.yaml"
SCHEMA_FILENAME = "schema.yaml"
TEMPLATES_FILENAME = "templates.yaml"


# ==============================================================================
# LOGGING CONSTANTS
# ==============================================================================
# Root directory for storing log files
LOGS_ROOT = "logs"


# ==============================================================================
# ARTIFACTS AND DATA HANDLING CONSTANTS
# ==============================================================================
# Root directory for all generated artifacts
ARTIFACTS_ROOT = "artifacts"

# Constants for database handlers (e.g., PostgreSQL)
POSTGRES_HANDLER_ROOT = "mongo_handler"
S3_HANDLER_ROOT = "s3_handler"

# Constants for data ingestion process
INGEST_ROOT = "data_ingestion"
INGEST_RAW_SUBDIR = "raw_data"
INGEST_INGESTED_SUBDIR = "ingested_data"

# Constants for DVC-managed data directories
DVC_ROOT = "data"
DVC_RAW_SUBDIR = "raw"
DVC_VALIDATED_SUBDIR = "validated"
DVC_TRANSFORMED_SUBDIR = "transformed"


# ==============================================================================
# DATA VALIDATION CONSTANTS
# ==============================================================================
# Directories for data validation outputs
VALID_ROOT = "data_validation"
VALID_VALIDATED_SUBDIR = "validated"
VALID_REPORTS_SUBDIR = "reports"


# ==============================================================================
# DATASET LABELS
# ==============================================================================
# Standard labels for dataset splits (train, validation, test)
X_TRAIN_LABEL = "X_train"
Y_TRAIN_LABEL = "y_train"
X_VAL_LABEL = "X_val"
Y_VAL_LABEL = "y_val"
X_TEST_LABEL = "X_test"
Y_TEST_LABEL = "y_test"
TRAIN_LABEL = "train"
VAL_LABEL = "val"
TEST_LABEL = "test"


# ==============================================================================
# DATA TRANSFORMATION CONSTANTS
# ==============================================================================
# Directories for data transformation artifacts
TRANSFORM_ROOT = "data_transformation"
TRANSFORM_TRAIN_SUBDIR = "train"
TRANSFORM_TEST_SUBDIR = "test"
TRANSFORM_VAL_SUBDIR = "val"
TRANSFORM_PROCESSOR_SUBDIR = "data_processor"


# ==============================================================================
# MODEL TRAINING CONSTANTS
# ==============================================================================
# Directories for model training artifacts
TRAINER_ROOT = "model_trainer"
TRAINER_MODEL_SUBDIR = "model"
TRAINER_REPORTS_SUBDIR = "reports"
TRAINER_INFERENCE_SUBDIR = "inference_model"


# ==============================================================================
# INFERENCE AND EVALUATION CONSTANTS
# ==============================================================================
# Root directory for inference models
INFERENCE_MODEL_ROOT = "inference_model"

# Directories for model evaluation artifacts
EVALUATION_ROOT = "model_evaluation"
EVALUATION_REPORT_SUBDIR = "reports"

# Root directory for storing predictions
PREDICTION_ROOT = "predictions"
