# FILE: src/textsummarizer/app/api.py
"""FastAPI application exposing training and prediction endpoints.

Design intent
-------------
- Provide a minimal HTTP interface for orchestrating the training pipeline and
  for producing summaries from input text.
- Keep business logic out of the web layer: the API delegates to pipeline
  components that are already configuration-driven and fully logged.
- Maintain consistent error handling: log contextual messages and re-raise
  failures as ``TextSummarizerError`` to ensure uniform exception semantics
  across the project.
"""

# ------------------------------- Standard Library --------------------------- #
from pathlib import Path

# --------------------------------- 3rd Party -------------------------------- #
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

# ----------------------------------- Local ---------------------------------- #
from src.textsummarizer.exception.exception import TextSummarizerError
from src.textsummarizer.logging import logger
from src.textsummarizer.pipeline.prediction_pipeline import PredictionPipeline
from src.textsummarizer.pipeline.training_pipeline import TrainingPipeline


# -----------------------------------------------------------------------------
# App and templating setup
# -----------------------------------------------------------------------------
app = FastAPI()

# Resolve templates directory relative to this file to avoid CWD surprises.
# Using Path keeps things cross-platform; as_posix() only when logging/printing.
_TEMPLATES_DIR = (Path(__file__).resolve().parent / "app" / "templates")
templates = Jinja2Templates(directory=_TEMPLATES_DIR.as_posix())


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def root(request: Request) -> HTMLResponse:
    """Render the home page.

    Args:
        request (Request): FastAPI request object (needed by Jinja2).

    Returns:
        HTMLResponse: Rendered index.html.
    """
    # Keep the handler lean; template contains the UI.
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/train")
async def train() -> dict:
    """Trigger the end-to-end training pipeline.

    Returns:
        dict: Status payload indicating success.

    Raises:
        TextSummarizerError: If pipeline execution fails.
    """
    try:
        logger.info("Train endpoint invoked. Starting training pipeline...")
        pipeline = TrainingPipeline()
        pipeline.run_pipeline()
        logger.info("Training pipeline completed successfully.")
        return {
            "status": "success",
            "message": "Training completed successfully.",
        }
    except Exception as e:  # noqa: BLE001
        # Log with context, then re-raise the project-specific exception
        # so upstream middleware / logs see a uniform error type.
        logger.error("Training pipeline failed in /train endpoint.")
        raise TextSummarizerError(e, logger) from e


@app.post("/predict")
async def predict(text: str = Form(...)) -> dict:
    """Generate a summary for the provided text input.

    This endpoint accepts a single text payload from a form field named ``text``
    and returns the generated summary. For multi-item batch prediction, prefer
    calling the underlying pipeline programmatically with a list.

    Args:
        text (str): Source text to summarize (from form field).

    Returns:
        dict: Status payload with the generated summary.

    Raises:
        TextSummarizerError: If prediction fails or input is invalid.
    """
    try:
        # Validate now to avoid spending cycles constructing the pipeline.
        cleaned = text.strip()
        if not cleaned:
            logger.warning("Predict endpoint received empty/blank text.")
            raise ValueError("Input text cannot be empty.")

        logger.info("Predict endpoint invoked. Running prediction pipeline...")
        pipeline = PredictionPipeline()

        # The pipeline returns a DataFrame with columns: ["text", "summary"].
        results_df = pipeline.run_pipeline(input_texts=[cleaned])

        # Extract the single summary for this request. We know we sent one item.
        summary: str = results_df.loc[0, "summary"]

        logger.info("Prediction completed successfully.")
        return {
            "status": "success",
            "summary": summary,
        }
    except Exception as e:  # noqa: BLE001
        logger.error("Prediction failed in /predict endpoint.")
        raise TextSummarizerError(e, logger) from e
