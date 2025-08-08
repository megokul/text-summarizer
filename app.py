from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from src.textsummarizer.pipeline.training_pipeline import TrainingPipeline
from src.textsummarizer.pipeline.prediction_pipeline import PredictionPipeline
import os

# now import your project modules
from src.textsummarizer.logging import logger


app = FastAPI()

# Setup templates directory
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "app", "templates"))

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/train")
async def train():
    """Endpoint to initiate model training"""
    try:
        trainer = TrainingPipeline()
        trainer.run_pipeline()
        return {"status": "success", "message": "Training completed successfully"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/predict")
async def predict(text: str = Form(...)):
    """Endpoint for text summarization"""
    # Validate input text
    if not text.strip():
        return {"status": "error", "message": "Input text cannot be empty"}
        
    try:
        predictor = PredictionPipeline()
        summary = predictor.run_pipeline(text)
        return {"status": "success", "summary": summary}
    except Exception as e:
        return {"status": "error", "message": str(e)}
