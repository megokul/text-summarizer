from src.textsummarizer.pipeline.training_pipeline import TrainingPipeline
from src.textsummarizer.exception.exception import TextSummarizerError

try:
    pipeline = TrainingPipeline()
    pipeline.run_pipeline()
except TextSummarizerError as e:
    print(f"An error occurred: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
