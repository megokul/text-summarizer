from src.textsummarizer.pipeline.prediction_pipeline import PredictionPipeline

pipe = PredictionPipeline()

texts = [
    "Amanda: I baked cookies. Do you want some?\nJerry: Sure!\nAmanda: I'll bring you tomorrow :-)",
    "Alice: Can you attend the 3pm sync?\nBob: Yes, but I’ll be 5 minutes late.",
]

summaries = pipe.run_pipeline(input_texts=texts)
print(summaries)
