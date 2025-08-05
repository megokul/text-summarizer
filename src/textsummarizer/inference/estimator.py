from transformers import AutoModelForSeq2SeqLM, AutoTokenizer
import torch

class SummarizerModel:
    def __init__(self, model_dir: str, tokenizer_dir: str, device: str = "cuda"):
        self.device = device
        self.tokenizer = AutoTokenizer.from_pretrained(tokenizer_dir)
        self.model = AutoModelForSeq2SeqLM.from_pretrained(model_dir).to(self.device)
        self.model.eval()

    def predict(self, text: str, max_length: int = 60, num_beams: int = 4) -> str:
        inputs = self.tokenizer(
            text,
            return_tensors="pt",
            truncation=True,
            max_length=512
        ).to(self.device)
        with torch.no_grad():
            summary_ids = self.model.generate(
                **inputs, num_beams=num_beams, max_length=max_length, early_stopping=True
            )
        return self.tokenizer.decode(summary_ids[0], skip_special_tokens=True)
