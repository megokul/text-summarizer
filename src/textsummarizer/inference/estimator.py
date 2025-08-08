from pathlib import Path
from typing import Iterable

import torch
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

from src.textsummarizer.exception.exception import TextSummarizerError
from src.textsummarizer.logging import logger


class TextSummarizerModel:
    """
    Thin estimator-style wrapper around a HF seq2seq model + tokenizer.

    All generation knobs are provided by the caller (config-driven).
    No implicit defaults/fallbacks are applied here.
    """

    def __init__(
        self,
        model_dir: Path | str,
        tokenizer_dir: Path | str,
        device: str,
        max_input_length: int,
        max_target_length: int,
        num_beams: int,
        length_penalty: float,
        no_repeat_ngram_size: int,
        early_stopping: bool,
    ) -> None:
        try:
            self.model_dir = Path(model_dir)
            self.tokenizer_dir = Path(tokenizer_dir)

            # Strict checks
            if not self.model_dir.exists():
                raise TextSummarizerError(
                    f"Model directory does not exist: {self.model_dir.as_posix()}",
                    logger,
                )
            if not self.tokenizer_dir.exists():
                raise TextSummarizerError(
                    f"Tokenizer directory does not exist: {self.tokenizer_dir.as_posix()}",
                    logger,
                )
            if not device:
                raise TextSummarizerError("`device` must be provided in config.", logger)

            self.device = device

            # Load artifacts
            logger.info("Loading tokenizer from %s", self.tokenizer_dir.as_posix())
            self.tokenizer = AutoTokenizer.from_pretrained(self.tokenizer_dir)

            logger.info("Loading model from %s", self.model_dir.as_posix())
            self.model = AutoModelForSeq2SeqLM.from_pretrained(self.model_dir).to(self.device)
            self.model.eval()

            # Gen params (strictly provided)
            self.max_input_length = max_input_length
            self.max_target_length = max_target_length
            self.num_beams = num_beams
            self.length_penalty = length_penalty
            self.no_repeat_ngram_size = no_repeat_ngram_size
            self.early_stopping = early_stopping

            logger.info("TextSummarizerModel initialized on device: %s", self.device)
        except Exception as e:
            logger.exception("Failed to initialize TextSummarizerModel.")
            raise TextSummarizerError(e, logger) from e

    def predict(self, text: str) -> str:
        try:
            print(text)
            if not isinstance(text, str) or not text.strip():
                raise TextSummarizerError("Input `text` must be a non-empty string.", logger)

            enc = self.tokenizer(
                text,
                return_tensors="pt",
                truncation=True,
                max_length=self.max_input_length,
                padding=False,
            )
            enc = {k: v.to(self.device) for k, v in enc.items()}

            with torch.no_grad():
                out = self.model.generate(
                    **enc,
                    max_length=self.max_target_length,
                    num_beams=self.num_beams,
                    length_penalty=self.length_penalty,
                    no_repeat_ngram_size=self.no_repeat_ngram_size,
                    early_stopping=self.early_stopping,
                )

            return self.tokenizer.decode(out[0], skip_special_tokens=True)
        except Exception as e:
            logger.exception("Single-text prediction failed.")
            raise TextSummarizerError(e, logger) from e

    def batch_predict(self, texts: list[str]) -> list[str]:
        """
        Simple looped batching (token-by-token beams are handled in HF generate).
        Using per-item generate to stay memory-safe and deterministic across inputs.
        """
        try:
            if not isinstance(texts, list) or not all(isinstance(t, str) for t in texts):
                raise TextSummarizerError("`texts` must be a list[str].", logger)

            return [self.predict(t) for t in texts]
        except Exception as e:
            logger.exception("Batch prediction failed.")
            raise TextSummarizerError(e, logger) from e
