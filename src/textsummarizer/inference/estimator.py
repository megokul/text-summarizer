# FILE: src/textsummarizer/inference/estimator.py
"""Inference-time estimator wrapper for sequence-to-sequence summarization.

This module defines a thin, config-driven wrapper around a Hugging Face
seq2seq model and its tokenizer to perform text summarization in production.

# Design intent
- Separation of concerns: Loading, device placement, and generation knobs are
  injected by the caller (e.g., via a config object) rather than hard-coded.
- Predictability: No hidden defaults or background state; the wrapper is a
  small façade over HF `generate`.
- Reliability: Every failure is logged with context and re-raised as the
  project’s custom ``TextSummarizerError`` to keep error handling uniform.
"""

from pathlib import Path

import torch
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

from src.textsummarizer.exception.exception import TextSummarizerError
from src.textsummarizer.logging import logger


class TextSummarizerModel:
    """Minimal estimator-style wrapper around a HF seq2seq model.

    The class focuses on:
    - Deterministic, explicit configuration of generation parameters.
    - Simple "single item" and "batch" predict APIs suitable for services.

    Args:
        model_dir (Path | str): Filesystem path to the model directory
            (e.g., the result of `save_pretrained`).
        tokenizer_dir (Path | str): Filesystem path to the tokenizer directory.
        device (str): Torch device string such as "cpu" or "cuda".
        max_input_length (int): Maximum number of tokens for the encoder input.
        max_target_length (int): Maximum number of tokens to generate.
        num_beams (int): Beam width used during generation.
        length_penalty (float): Penalty to discourage overly long generations.
        no_repeat_ngram_size (int): N-gram constraint for repetition control.
        early_stopping (bool): Whether to enable early stopping in generation.

    Raises:
        TextSummarizerError: If initialization fails at any step.
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
            # Normalize user-provided paths so downstream calls can rely on
            # `Path` operations and POSIX formatting for logs.
            self.model_dir = Path(model_dir)
            self.tokenizer_dir = Path(tokenizer_dir)

            # Validate that required directories exist before attempting to
            # load large artifacts. This avoids opaque HF errors later.
            if not self.model_dir.exists():
                raise TextSummarizerError(
                    (
                        "Model directory does not exist: "
                        f"{self.model_dir.as_posix()}"
                    ),
                    logger,
                )
            if not self.tokenizer_dir.exists():
                raise TextSummarizerError(
                    (
                        "Tokenizer directory does not exist: "
                        f"{self.tokenizer_dir.as_posix()}"
                    ),
                    logger,
                )
            if not device:
                raise TextSummarizerError(
                    "`device` must be provided in config.", logger
                )

            # Keep the device string rather than resolving a torch.device so
            # callers can pass "cuda:0" or similar without translation here.
            self.device = device

            # Load tokenizer and model from disk. We place the model on the
            # requested device and switch it to eval mode for inference.
            logger.info(
                "Loading tokenizer from %s", self.tokenizer_dir.as_posix()
            )
            self.tokenizer = AutoTokenizer.from_pretrained(self.tokenizer_dir)

            logger.info("Loading model from %s", self.model_dir.as_posix())
            self.model = AutoModelForSeq2SeqLM.from_pretrained(
                self.model_dir
            ).to(self.device)
            self.model.eval()

            # Persist generation parameters as attributes; we deliberately do
            # not mutate them internally so the caller retains full control.
            self.max_input_length = max_input_length
            self.max_target_length = max_target_length
            self.num_beams = num_beams
            self.length_penalty = length_penalty
            self.no_repeat_ngram_size = no_repeat_ngram_size
            self.early_stopping = early_stopping

            logger.info(
                "TextSummarizerModel initialized on device: %s", self.device
            )
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to initialize TextSummarizerModel.")
            raise TextSummarizerError(e, logger) from e
        return None

    def predict(self, text: str) -> str:
        """Generate a summary for a single input string.

        Args:
            text (str): Input document/dialogue to summarize.

        Returns:
            str: Generated summary text.

        Raises:
            TextSummarizerError: If tokenization or generation fails.
        """
        try:
            # Validate input early to provide clear feedback to API callers.
            if not isinstance(text, str) or not text.strip():
                raise TextSummarizerError(
                    "Input `text` must be a non-empty string.", logger
                )

            # Tokenize to PyTorch tensors, enforce truncation to guarantee we
            # never exceed model positional limits, and avoid padding to speed
            # up single-example inference.
            enc = self.tokenizer(
                text,
                return_tensors="pt",
                truncation=True,
                max_length=self.max_input_length,
                padding=False,
            )
            # Move all encoded tensors to the configured device (CPU/GPU).
            enc = {k: v.to(self.device) for k, v in enc.items()}

            # Disable gradient tracking for inference; reduce memory footprint
            # and avoid autograd overhead.
            with torch.no_grad():
                out = self.model.generate(
                    **enc,
                    max_length=self.max_target_length,
                    num_beams=self.num_beams,
                    length_penalty=self.length_penalty,
                    no_repeat_ngram_size=self.no_repeat_ngram_size,
                    early_stopping=self.early_stopping,
                )

            # Decode the first (and only) sequence from the output batch.
            summary = self.tokenizer.decode(
                out[0], skip_special_tokens=True
            )
            return summary
        except Exception as e:  # noqa: BLE001
            logger.error("Single-text prediction failed.")
            raise TextSummarizerError(e, logger) from e

    def batch_predict(self, texts: list[str]) -> list[str]:
        """Generate summaries for a list of input strings.

        This is intentionally implemented as a simple loop over `predict`
        calls. While Hugging Face supports batched generation, this approach:
        - Keeps memory usage stable across variable-length inputs.
        - Preserves per-example validation and clear error reporting.

        Args:
            texts (list[str]): List of input documents/dialogues.

        Returns:
            list[str]: Generated summaries aligned with the input order.

        Raises:
            TextSummarizerError: If inputs are invalid or generation fails.
        """
        try:
            # Validate the container shape and element types explicitly to
            # prevent subtle bugs from Numpy arrays or non-string entries.
            if not isinstance(texts, list) or not all(
                isinstance(t, str) for t in texts
            ):
                raise TextSummarizerError(
                    "`texts` must be a list[str].", logger
                )

            # Process items serially to keep memory usage predictable under
            # load and to keep error boundaries per-example.
            results = [self.predict(t) for t in texts]
            return results
        except Exception as e:  # noqa: BLE001
            logger.error("Batch prediction failed.")
            raise TextSummarizerError(e, logger) from e
