"""LLM client with automatic backend selection.

Backend priority:
  1. Apple Silicon (macOS arm64) → mlx_lm
  2. CUDA available             → HuggingFace transformers (GPU)
  3. Fallback                   → HuggingFace transformers (CPU)
"""

from __future__ import annotations

import logging
import platform
from typing import Optional

logger = logging.getLogger(__name__)


def _is_apple_silicon() -> bool:
    return platform.system() == "Darwin" and platform.machine() == "arm64"


def _has_cuda() -> bool:
    try:
        import torch
        return torch.cuda.is_available()
    except ImportError:
        return False


class LLMClient:
    """Runs inference locally, selecting the best available backend automatically."""

    def __init__(
        self,
        model: str = "Qwen/Qwen2.5-Coder-7B-Instruct",
        device_map: str = "auto",
        batch_size: int = 8,
    ) -> None:
        self.model_name = model
        self.batch_size = batch_size

        if _is_apple_silicon():
            self._backend = "mlx"
            self._load_mlx(model)
        else:
            self._backend = "transformers"
            self._load_transformers(model, device_map)

        logger.info("Model loaded via backend=%s.", self._backend)

    # ------------------------------------------------------------------
    # Backend loaders
    # ------------------------------------------------------------------

    def _load_mlx(self, model: str) -> None:
        try:
            from mlx_lm import load  # type: ignore
        except ImportError as exc:
            raise ImportError(
                "mlx_lm is required on Apple Silicon. Install it with: pip install mlx-lm"
            ) from exc
        logger.info("Loading model %s with mlx_lm ...", model)
        self._mlx_model, self._mlx_tokenizer = load(model)
        self._tokenizer = self._mlx_tokenizer

    def _load_transformers(self, model: str, device_map: str) -> None:
        from transformers import pipeline  # type: ignore
        if _has_cuda():
            logger.info("Loading model %s with transformers (CUDA) ...", model)
        else:
            logger.info("Loading model %s with transformers (CPU) ...", model)
        self._pipe = pipeline(
            "text-generation",
            model=model,
            device_map=device_map,
            torch_dtype="auto",
        )
        self._tokenizer = self._pipe.tokenizer

    # ------------------------------------------------------------------
    # Inference helpers
    # ------------------------------------------------------------------

    def _messages_to_prompt(self, messages: list[dict]) -> str:
        """Apply chat template to convert messages to a single prompt string."""
        return self._tokenizer.apply_chat_template(
            messages, tokenize=False, add_generation_prompt=True
        )

    def _mlx_generate(self, messages: list[dict], max_tokens: int) -> str:
        from mlx_lm import generate  # type: ignore
        prompt = self._messages_to_prompt(messages)
        return generate(
            self._mlx_model,
            self._mlx_tokenizer,
            prompt=prompt,
            max_tokens=max_tokens,
            verbose=False,
        ).strip()

    def _parse_transformers_output(self, generated) -> str:
        """Extract text from a transformers pipeline output."""
        if isinstance(generated, list):
            return generated[-1]["content"].strip()
        return generated.strip()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def complete(self, prompt: str, max_tokens: int = 256) -> Optional[str]:
        """Run a single-turn chat completion; return the assistant reply."""
        messages = [{"role": "user", "content": prompt}]
        try:
            if self._backend == "mlx":
                return self._mlx_generate(messages, max_tokens)
            result = self._pipe(
                messages,
                max_new_tokens=max_tokens,
                do_sample=False,
                return_full_text=False,
            )
            return self._parse_transformers_output(result[0]["generated_text"])
        except Exception as e:
            logger.warning("LLM inference error: %s", e)
            return None

    def complete_batch(self, prompts: list[str], max_tokens: int = 256) -> list[Optional[str]]:
        """Run batch inference on a list of prompts; return one reply per prompt (None on error)."""
        if self._backend == "mlx":
            # Run sequentially: quantized MLX models are fast enough that
            # sequential throughput typically exceeds transformers batching.
            # For true parallel batching, use mlx_lm.generate_step directly.
            results: list[Optional[str]] = []
            for p in prompts:
                try:
                    results.append(self._mlx_generate([{"role": "user", "content": p}], max_tokens))
                except Exception as e:
                    logger.warning("LLM inference error: %s", e)
                    results.append(None)
            return results

        messages_batch = [[{"role": "user", "content": p}] for p in prompts]
        try:
            outputs = self._pipe(
                messages_batch,
                max_new_tokens=max_tokens,
                do_sample=False,
                return_full_text=False,
                batch_size=len(messages_batch),
            )
            return [self._parse_transformers_output(out[0]["generated_text"]) for out in outputs]
        except Exception as e:
            logger.warning("LLM batch inference error: %s", e)
            return [None] * len(prompts)
