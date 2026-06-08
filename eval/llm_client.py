"""LLM client with automatic backend selection.

Backend priority:
  1. Apple Silicon (macOS arm64) → mlx_lm
  2. CUDA available             → HuggingFace transformers (GPU)
  3. Fallback                   → HuggingFace transformers (CPU)

For remote inference, use SenseNovaClient which calls the SenseNova
OpenAI-compatible API endpoint.
"""

from __future__ import annotations

import logging
import platform
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Special tokens that mlx_lm may leak into generated text
_SPECIAL_TOKEN_RE = re.compile(
    r"<\|(?:im_end|endoftext|end|eos|pad|user|assistant|system)\|>.*",
    re.DOTALL,
)


def _strip_special_tokens(text: str) -> str:
    """Remove EOS/special tokens that the model may emit verbatim."""
    return _SPECIAL_TOKEN_RE.sub("", text).strip()


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
        batch_size: int = 1,
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
        raw = generate(
            self._mlx_model,
            self._mlx_tokenizer,
            prompt=prompt,
            max_tokens=max_tokens,
            verbose=False,
        )
        return _strip_special_tokens(raw)

    def _parse_transformers_output(self, generated) -> str:
        """Extract text from a transformers pipeline output."""
        if isinstance(generated, list):
            return _strip_special_tokens(generated[-1]["content"])
        return _strip_special_tokens(generated)

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
            # Sequential is faster than batch_generate for quantized MLX models on Apple Silicon:
            # the bottleneck is memory bandwidth (reading weights), and continuous batching
            # multiplies KV-cache size by batch_size, saturating memory and hurting throughput.
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


class RemoteClient:
    """Remote inference client for any OpenAI-compatible API endpoint.

    Usage::

        client = RemoteClient(api_key="<your-key>", base_url="https://...", model="...")
        reply = client.complete("What is FastAPI?")
    """

    DEFAULT_BASE_URL = "https://token.sensenova.cn/v1"
    DEFAULT_MODEL = "deepseek-v4-flash"

    def __init__(
        self,
        api_key: str,
        model: str = DEFAULT_MODEL,
        base_url: str = DEFAULT_BASE_URL,
        max_workers: int = 1,
        temperature: float = 0.0,
        reasoning_effort: Optional[str] = None,
        thinking: Optional[str] = None,
    ) -> None:
        try:
            from openai import OpenAI  # type: ignore
        except ImportError as exc:
            raise ImportError(
                "openai package is required for RemoteClient. "
                "Install it with: pip install openai"
            ) from exc

        self.model = model
        self.max_workers = max_workers
        self.base_url = base_url
        self.temperature = temperature
        self.reasoning_effort = reasoning_effort
        self.thinking = thinking
        self._client = OpenAI(api_key=api_key, base_url=base_url)
        logger.info(
            "RemoteClient ready: model=%s base_url=%s temperature=%s "
            "reasoning_effort=%s thinking=%s",
            model,
            base_url,
            temperature,
            reasoning_effort,
            thinking,
        )

    # ------------------------------------------------------------------
    # Public API (same interface as LLMClient)
    # ------------------------------------------------------------------

    @staticmethod
    def _as_dict(obj: Any) -> dict:
        if obj is None:
            return {}
        if isinstance(obj, dict):
            return obj
        if hasattr(obj, "model_dump"):
            return obj.model_dump()
        if hasattr(obj, "dict"):
            return obj.dict()
        return {}

    @staticmethod
    def _message_content_text(message: Any) -> Optional[str]:
        """Return text content from OpenAI-compatible message shapes."""
        content = getattr(message, "content", None)
        if isinstance(content, str) or content is None:
            return content
        if isinstance(content, list):
            parts: list[str] = []
            for item in content:
                item_dict = RemoteClient._as_dict(item)
                text = item_dict.get("text")
                if isinstance(text, str):
                    parts.append(text)
            return "".join(parts) if parts else None
        return str(content)

    @classmethod
    def _empty_response_diagnostics(cls, response: Any) -> dict[str, Any]:
        response_dict = cls._as_dict(response)
        choice = response.choices[0] if getattr(response, "choices", None) else None
        choice_dict = cls._as_dict(choice)
        message = getattr(choice, "message", None) if choice is not None else None
        message_dict = cls._as_dict(message)

        usage = response_dict.get("usage") or {}
        completion_details = usage.get("completion_tokens_details") or {}
        reasoning_text = (
            message_dict.get("reasoning")
            or message_dict.get("reasoning_content")
            or message_dict.get("reasoning_details")
        )
        return {
            "finish_reason": getattr(choice, "finish_reason", None)
            or choice_dict.get("finish_reason"),
            "completion_tokens": usage.get("completion_tokens"),
            "reasoning_tokens": completion_details.get("reasoning_tokens"),
            "reasoning_present": bool(reasoning_text),
            "tool_calls": bool(message_dict.get("tool_calls")),
            "refusal": bool(message_dict.get("refusal")),
        }

    def _is_deepseek_api(self) -> bool:
        return "deepseek.com" in self.base_url

    def _extra_body(self) -> dict[str, Any]:
        extra_body: dict[str, Any] = {}
        reasoning_effort = self.reasoning_effort

        if self.thinking == "disabled":
            extra_body["thinking"] = {"type": "disabled"}
            return extra_body

        if self.thinking == "enabled":
            extra_body["thinking"] = {"type": "enabled"}

        if reasoning_effort == "none" and self._is_deepseek_api():
            # DeepSeek V4 disables thinking via `thinking`, not via
            # `reasoning_effort=none`.
            extra_body["thinking"] = {"type": "disabled"}
        elif reasoning_effort:
            extra_body["reasoning_effort"] = reasoning_effort

        return extra_body

    @staticmethod
    def _is_retryable_error(exc: Exception) -> bool:
        status_code = getattr(exc, "status_code", None)
        if status_code is None:
            return True
        try:
            code = int(status_code)
        except (TypeError, ValueError):
            return True
        return code == 429 or code >= 500

    def complete(self, prompt: str, max_tokens: int = 256) -> Optional[str]:
        """Single-turn chat completion; returns the assistant reply or None on error."""
        import time
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                request_args: dict[str, Any] = {
                    "model": self.model,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": max_tokens,
                    "temperature": self.temperature,
                }
                extra_body = self._extra_body()
                if extra_body:
                    request_args["extra_body"] = extra_body
                response = self._client.chat.completions.create(**request_args)
                content = self._message_content_text(response.choices[0].message)
                if content is None or not content.strip():
                    diag = self._empty_response_diagnostics(response)
                    logger.warning(
                        "Remote API returned empty content "
                        "(attempt %d/%d, finish_reason=%s, completion_tokens=%s, "
                        "reasoning_tokens=%s, reasoning_present=%s, tool_calls=%s, refusal=%s)",
                        attempt,
                        max_retries,
                        diag["finish_reason"],
                        diag["completion_tokens"],
                        diag["reasoning_tokens"],
                        diag["reasoning_present"],
                        diag["tool_calls"],
                        diag["refusal"],
                    )
                    if attempt < max_retries:
                        time.sleep(2 ** attempt)
                        continue
                    return None
                return content
            except Exception as e:
                retryable = self._is_retryable_error(e)
                logger.warning(
                    "Remote API error (attempt %d/%d, retryable=%s): %s",
                    attempt,
                    max_retries,
                    retryable,
                    e,
                )
                if retryable and attempt < max_retries:
                    time.sleep(2 ** attempt)
                    continue
                return None
        return None

    def complete_batch(
        self, prompts: list[str], max_tokens: int = 256
    ) -> list[Optional[str]]:
        """Send all prompts concurrently; return one reply per prompt (None on error)."""
        results: list[Optional[str]] = [None] * len(prompts)
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_idx = {
                executor.submit(self.complete, p, max_tokens): i
                for i, p in enumerate(prompts)
            }
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    results[idx] = future.result()
                except Exception as e:
                    logger.warning(
                        "Remote API batch error at index %d: %s", idx, e
                    )
        return results
