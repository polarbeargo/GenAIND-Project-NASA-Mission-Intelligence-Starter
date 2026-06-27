import asyncio
import inspect
import math
import re
from threading import Lock
from ragas.llms import LangchainLLMWrapper
from ragas.embeddings import LangchainEmbeddingsWrapper
from langchain_openai import ChatOpenAI
from langchain_openai import OpenAIEmbeddings
from typing import Any, Dict, List

from openai_config import (
    get_openai_api_key,
    get_openai_base_url,
    get_openai_chat_model,
    get_openai_embedding_model,
)

try:
    from ragas import SingleTurnSample
    from ragas.metrics import (
        BleuScore,
        LLMContextPrecisionWithoutReference,
        NonLLMContextPrecisionWithReference,
        ResponseRelevancy,
        Faithfulness,
        RougeScore,
    )
    from ragas import evaluate
    RAGAS_AVAILABLE = True
except ImportError:
    RAGAS_AVAILABLE = False


_EVALUATOR_CACHE: Dict[tuple[str, str, str, str], tuple[Any, Any]] = {}
_EVALUATOR_CACHE_LOCK = Lock()
_EVALUATOR_CACHE_HITS = 0
_EVALUATOR_CACHE_MISSES = 0
_TOKEN_PATTERN = re.compile(r"[a-z0-9]+")
_MIN_CONTEXT_OVERLAP = 0.08
_STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "had", "has", "have",
    "he", "her", "his", "in", "is", "it", "its", "of", "on", "or", "that", "the", "their",
    "there", "they", "this", "to", "was", "were", "with", "what", "when", "where", "which", "who",
    "why", "how", "during", "into", "about", "after", "before", "can", "could", "should", "would",
}


def _resolve_result(value):
    if inspect.isawaitable(value):
        try:
            return asyncio.run(value)
        except RuntimeError:
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(value)
            finally:
                loop.close()
    return value


def _score_single_metric(metric, sample) -> float:
    if hasattr(metric, "single_turn_score"):
        return float(_resolve_result(metric.single_turn_score(sample)))
    if hasattr(metric, "single_turn_ascore"):
        return float(_resolve_result(metric.single_turn_ascore(sample)))
    raise AttributeError("Metric does not expose single-turn scoring methods")


def _tokenize_for_overlap(text: str) -> set[str]:
    if not isinstance(text, str) or not text.strip():
        return set()
    return {
        token
        for token in _TOKEN_PATTERN.findall(text.lower())
        if len(token) > 1 and token not in _STOPWORDS
    }


def _calculate_context_precision_fallback(question: str, answer: str, contexts: List[str]) -> float:
    """Estimate context precision as relevant-context ratio using lexical overlap.

    This fallback is deterministic and lightweight so we can still surface a
    stable context precision signal when the RAGAS metric is unavailable.
    """
    cleaned_contexts = [context for context in contexts if isinstance(context, str) and context.strip()]
    if not cleaned_contexts:
        return 0.0

    anchor_tokens = _tokenize_for_overlap(question)
    anchor_tokens.update(_tokenize_for_overlap(answer))
    if not anchor_tokens:
        return 0.0

    relevant_contexts = 0
    for context in cleaned_contexts:
        context_tokens = _tokenize_for_overlap(context)
        if not context_tokens:
            continue
        overlap = len(context_tokens & anchor_tokens) / float(len(context_tokens))
        if overlap >= _MIN_CONTEXT_OVERLAP:
            relevant_contexts += 1

    return relevant_contexts / float(len(cleaned_contexts))


def _is_finite_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not math.isnan(float(value)) and math.isfinite(float(value))


def _create_single_turn_sample(question: str, answer: str, contexts: List[str]):
    try:
        return SingleTurnSample(
            user_input=question,
            response=answer,
            retrieved_contexts=contexts,
            reference=contexts[0],
            reference_contexts=contexts,
        )
    except TypeError:
        try:
            return SingleTurnSample(
                user_input=question,
                response=answer,
                retrieved_contexts=contexts,
                reference=contexts[0],
            )
        except TypeError:
            return SingleTurnSample(
                user_input=question,
                response=answer,
                retrieved_contexts=contexts,
            )


def _get_evaluator_resources(
    openai_api_key: str,
    openai_base: str,
):
    cache_key = (
        openai_api_key,
        openai_base,
        get_openai_chat_model(),
        get_openai_embedding_model(),
    )
    global _EVALUATOR_CACHE_HITS
    global _EVALUATOR_CACHE_MISSES
    with _EVALUATOR_CACHE_LOCK:
        resources = _EVALUATOR_CACHE.get(cache_key)
        if resources is not None:
            _EVALUATOR_CACHE_HITS += 1
            return resources

        _EVALUATOR_CACHE_MISSES += 1
        evaluator_llm = LangchainLLMWrapper(
            ChatOpenAI(
                model=get_openai_chat_model(),
                temperature=0,
                api_key=openai_api_key,
                base_url=openai_base,
            )
        )
        evaluator_embeddings = LangchainEmbeddingsWrapper(
            OpenAIEmbeddings(
                model=get_openai_embedding_model(),
                api_key=openai_api_key,
                base_url=openai_base,
            )
        )
        resources = (evaluator_llm, evaluator_embeddings)
        _EVALUATOR_CACHE[cache_key] = resources
        return resources


def get_evaluator_cache_metrics() -> Dict[str, int]:
    """Return lightweight cache metrics for RAGAS evaluator resources."""
    with _EVALUATOR_CACHE_LOCK:
        return {
            "current_size": len(_EVALUATOR_CACHE),
            "hits": _EVALUATOR_CACHE_HITS,
            "misses": _EVALUATOR_CACHE_MISSES,
        }

def evaluate_response_quality(question: str, answer: str, contexts: List[str]) -> Dict[str, float]:
    """Evaluate response quality using RAGAS metrics"""
    if not RAGAS_AVAILABLE:
        return {"error": "RAGAS not available"}

    cleaned_contexts = [context for context in contexts if isinstance(context, str) and context.strip()]
    if not cleaned_contexts:
        return {"error": "No contexts available for evaluation"}

    openai_api_key = get_openai_api_key()
    if not openai_api_key:
        return {"error": "OpenAI API key not configured for evaluation"}

    try:
        _openai_base = get_openai_base_url()
        evaluator_llm, evaluator_embeddings = _get_evaluator_resources(
            openai_api_key=openai_api_key,
            openai_base=_openai_base,
        )

        metrics = {
            "response_relevancy": ResponseRelevancy(
                llm=evaluator_llm,
                embeddings=evaluator_embeddings,
            ),
            "faithfulness": Faithfulness(llm=evaluator_llm),
            "bleu_score": BleuScore(),
            "rouge_score": RougeScore(),
        }

        context_precision_metric = None
        try:
            # When no ground-truth reference answer is available, this aligns
            # with RAGAS context precision guidance by evaluating retrieved
            # contexts against the generated response.
            context_precision_metric = LLMContextPrecisionWithoutReference(llm=evaluator_llm)
        except Exception:
            try:
                # Backward-compatible non-LLM path: this approximation uses the
                # top retrieved context as a lightweight reference when only
                # retrieved contexts are available.
                context_precision_metric = NonLLMContextPrecisionWithReference()
            except Exception:
                context_precision_metric = None

        if context_precision_metric is not None:
            metrics["context_precision"] = context_precision_metric

        sample = _create_single_turn_sample(question, answer, cleaned_contexts)

        scores: Dict[str, float] = {}
        metric_errors: Dict[str, str] = {}

        for metric_name, metric in metrics.items():
            try:
                scores[metric_name] = _score_single_metric(metric, sample)
            except Exception as metric_error:
                metric_errors[f"{metric_name}_error"] = str(metric_error)

        # Context precision can fail with upstream metric/library incompatibilities.
        # Ensure the monitoring pipeline still receives a bounded numeric signal.
        if not _is_finite_number(scores.get("context_precision")):
            fallback_precision = _calculate_context_precision_fallback(question, answer, cleaned_contexts)
            scores["context_precision"] = max(0.0, min(1.0, float(fallback_precision)))
            scores["context_precision_fallback"] = 1.0

        if scores:
            return {**scores, **metric_errors}

        return {
            "error": "Unable to compute any evaluation metric",
            **metric_errors,
        }
    except Exception as error:
        return {"error": f"Evaluation failed: {error}"}
