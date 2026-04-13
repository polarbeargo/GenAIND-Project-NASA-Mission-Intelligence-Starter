import asyncio
import inspect
from threading import Lock
from ragas.llms import LangchainLLMWrapper
from ragas.embeddings import LangchainEmbeddingsWrapper
from langchain_openai import ChatOpenAI
from langchain_openai import OpenAIEmbeddings
from typing import Any, Dict, List, Optional

from openai_config import (
    get_openai_api_key,
    get_openai_base_url,
    get_openai_chat_model,
    get_openai_embedding_model,
)

try:
    from ragas import SingleTurnSample
    from ragas.metrics import BleuScore, NonLLMContextPrecisionWithReference, ResponseRelevancy, Faithfulness, RougeScore
    from ragas import evaluate
    RAGAS_AVAILABLE = True
except ImportError:
    RAGAS_AVAILABLE = False


_EVALUATOR_CACHE: Dict[tuple[str, str, str, str], tuple[Any, Any]] = {}
_EVALUATOR_CACHE_LOCK = Lock()
_EVALUATOR_CACHE_HITS = 0
_EVALUATOR_CACHE_MISSES = 0


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

        try:
            metrics["context_precision"] = NonLLMContextPrecisionWithReference()
        except Exception:
            pass

        sample = _create_single_turn_sample(question, answer, cleaned_contexts)

        scores: Dict[str, float] = {}
        metric_errors: Dict[str, str] = {}

        for metric_name, metric in metrics.items():
            try:
                scores[metric_name] = _score_single_metric(metric, sample)
            except Exception as metric_error:
                metric_errors[f"{metric_name}_error"] = str(metric_error)

        if scores:
            return {**scores, **metric_errors}

        return {
            "error": "Unable to compute any evaluation metric",
            **metric_errors,
        }
    except Exception as error:
        return {"error": f"Evaluation failed: {error}"}
