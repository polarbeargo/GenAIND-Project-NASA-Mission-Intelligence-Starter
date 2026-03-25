import asyncio
import inspect
import os
from ragas.llms import LangchainLLMWrapper
from ragas.embeddings import LangchainEmbeddingsWrapper
from langchain_openai import ChatOpenAI
from langchain_openai import OpenAIEmbeddings
from typing import Dict, List, Optional

try:
    from ragas import SingleTurnSample
    from ragas.metrics import BleuScore, NonLLMContextPrecisionWithReference, ResponseRelevancy, Faithfulness, RougeScore
    from ragas import evaluate
    RAGAS_AVAILABLE = True
except ImportError:
    RAGAS_AVAILABLE = False


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

def evaluate_response_quality(question: str, answer: str, contexts: List[str]) -> Dict[str, float]:
    """Evaluate response quality using RAGAS metrics"""
    if not RAGAS_AVAILABLE:
        return {"error": "RAGAS not available"}

    cleaned_contexts = [context for context in contexts if isinstance(context, str) and context.strip()]
    if not cleaned_contexts:
        return {"error": "No contexts available for evaluation"}

    openai_api_key = os.getenv("OPENAI_API_KEY") or os.getenv("CHROMA_OPENAI_API_KEY")
    if not openai_api_key:
        return {"error": "OpenAI API key not configured for evaluation"}

    try:
        _openai_base = os.getenv("OPENAI_BASE_URL", "https://openai.vocareum.com/v1")
        evaluator_llm = LangchainLLMWrapper(
            ChatOpenAI(
                model="gpt-3.5-turbo",
                temperature=0,
                api_key=openai_api_key,
                base_url=_openai_base,
            )
        )
        evaluator_embeddings = LangchainEmbeddingsWrapper(
            OpenAIEmbeddings(
                model="text-embedding-3-small",
                api_key=openai_api_key,
                base_url=_openai_base,
            )
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
