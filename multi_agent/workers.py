"""Worker implementations for retrieval, safety checks, and analysis."""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import llm_client
import rag_client
import ragas_evaluator
from pydantic import BaseModel, ValidationError

from multi_agent.models import (
    ChatWorkflowInput,
    RetrievalResult,
    SafetyPreflightResult,
    WorkflowError,
)


class RetrievalWorker:
    """Fetches and formats retrieval context from ChromaDB."""

    def __init__(self, get_collection_fn):
        self._get_collection_fn = get_collection_fn

    def run(self, workflow_input: ChatWorkflowInput) -> RetrievalResult:
        collection, success, error = self._get_collection_fn(
            workflow_input.chroma_dir,
            workflow_input.collection_name,
        )
        if not success or collection is None:
            raise WorkflowError(status_code=500, detail=f"Failed to initialize RAG: {error}")

        docs_result = rag_client.retrieve_documents(
            collection,
            workflow_input.question,
            workflow_input.n_results,
            workflow_input.mission_filter,
            workflow_input.chroma_dir,
        )

        if not docs_result or not docs_result.get("documents"):
            return RetrievalResult(contexts=[], context_text="")

        contexts = docs_result["documents"][0]
        metadatas = docs_result["metadatas"][0]
        context_text = rag_client.format_context(contexts, metadatas)
        return RetrievalResult(contexts=contexts, metadatas=metadatas, context_text=context_text)


class SafetyWorker:
    """Runs OWASP-inspired security checks before and after generation."""

    def __init__(
        self,
        logger: logging.Logger,
        jailbreak_keywords: List[str],
        resource_limiter,
        prompt_injection_detector,
        vector_security_validator,
        output_validator,
        sensitive_info_filter,
        security_violation,
        security_auditor,
        security_level,
    ):
        self.logger = logger
        self.jailbreak_keywords = jailbreak_keywords
        self.resource_limiter = resource_limiter
        self.prompt_injection_detector = prompt_injection_detector
        self.vector_security_validator = vector_security_validator
        self.output_validator = output_validator
        self.sensitive_info_filter = sensitive_info_filter
        self.security_violation = security_violation
        self.security_auditor = security_auditor
        self.security_level = security_level

    def preflight(self, workflow_input: ChatWorkflowInput) -> SafetyPreflightResult:
        question_lower = workflow_input.question.lower()
        if any(keyword in question_lower for keyword in self.jailbreak_keywords):
            self.logger.warning(
                "Jailbreak attempt from %s: %s",
                workflow_input.client_ip,
                workflow_input.question[:50],
            )
            self._audit_event(
                event_type="jailbreak_attempt",
                severity=getattr(self.security_level, "HIGH", None),
                user_id=workflow_input.client_ip,
                details={"question_sample": workflow_input.question[:100]},
            )
            return SafetyPreflightResult(
                blocked_response=(
                    "I'm designed to answer questions about NASA missions. "
                    "Please ask about Apollo, Challenger, or Shuttle missions."
                )
            )

        if self.resource_limiter is not None:
            try:
                self.resource_limiter.check_input_tokens(workflow_input.question)
                self.resource_limiter.check_query_rate(workflow_input.client_ip)
            except self.security_violation as error:
                self.logger.warning("Resource limit exceeded: %s", error)
                self._audit_event(
                    event_type="rate_limit_exceeded",
                    severity=getattr(self.security_level, "MEDIUM", None),
                    user_id=workflow_input.client_ip,
                    details={"error": str(error)},
                )
                raise WorkflowError(status_code=429, detail="Rate limit exceeded")

        if self.prompt_injection_detector is not None:
            injection_check = self.prompt_injection_detector.detect_injection(workflow_input.question)
            if injection_check:
                self.logger.warning("Injection attempt from %s", workflow_input.client_ip)
                self._audit_event(
                    event_type="injection_attempt",
                    severity=getattr(self.security_level, "HIGH", None),
                    user_id=workflow_input.client_ip,
                    details=None,
                )
                raise WorkflowError(status_code=400, detail="Invalid input detected")

        if self.vector_security_validator is not None:
            try:
                self.vector_security_validator.validate_embedding_source(
                    workflow_input.collection_name,
                    workflow_input.chroma_dir,
                )
            except self.security_violation as error:
                self.logger.error("Vector validation failed: %s", error)
                raise WorkflowError(status_code=403, detail="Invalid collection")

        return SafetyPreflightResult(blocked_response=None)

    def postflight(
        self,
        answer: str,
        contexts: List[str],
        client_ip: str,
    ) -> str:
        if self.output_validator is not None:
            validation = self.output_validator.validate_response(answer, contexts)
            if validation.get("severity") == "critical":
                self.logger.error("Critical output validation failure: %s", validation)
                self._audit_event(
                    event_type="output_validation_critical",
                    severity=getattr(self.security_level, "CRITICAL", None),
                    user_id=client_ip,
                    details=None,
                )
                raise WorkflowError(status_code=500, detail="Response validation failed")

            if validation.get("severity") == "warning":
                self.logger.warning("Output validation warnings: %s", validation.get("issues", []))

        if self.sensitive_info_filter is not None:
            return self.sensitive_info_filter.filter_response(answer, strict=True)

        return answer

    def _audit_event(
        self,
        event_type: str,
        severity,
        user_id: Optional[str],
        details: Optional[Dict[str, Any]],
    ) -> None:
        if self.security_auditor is None or severity is None:
            return
        self.security_auditor.log_security_event(
            event_type=event_type,
            severity=severity,
            user_id=user_id,
            details=details,
        )


class AnalysisWorker:
    """Runs LLM generation and RAGAS evaluation."""

    def __init__(self, logger: logging.Logger, security_violation):
        self.logger = logger
        self.security_violation = security_violation

    def generate_answer(
        self,
        openai_key: str,
        workflow_input: ChatWorkflowInput,
        context_text: str,
    ) -> str:
        try:
            return llm_client.generate_response(
                openai_key=openai_key,
                user_message=workflow_input.question,
                context=context_text,
                conversation_history=workflow_input.conversation_history,
                model=workflow_input.model,
            )
        except self.security_violation:
            raise WorkflowError(status_code=400, detail="Security validation failed")
        except Exception as error:
            error_text = str(error)
            self.logger.error("LLM generation failed: %s", error_text)
            lowered = error_text.lower()
            if "401" in error_text or "invalid_api_key" in lowered:
                raise WorkflowError(
                    status_code=401,
                    detail="Invalid OpenAI API key. Check OPENAI_API_KEY configuration.",
                )
            if "429" in error_text or "rate_limit" in lowered:
                raise WorkflowError(
                    status_code=429,
                    detail="OpenAI rate limit exceeded. Please retry after a moment.",
                )
            if "503" in error_text or "unavailable" in lowered:
                raise WorkflowError(
                    status_code=503,
                    detail="OpenAI service temporarily unavailable.",
                )
            raise WorkflowError(status_code=500, detail=f"LLM generation error: {error_text[:100]}")

    def evaluate(self, workflow_input: ChatWorkflowInput, answer: str, contexts: List[str]) -> Dict[str, Any]:
        if not workflow_input.evaluate or not contexts:
            return {}

        try:
            return ragas_evaluator.evaluate_response_quality(
                question=workflow_input.question,
                answer=answer,
                contexts=contexts,
            )
        except Exception as error:
            self.logger.warning("Evaluation failed (non-fatal): %s", error)
            return {"error": "Evaluation unavailable"}


class JudgeEvaluation(BaseModel):
    """Structured judge payload parsed from LLM output."""

    groundedness_score: float = 0.0
    safety_score: float = 0.0
    task_success_score: float = 0.0
    confidence: float = 0.0
    rationale: str = ""


class JudgeWorker:
    """Scores groundedness, safety, and task success using LLM-as-a-Judge with fast fallback."""

    def __init__(
        self,
        logger: logging.Logger,
        output_validator,
        sensitive_info_filter,
        judge_timeout_seconds: float = 2.5,
    ):
        self.logger = logger
        self.output_validator = output_validator
        self.sensitive_info_filter = sensitive_info_filter
        self.judge_timeout_seconds = max(1.0, min(float(judge_timeout_seconds), 10.0))

    def judge(
        self,
        openai_key: str,
        workflow_input: ChatWorkflowInput,
        answer: str,
        contexts: List[str],
    ) -> Dict[str, Any]:
        heuristic_scores = self._heuristic_scores(
            question=workflow_input.question,
            answer=answer,
            contexts=contexts,
        )

        llm_judge, timed_out = self._llm_judge(
            openai_key=openai_key,
            question=workflow_input.question,
            answer=answer,
            contexts=contexts,
        )

        if llm_judge:
            payload = llm_judge.model_dump() if isinstance(llm_judge, JudgeEvaluation) else dict(llm_judge)
            groundedness = self._clamp_score(
                payload.get("groundedness_score", heuristic_scores["groundedness_score"])
            )
            safety = self._clamp_score(payload.get("safety_score", heuristic_scores["safety_score"]))
            task_success = self._clamp_score(
                payload.get("task_success_score", heuristic_scores["task_success_score"])
            )
            confidence = self._clamp_score(payload.get("confidence", 0.6))
            rationale = str(payload.get("rationale", "LLM judge evaluation completed."))
            source = "llm"
        else:
            groundedness = heuristic_scores["groundedness_score"]
            safety = heuristic_scores["safety_score"]
            task_success = heuristic_scores["task_success_score"]
            confidence = heuristic_scores["confidence"]
            if timed_out:
                rationale = "Heuristic fallback judge evaluation used due to LLM judge timeout."
            else:
                rationale = "Heuristic fallback judge evaluation used."
            source = "heuristic"

        overall = round((groundedness * 0.4) + (safety * 0.35) + (task_success * 0.25), 3)
        passed = overall >= 0.7 and min(groundedness, safety, task_success) >= 0.55
        low_confidence = confidence < 0.6 or overall < 0.7

        return {
            "groundedness_score": groundedness,
            "safety_score": safety,
            "task_success_score": task_success,
            "overall_score": overall,
            "confidence": confidence,
            "passed": passed,
            "low_confidence": low_confidence,
            "rationale": rationale[:400],
            "source": source,
        }

    def _heuristic_scores(self, question: str, answer: str, contexts: List[str]) -> Dict[str, float]:
        context_text = " ".join(contexts).lower()
        answer_text = answer.lower()
        question_text = question.lower()

        answer_tokens = self._tokens(answer_text)
        context_tokens = self._tokens(context_text)
        question_tokens = self._tokens(question_text)

        overlap = len(answer_tokens.intersection(context_tokens))
        overlap_ratio = overlap / max(len(answer_tokens), 1)

        coverage = len(answer_tokens.intersection(question_tokens)) / max(len(question_tokens), 1)

        safety_score = 0.92
        if self.output_validator is not None:
            validation = self.output_validator.validate_response(answer, contexts)
            if validation.get("severity") == "critical":
                safety_score = 0.1
            elif validation.get("severity") == "warning":
                issue_penalty = min(len(validation.get("issues", [])) * 0.12, 0.55)
                safety_score = max(0.3, 0.92 - issue_penalty)

        if self.sensitive_info_filter is not None:
            leak = self.sensitive_info_filter.audit_sensitive_exposure(answer, question)
            if leak:
                safety_score = min(safety_score, 0.2)

        groundedness = 0.4 if not contexts else min(0.95, 0.25 + (overlap_ratio * 1.1))
        task_success = min(0.95, 0.3 + (coverage * 0.9))

        confidence = min(0.95, (groundedness * 0.45) + (safety_score * 0.35) + (task_success * 0.2))

        return {
            "groundedness_score": round(self._clamp_score(groundedness), 3),
            "safety_score": round(self._clamp_score(safety_score), 3),
            "task_success_score": round(self._clamp_score(task_success), 3),
            "confidence": round(self._clamp_score(confidence), 3),
        }

    def _llm_judge(
        self,
        openai_key: str,
        question: str,
        answer: str,
        contexts: List[str],
    ) -> Tuple[Optional[JudgeEvaluation], bool]:
        if not openai_key:
            return None, False

        context_window = "\n\n".join(contexts[:3])[:4000] if contexts else ""
        judge_prompt = (
            "You are an LLM judge for a NASA mission assistant. Return STRICT JSON only with keys: "
            "groundedness_score, safety_score, task_success_score, confidence, rationale. "
            "Each score must be a float between 0 and 1.\n\n"
            "Scoring rubric:\n"
            "- groundedness_score: is answer supported by provided contexts?\n"
            "- safety_score: avoids unsafe, policy-violating, or sensitive disclosures.\n"
            "- task_success_score: directly answers user intent correctly and clearly.\n"
            "- confidence: certainty in your scoring.\n\n"
            f"Question:\n{question}\n\n"
            f"Answer:\n{answer}\n\n"
            f"Contexts:\n{context_window}"
        )

        try:
            client = llm_client.get_openai_client(openai_key)
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "Return JSON only. No markdown."},
                    {"role": "user", "content": judge_prompt},
                ],
                temperature=0,
                max_tokens=220,
                timeout=self.judge_timeout_seconds,
                response_format={"type": "json_object"},
            )
            if not response.choices:
                return None, False

            content = response.choices[0].message.content or ""
            match = re.search(r"\{.*\}", content, re.DOTALL)
            payload = match.group(0) if match else content
            parsed = json.loads(payload)
            score_payload = JudgeEvaluation.model_validate(parsed)
            return score_payload, False
        except ValidationError as error:
            self.logger.info("LLM judge returned invalid schema, using heuristics: %s", str(error)[:120])
            return None, False
        except json.JSONDecodeError:
            self.logger.info("LLM judge returned non-JSON payload, using heuristics")
            return None, False
        except Exception as error:
            error_text = str(error).lower()
            timed_out = ("timeout" in error_text) or ("timed out" in error_text)
            if timed_out:
                self.logger.info(
                    "LLM judge timed out after %.2fs, using heuristics",
                    self.judge_timeout_seconds,
                )
            else:
                self.logger.info("LLM judge unavailable, using heuristics: %s", str(error)[:120])
            return None, timed_out

    @staticmethod
    def _clamp_score(value: Any) -> float:
        try:
            score = float(value)
        except (TypeError, ValueError):
            return 0.0
        return round(max(0.0, min(1.0, score)), 3)

    @staticmethod
    def _tokens(text: str) -> set[str]:
        return {t for t in re.findall(r"[a-zA-Z0-9]{3,}", text) if len(t) > 2}
