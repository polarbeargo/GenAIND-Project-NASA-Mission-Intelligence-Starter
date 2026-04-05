"""Worker implementations for retrieval, safety checks, and analysis."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import llm_client
import rag_client
import ragas_evaluator

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
        context_text = rag_client.format_context(
            docs_result["documents"][0],
            docs_result["metadatas"][0],
        )
        return RetrievalResult(contexts=contexts, context_text=context_text)


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
