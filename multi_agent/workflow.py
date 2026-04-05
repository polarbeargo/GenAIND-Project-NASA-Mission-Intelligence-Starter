"""Orchestrator for parallel multi-agent chat processing."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

from multi_agent.models import (
    ChatWorkflowInput,
    ChatWorkflowResult,
    WorkflowError,
)
from multi_agent.workers import AnalysisWorker, RetrievalWorker, SafetyWorker


class MultiAgentChatWorkflow:
    """Coordinates retrieval, safety, and analysis workers."""

    def __init__(
        self,
        get_collection_fn,
        logger: logging.Logger,
        jailbreak_keywords,
        resource_limiter,
        prompt_injection_detector,
        vector_security_validator,
        output_validator,
        sensitive_info_filter,
        security_violation,
        security_auditor,
        security_level,
    ):
        self.retrieval_worker = RetrievalWorker(get_collection_fn=get_collection_fn)
        self.safety_worker = SafetyWorker(
            logger=logger,
            jailbreak_keywords=jailbreak_keywords,
            resource_limiter=resource_limiter,
            prompt_injection_detector=prompt_injection_detector,
            vector_security_validator=vector_security_validator,
            output_validator=output_validator,
            sensitive_info_filter=sensitive_info_filter,
            security_violation=security_violation,
            security_auditor=security_auditor,
            security_level=security_level,
        )
        self.analysis_worker = AnalysisWorker(
            logger=logger,
            security_violation=security_violation,
        )

    def run(self, workflow_input: ChatWorkflowInput, openai_key: str) -> ChatWorkflowResult:
        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="nasa-chat-agents") as executor:
            retrieval_future = executor.submit(self.retrieval_worker.run, workflow_input)
            preflight_future = executor.submit(self.safety_worker.preflight, workflow_input)

            preflight_result = preflight_future.result()
            retrieval_result = retrieval_future.result()

        if preflight_result.blocked_response:
            return ChatWorkflowResult(
                answer=preflight_result.blocked_response,
                contexts=[],
                evaluation={},
                blocked=True,
            )

        answer = self.analysis_worker.generate_answer(
            openai_key=openai_key,
            workflow_input=workflow_input,
            context_text=retrieval_result.context_text,
        )

        answer = self.safety_worker.postflight(
            answer=answer,
            contexts=retrieval_result.contexts,
            client_ip=workflow_input.client_ip,
        )

        evaluation = self.analysis_worker.evaluate(
            workflow_input=workflow_input,
            answer=answer,
            contexts=retrieval_result.contexts,
        )

        return ChatWorkflowResult(
            answer=answer,
            contexts=retrieval_result.contexts,
            evaluation=evaluation,
            blocked=False,
        )
