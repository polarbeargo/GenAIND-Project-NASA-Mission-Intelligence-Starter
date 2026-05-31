"""Security utilities for the NASA Mission Intelligence RAG system.

Implements controls aligned with the OWASP Top 10 for LLM Applications.

Reference:
https://genai.owasp.org/llm-top-10/
"""

import logging
import os
import re
from threading import Lock
from typing import Dict, List, Optional, Any
from enum import Enum

logger = logging.getLogger(__name__)


class SecurityLevel(Enum):
    """Risk levels for security violations."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class SecurityViolation(Exception):
    """Raised when a security check fails."""
    def __init__(self, level: SecurityLevel, message: str, details: Optional[Dict[str, Any]] = None):
        self.level = level
        self.message = message
        self.details = details or {}
        super().__init__(f"[{level.value.upper()}] {message}")


class PromptInjectionDetector:
    """LLM01: Detects common prompt injection patterns."""

    INJECTION_PATTERNS = [
        re.compile(r"(?i)(ignore|disregard|forget|override).*?(previous|system|instruction|prompt)"),
        re.compile(r"(?i)\b(override|system)\s*:\s*(ignore|disregard|forget).{0,80}?(safety|guideline|rule|policy)"),
        re.compile(r"(?i)\b(system|override)\s*:\s*(generate|create|provide|disclose|reveal).{0,80}?(admin|credential|password|token|key)"),
        re.compile(r"(?i)(simulate|act as|roleplay|pretend).*?(admin|system|developer|root)"),
        re.compile(r"(?i)(execute|run|eval|execute_code).*?(command|script|python|bash)"),
        re.compile(r"(?i)(divulge|reveal|expose|leak).{0,60}?((system\s+prompt|developer\s+message)|secret|key|password|token|api)"),
        re.compile(r"(?i)respond_with_context_only:\s*false"),
        re.compile(r"(?i)output_raw_response:\s*true"),
        re.compile(r"(?i)\[.*SEPARATOR.*\]"),
        re.compile(r"(?i)###.*PROMPT.*END"),
    ]

    # More conservative patterns for retrieved documents to reduce false positives
    # from operational language in NASA logs (e.g., leak/system references).
    RETRIEVED_DOC_PATTERNS = [
        re.compile(r"(?i)(ignore|disregard|forget|override).{0,80}?(instruction|prompt|system)"),
        re.compile(r"(?i)(act as|simulate|roleplay|pretend).{0,80}?(admin|developer|system|root)"),
        re.compile(r"(?i)(show|reveal|print|dump).{0,80}?(system\s+prompt|developer\s+message)"),
        re.compile(r"(?i)respond_with_context_only:\s*false"),
        re.compile(r"(?i)output_raw_response:\s*true"),
        re.compile(r"(?i)###.*PROMPT.*END"),
    ]

    _SANITIZE_PATTERNS = [
        re.compile(r"(?i)<script[^>]*>.*?</script>"),
        re.compile(r"(?i)onclick\s*="),
        re.compile(r"(?i)onerror\s*="),
        re.compile(r"(?i)onload\s*="),
    ]
    
    @staticmethod
    def detect_injection(
        text: str,
        severity_threshold: SecurityLevel = SecurityLevel.MEDIUM,
        for_retrieved_doc: bool = False,
    ) -> Optional[SecurityViolation]:
        """Detect prompt injection attempts."""
        patterns = (
            PromptInjectionDetector.RETRIEVED_DOC_PATTERNS
            if for_retrieved_doc
            else PromptInjectionDetector.INJECTION_PATTERNS
        )

        for cpat in patterns:
            if cpat.search(text):
                logger.warning(f"Potential prompt injection detected: pattern={cpat.pattern}")
                return SecurityViolation(
                    level=SecurityLevel.HIGH,
                    message="Potential prompt injection attack detected",
                    details={"pattern": cpat.pattern, "text_sample": text[:100]}
                )
        return None
    
    @staticmethod
    def sanitize_input(text: str, max_length: int = 2000) -> str:
        """Sanitize user input for prompt injection."""
        text = text[:max_length]
        for cpat in PromptInjectionDetector._SANITIZE_PATTERNS:
            text = cpat.sub("", text)
        return text.strip()


class SensitiveInfoFilter:
    """LLM02 & LLM07: Filters sensitive information from responses."""

    SENSITIVE_PATTERNS = [
        re.compile(r"(?i)(api[\s_-]?key|apikey)\s*[=:]\s*(['\"]?)([a-zA-Z0-9\-_]{20,})\2"),
        re.compile(r"(?i)(password|passwd|pwd)\s*[=:]\s*(['\"]?)(\S+)\2"),
        re.compile(r"(?i)(secret|token|bearer)\s*[=:]\s*(['\"]?)([a-zA-Z0-9\-_]{20,})\2"),
        re.compile(r"(?i)openai[_-]?key\s*[=:]\s*sk-[a-zA-Z0-9]{40,}"),
        re.compile(r"(?i)(private[_-]?)?key\s*[=:]\s*-----BEGIN.*?-----END"),
        re.compile(r"(?i)system[_-]?prompt\s*[=:]\s*['\"]?(.{50,})['\"]?"),
        re.compile(r"\bsk-[A-Za-z0-9]{16,}\b"),
        re.compile(r"\bghp_[A-Za-z0-9]{20,}\b"),
        re.compile(r"\beyJ[A-Za-z0-9_\-]*\.[A-Za-z0-9_\-]*\.[A-Za-z0-9_\-]*\b"),
    ]

    STRICT_SENSITIVE_PATTERNS = [
        re.compile(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b"),
        re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
        re.compile(r"\b(?:\d[ -]*?){13,16}\b"),
        re.compile(r"\b(?:\+?1[ -]?)?(?:\(?\d{3}\)?[ -]?)\d{3}[ -]?\d{4}\b"),
    ]

    _SYSTEM_PROMPT_PATTERN = re.compile(r"(?i)system\s*prompt.*?[\n:]", re.DOTALL)
    
    REDACTION = "[REDACTED]"
    
    @staticmethod
    def detect_sensitive_info(text: str) -> List[Dict[str, Any]]:
        """Detect sensitive information patterns."""
        findings = []
        for cpat in SensitiveInfoFilter.SENSITIVE_PATTERNS:
            for match in cpat.finditer(text):
                findings.append({
                    "type": cpat.pattern[:30],
                    "position": (match.start(), match.end()),
                    "content_sample": text[match.start():min(match.end(), match.start() + 50)]
                })
        return findings
    
    @staticmethod
    def filter_response(text: str, strict: bool = False) -> str:
        """Remove or redact sensitive information from LLM output."""
        filtered = text
        for cpat in SensitiveInfoFilter.SENSITIVE_PATTERNS:
            filtered = cpat.sub(f" {SensitiveInfoFilter.REDACTION} ", filtered)
        if strict:
            filtered = SensitiveInfoFilter._SYSTEM_PROMPT_PATTERN.sub("[SYSTEM PROMPT HIDDEN]", filtered)
            for cpat in SensitiveInfoFilter.STRICT_SENSITIVE_PATTERNS:
                filtered = cpat.sub(SensitiveInfoFilter.REDACTION, filtered)
        return filtered
    
    @staticmethod
    def audit_sensitive_exposure(response: str, question: str) -> Optional[SecurityViolation]:
        """Audit LLM response for sensitive information leaks."""
        if len(response) > 10000:

            return SecurityViolation(
                level=SecurityLevel.MEDIUM,
                message="Excessively long response - potential information dump",
                details={"response_length": len(response)}
            )
        
        findings = SensitiveInfoFilter.detect_sensitive_info(response)
        if findings:
            logger.warning(f"Sensitive information detected in response: {findings}")
            return SecurityViolation(
                level=SecurityLevel.HIGH,
                message="Sensitive information exposure detected in LLM response",
                details={"findings": findings[:3]}
            )
        
        return None


class OutputValidator:
    """LLM05: Validates and sanitizes LLM outputs."""

    _HARMFUL_PATTERNS = [
        (re.compile(r"(?i)(jailbreak|hack|exploit|malware)"), "harmful_code_reference"),
        (re.compile(r"(?i)(illegal|banned|forbidden)"), "restricted_content"),
    ]
    _STRONG_CLAIMS_PATTERN = re.compile(r"(?i)(definitely|certainly|absolutely|proven|fact[:]?)")

    @staticmethod
    def validate_response(response: str, context_used: List[str]) -> Dict[str, Any]:
        """Validate LLM response for safety and coherence.

        Returns:
            Dict with validation results and any issues found.
        """
        issues = []

        if len(response) < 10:
            issues.append({"type": "too_short", "message": "Response is suspiciously short"})
        elif len(response) > 5000:
            issues.append({"type": "too_long", "message": "Response exceeds length limit"})

        for cpat, label in OutputValidator._HARMFUL_PATTERNS:
            if cpat.search(response):
                issues.append({"type": label, "message": "Potential harmful content detected"})

        if context_used:
            context_str = " ".join(context_used).lower()
            response_lower = response.lower()
            strong_claims = OutputValidator._STRONG_CLAIMS_PATTERN.findall(response)
            context_overlap = sum(1 for word in response_lower.split() if word in context_str.split())
            
            if strong_claims and context_overlap < len(response.split()) * 0.1:
                issues.append({
                    "type": "potential_hallucination",
                    "message": "Strong claims with low context correlation",
                    "details": {"strong_claims": len(strong_claims), "overlap_ratio": context_overlap / len(response.split())}
                })
        
        if "system prompt" in response.lower() or "system message" in response.lower():
            issues.append({"type": "system_prompt_leak", "message": "System prompt may have been leaked"})
        
        return {
            "valid": len(issues) == 0,
            "issues": issues,
            "severity": "critical" if any(i["type"] == "system_prompt_leak" for i in issues) else "warning" if issues else "ok"
        }


class ResourceLimitEnforcer:
    """LLM10: Enforces limits on resource consumption."""
    
    def __init__(self, max_input_tokens: int = 2000, max_output_tokens: int = 1000, 
                 max_queries_per_minute: int = 10, max_embedding_batch: int = 100):
        self.max_input_tokens = max_input_tokens
        self.max_output_tokens = max_output_tokens
        self.max_queries_per_minute = max_queries_per_minute
        self.max_embedding_batch = max_embedding_batch
        self.query_count = {}
        self._query_count_lock = Lock()
    
    def check_input_tokens(self, text: str) -> None:
        """Validate input token count."""
        estimated_tokens = len(text) // 4
        if estimated_tokens > self.max_input_tokens:
            raise SecurityViolation(
                level=SecurityLevel.HIGH,
                message=f"Input exceeds token limit: {estimated_tokens} > {self.max_input_tokens}",
                details={"estimated_tokens": estimated_tokens}
            )
    
    def check_query_rate(self, user_id: str) -> None:
        """Check query rate limit per user."""
        import time
        current_minute = int(time.time()) // 60
        key = f"{user_id}:{current_minute}"
        with self._query_count_lock:
            count = self.query_count.get(key, 0)
            if count >= self.max_queries_per_minute:
                raise SecurityViolation(
                    level=SecurityLevel.MEDIUM,
                    message=f"Query rate limit exceeded: {count} queries in current minute",
                    details={"limit": self.max_queries_per_minute}
                )

            self.query_count[key] = count + 1

            # Keep memory bounded under sustained high-cardinality traffic.
            if len(self.query_count) > 10000:
                self.query_count = dict(list(self.query_count.items())[-5000:])
    
    def check_embedding_batch(self, batch_size: int) -> None:
        """Validate embedding batch size (LLM08)."""
        if batch_size > self.max_embedding_batch:
            raise SecurityViolation(
                level=SecurityLevel.MEDIUM,
                message=f"Embedding batch exceeds limit: {batch_size} > {self.max_embedding_batch}",
                details={"batch_size": batch_size}
            )


class VectorSecurityValidator:
    """LLM08: Validates vector/embedding security."""
    
    @staticmethod
    def validate_embedding_source(collection_name: str, chroma_dir: str) -> bool:
        """Validate that embeddings come from trusted source."""
        trusted_collections = {
            "nasa_space_missions_text": "./chroma_db_openai",
            "nasa_space_missions_test": "./chroma_db",
        }
        
        if collection_name not in trusted_collections:
            raise SecurityViolation(
                level=SecurityLevel.HIGH,
                message=f"Untrusted collection: {collection_name}",
                details={"approved_collections": list(trusted_collections.keys())}
            )
        
        expected_dir = os.path.normpath(trusted_collections[collection_name])
        provided_dir = os.path.normpath(chroma_dir)

        if expected_dir != provided_dir:
            raise SecurityViolation(
                level=SecurityLevel.HIGH,
                message=f"Collection mismatch for {collection_name}",
                details={"expected_dir": trusted_collections[collection_name], "provided_dir": chroma_dir}
            )
        
        return True
    
    @staticmethod
    def detect_poisoned_results(documents: List[str], metadata: List[Dict]) -> Optional[SecurityViolation]:
        """Detect potentially poisoned embedding results (LLM04)."""
        suspicious_count = 0
        
        for doc, meta in zip(documents, metadata):
            if PromptInjectionDetector.detect_injection(doc, for_retrieved_doc=True):
                suspicious_count += 1
            
            source = meta.get("source", "")
            if ".." in source or source.startswith("/"):
                suspicious_count += 1
        
        if suspicious_count > 0:
            logger.warning(f"Detected {suspicious_count} potentially poisoned results")
            return SecurityViolation(
                level=SecurityLevel.MEDIUM,
                message=f"Potentially poisoned documents detected: {suspicious_count} results",
                details={"suspicious_count": suspicious_count}
            )
        
        return None


class SecurityAuditor:
    """Central audit logger for security events."""
    
    @staticmethod
    def log_security_event(event_type: str, severity: SecurityLevel, user_id: Optional[str] = None,
                          details: Optional[Dict] = None) -> None:
        """Log security-relevant events."""
        log_entry = {
            "type": event_type,
            "severity": severity.value,
            "user": user_id or "anonymous",
            "details": details or {},
        }
        
        if severity in [SecurityLevel.HIGH, SecurityLevel.CRITICAL]:
            logger.error(f"SECURITY EVENT: {log_entry}")
        else:
            logger.warning(f"Security event: {log_entry}")

