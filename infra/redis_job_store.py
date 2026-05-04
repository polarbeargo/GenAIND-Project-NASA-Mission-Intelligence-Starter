"""Redis-backed async job storage for judge and evaluation results."""

import json
import logging
import time
from typing import Any, Dict, Optional

from infra.redis_client import RedisClient

logger = logging.getLogger(__name__)


class RedisAsyncJobStore:
    """
    Store async job state (judge, evaluation) in Redis.
    
    Allows any pod to:
    - Submit a job (store result when ready)
    - Query job status by ID
    - Retrieve without waiting
    """

    def __init__(self, redis_client: RedisClient, retention_ttl_seconds: int = 3600):
        self.redis = redis_client
        self.retention_ttl = retention_ttl_seconds

    def _job_key(self, job_id: str) -> str:
        """Get Redis key for job metadata."""
        return f"job:{job_id}"

    def _result_key(self, job_id: str) -> str:
        """Get Redis key for job result."""
        return f"job:result:{job_id}"

    def _processing_key(self, job_id: str) -> str:
        """Get Redis key for in-flight idempotency lock."""
        return f"job:processing:{job_id}"

    def _completed_key(self, job_id: str) -> str:
        """Get Redis key for completed-idempotency marker."""
        return f"job:completed:{job_id}"

    @staticmethod
    def _is_terminal_status(status: str) -> bool:
        return status in {"completed", "error", "dead_lettered", "poisoned", "skipped"}

    def acquire_processing(self, job_id: str, processing_ttl_seconds: int = 300) -> bool:
        """Acquire a best-effort distributed idempotency lock for one job."""
        if not self.redis.is_available() or self.redis._client is None:
            return False

        try:
            ttl = max(30, int(processing_ttl_seconds))
            # NX avoids duplicate concurrent processing across workers.
            locked = self.redis._client.set(
                self._processing_key(job_id),
                str(time.time()),
                nx=True,
                ex=ttl,
            )
            return bool(locked)
        except Exception as error:
            logger.warning("Failed to acquire processing lock for job %s: %s", job_id, error)
            return False

    def release_processing(self, job_id: str) -> bool:
        """Release the in-flight idempotency lock for one job."""
        if not self.redis.is_available():
            return False

        try:
            self.redis.delete(self._processing_key(job_id))
            return True
        except Exception as error:
            logger.warning("Failed to release processing lock for job %s: %s", job_id, error)
            return False

    def is_completed(self, job_id: str) -> bool:
        """Check terminal completion marker to deduplicate retried deliveries."""
        if not self.redis.is_available():
            return False

        try:
            if self.redis.exists(self._completed_key(job_id)):
                return True
            result = self.get_result(job_id)
            if not isinstance(result, dict):
                return False
            status = str(result.get("status", "")).strip().lower()
            return self._is_terminal_status(status)
        except Exception as error:
            logger.debug("Failed to check completion for job %s: %s", job_id, error)
            return False

    def create_job(
        self, job_id: str, job_type: str, request_id: str
    ) -> bool:
        """Create a new async job entry."""
        if not self.redis.is_available():
            return False

        try:
            key = self._job_key(job_id)
            job_data = {
                "id": job_id,
                "type": job_type,
                "request_id": request_id,
                "created_at": time.time(),
                "status": "pending",
                "completed_at": None,
            }
            self.redis.set(key, job_data, ex=self.retention_ttl)
            logger.debug(f"Created async job: {job_id} ({job_type})")
            return True
        except Exception as error:
            logger.warning(f"Failed to create job {job_id}: {error}")
            return False

    def set_result(self, job_id: str, result: Dict[str, Any]) -> bool:
        """Store job state/result and update terminal markers."""
        if not self.redis.is_available():
            return False

        try:
            status = str(result.get("status", "completed")).strip().lower() or "completed"

            # Update job metadata
            job_key = self._job_key(job_id)
            job_data = self.redis.get(job_key) or {}
            if not isinstance(job_data, dict):
                job_data = {}
            job_data.setdefault("id", job_id)
            job_data.setdefault("created_at", time.time())
            job_data["status"] = status
            job_data["completed_at"] = time.time() if self._is_terminal_status(status) else None
            self.redis.set(job_key, job_data, ex=self.retention_ttl)

            # Store result
            result_key = self._result_key(job_id)
            self.redis.set(result_key, result, ex=self.retention_ttl)

            # Terminal marker supports idempotent redelivery suppression.
            completed_key = self._completed_key(job_id)
            if self._is_terminal_status(status):
                self.redis.set(completed_key, {"status": status}, ex=self.retention_ttl)
            else:
                self.redis.delete(completed_key)

            # Processing lock should not outlive terminal outcomes.
            if self._is_terminal_status(status):
                self.release_processing(job_id)

            logger.debug(f"Completed async job: {job_id}")
            return True
        except Exception as error:
            logger.warning(f"Failed to set result for job {job_id}: {error}")
            return False

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job metadata."""
        if not self.redis.is_available():
            return None

        try:
            key = self._job_key(job_id)
            return self.redis.get(key)
        except Exception as error:
            logger.debug(f"Failed to get job {job_id}: {error}")
            return None

    def get_result(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job result (only if completed)."""
        if not self.redis.is_available():
            return None

        try:
            key = self._result_key(job_id)
            return self.redis.get(key)
        except Exception as error:
            logger.debug(f"Failed to get result for job {job_id}: {error}")
            return None

    def get_status(self, job_id: str) -> str:
        """Get job status: 'pending', 'completed', or 'not_found'."""
        if not self.redis.is_available():
            return "pending"  # Assume pending if Redis unavailable

        job = self.get_job(job_id)
        if job is None:
            return "not_found"
        return str(job.get("status", "pending"))

    def delete_job(self, job_id: str) -> bool:
        """Delete job and result (cleanup)."""
        if not self.redis.is_available():
            return False

        try:
            self.redis.delete(
                self._job_key(job_id),
                self._result_key(job_id),
                self._processing_key(job_id),
                self._completed_key(job_id),
            )
            logger.debug(f"Deleted async job: {job_id}")
            return True
        except Exception as error:
            logger.warning(f"Failed to delete job {job_id}: {error}")
            return False

    def list_recent_jobs(self, limit: int = 100) -> list[Dict[str, Any]]:
        """List recent jobs (note: requires scanning Redis, use carefully)."""
        if not self.redis.is_available():
            return []

        try:
            # This is a simplified version; in production, mantain a separate list
            jobs = []
            if self.redis._client:
                for key in self.redis._client.keys("job:?*"):
                    if not key.startswith("job:result:"):
                        job = self.redis.get(key)
                        if job:
                            jobs.append(job)
            return sorted(jobs, key=lambda j: j.get("created_at", 0), reverse=True)[
                :limit
            ]
        except Exception as error:
            logger.debug(f"Failed to list jobs: {error}")
            return []

    def stats(self) -> Dict[str, Any]:
        """Return job store statistics."""
        return {
            "available": self.redis.is_available(),
            "retention_ttl_seconds": self.retention_ttl,
        }
