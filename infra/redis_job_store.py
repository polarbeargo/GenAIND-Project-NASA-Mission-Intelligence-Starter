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
        """Store job result and mark as completed."""
        if not self.redis.is_available():
            return False

        try:
            # Update job metadata
            job_key = self._job_key(job_id)
            job_data = self.redis.get(job_key) or {}
            job_data["status"] = "completed"
            job_data["completed_at"] = time.time()
            self.redis.set(job_key, job_data, ex=self.retention_ttl)

            # Store result
            result_key = self._result_key(job_id)
            self.redis.set(result_key, result, ex=self.retention_ttl)

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
        return job.get("status", "pending")

    def delete_job(self, job_id: str) -> bool:
        """Delete job and result (cleanup)."""
        if not self.redis.is_available():
            return False

        try:
            self.redis.delete(self._job_key(job_id), self._result_key(job_id))
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
