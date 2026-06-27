"""Lightweight Evidently integration for response quality and RAG monitoring."""

from __future__ import annotations

import atexit
import json
import logging
import math
import os
import queue
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, Sequence, Tuple
from urllib.parse import quote, quote_plus, urlparse
from uuid import uuid4

import pandas as pd

try:
    import polars as pl
    POLARS_AVAILABLE = True
except Exception:
    pl = None
    POLARS_AVAILABLE = False

try:
    import psycopg
    from psycopg import sql
    PSYCOPG_AVAILABLE = True
except Exception:
    psycopg = None
    sql = None
    PSYCOPG_AVAILABLE = False

try:
    import boto3
    BOTO3_AVAILABLE = True
except Exception:
    boto3 = None
    BOTO3_AVAILABLE = False

try:
    from azure.storage.blob import BlobServiceClient
    AZURE_BLOB_AVAILABLE = True
except Exception:
    BlobServiceClient = None
    AZURE_BLOB_AVAILABLE = False

try:
    from opentelemetry.sdk.resources import Resource as OtelResource
    from opentelemetry.sdk._logs import LoggerProvider as OtelLoggerProvider
    from opentelemetry.sdk._logs import LoggingHandler as OtelLoggingHandler
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
    from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
    OTEL_LOGS_AVAILABLE = True
except Exception:
    OtelResource = None
    OtelLoggerProvider = None
    OtelLoggingHandler = None
    BatchLogRecordProcessor = None
    OTLPLogExporter = None
    OTEL_LOGS_AVAILABLE = False


EVIDENTLY_AVAILABLE = False
Report = None
DataDriftPreset = None

try:
    from evidently.report import Report
    from evidently.metric_preset import DataDriftPreset
    EVIDENTLY_AVAILABLE = True
except Exception:
    try:
        from evidently import Report
        from evidently.presets import DataDriftPreset
        EVIDENTLY_AVAILABLE = True
    except Exception:
        EVIDENTLY_AVAILABLE = False


def _load_dataframe_from_ndjson(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()

    if POLARS_AVAILABLE and pl is not None:
        try:
            dataset = pl.read_ndjson(str(path), ignore_errors=True)
            if dataset.is_empty():
                return pd.DataFrame()
            return dataset.to_pandas()
        except Exception:
            pass

    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def _extract_numeric_metrics(record: Dict[str, Any]) -> Dict[str, float]:
    metrics: Dict[str, float] = {}
    for key, value in record.items():
        if key.startswith("metric_") and isinstance(value, (int, float)):
            metrics[key] = float(value)
    return metrics


def _compute_retrieval_quality(record: Dict[str, Any]) -> Optional[float]:
    score_keys = [
        "metric_faithfulness",
        "metric_response_relevancy",
        "metric_context_precision",
    ]
    scores: List[float] = []
    for key in score_keys:
        value = record.get(key)
        if isinstance(value, (int, float)):
            scores.append(float(value))
    if not scores:
        return None
    return sum(scores) / len(scores)


def _parse_csv_env(name: str) -> List[str]:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return []
    return [item.strip().lower() for item in raw.split(",") if item.strip()]


class PrimaryInteractionSink(Protocol):
    sink_type: str

    def persist_batch(self, records: Sequence[Dict[str, Any]]) -> None:
        ...

    def load_dataframe(self) -> pd.DataFrame:
        ...

    def get_signature(self) -> Optional[Tuple[Any, ...]]:
        ...

    def describe(self) -> Dict[str, str]:
        ...

    def shutdown(self) -> None:
        ...

    def native_ndjson_path(self) -> Optional[Path]:
        ...


class MirrorInteractionSink(Protocol):
    sink_type: str

    def persist_batch(self, records: Sequence[Dict[str, Any]]) -> None:
        ...

    def describe(self) -> Dict[str, str]:
        ...

    def shutdown(self) -> None:
        ...


class FileInteractionSink:
    sink_type = "file"

    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def persist_batch(self, records: Sequence[Dict[str, Any]]) -> None:
        if not records:
            return
        lines = [json.dumps(record, ensure_ascii=True) + "\n" for record in records]
        with self.path.open("a", encoding="utf-8") as handle:
            handle.writelines(lines)

    def load_dataframe(self) -> pd.DataFrame:
        return _load_dataframe_from_ndjson(self.path)

    def get_signature(self) -> Optional[Tuple[Any, ...]]:
        if not self.path.exists():
            return None
        stat = self.path.stat()
        return (stat.st_mtime_ns, stat.st_size)

    def describe(self) -> Dict[str, str]:
        return {
            "type": self.sink_type,
            "target": str(self.path),
        }

    def shutdown(self) -> None:
        return None

    def native_ndjson_path(self) -> Optional[Path]:
        return self.path


class PostgresInteractionSink:
    sink_type = "postgres"

    def __init__(self, dsn: str, table_name: str = "monitoring_interactions"):
        if not PSYCOPG_AVAILABLE or psycopg is None or sql is None:
            raise RuntimeError(
                "Postgres monitoring sink selected but psycopg is not installed. "
                "Install the monitoring-postgres dependency group."
            )

        self.dsn = dsn
        self.table_name = table_name
        self._latest_table_name = f"{table_name}_latest"
        self._agg_overall_table_name = f"{table_name}_agg_overall"
        self._agg_backend_table_name = f"{table_name}_agg_backend"
        self._agg_model_table_name = f"{table_name}_agg_model"
        self._incremental_aggregates_enabled = (os.getenv("MONITORING_POSTGRES_INCREMENTAL_AGGREGATES") or "false").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self._p95_refresh_seconds = self._as_float(
            os.getenv("MONITORING_POSTGRES_P95_REFRESH_SECONDS"),
            5.0,
        )
        if self._p95_refresh_seconds < 0.5:
            self._p95_refresh_seconds = 0.5
        self._p95_cache_lock = threading.Lock()
        self._p95_latency_cached_ms = 0.0
        self._p95_cache_refreshed_at_monotonic = 0.0
        self._setup_lock = threading.Lock()
        self._setup_complete = False

    def _connect(self):
        return psycopg.connect(self.dsn)

    @staticmethod
    def _as_float(value: Any, default: float = 0.0) -> float:
        try:
            parsed = float(value)
            if math.isnan(parsed):
                return default
            return parsed
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _as_optional_float(value: Any) -> Optional[float]:
        try:
            parsed = float(value)
            if math.isnan(parsed):
                return None
            return parsed
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _row_retrieval_quality(
        faithfulness: Optional[float],
        response_relevancy: Optional[float],
        context_precision: Optional[float],
    ) -> Optional[float]:
        values = [value for value in [faithfulness, response_relevancy, context_precision] if value is not None]
        if not values:
            return None
        return float(sum(values) / len(values))

    @staticmethod
    def _row_contributions(row: Optional[Dict[str, Any]]) -> Dict[str, float]:
        if row is None:
            return {
                "total_requests": 0.0,
                "total_errors": 0.0,
                "latency_sum": 0.0,
                "latency_count": 0.0,
                "rag_scored_requests": 0.0,
                "rag_faithfulness_sum": 0.0,
                "rag_faithfulness_count": 0.0,
                "rag_response_relevancy_sum": 0.0,
                "rag_response_relevancy_count": 0.0,
                "rag_context_precision_sum": 0.0,
                "rag_context_precision_count": 0.0,
                "rag_retrieval_quality_sum": 0.0,
                "rag_retrieval_quality_count": 0.0,
            }

        faithfulness = PostgresInteractionSink._as_optional_float(row.get("metric_faithfulness"))
        response_relevancy = PostgresInteractionSink._as_optional_float(row.get("metric_response_relevancy"))
        context_precision = PostgresInteractionSink._as_optional_float(row.get("metric_context_precision"))
        retrieval_quality = PostgresInteractionSink._as_optional_float(row.get("retrieval_quality"))
        if retrieval_quality is None:
            retrieval_quality = PostgresInteractionSink._row_retrieval_quality(
                faithfulness,
                response_relevancy,
                context_precision,
            )

        latency_ms = PostgresInteractionSink._as_optional_float(row.get("latency_ms"))
        rag_scored = any(value is not None for value in [faithfulness, response_relevancy, context_precision])

        return {
            "total_requests": 1.0,
            "total_errors": PostgresInteractionSink._as_float(row.get("is_error"), 0.0),
            "latency_sum": float(latency_ms or 0.0),
            "latency_count": 1.0 if latency_ms is not None else 0.0,
            "rag_scored_requests": 1.0 if rag_scored else 0.0,
            "rag_faithfulness_sum": float(faithfulness or 0.0),
            "rag_faithfulness_count": 1.0 if faithfulness is not None else 0.0,
            "rag_response_relevancy_sum": float(response_relevancy or 0.0),
            "rag_response_relevancy_count": 1.0 if response_relevancy is not None else 0.0,
            "rag_context_precision_sum": float(context_precision or 0.0),
            "rag_context_precision_count": 1.0 if context_precision is not None else 0.0,
            "rag_retrieval_quality_sum": float(retrieval_quality or 0.0),
            "rag_retrieval_quality_count": 1.0 if retrieval_quality is not None else 0.0,
        }

    @staticmethod
    def _contribution_delta(new_row: Optional[Dict[str, Any]], old_row: Optional[Dict[str, Any]]) -> Dict[str, float]:
        new_values = PostgresInteractionSink._row_contributions(new_row)
        old_values = PostgresInteractionSink._row_contributions(old_row)
        return {key: float(new_values.get(key, 0.0) - old_values.get(key, 0.0)) for key in new_values.keys()}

    def _ensure_table(self) -> None:
        if self._setup_complete:
            return

        with self._setup_lock:
            if self._setup_complete:
                return

            table_identifier = sql.Identifier(self.table_name)
            latest_table_identifier = sql.Identifier(self._latest_table_name)
            agg_overall_table_identifier = sql.Identifier(self._agg_overall_table_name)
            agg_backend_table_identifier = sql.Identifier(self._agg_backend_table_name)
            agg_model_table_identifier = sql.Identifier(self._agg_model_table_name)
            safe_prefix = "".join(character if character.isalnum() else "_" for character in self.table_name)
            with self._connect() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        sql.SQL(
                            """
                            CREATE TABLE IF NOT EXISTS {} (
                                id BIGSERIAL PRIMARY KEY,
                                recorded_at TIMESTAMPTZ NOT NULL,
                                question TEXT NOT NULL,
                                answer TEXT NOT NULL,
                                model TEXT NOT NULL,
                                backend TEXT NOT NULL,
                                mission TEXT,
                                context_count INTEGER NOT NULL,
                                answer_length INTEGER NOT NULL,
                                question_length INTEGER NOT NULL,
                                is_error DOUBLE PRECISION NOT NULL,
                                latency_ms DOUBLE PRECISION,
                                retrieval_quality DOUBLE PRECISION,
                                metric_values JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                                raw_record JSONB NOT NULL,
                                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                            )
                            """
                        ).format(table_identifier)
                    )
                    index_specs = [
                        (f"{safe_prefix}_recorded_at_idx", "recorded_at"),
                        (f"{safe_prefix}_backend_idx", "backend"),
                        (f"{safe_prefix}_mission_idx", "mission"),
                        (f"{safe_prefix}_model_idx", "model"),
                    ]
                    for index_name, column_name in index_specs:
                        cursor.execute(
                            sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                                sql.Identifier(index_name),
                                table_identifier,
                                sql.Identifier(column_name),
                            )
                        )

                    if self._incremental_aggregates_enabled:
                        cursor.execute(
                            sql.SQL(
                                """
                                CREATE TABLE IF NOT EXISTS {} (
                                    interaction_id TEXT PRIMARY KEY,
                                    recorded_at TIMESTAMPTZ,
                                    question TEXT,
                                    answer TEXT,
                                    model TEXT NOT NULL,
                                    backend TEXT NOT NULL,
                                    mission TEXT,
                                    context_count INTEGER NOT NULL,
                                    answer_length INTEGER NOT NULL,
                                    question_length INTEGER NOT NULL,
                                    is_error DOUBLE PRECISION NOT NULL,
                                    latency_ms DOUBLE PRECISION,
                                    metric_faithfulness DOUBLE PRECISION,
                                    metric_response_relevancy DOUBLE PRECISION,
                                    metric_context_precision DOUBLE PRECISION,
                                    retrieval_quality DOUBLE PRECISION,
                                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                                )
                                """
                            ).format(latest_table_identifier)
                        )
                        cursor.execute(
                            sql.SQL(
                                """
                                CREATE TABLE IF NOT EXISTS {} (
                                    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE,
                                    total_requests DOUBLE PRECISION NOT NULL DEFAULT 0,
                                    total_errors DOUBLE PRECISION NOT NULL DEFAULT 0,
                                    latency_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
                                    latency_count DOUBLE PRECISION NOT NULL DEFAULT 0,
                                    rag_scored_requests DOUBLE PRECISION NOT NULL DEFAULT 0,
                                    rag_faithfulness_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
                                    rag_faithfulness_count DOUBLE PRECISION NOT NULL DEFAULT 0,
                                    rag_response_relevancy_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
                                    rag_response_relevancy_count DOUBLE PRECISION NOT NULL DEFAULT 0,
                                    rag_context_precision_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
                                    rag_context_precision_count DOUBLE PRECISION NOT NULL DEFAULT 0,
                                    rag_retrieval_quality_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
                                    rag_retrieval_quality_count DOUBLE PRECISION NOT NULL DEFAULT 0,
                                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                                )
                                """
                            ).format(agg_overall_table_identifier)
                        )
                        cursor.execute(
                            sql.SQL(
                                """
                                INSERT INTO {} (singleton)
                                VALUES (TRUE)
                                ON CONFLICT (singleton) DO NOTHING
                                """
                            ).format(agg_overall_table_identifier)
                        )
                        cursor.execute(
                            sql.SQL(
                                """
                                CREATE TABLE IF NOT EXISTS {} (
                                    backend TEXT PRIMARY KEY,
                                    total_requests DOUBLE PRECISION NOT NULL DEFAULT 0,
                                    total_errors DOUBLE PRECISION NOT NULL DEFAULT 0,
                                    latency_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
                                    latency_count DOUBLE PRECISION NOT NULL DEFAULT 0,
                                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                                )
                                """
                            ).format(agg_backend_table_identifier)
                        )
                        cursor.execute(
                            sql.SQL(
                                """
                                CREATE TABLE IF NOT EXISTS {} (
                                    model TEXT PRIMARY KEY,
                                    total_requests DOUBLE PRECISION NOT NULL DEFAULT 0,
                                    total_errors DOUBLE PRECISION NOT NULL DEFAULT 0,
                                    latency_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
                                    latency_count DOUBLE PRECISION NOT NULL DEFAULT 0,
                                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                                )
                                """
                            ).format(agg_model_table_identifier)
                        )
            self._setup_complete = True

    def _record_to_latest_projection(self, record: Dict[str, Any]) -> Dict[str, Any]:
        interaction_id = str(record.get("interaction_id") or "").strip() or f"legacy-{uuid4().hex}"
        metric_faithfulness = self._as_optional_float(record.get("metric_faithfulness"))
        metric_response_relevancy = self._as_optional_float(record.get("metric_response_relevancy"))
        metric_context_precision = self._as_optional_float(record.get("metric_context_precision"))
        retrieval_quality = self._as_optional_float(record.get("retrieval_quality"))
        if retrieval_quality is None:
            retrieval_quality = self._row_retrieval_quality(
                metric_faithfulness,
                metric_response_relevancy,
                metric_context_precision,
            )

        return {
            "interaction_id": interaction_id,
            "recorded_at": str(record.get("timestamp") or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")),
            "question": str(record.get("question") or ""),
            "answer": str(record.get("answer") or ""),
            "model": str(record.get("model") or "unknown"),
            "backend": str(record.get("backend") or "unknown"),
            "mission": str(record.get("mission") or "all"),
            "context_count": int(record.get("context_count") or 0),
            "answer_length": int(record.get("answer_length") or len(str(record.get("answer") or ""))),
            "question_length": int(record.get("question_length") or len(str(record.get("question") or ""))),
            "is_error": self._as_float(record.get("is_error"), 0.0),
            "latency_ms": self._as_optional_float(record.get("latency_ms")),
            "metric_faithfulness": metric_faithfulness,
            "metric_response_relevancy": metric_response_relevancy,
            "metric_context_precision": metric_context_precision,
            "retrieval_quality": retrieval_quality,
        }

    def _merge_latest_projection(self, previous: Optional[Dict[str, Any]], incoming: Dict[str, Any]) -> Dict[str, Any]:
        if previous is None:
            return dict(incoming)

        merged = dict(previous)
        for key, value in incoming.items():
            if value is None:
                continue
            if isinstance(value, str) and value == "":
                continue
            merged[key] = value

        merged["retrieval_quality"] = self._row_retrieval_quality(
            self._as_optional_float(merged.get("metric_faithfulness")),
            self._as_optional_float(merged.get("metric_response_relevancy")),
            self._as_optional_float(merged.get("metric_context_precision")),
        )
        return merged

    def _fetch_latest_row(self, cursor: Any, interaction_id: str) -> Optional[Dict[str, Any]]:
        latest_table_identifier = sql.Identifier(self._latest_table_name)
        query = sql.SQL(
            """
            SELECT
                interaction_id,
                recorded_at,
                question,
                answer,
                model,
                backend,
                mission,
                context_count,
                answer_length,
                question_length,
                is_error,
                latency_ms,
                metric_faithfulness,
                metric_response_relevancy,
                metric_context_precision,
                retrieval_quality
            FROM {}
            WHERE interaction_id = %s
            FOR UPDATE
            """
        ).format(latest_table_identifier)
        cursor.execute(query, (interaction_id,))
        row = cursor.fetchone()
        if row is None:
            return None
        keys = [
            "interaction_id",
            "recorded_at",
            "question",
            "answer",
            "model",
            "backend",
            "mission",
            "context_count",
            "answer_length",
            "question_length",
            "is_error",
            "latency_ms",
            "metric_faithfulness",
            "metric_response_relevancy",
            "metric_context_precision",
            "retrieval_quality",
        ]
        return {key: row[idx] for idx, key in enumerate(keys)}

    def _upsert_latest_row(self, cursor: Any, merged: Dict[str, Any]) -> None:
        latest_table_identifier = sql.Identifier(self._latest_table_name)
        cursor.execute(
            sql.SQL(
                """
                INSERT INTO {} (
                    interaction_id,
                    recorded_at,
                    question,
                    answer,
                    model,
                    backend,
                    mission,
                    context_count,
                    answer_length,
                    question_length,
                    is_error,
                    latency_ms,
                    metric_faithfulness,
                    metric_response_relevancy,
                    metric_context_precision,
                    retrieval_quality,
                    updated_at
                ) VALUES (
                    %s,
                    %s::timestamptz,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    now()
                )
                ON CONFLICT (interaction_id) DO UPDATE SET
                    recorded_at = EXCLUDED.recorded_at,
                    question = EXCLUDED.question,
                    answer = EXCLUDED.answer,
                    model = EXCLUDED.model,
                    backend = EXCLUDED.backend,
                    mission = EXCLUDED.mission,
                    context_count = EXCLUDED.context_count,
                    answer_length = EXCLUDED.answer_length,
                    question_length = EXCLUDED.question_length,
                    is_error = EXCLUDED.is_error,
                    latency_ms = EXCLUDED.latency_ms,
                    metric_faithfulness = EXCLUDED.metric_faithfulness,
                    metric_response_relevancy = EXCLUDED.metric_response_relevancy,
                    metric_context_precision = EXCLUDED.metric_context_precision,
                    retrieval_quality = EXCLUDED.retrieval_quality,
                    updated_at = now()
                """
            ).format(latest_table_identifier),
            (
                merged.get("interaction_id"),
                merged.get("recorded_at"),
                merged.get("question"),
                merged.get("answer"),
                merged.get("model"),
                merged.get("backend"),
                merged.get("mission"),
                int(merged.get("context_count") or 0),
                int(merged.get("answer_length") or 0),
                int(merged.get("question_length") or 0),
                self._as_float(merged.get("is_error"), 0.0),
                self._as_optional_float(merged.get("latency_ms")),
                self._as_optional_float(merged.get("metric_faithfulness")),
                self._as_optional_float(merged.get("metric_response_relevancy")),
                self._as_optional_float(merged.get("metric_context_precision")),
                self._as_optional_float(merged.get("retrieval_quality")),
            ),
        )

    def _apply_overall_delta(self, cursor: Any, delta: Dict[str, float]) -> None:
        agg_overall_table_identifier = sql.Identifier(self._agg_overall_table_name)
        cursor.execute(
            sql.SQL(
                """
                UPDATE {}
                SET
                    total_requests = total_requests + %s,
                    total_errors = total_errors + %s,
                    latency_sum = latency_sum + %s,
                    latency_count = latency_count + %s,
                    rag_scored_requests = rag_scored_requests + %s,
                    rag_faithfulness_sum = rag_faithfulness_sum + %s,
                    rag_faithfulness_count = rag_faithfulness_count + %s,
                    rag_response_relevancy_sum = rag_response_relevancy_sum + %s,
                    rag_response_relevancy_count = rag_response_relevancy_count + %s,
                    rag_context_precision_sum = rag_context_precision_sum + %s,
                    rag_context_precision_count = rag_context_precision_count + %s,
                    rag_retrieval_quality_sum = rag_retrieval_quality_sum + %s,
                    rag_retrieval_quality_count = rag_retrieval_quality_count + %s,
                    updated_at = now()
                WHERE singleton = TRUE
                """
            ).format(agg_overall_table_identifier),
            (
                delta["total_requests"],
                delta["total_errors"],
                delta["latency_sum"],
                delta["latency_count"],
                delta["rag_scored_requests"],
                delta["rag_faithfulness_sum"],
                delta["rag_faithfulness_count"],
                delta["rag_response_relevancy_sum"],
                delta["rag_response_relevancy_count"],
                delta["rag_context_precision_sum"],
                delta["rag_context_precision_count"],
                delta["rag_retrieval_quality_sum"],
                delta["rag_retrieval_quality_count"],
            ),
        )

    def _apply_dimensional_delta(self, cursor: Any, table_name: str, dim_name: str, dim_value: str, delta: Dict[str, float]) -> None:
        table_identifier = sql.Identifier(table_name)
        dim_identifier = sql.Identifier(dim_name)
        value = str(dim_value or "unknown")
        cursor.execute(
            sql.SQL(
                """
                INSERT INTO {} ({}, total_requests, total_errors, latency_sum, latency_count, updated_at)
                VALUES (%s, %s, %s, %s, %s, now())
                ON CONFLICT ({}) DO UPDATE SET
                    total_requests = {}.total_requests + EXCLUDED.total_requests,
                    total_errors = {}.total_errors + EXCLUDED.total_errors,
                    latency_sum = {}.latency_sum + EXCLUDED.latency_sum,
                    latency_count = {}.latency_count + EXCLUDED.latency_count,
                    updated_at = now()
                """
            ).format(
                table_identifier,
                dim_identifier,
                dim_identifier,
                table_identifier,
                table_identifier,
                table_identifier,
                table_identifier,
            ),
            (
                value,
                delta["total_requests"],
                delta["total_errors"],
                delta["latency_sum"],
                delta["latency_count"],
            ),
        )

    def _apply_incremental_aggregates_for_record(self, cursor: Any, record: Dict[str, Any]) -> None:
        incoming = self._record_to_latest_projection(record)
        interaction_id = str(incoming.get("interaction_id") or "")
        previous = self._fetch_latest_row(cursor, interaction_id)
        merged = self._merge_latest_projection(previous, incoming)
        self._upsert_latest_row(cursor, merged)

        delta = self._contribution_delta(merged, previous)
        self._apply_overall_delta(cursor, delta)

        merged_backend = str(merged.get("backend") or "unknown")
        previous_backend = str((previous or {}).get("backend") or "unknown")
        merged_model = str(merged.get("model") or "unknown")
        previous_model = str((previous or {}).get("model") or "unknown")

        if previous is None:
            self._apply_dimensional_delta(cursor, self._agg_backend_table_name, "backend", merged_backend, delta)
            self._apply_dimensional_delta(cursor, self._agg_model_table_name, "model", merged_model, delta)
            return

        if previous_backend == merged_backend:
            self._apply_dimensional_delta(cursor, self._agg_backend_table_name, "backend", merged_backend, delta)
        else:
            old_backend_contrib = self._row_contributions(previous)
            new_backend_contrib = self._row_contributions(merged)
            self._apply_dimensional_delta(
                cursor,
                self._agg_backend_table_name,
                "backend",
                previous_backend,
                {key: -value for key, value in old_backend_contrib.items()},
            )
            self._apply_dimensional_delta(cursor, self._agg_backend_table_name, "backend", merged_backend, new_backend_contrib)

        if previous_model == merged_model:
            self._apply_dimensional_delta(cursor, self._agg_model_table_name, "model", merged_model, delta)
        else:
            old_model_contrib = self._row_contributions(previous)
            new_model_contrib = self._row_contributions(merged)
            self._apply_dimensional_delta(
                cursor,
                self._agg_model_table_name,
                "model",
                previous_model,
                {key: -value for key, value in old_model_contrib.items()},
            )
            self._apply_dimensional_delta(cursor, self._agg_model_table_name, "model", merged_model, new_model_contrib)

    def supports_incremental_rollups(self) -> bool:
        return bool(self._incremental_aggregates_enabled)

    def _get_cached_or_refreshed_p95_latency_ms(self, cursor: Any, latest_table_identifier: Any) -> float:
        now = time.monotonic()
        with self._p95_cache_lock:
            if (now - self._p95_cache_refreshed_at_monotonic) < self._p95_refresh_seconds:
                return float(self._p95_latency_cached_ms)

        cursor.execute(
            sql.SQL(
                """
                SELECT
                    COALESCE(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms), 0)
                FROM {}
                WHERE latency_ms IS NOT NULL
                """
            ).format(latest_table_identifier)
        )
        p95_row = cursor.fetchone()
        refreshed_value = self._as_float(p95_row[0] if p95_row else 0.0, 0.0)

        with self._p95_cache_lock:
            self._p95_latency_cached_ms = refreshed_value
            self._p95_cache_refreshed_at_monotonic = now
            return float(self._p95_latency_cached_ms)

    def load_incremental_rollups(self) -> Optional[Dict[str, Any]]:
        if not self._incremental_aggregates_enabled:
            return None

        self._ensure_table()
        agg_overall_table_identifier = sql.Identifier(self._agg_overall_table_name)
        agg_backend_table_identifier = sql.Identifier(self._agg_backend_table_name)
        agg_model_table_identifier = sql.Identifier(self._agg_model_table_name)
        latest_table_identifier = sql.Identifier(self._latest_table_name)

        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    sql.SQL(
                        """
                        SELECT
                            total_requests,
                            total_errors,
                            latency_sum,
                            latency_count,
                            rag_scored_requests,
                            rag_faithfulness_sum,
                            rag_faithfulness_count,
                            rag_response_relevancy_sum,
                            rag_response_relevancy_count,
                            rag_context_precision_sum,
                            rag_context_precision_count,
                            rag_retrieval_quality_sum,
                            rag_retrieval_quality_count
                        FROM {}
                        WHERE singleton = TRUE
                        """
                    ).format(agg_overall_table_identifier)
                )
                overall = cursor.fetchone()
                if overall is None:
                    return None

                p95_latency_ms = self._get_cached_or_refreshed_p95_latency_ms(
                    cursor,
                    latest_table_identifier,
                )

                cursor.execute(
                    sql.SQL(
                        """
                        SELECT backend, total_requests, total_errors, latency_sum, latency_count
                        FROM {}
                        ORDER BY total_requests DESC
                        """
                    ).format(agg_backend_table_identifier)
                )
                backend_rows = cursor.fetchall()

                cursor.execute(
                    sql.SQL(
                        """
                        SELECT model, total_requests, total_errors, latency_sum, latency_count
                        FROM {}
                        ORDER BY total_requests DESC
                        """
                    ).format(agg_model_table_identifier)
                )
                model_rows = cursor.fetchall()

        total_requests = self._as_float(overall[0], 0.0)
        total_errors = self._as_float(overall[1], 0.0)
        latency_sum = self._as_float(overall[2], 0.0)
        latency_count = self._as_float(overall[3], 0.0)
        rag_scored_requests = self._as_float(overall[4], 0.0)
        rag_faithfulness_sum = self._as_float(overall[5], 0.0)
        rag_faithfulness_count = self._as_float(overall[6], 0.0)
        rag_response_relevancy_sum = self._as_float(overall[7], 0.0)
        rag_response_relevancy_count = self._as_float(overall[8], 0.0)
        rag_context_precision_sum = self._as_float(overall[9], 0.0)
        rag_context_precision_count = self._as_float(overall[10], 0.0)
        rag_retrieval_quality_sum = self._as_float(overall[11], 0.0)
        rag_retrieval_quality_count = self._as_float(overall[12], 0.0)

        backend_rollups = []
        for backend, requests, errors, latency_total, latency_samples in backend_rows:
            request_count = self._as_float(requests, 0.0)
            backend_rollups.append(
                {
                    "backend": str(backend or "unknown"),
                    "requests": int(request_count),
                    "error_rate_percent": (self._as_float(errors, 0.0) / request_count * 100.0) if request_count > 0 else 0.0,
                    "avg_latency_ms": (self._as_float(latency_total, 0.0) / self._as_float(latency_samples, 0.0))
                    if self._as_float(latency_samples, 0.0) > 0
                    else None,
                }
            )

        model_rollups = []
        for model, requests, errors, latency_total, latency_samples in model_rows:
            request_count = self._as_float(requests, 0.0)
            model_rollups.append(
                {
                    "model": str(model or "unknown"),
                    "requests": int(request_count),
                    "error_rate_percent": (self._as_float(errors, 0.0) / request_count * 100.0) if request_count > 0 else 0.0,
                    "avg_latency_ms": (self._as_float(latency_total, 0.0) / self._as_float(latency_samples, 0.0))
                    if self._as_float(latency_samples, 0.0) > 0
                    else None,
                }
            )

        return {
            "status": "ok",
            "engine": "postgres-incremental-rollups",
            "overall": {
                "total_requests": int(total_requests),
                "total_errors": int(total_errors),
                "error_rate_percent": (total_errors / total_requests * 100.0) if total_requests > 0 else 0.0,
                "avg_latency_ms": (latency_sum / latency_count) if latency_count > 0 else None,
                "p95_latency_ms": p95_latency_ms,
            },
            "backend_rollups": backend_rollups,
            "model_rollups": model_rollups,
            "rag_overall": {
                "scored_requests": int(rag_scored_requests),
                "avg_faithfulness": (rag_faithfulness_sum / rag_faithfulness_count)
                if rag_faithfulness_count > 0
                else None,
                "avg_response_relevancy": (rag_response_relevancy_sum / rag_response_relevancy_count)
                if rag_response_relevancy_count > 0
                else None,
                "avg_context_precision": (rag_context_precision_sum / rag_context_precision_count)
                if rag_context_precision_count > 0
                else None,
                "avg_retrieval_quality": (rag_retrieval_quality_sum / rag_retrieval_quality_count)
                if rag_retrieval_quality_count > 0
                else None,
            },
        }

    def persist_batch(self, records: Sequence[Dict[str, Any]]) -> None:
        if not records:
            return

        self._ensure_table()
        table_identifier = sql.Identifier(self.table_name)
        insert_stmt = sql.SQL(
            """
            INSERT INTO {} (
                recorded_at,
                question,
                answer,
                model,
                backend,
                mission,
                context_count,
                answer_length,
                question_length,
                is_error,
                latency_ms,
                retrieval_quality,
                metric_values,
                raw_record
            ) VALUES (
                %s::timestamptz,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s::jsonb,
                %s::jsonb
            )
            """
        ).format(table_identifier)

        params: List[Tuple[Any, ...]] = []
        for record in records:
            metrics = _extract_numeric_metrics(record)
            params.append(
                (
                    str(record.get("timestamp") or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")),
                    str(record.get("question") or ""),
                    str(record.get("answer") or ""),
                    str(record.get("model") or "unknown"),
                    str(record.get("backend") or "unknown"),
                    str(record.get("mission") or "all"),
                    int(record.get("context_count") or 0),
                    int(record.get("answer_length") or len(str(record.get("answer") or ""))),
                    int(record.get("question_length") or len(str(record.get("question") or ""))),
                    float(record.get("is_error") or 0.0),
                    float(record["latency_ms"]) if record.get("latency_ms") is not None else None,
                    _compute_retrieval_quality(record),
                    json.dumps(metrics, ensure_ascii=True),
                    json.dumps(record, ensure_ascii=True),
                )
            )

        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(insert_stmt, params)
                if self._incremental_aggregates_enabled:
                    for record in records:
                        if isinstance(record, dict):
                            self._apply_incremental_aggregates_for_record(cursor, record)
            conn.commit()

    def load_dataframe(self) -> pd.DataFrame:
        self._ensure_table()
        table_identifier = sql.Identifier(self.table_name)
        select_stmt = sql.SQL("SELECT raw_record FROM {} ORDER BY id ASC").format(table_identifier)

        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(select_stmt)
                rows = cursor.fetchall()

        if not rows:
            return pd.DataFrame()

        records: List[Dict[str, Any]] = []
        for row in rows:
            payload = row[0]
            if isinstance(payload, dict):
                records.append(payload)
            elif isinstance(payload, str):
                try:
                    records.append(json.loads(payload))
                except json.JSONDecodeError:
                    continue

        if not records:
            return pd.DataFrame()
        return pd.DataFrame(records)

    def get_signature(self) -> Optional[Tuple[Any, ...]]:
        self._ensure_table()
        table_identifier = sql.Identifier(self.table_name)
        query = sql.SQL(
            "SELECT COUNT(*), COALESCE(MAX(id), 0), COALESCE(EXTRACT(EPOCH FROM MAX(created_at)), 0) FROM {}"
        ).format(table_identifier)

        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                count, max_id, last_epoch = cursor.fetchone()

        if not count:
            return None
        return (int(count), int(max_id), float(last_epoch))

    def describe(self) -> Dict[str, str]:
        parsed = urlparse(self.dsn)
        host = parsed.hostname or os.getenv("MONITORING_POSTGRES_HOST", "localhost")
        database = parsed.path.lstrip("/") or os.getenv("MONITORING_POSTGRES_DB", "postgres")
        return {
            "type": self.sink_type,
            "target": f"{host}/{database}#{self.table_name}",
        }

    def shutdown(self) -> None:
        return None

    def native_ndjson_path(self) -> Optional[Path]:
        return None


class S3ObjectStorageMirrorSink:
    sink_type = "s3"

    def __init__(self, bucket: str, prefix: str = "monitoring/interactions", endpoint_url: Optional[str] = None):
        if not BOTO3_AVAILABLE or boto3 is None:
            raise RuntimeError(
                "S3 monitoring mirror selected but boto3 is not installed. "
                "Install the monitoring-object-storage dependency group."
            )
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.client = boto3.client("s3", endpoint_url=endpoint_url or None)

    def persist_batch(self, records: Sequence[Dict[str, Any]]) -> None:
        if not records:
            return
        now = datetime.now(timezone.utc)
        object_key = (
            f"{self.prefix}/{now.strftime('%Y/%m/%d/%H')}/"
            f"interactions-{int(now.timestamp() * 1000)}-{uuid4().hex}.jsonl"
        )
        payload = "".join(json.dumps(record, ensure_ascii=True) + "\n" for record in records).encode("utf-8")
        self.client.put_object(Bucket=self.bucket, Key=object_key, Body=payload, ContentType="application/x-ndjson")

    def describe(self) -> Dict[str, str]:
        return {"type": self.sink_type, "target": f"s3://{self.bucket}/{self.prefix}"}

    def shutdown(self) -> None:
        return None


class AzureBlobObjectStorageMirrorSink:
    sink_type = "azure_blob"

    def __init__(self, connection_string: str, container: str, prefix: str = "monitoring/interactions"):
        if not AZURE_BLOB_AVAILABLE or BlobServiceClient is None:
            raise RuntimeError(
                "Azure Blob monitoring mirror selected but azure-storage-blob is not installed. "
                "Install the monitoring-object-storage dependency group."
            )
        self.container = container
        self.prefix = prefix.strip("/")
        self.service_client = BlobServiceClient.from_connection_string(connection_string)
        self.container_client = self.service_client.get_container_client(container)

    def persist_batch(self, records: Sequence[Dict[str, Any]]) -> None:
        if not records:
            return
        now = datetime.now(timezone.utc)
        blob_name = (
            f"{self.prefix}/{now.strftime('%Y/%m/%d/%H')}/"
            f"interactions-{int(now.timestamp() * 1000)}-{uuid4().hex}.jsonl"
        )
        payload = "".join(json.dumps(record, ensure_ascii=True) + "\n" for record in records)
        blob_client = self.container_client.get_blob_client(blob_name)
        blob_client.upload_blob(payload, overwrite=False)

    def describe(self) -> Dict[str, str]:
        return {"type": self.sink_type, "target": f"azureblob://{self.container}/{self.prefix}"}

    def shutdown(self) -> None:
        return None


class OtlpLogMirrorSink:
    sink_type = "otlp"

    def __init__(self, endpoint: str):
        if (
            not OTEL_LOGS_AVAILABLE
            or OtelResource is None
            or OtelLoggerProvider is None
            or OtelLoggingHandler is None
            or BatchLogRecordProcessor is None
            or OTLPLogExporter is None
        ):
            raise RuntimeError(
                "OTLP monitoring mirror selected but OTLP log exporter packages are not installed. "
                "Install the monitoring-otlp dependency group."
            )

        self.endpoint = endpoint
        self._provider = OtelLoggerProvider(
            resource=OtelResource.create({"service.name": "nasa-monitoring-interactions"})
        )
        exporter = OTLPLogExporter(endpoint=endpoint)
        processor = BatchLogRecordProcessor(exporter)
        self._provider.add_log_record_processor(processor)
        self._handler = OtelLoggingHandler(level=logging.INFO, logger_provider=self._provider)
        self._logger = logging.getLogger("evidently_monitor_otlp")
        self._logger.setLevel(logging.INFO)
        self._logger.propagate = False
        self._logger.addHandler(self._handler)

    def persist_batch(self, records: Sequence[Dict[str, Any]]) -> None:
        for record in records:
            self._logger.info(json.dumps(record, ensure_ascii=True))

    def describe(self) -> Dict[str, str]:
        return {"type": self.sink_type, "target": self.endpoint}

    def shutdown(self) -> None:
        try:
            self._logger.removeHandler(self._handler)
        except Exception:
            pass
        self._provider.shutdown()


class EvidentlyMonitor:
    """Persist chat interactions and generate drift reports when requested."""

    def __init__(
        self,
        log_path: str = "monitoring/interactions.jsonl",
        sink_type: Optional[str] = None,
        mirror_sink_types: Optional[Sequence[str]] = None,
    ):
        self._logger = logging.getLogger(__name__)

        self._primary_sink = self._build_primary_sink(log_path=log_path, sink_type=sink_type)
        self._mirror_sinks = self._build_mirror_sinks(mirror_sink_types=mirror_sink_types)
        native_path = self._primary_sink.native_ndjson_path()
        self.log_path = native_path if native_path is not None else Path("monitoring/interactions.jsonl")

        self._write_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._queue_maxsize = self._parse_int_env("MONITORING_WRITE_QUEUE_MAXSIZE", 5000, minimum=100)
        self._write_queue: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=self._queue_maxsize)
        self._flush_interval_seconds = self._parse_float_env("MONITORING_WRITE_FLUSH_SECONDS", 0.25, minimum=0.01)
        self._batch_size = self._parse_int_env("MONITORING_WRITE_BATCH_SIZE", 64, minimum=1)
        self._dropped_records = 0
        self._write_failures = 0
        self._mirror_write_failures = 0

        self._writer_thread = threading.Thread(
            target=self._writer_loop,
            name="evidently-monitor-writer",
            daemon=True,
        )
        self._writer_thread.start()
        atexit.register(self.shutdown)

        # Keyed, incremental materialization state used by analytics/RAG endpoints.
        # This replaces full read-time canonicalization on every query.
        self._state_lock = threading.RLock()
        self._materialized_by_interaction: Dict[str, Dict[str, Any]] = {}
        self._materialized_order: Dict[str, int] = {}
        self._materialized_bootstrapped = False
        self._materialized_next_order = 0
        self._materialized_next_legacy_id = 0
        self._materialized_version = 0
        self._materialized_refresh_seconds = self._parse_float_env(
            "MONITORING_MATERIALIZED_REFRESH_SECONDS",
            30.0,
            minimum=1.0,
        )
        self._materialized_last_refresh_monotonic = 0.0
        self._materialized_last_signature: Optional[Tuple[Any, ...]] = None

        self._analytics_cache: Dict[str, Any] = {
            "version": None,
            "result": None,
        }
        self._rag_cache: Dict[str, Any] = {
            "version": None,
            "recent_failures_limit": None,
            "result": None,
        }
        self._postgres_rollup_cache_ttl_seconds = self._parse_float_env(
            "MONITORING_POSTGRES_ROLLUP_CACHE_TTL_SECONDS",
            1.0,
            minimum=0.1,
        )
        self._postgres_rollup_cache_lock = threading.Lock()
        self._postgres_rollup_cache: Dict[str, Any] = {
            "expires_at_monotonic": 0.0,
            "payload": None,
        }

    @staticmethod
    def _parse_int_env(name: str, default: int, minimum: int = 1) -> int:
        value = (os.getenv(name) or "").strip()
        if not value:
            return default
        try:
            return max(minimum, int(value))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _parse_float_env(name: str, default: float, minimum: float = 0.0) -> float:
        value = (os.getenv(name) or "").strip()
        if not value:
            return default
        try:
            return max(minimum, float(value))
        except (TypeError, ValueError):
            return default

    def _build_postgres_dsn(self) -> str:
        explicit_dsn = (os.getenv("MONITORING_POSTGRES_DSN") or "").strip()
        if explicit_dsn:
            return explicit_dsn

        host = (os.getenv("MONITORING_POSTGRES_HOST") or "").strip()
        dbname = (os.getenv("MONITORING_POSTGRES_DB") or "").strip()
        if not host or not dbname:
            return ""

        user = (os.getenv("MONITORING_POSTGRES_USER") or "").strip()
        password = os.getenv("MONITORING_POSTGRES_PASSWORD") or ""
        port = (os.getenv("MONITORING_POSTGRES_PORT") or "5432").strip()
        sslmode = (os.getenv("MONITORING_POSTGRES_SSLMODE") or "prefer").strip()

        auth = ""
        if user:
            auth = quote(user, safe="")
            if password:
                auth += ":" + quote(password, safe="")
            auth += "@"

        return f"postgresql://{auth}{host}:{port}/{quote(dbname, safe='')}?sslmode={quote_plus(sslmode)}"

    def _build_primary_sink(self, log_path: str, sink_type: Optional[str]) -> PrimaryInteractionSink:
        configured_path = (os.getenv("MONITORING_INTERACTIONS_LOG_PATH") or "").strip()
        configured_sink_path = (os.getenv("MONITORING_CENTRAL_SINK_PATH") or "").strip()
        effective_log_path = configured_sink_path or configured_path or log_path

        resolved_sink_type = (sink_type or os.getenv("MONITORING_PRIMARY_SINK") or "file").strip().lower()
        if resolved_sink_type in {"file", "jsonl", "shared_file"}:
            return FileInteractionSink(effective_log_path)
        if resolved_sink_type == "postgres":
            dsn = self._build_postgres_dsn()
            if not dsn:
                raise RuntimeError(
                    "MONITORING_PRIMARY_SINK=postgres requires MONITORING_POSTGRES_DSN "
                    "or MONITORING_POSTGRES_HOST/MONITORING_POSTGRES_DB configuration."
                )
            table_name = (os.getenv("MONITORING_POSTGRES_TABLE") or "monitoring_interactions").strip()
            return PostgresInteractionSink(dsn=dsn, table_name=table_name)
        raise ValueError(f"Unsupported monitoring primary sink: {resolved_sink_type}")

    def _build_mirror_sinks(self, mirror_sink_types: Optional[Sequence[str]]) -> List[MirrorInteractionSink]:
        configured_types = list(mirror_sink_types) if mirror_sink_types is not None else _parse_csv_env("MONITORING_MIRROR_SINKS")
        mirrors: List[MirrorInteractionSink] = []

        for sink_name in configured_types:
            normalized = str(sink_name).strip().lower()
            if not normalized:
                continue
            if normalized == "otlp":
                endpoint = (
                    os.getenv("MONITORING_OTLP_LOGS_ENDPOINT")
                    or os.getenv("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT")
                    or ""
                ).strip()
                if not endpoint:
                    raise RuntimeError("MONITORING_MIRROR_SINKS includes otlp but no OTLP logs endpoint is configured")
                mirrors.append(OtlpLogMirrorSink(endpoint=endpoint))
                continue
            if normalized == "s3":
                bucket = (os.getenv("MONITORING_S3_BUCKET") or "").strip()
                if not bucket:
                    raise RuntimeError("MONITORING_MIRROR_SINKS includes s3 but MONITORING_S3_BUCKET is not configured")
                mirrors.append(
                    S3ObjectStorageMirrorSink(
                        bucket=bucket,
                        prefix=(os.getenv("MONITORING_S3_PREFIX") or "monitoring/interactions").strip(),
                        endpoint_url=(os.getenv("MONITORING_S3_ENDPOINT_URL") or "").strip() or None,
                    )
                )
                continue
            if normalized in {"azure_blob", "azureblob"}:
                connection_string = (os.getenv("MONITORING_AZURE_BLOB_CONNECTION_STRING") or "").strip()
                container = (os.getenv("MONITORING_AZURE_BLOB_CONTAINER") or "").strip()
                if not connection_string or not container:
                    raise RuntimeError(
                        "MONITORING_MIRROR_SINKS includes azure_blob but Azure Blob connection settings are not configured"
                    )
                mirrors.append(
                    AzureBlobObjectStorageMirrorSink(
                        connection_string=connection_string,
                        container=container,
                        prefix=(os.getenv("MONITORING_AZURE_BLOB_PREFIX") or "monitoring/interactions").strip(),
                    )
                )
                continue
            raise ValueError(f"Unsupported monitoring mirror sink: {normalized}")

        return mirrors

    def _invalidate_caches(self) -> None:
        self._analytics_cache["version"] = None
        self._analytics_cache["result"] = None
        self._rag_cache["version"] = None
        self._rag_cache["recent_failures_limit"] = None
        self._rag_cache["result"] = None

    def _invalidate_postgres_rollup_cache(self) -> None:
        with self._postgres_rollup_cache_lock:
            self._postgres_rollup_cache["expires_at_monotonic"] = 0.0
            self._postgres_rollup_cache["payload"] = None

    def _get_postgres_rollup_payload_cached(self) -> Optional[Dict[str, Any]]:
        if not (hasattr(self._primary_sink, "supports_incremental_rollups") and hasattr(self._primary_sink, "load_incremental_rollups")):
            return None

        try:
            if not bool(getattr(self._primary_sink, "supports_incremental_rollups")()):
                return None
        except Exception:
            self._logger.exception("Failed checking incremental rollup capability")
            return None

        now = time.monotonic()
        with self._postgres_rollup_cache_lock:
            expires_at = float(self._postgres_rollup_cache.get("expires_at_monotonic") or 0.0)
            cached_payload = self._postgres_rollup_cache.get("payload")
            if isinstance(cached_payload, dict) and expires_at > now:
                return cached_payload

        try:
            payload = getattr(self._primary_sink, "load_incremental_rollups")()
        except Exception:
            self._logger.exception("Incremental Postgres rollup read failed")
            return None

        if not (isinstance(payload, dict) and payload.get("status") == "ok"):
            return None

        with self._postgres_rollup_cache_lock:
            self._postgres_rollup_cache["payload"] = payload
            self._postgres_rollup_cache["expires_at_monotonic"] = now + self._postgres_rollup_cache_ttl_seconds
        return payload

    @staticmethod
    def _value_is_present(value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, str) and value == "":
            return False
        if isinstance(value, float) and math.isnan(value):
            return False
        return True

    def _next_legacy_interaction_key_locked(self) -> str:
        key = f"legacy-{self._materialized_next_legacy_id}"
        self._materialized_next_legacy_id += 1
        return key

    def _resolve_interaction_key_locked(self, record: Dict[str, Any]) -> str:
        interaction_id = record.get("interaction_id")
        if isinstance(interaction_id, str) and interaction_id.strip():
            return interaction_id.strip()
        if interaction_id is not None:
            interaction_id_text = str(interaction_id).strip()
            if interaction_id_text:
                return interaction_id_text
        return self._next_legacy_interaction_key_locked()

    def _apply_record_to_materialized_state_locked(self, record: Dict[str, Any]) -> None:
        interaction_key = self._resolve_interaction_key_locked(record)
        existing = self._materialized_by_interaction.get(interaction_key)
        if existing is None:
            merged = {
                "interaction_id": interaction_key,
                "record_kind": "interaction",
            }
            self._materialized_order[interaction_key] = self._materialized_next_order
            self._materialized_next_order += 1
        else:
            merged = dict(existing)

        # Forward-fill semantics by key: only present values overwrite existing values.
        for key, value in record.items():
            if self._value_is_present(value):
                merged[key] = value

        merged["interaction_id"] = interaction_key
        merged["record_kind"] = "interaction"
        self._materialized_by_interaction[interaction_key] = merged

    def _rebuild_materialized_state_from_sink_locked(self) -> None:
        dataset = self.load_dataframe()
        self._materialized_by_interaction = {}
        self._materialized_order = {}
        self._materialized_next_order = 0
        self._materialized_next_legacy_id = 0

        if not dataset.empty:
            for record in dataset.to_dict(orient="records"):
                if isinstance(record, dict):
                    self._apply_record_to_materialized_state_locked(record)

        self._materialized_bootstrapped = True
        self._materialized_last_signature = self._log_signature()
        self._materialized_last_refresh_monotonic = time.monotonic()
        self._materialized_version += 1
        self._invalidate_caches()

    def _ensure_materialized_state_loaded(self) -> bool:
        should_check_signature = False
        with self._state_lock:
            if self._materialized_bootstrapped:
                elapsed = time.monotonic() - self._materialized_last_refresh_monotonic
                if elapsed < self._materialized_refresh_seconds:
                    return False
                self._materialized_last_refresh_monotonic = time.monotonic()
                should_check_signature = True
            else:
                self._rebuild_materialized_state_from_sink_locked()
                return True

        if should_check_signature:
            latest_signature = self._log_signature()
            with self._state_lock:
                if self._materialized_last_signature is None:
                    # When baseline signature is unknown (e.g. after local writes),
                    # rebuild from sink so externally written worker updates are not missed.
                    self._rebuild_materialized_state_from_sink_locked()
                    return True
                if latest_signature == self._materialized_last_signature:
                    return False
                self._rebuild_materialized_state_from_sink_locked()
                return True

        return False

    def _materialized_records_snapshot(self) -> Tuple[List[Dict[str, Any]], int]:
        self._ensure_materialized_state_loaded()
        with self._state_lock:
            ordered_keys = sorted(
                self._materialized_order.keys(),
                key=lambda key: self._materialized_order[key],
            )
            records = [dict(self._materialized_by_interaction[key]) for key in ordered_keys]
            version = int(self._materialized_version)
        return records, version

    def _persist_batch(self, records: Sequence[Dict[str, Any]]) -> None:
        if not records:
            return

        try:
            with self._write_lock:
                self._primary_sink.persist_batch(records)
            just_bootstrapped = self._ensure_materialized_state_loaded()
            if not just_bootstrapped:
                with self._state_lock:
                    for record in records:
                        if isinstance(record, dict):
                            self._apply_record_to_materialized_state_locked(record)
                    # Local writes are already reflected in state; defer expensive
                    # sink signature refresh to periodic checks.
                    self._materialized_last_signature = None
                    self._materialized_last_refresh_monotonic = time.monotonic()
                    self._materialized_version += 1
                    self._invalidate_caches()
                    self._invalidate_postgres_rollup_cache()
        except Exception:
            self._write_failures += len(records)
            self._logger.exception("Failed to persist monitoring interaction records to primary sink")
            return

        for mirror_sink in self._mirror_sinks:
            try:
                mirror_sink.persist_batch(records)
            except Exception:
                self._mirror_write_failures += len(records)
                self._logger.exception(
                    "Failed to persist monitoring interaction records to mirror sink %s",
                    mirror_sink.describe().get("type", mirror_sink.sink_type),
                )

    def _writer_loop(self) -> None:
        buffered: List[Dict[str, Any]] = []
        last_flush_at = time.monotonic()

        while not self._stop_event.is_set():
            timeout = max(0.01, self._flush_interval_seconds)
            try:
                record = self._write_queue.get(timeout=timeout)
                buffered.append(record)
                if len(buffered) >= self._batch_size:
                    self._persist_batch(buffered)
                    buffered = []
                    last_flush_at = time.monotonic()
            except queue.Empty:
                now = time.monotonic()
                if buffered and (now - last_flush_at) >= self._flush_interval_seconds:
                    self._persist_batch(buffered)
                    buffered = []
                    last_flush_at = now

        while True:
            try:
                buffered.append(self._write_queue.get_nowait())
            except queue.Empty:
                break
        if buffered:
            self._persist_batch(buffered)

    def shutdown(self, timeout_seconds: float = 2.0) -> None:
        self._stop_event.set()
        if self._writer_thread.is_alive():
            self._writer_thread.join(timeout=max(0.1, float(timeout_seconds)))
        try:
            self._primary_sink.shutdown()
        except Exception:
            self._logger.exception("Failed to shut down monitoring primary sink cleanly")
        for mirror_sink in self._mirror_sinks:
            try:
                mirror_sink.shutdown()
            except Exception:
                self._logger.exception(
                    "Failed to shut down monitoring mirror sink %s cleanly",
                    mirror_sink.describe().get("type", mirror_sink.sink_type),
                )

    def log_interaction(
        self,
        question: str,
        answer: str,
        model: str,
        backend: str,
        context_count: int,
        mission: Optional[str] = None,
        evaluation: Optional[Dict[str, float]] = None,
        error: bool = False,
        latency_ms: Optional[float] = None,
        interaction_id: Optional[str] = None,
        record_kind: str = "interaction",
        synchronous: bool = False,
    ) -> None:
        """Log an interaction (success or error) for drift monitoring."""
        record: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "question": question,
            "answer": answer,
            "model": model,
            "backend": backend,
            "mission": mission or self._derive_mission(backend),
            "context_count": context_count,
            "answer_length": len(answer or ""),
            "question_length": len(question or ""),
            "is_error": 1.0 if error else 0.0,
            "record_kind": str(record_kind or "interaction"),
        }
        if interaction_id:
            record["interaction_id"] = str(interaction_id)
        if latency_ms is not None:
            record["latency_ms"] = float(latency_ms)
        if evaluation:
            for key, value in evaluation.items():
                if isinstance(value, (int, float)):
                    record[f"metric_{key}"] = float(value)

        if synchronous:
            self._persist_batch([record])
            return

        try:
            self._write_queue.put_nowait(record)
        except queue.Full:
            self._dropped_records += 1
            self._persist_batch([record])

    @staticmethod
    def _derive_mission(backend: Optional[str]) -> str:
        if not backend:
            return "all"

        backend_text = str(backend).lower()
        known_missions = ["apollo_11", "apollo_13", "challenger"]
        for mission in known_missions:
            if mission in backend_text:
                return mission
        return "all"

    @staticmethod
    def _score_band(value: Any) -> str:
        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            return "unknown"

        if math.isnan(numeric_value):
            return "unknown"
        if numeric_value < 0.4:
            return "low"
        if numeric_value < 0.7:
            return "medium"
        return "high"

    def _prepare_rag_dataframe(self) -> pd.DataFrame:
        records, _ = self._materialized_records_snapshot()
        dataset = pd.DataFrame(records)
        if dataset.empty:
            return dataset

        if "timestamp" in dataset.columns:
            dataset["timestamp"] = pd.to_datetime(dataset["timestamp"], errors="coerce", utc=True)
        else:
            dataset["timestamp"] = pd.NaT

        if "mission" not in dataset.columns:
            backend_series = dataset.get("backend")
            if backend_series is None:
                dataset["mission"] = "all"
            else:
                dataset["mission"] = backend_series.apply(self._derive_mission)
        else:
            dataset["mission"] = dataset["mission"].fillna("all").astype(str)

        for column in [
            "context_count",
            "is_error",
            "latency_ms",
            "metric_faithfulness",
            "metric_response_relevancy",
            "metric_context_precision",
            "metric_bleu_score",
            "metric_rouge_score",
        ]:
            if column not in dataset.columns:
                dataset[column] = None
            dataset[column] = pd.to_numeric(dataset[column], errors="coerce")

        dataset["retrieval_quality"] = dataset[
            ["metric_faithfulness", "metric_response_relevancy", "metric_context_precision"]
        ].mean(axis=1, skipna=True)
        dataset["score_band"] = dataset["retrieval_quality"].apply(self._score_band)
        return dataset

    @staticmethod
    def _canonicalize_dataset(dataset: pd.DataFrame) -> pd.DataFrame:
        if dataset.empty:
            return dataset

        if "interaction_id" not in dataset.columns and "record_kind" not in dataset.columns:
            return dataset

        canonical = dataset.copy()
        canonical["_row_order"] = range(len(canonical))

        if "interaction_id" not in canonical.columns:
            canonical["interaction_id"] = canonical["_row_order"].apply(lambda value: f"legacy-{value}")
        else:
            canonical["interaction_id"] = canonical["interaction_id"].astype("string")
            missing_mask = canonical["interaction_id"].isna() | canonical["interaction_id"].eq("")
            if missing_mask.any():
                canonical.loc[missing_mask, "interaction_id"] = canonical.loc[missing_mask, "_row_order"].apply(
                    lambda value: f"legacy-{value}"
                )

        if "record_kind" not in canonical.columns:
            canonical["record_kind"] = "interaction"
        else:
            canonical["record_kind"] = canonical["record_kind"].fillna("interaction").astype(str)

        sort_columns = ["interaction_id"]
        if "timestamp" in canonical.columns:
            canonical["timestamp"] = pd.to_datetime(canonical["timestamp"], errors="coerce", utc=True)
            sort_columns.append("timestamp")
        sort_columns.append("_row_order")
        canonical = canonical.sort_values(sort_columns, kind="stable")

        merged_groups: List[pd.Series] = []
        for _, group in canonical.groupby("interaction_id", dropna=False, sort=False):
            merged_groups.append(group.ffill().iloc[-1])

        merged = pd.DataFrame(merged_groups).reset_index(drop=True) if merged_groups else canonical.iloc[0:0].copy()

        if "record_kind" in merged.columns:
            merged["record_kind"] = "interaction"
        if "_row_order" in merged.columns:
            merged = merged.drop(columns=["_row_order"])
        return merged

    def load_dataframe(self) -> pd.DataFrame:
        return self._primary_sink.load_dataframe()

    @staticmethod
    def _select_viable_drift_columns(reference: pd.DataFrame, current: pd.DataFrame) -> List[str]:
        """Keep only columns with usable values in both drift windows.

        Evidently raises when a reference/current column is entirely empty.
        """
        viable_columns: List[str] = []

        for column in reference.columns:
            reference_non_null = reference[column].dropna()
            current_non_null = current[column].dropna()
            if reference_non_null.empty or current_non_null.empty:
                continue
            viable_columns.append(column)

        return viable_columns

    @staticmethod
    def _save_report_html(report: Any, output_path: Path) -> Optional[str]:
        """Persist an Evidently report across package API variants.

        Returns an error string when the installed Evidently version does not
        expose a supported HTML export method.
        """
        if hasattr(report, "save_html"):
            report.save_html(str(output_path))
            return None

        if hasattr(report, "save"):
            report.save(str(output_path))
            return None

        if hasattr(report, "html"):
            output_path.write_text(report.html(), encoding="utf-8")
            return None

        return "Installed Evidently version does not support HTML export"

    def _log_signature(self) -> Optional[Tuple[Any, ...]]:
        return self._primary_sink.get_signature()

    @staticmethod
    def _round_float(value: Any) -> Any:
        if isinstance(value, float):
            return round(value, 2)
        return value

    @classmethod
    def _round_records(cls, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [
            {key: cls._round_float(value) for key, value in record.items()}
            for record in records
        ]

    def get_analytics_summary(self) -> Dict[str, Any]:
        """Return cached analytics rollups using keyed incremental materialization."""
        rollups = self._get_postgres_rollup_payload_cached()
        if isinstance(rollups, dict) and rollups.get("status") == "ok":
            return {
                "status": "ok",
                "engine": rollups.get("engine", "postgres-incremental-rollups"),
                "overall": rollups.get("overall", {}),
                "backend_rollups": self._round_records(list(rollups.get("backend_rollups", []))),
                "model_rollups": self._round_records(list(rollups.get("model_rollups", []))),
            }

        records, version = self._materialized_records_snapshot()
        if not records:
            return {"error": "No monitoring data found"}

        if self._analytics_cache["version"] == version:
            cached_result = self._analytics_cache["result"]
            if cached_result is not None:
                return cached_result

        dataset = pd.DataFrame(records)
        if "latency_ms" not in dataset.columns:
            dataset["latency_ms"] = None
        if "is_error" not in dataset.columns:
            dataset["is_error"] = 0.0
        if "backend" not in dataset.columns:
            dataset["backend"] = "unknown"
        if "model" not in dataset.columns:
            dataset["model"] = "unknown"

        dataset["is_error"] = dataset["is_error"].fillna(0).astype(float)

        total_requests = len(dataset)
        total_errors = int(dataset["is_error"].sum())
        latency_numeric = pd.to_numeric(dataset["latency_ms"], errors="coerce")
        backend_rollups = (
            dataset.groupby("backend", dropna=False)
            .agg(
                requests=("backend", "count"),
                error_rate_percent=("is_error", lambda series: float(series.mean() * 100)),
                avg_latency_ms=("latency_ms", "mean"),
                p95_latency_ms=("latency_ms", lambda series: pd.to_numeric(series, errors="coerce").quantile(0.95)),
            )
            .reset_index()
            .sort_values("requests", ascending=False)
            .to_dict(orient="records")
        )
        model_rollups = (
            dataset.groupby("model", dropna=False)
            .agg(
                requests=("model", "count"),
                error_rate_percent=("is_error", lambda series: float(series.mean() * 100)),
                avg_latency_ms=("latency_ms", "mean"),
            )
            .reset_index()
            .sort_values("requests", ascending=False)
            .to_dict(orient="records")
        )

        result = {
            "status": "ok",
            "engine": "keyed-materialized",
            "overall": {
                "total_requests": total_requests,
                "total_errors": total_errors,
                "error_rate_percent": round((total_errors / total_requests) * 100, 2) if total_requests else 0.0,
                "avg_latency_ms": self._round_float(latency_numeric.mean()),
                "p95_latency_ms": self._round_float(latency_numeric.quantile(0.95)),
            },
            "backend_rollups": self._round_records(backend_rollups),
            "model_rollups": self._round_records(model_rollups),
        }

        self._analytics_cache["version"] = version
        self._analytics_cache["result"] = result
        return result

    def get_rag_dashboard_summary(self, recent_failures_limit: int = 20) -> Dict[str, Any]:
        """Return RAG-specific rollups built from logged RAGAS scores."""
        records, version = self._materialized_records_snapshot()
        if not records:
            return {"error": "No monitoring data found"}

        cached_signature = self._rag_cache["version"]
        cached_result = self._rag_cache["result"]
        cached_limit = self._rag_cache["recent_failures_limit"]
        if cached_signature == version and cached_limit == recent_failures_limit and cached_result is not None:
            return cached_result

        dataset = self._prepare_rag_dataframe()
        if dataset.empty:
            return {"error": "Monitoring data is empty"}

        rag_scored = dataset[
            dataset[["metric_faithfulness", "metric_response_relevancy", "metric_context_precision"]]
            .notna()
            .any(axis=1)
        ].copy()
        if rag_scored.empty:
            return {"error": "No RAGAS-scored monitoring data found"}

        avg_faithfulness_by_backend = (
            rag_scored.groupby("backend", dropna=False)["metric_faithfulness"]
            .mean()
            .dropna()
            .sort_values(ascending=False)
            .reset_index(name="avg_faithfulness")
            .to_dict(orient="records")
        )

        avg_relevancy_by_mission = (
            rag_scored.groupby("mission", dropna=False)["metric_response_relevancy"]
            .mean()
            .dropna()
            .sort_values(ascending=False)
            .reset_index(name="avg_response_relevancy")
            .to_dict(orient="records")
        )

        context_band_rollups = (
            rag_scored.groupby(["context_count", "score_band"], dropna=False)
            .agg(
                requests=("question", "count"),
                avg_retrieval_quality=("retrieval_quality", "mean"),
                avg_faithfulness=("metric_faithfulness", "mean"),
                avg_response_relevancy=("metric_response_relevancy", "mean"),
            )
            .reset_index()
            .sort_values(["context_count", "score_band"], ascending=[True, True])
            .to_dict(orient="records")
        )

        low_score_failures = (
            rag_scored[
                (rag_scored["retrieval_quality"].fillna(1.0) < 0.5)
                | (rag_scored["is_error"].fillna(0.0) > 0)
            ]
            .sort_values("timestamp", ascending=False)
            .head(recent_failures_limit)
            [[
                "timestamp",
                "mission",
                "backend",
                "question",
                "context_count",
                "metric_faithfulness",
                "metric_response_relevancy",
                "metric_context_precision",
                "retrieval_quality",
                "is_error",
            ]]
            .to_dict(orient="records")
        )

        trend_frame = rag_scored.copy()
        trend_frame["time_bucket"] = trend_frame["timestamp"].dt.floor("D")
        retrieval_quality_trend = (
            trend_frame.dropna(subset=["time_bucket"])
            .groupby("time_bucket", dropna=False)
            .agg(
                requests=("question", "count"),
                avg_retrieval_quality=("retrieval_quality", "mean"),
                avg_faithfulness=("metric_faithfulness", "mean"),
                avg_response_relevancy=("metric_response_relevancy", "mean"),
                avg_context_precision=("metric_context_precision", "mean"),
            )
            .reset_index()
            .sort_values("time_bucket", ascending=True)
        )
        if not retrieval_quality_trend.empty:
            retrieval_quality_trend["time_bucket"] = retrieval_quality_trend["time_bucket"].dt.strftime("%Y-%m-%d")

        ranking_view = (
            rag_scored.groupby(["backend", "mission"], dropna=False)
            .agg(
                requests=("question", "count"),
                avg_context_precision=("metric_context_precision", "mean"),
                avg_response_relevancy=("metric_response_relevancy", "mean"),
                avg_retrieval_quality=("retrieval_quality", "mean"),
                avg_context_count=("context_count", "mean"),
            )
            .reset_index()
            .sort_values(["avg_retrieval_quality", "avg_context_precision"], ascending=False)
            .to_dict(orient="records")
        )

        result = {
            "status": "ok",
            "overall": {
                "scored_requests": int(len(rag_scored)),
                "avg_faithfulness": self._round_float(rag_scored["metric_faithfulness"].mean()),
                "avg_response_relevancy": self._round_float(rag_scored["metric_response_relevancy"].mean()),
                "avg_context_precision": self._round_float(rag_scored["metric_context_precision"].mean()),
                "avg_retrieval_quality": self._round_float(rag_scored["retrieval_quality"].mean()),
            },
            "avg_faithfulness_by_backend": self._round_records(avg_faithfulness_by_backend),
            "avg_response_relevancy_by_mission": self._round_records(avg_relevancy_by_mission),
            "context_count_vs_score_bands": self._round_records(context_band_rollups),
            "low_score_recent_failures": self._round_records(low_score_failures),
            "retrieval_quality_trend": self._round_records(retrieval_quality_trend.to_dict(orient="records")),
            "ranking_inc_rag": self._round_records(ranking_view),
        }

        self._rag_cache["version"] = version
        self._rag_cache["recent_failures_limit"] = recent_failures_limit
        self._rag_cache["result"] = result
        return result

    def get_prometheus_curated_snapshot(self) -> Dict[str, Any]:
        """Return a small curated metrics set for Prometheus/Grafana dashboards."""
        analytics = self.get_analytics_summary()
        rag = self.get_rag_dashboard_summary(recent_failures_limit=20)

        rollup_payload = self._get_postgres_rollup_payload_cached()

        analytics_overall = analytics.get("overall", {}) if isinstance(analytics, dict) else {}
        rag_overall = rag.get("overall", {}) if isinstance(rag, dict) else {}
        if isinstance(rollup_payload, dict):
            rag_overall = rollup_payload.get("rag_overall", {}) or rag_overall
        sink_info = self._primary_sink.describe()
        mirror_sink_types = ",".join(mirror.describe().get("type", mirror.sink_type) for mirror in self._mirror_sinks)

        def _as_float(value: Any, default: float = 0.0) -> float:
            try:
                result = float(value)
                if math.isnan(result):
                    return default
                return result
            except (TypeError, ValueError):
                return default

        total_requests = _as_float(analytics_overall.get("total_requests"), 0.0)
        total_errors = _as_float(analytics_overall.get("total_errors"), 0.0)

        return {
            "generated_at_unix": time.time(),
            "sink_type": sink_info.get("type", self._primary_sink.sink_type),
            "sink_target": sink_info.get("target", "unknown"),
            "sink_path": sink_info.get("target", "unknown"),
            "mirror_sinks": mirror_sink_types,
            "requests_total": total_requests,
            "errors_total": total_errors,
            "error_rate_percent": _as_float(analytics_overall.get("error_rate_percent"), 0.0),
            "avg_latency_ms": _as_float(analytics_overall.get("avg_latency_ms"), 0.0),
            "p95_latency_ms": _as_float(analytics_overall.get("p95_latency_ms"), 0.0),
            "rag_scored_requests": _as_float(rag_overall.get("scored_requests"), 0.0),
            "rag_avg_retrieval_quality": _as_float(rag_overall.get("avg_retrieval_quality"), 0.0),
            "rag_avg_faithfulness": _as_float(rag_overall.get("avg_faithfulness"), 0.0),
            "rag_avg_response_relevancy": _as_float(rag_overall.get("avg_response_relevancy"), 0.0),
            "rag_avg_context_precision": _as_float(rag_overall.get("avg_context_precision"), 0.0),
            "sink_queue_depth": float(self._write_queue.qsize()),
            "sink_queue_capacity": float(self._queue_maxsize),
            "sink_dropped_total": float(self._dropped_records),
            "sink_write_failures_total": float(self._write_failures),
            "mirror_write_failures_total": float(self._mirror_write_failures),
        }

    def build_drift_report(
        self,
        reference_rows: int = 100,
        output_html: str = "monitoring/evidently_report.html",
    ) -> Dict[str, str]:
        if not EVIDENTLY_AVAILABLE or Report is None or DataDriftPreset is None:
            return {"error": "Evidently not available in current environment"}

        dataset = self.load_dataframe()
        if dataset.empty or len(dataset) < 20:
            return {"error": "Not enough monitoring data to build report (need >= 20 rows)"}

        split_index = min(reference_rows, max(1, len(dataset) // 2))
        reference = dataset.iloc[:split_index].copy()
        current = dataset.iloc[split_index:].copy()

        if current.empty:
            return {"error": "Current dataset is empty after reference split"}

        viable_columns = self._select_viable_drift_columns(reference, current)
        if not viable_columns:
            return {"error": "Not enough populated monitoring columns to build report"}

        reference = reference[viable_columns].copy()
        current = current[viable_columns].copy()

        try:
            report = Report(metrics=[DataDriftPreset()])
            report.run(reference_data=reference, current_data=current)
        except Exception as error:
            return {"error": f"Unable to build report: {str(error)[:200]}"}

        output_path = Path(output_html)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        export_error = self._save_report_html(report, output_path)
        if export_error:
            return {"error": export_error}

        return {
            "status": "ok",
            "output_html": str(output_path),
            "rows": str(len(dataset)),
            "reference_rows": str(len(reference)),
            "current_rows": str(len(current)),
        }

    def build_rag_report(
        self,
        reference_rows: int = 100,
        output_html: str = "monitoring/evidently_rag_report.html",
    ) -> Dict[str, str]:
        """Generate an Evidently HTML report focused on RAG-scored interactions."""
        if not EVIDENTLY_AVAILABLE or Report is None or DataDriftPreset is None:
            return {"error": "Evidently not available in current environment"}

        dataset = self._prepare_rag_dataframe()
        rag_dataset = dataset[
            dataset[["metric_faithfulness", "metric_response_relevancy", "metric_context_precision", "retrieval_quality"]]
            .notna()
            .any(axis=1)
        ].copy()
        if rag_dataset.empty or len(rag_dataset) < 20:
            return {"error": "Not enough RAG monitoring data to build report (need >= 20 rows)"}

        report_columns = [
            "backend",
            "mission",
            "context_count",
            "latency_ms",
            "metric_faithfulness",
            "metric_response_relevancy",
            "metric_context_precision",
            "retrieval_quality",
        ]
        report_dataset = rag_dataset[[column for column in report_columns if column in rag_dataset.columns]].copy()

        split_index = min(reference_rows, max(1, len(report_dataset) // 2))
        reference = report_dataset.iloc[:split_index].copy()
        current = report_dataset.iloc[split_index:].copy()
        if current.empty:
            return {"error": "Current RAG dataset is empty after reference split"}

        viable_columns = self._select_viable_drift_columns(reference, current)
        if not viable_columns:
            return {"error": "Not enough populated RAG monitoring columns to build report"}

        reference = reference[viable_columns].copy()
        current = current[viable_columns].copy()

        try:
            report = Report(metrics=[DataDriftPreset()])
            report.run(reference_data=reference, current_data=current)
        except Exception as error:
            return {"error": f"Unable to build RAG report: {str(error)[:200]}"}

        output_path = Path(output_html)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        export_error = self._save_report_html(report, output_path)
        if export_error:
            return {"error": export_error}

        return {
            "status": "ok",
            "output_html": str(output_path),
            "rows": str(len(report_dataset)),
            "reference_rows": str(len(reference)),
            "current_rows": str(len(current)),
        }