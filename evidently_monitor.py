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
        self._setup_lock = threading.Lock()
        self._setup_complete = False

    def _connect(self):
        return psycopg.connect(self.dsn, autocommit=True)

    def _ensure_table(self) -> None:
        if self._setup_complete:
            return

        with self._setup_lock:
            if self._setup_complete:
                return

            table_identifier = sql.Identifier(self.table_name)
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
            self._setup_complete = True

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

        self._analytics_cache: Dict[str, Any] = {
            "signature": None,
            "result": None,
        }
        self._rag_cache: Dict[str, Any] = {
            "signature": None,
            "result": None,
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
        self._analytics_cache["signature"] = None
        self._analytics_cache["result"] = None
        self._rag_cache["signature"] = None
        self._rag_cache["result"] = None

    def _persist_batch(self, records: Sequence[Dict[str, Any]]) -> None:
        if not records:
            return

        try:
            with self._write_lock:
                self._primary_sink.persist_batch(records)
            self._invalidate_caches()
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
        }
        if latency_ms is not None:
            record["latency_ms"] = float(latency_ms)
        if evaluation:
            for key, value in evaluation.items():
                if isinstance(value, (int, float)):
                    record[f"metric_{key}"] = float(value)

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
        dataset = self.load_dataframe()
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
        """Return cached analytics rollups with a Polars-first implementation."""
        signature = self._log_signature()
        if signature is None:
            return {"error": "No monitoring data found"}

        if self._analytics_cache["signature"] == signature:
            cached_result = self._analytics_cache["result"]
            if cached_result is not None:
                return cached_result

        result: Dict[str, Any]
        native_path = self._primary_sink.native_ndjson_path()

        if native_path is not None and POLARS_AVAILABLE and pl is not None:
            try:
                scan = pl.scan_ndjson(str(native_path), ignore_errors=True)
                schema_names = set(scan.collect_schema().names())

                if not schema_names:
                    result = {"error": "Monitoring data is empty"}
                else:
                    columns = []
                    if "latency_ms" in schema_names:
                        columns.append(pl.col("latency_ms").cast(pl.Float64, strict=False).alias("latency_ms"))
                    else:
                        columns.append(pl.lit(None, dtype=pl.Float64).alias("latency_ms"))

                    if "is_error" in schema_names:
                        columns.append(pl.col("is_error").cast(pl.Float64, strict=False).fill_null(0.0).alias("is_error"))
                    else:
                        columns.append(pl.lit(0.0).alias("is_error"))

                    if "backend" in schema_names:
                        columns.append(pl.col("backend").cast(pl.Utf8, strict=False).fill_null("unknown").alias("backend"))
                    else:
                        columns.append(pl.lit("unknown").alias("backend"))

                    if "model" in schema_names:
                        columns.append(pl.col("model").cast(pl.Utf8, strict=False).fill_null("unknown").alias("model"))
                    else:
                        columns.append(pl.lit("unknown").alias("model"))

                    dataset = scan.select(columns)

                    overall_row = dataset.select([
                        pl.len().alias("total_requests"),
                        pl.col("is_error").sum().alias("total_errors"),
                        pl.col("latency_ms").mean().alias("avg_latency_ms"),
                        pl.col("latency_ms").quantile(0.95).alias("p95_latency_ms"),
                    ]).collect().to_dicts()[0]

                    total_requests = int(overall_row.get("total_requests") or 0)
                    if total_requests == 0:
                        result = {"error": "Monitoring data is empty"}
                    else:
                        total_errors = int(overall_row.get("total_errors") or 0)
                        avg_latency_ms = overall_row.get("avg_latency_ms")
                        p95_latency_ms = overall_row.get("p95_latency_ms")
                        backend_rollups = (
                            dataset.group_by("backend")
                            .agg([
                                pl.len().alias("requests"),
                                (pl.col("is_error").mean() * 100).alias("error_rate_percent"),
                                pl.col("latency_ms").mean().alias("avg_latency_ms"),
                                pl.col("latency_ms").quantile(0.95).alias("p95_latency_ms"),
                            ])
                            .sort("requests", descending=True)
                            .collect()
                            .to_dicts()
                        )

                        model_rollups = (
                            dataset.group_by("model")
                            .agg([
                                pl.len().alias("requests"),
                                (pl.col("is_error").mean() * 100).alias("error_rate_percent"),
                                pl.col("latency_ms").mean().alias("avg_latency_ms"),
                            ])
                            .sort("requests", descending=True)
                            .collect()
                            .to_dicts()
                        )

                        result = {
                            "status": "ok",
                            "engine": "polars",
                            "overall": {
                                "total_requests": total_requests,
                                "total_errors": total_errors,
                                "error_rate_percent": round((total_errors / total_requests) * 100, 2),
                                "avg_latency_ms": self._round_float(avg_latency_ms),
                                "p95_latency_ms": self._round_float(p95_latency_ms),
                            },
                            "backend_rollups": self._round_records(backend_rollups),
                            "model_rollups": self._round_records(model_rollups),
                        }
            except Exception:
                result = {"error": "polars_fallback"}
        else:
            result = {"error": "polars_fallback"}

        if result.get("error") == "polars_fallback":
            dataset = self.load_dataframe()
            if dataset.empty:
                result = {"error": "Monitoring data is empty"}
            else:
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
                    "engine": "pandas",
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

        self._analytics_cache["signature"] = signature
        self._analytics_cache["result"] = result
        return result

    def get_rag_dashboard_summary(self, recent_failures_limit: int = 20) -> Dict[str, Any]:
        """Return RAG-specific rollups built from logged RAGAS scores."""
        signature = self._log_signature()
        if signature is None:
            return {"error": "No monitoring data found"}

        cached_signature = self._rag_cache["signature"]
        cached_result = self._rag_cache["result"]
        if cached_signature == (signature, recent_failures_limit) and cached_result is not None:
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

        self._rag_cache["signature"] = (signature, recent_failures_limit)
        self._rag_cache["result"] = result
        return result

    def get_prometheus_curated_snapshot(self) -> Dict[str, Any]:
        """Return a small curated metrics set for Prometheus/Grafana dashboards."""
        analytics = self.get_analytics_summary()
        rag = self.get_rag_dashboard_summary(recent_failures_limit=20)

        analytics_overall = analytics.get("overall", {}) if isinstance(analytics, dict) else {}
        rag_overall = rag.get("overall", {}) if isinstance(rag, dict) else {}
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