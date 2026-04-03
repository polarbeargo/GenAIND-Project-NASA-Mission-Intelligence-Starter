"""Lightweight Evidently integration for response quality and RAG monitoring."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
import math
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

try:
    import polars as pl
    POLARS_AVAILABLE = True
except Exception:
    pl = None
    POLARS_AVAILABLE = False

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


class EvidentlyMonitor:
    """Persist chat interactions and generate drift reports when requested."""

    def __init__(self, log_path: str = "monitoring/interactions.jsonl"):
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._analytics_cache: Dict[str, Any] = {
            "signature": None,
            "result": None,
        }
        self._rag_cache: Dict[str, Any] = {
            "signature": None,
            "result": None,
        }

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
        record = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
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

        with self.log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=True) + "\n")

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
            dataset["mission"] = dataset.get("backend", "unknown").apply(self._derive_mission)
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
        if not self.log_path.exists():
            return pd.DataFrame()

        if POLARS_AVAILABLE and pl is not None:
            try:
                dataset = pl.read_ndjson(str(self.log_path), ignore_errors=True)
                if dataset.is_empty():
                    return pd.DataFrame()
                return dataset.to_pandas()
            except Exception:
                # Fallback to robust line-by-line parser when NDJSON read fails.
                pass

        rows: List[Dict] = []
        with self.log_path.open("r", encoding="utf-8") as handle:
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

    def _log_signature(self) -> Optional[Tuple[int, int]]:
        if not self.log_path.exists():
            return None

        stat = self.log_path.stat()
        return (stat.st_mtime_ns, stat.st_size)

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

        if POLARS_AVAILABLE and pl is not None:
            try:
                scan = pl.scan_ndjson(str(self.log_path), ignore_errors=True)
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
                    ]).collect().to_dicts()[0]

                    total_requests = int(overall_row.get("total_requests") or 0)
                    if total_requests == 0:
                        result = {"error": "Monitoring data is empty"}
                    else:
                        total_errors = int(overall_row.get("total_errors") or 0)
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
