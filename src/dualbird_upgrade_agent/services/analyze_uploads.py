"""
Turn uploaded file name + bytes into an acceleration report (or error body).
"""

from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass
from pathlib import Path

from dualbird_upgrade_agent.analyzer.estimator import estimate_application
from dualbird_upgrade_agent.parser.dag_parser import PipelineDefinition, parse_dag
from dualbird_upgrade_agent.parser.event_log import parse_event_log
from dualbird_upgrade_agent.parser.pyspark_parser import (
    analysis_to_operators,
    analyze_pyspark_file,
)
from dualbird_upgrade_agent.report.generator import generate_report


@dataclass
class AnalysisResult:
    """HTTP JSON body + status for /analyze."""

    body: dict
    status_code: int = 200


def run_analysis(
    file_items: list[tuple[str | None, bytes]],
    pipeline_name: str | None = None,
) -> AnalysisResult:
    """
    Process uploaded files: Spark logs, DAGs/workflows, PySpark, or a pre-built report JSON.

    ``file_items`` is (original_filename, raw_bytes) in upload order.
    """
    for fname, content in file_items:
        if fname and fname.endswith(".json"):
            try:
                data = json.loads(content)
                if isinstance(data, dict) and "pipeline" in data and "coverage" in data:
                    return AnalysisResult(body=data, status_code=200)
            except json.JSONDecodeError:
                pass

    estimates = []
    pipeline_def: PipelineDefinition | None = None
    pyspark_predictions: list[dict] = []

    with tempfile.TemporaryDirectory() as tmpdir:
        base = Path(tmpdir)
        for i, (fname, content) in enumerate(file_items):
            safe_name = fname or "unknown"
            tmp_path = base / f"{i}_{safe_name}"
            tmp_path.write_bytes(content)

            dag = parse_dag(tmp_path)
            if dag and dag.tasks:
                pipeline_def = dag
                continue

            app_data = parse_event_log(tmp_path)
            if app_data.app_id or app_data.stages:
                estimates.append(estimate_application(app_data))
                continue

            if tmp_path.suffix == ".py":
                pyspark = analyze_pyspark_file(tmp_path)
                if pyspark.predicted_ops:
                    pyspark_predictions.append({
                        "file": safe_name,
                        "operators": analysis_to_operators(pyspark),
                        "has_udfs": pyspark.has_udfs,
                        "confidence": pyspark.confidence,
                    })

    if not estimates and not pipeline_def and not pyspark_predictions:
        return AnalysisResult(
            body={
                "error": "No recognizable files found.",
                "hint": (
                    "Upload Spark event logs (.jsonl/.json), Airflow DAGs (.py), "
                    "Databricks workflows (.json), or PySpark scripts (.py)."
                ),
            },
            status_code=400,
        )

    orchestrator = pipeline_def.orchestrator if pipeline_def else None
    name = pipeline_name or (
        pipeline_def.name if pipeline_def
        else estimates[0].app_name if estimates
        else None
    )

    report = generate_report(
        estimates,
        pipeline_name=name,
        orchestrator=orchestrator,
        pipeline_def=pipeline_def,
    )

    if pyspark_predictions:
        report["pyspark_predictions"] = pyspark_predictions

    return AnalysisResult(body=report, status_code=200)
